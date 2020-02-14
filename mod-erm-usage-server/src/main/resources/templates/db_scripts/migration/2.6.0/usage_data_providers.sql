UPDATE  ${myuniversity}_${mymodule}.usage_data_providers
SET     jsonb = jsonb || json_build_object('hasFailedReport', isFailed.case)::jsonb
FROM (
  WITH
    s AS (
      SELECT    count(*) AS c,
                jsonb->>'providerId' as pId
      FROM      ${myuniversity}_${mymodule}.counter_reports
      WHERE 	  jsonb->>'failedAttempts' IS NOT NULL
      GROUP BY  pId
    )
    SELECT  u.id,
            CASE  WHEN s.c IS NULL THEN 'no'
                  WHEN s.c=0 THEN 'no'
                  ELSE 'yes'
            END
    FROM s
    RIGHT JOIN ${myuniversity}_${mymodule}.usage_data_providers u
    ON s.pId::text = u.id::text
    GROUP BY s.c, u.id
  ) AS isFailed
WHERE NOT jsonb ? 'hasFailedReport' AND isFailed.id::text = usage_data_providers.id::text;

