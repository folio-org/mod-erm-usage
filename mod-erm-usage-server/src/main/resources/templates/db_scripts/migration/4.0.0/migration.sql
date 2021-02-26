-- update report types
UPDATE  ${myuniversity}_${mymodule}.usage_data_providers
SET     jsonb = jsonb || json_build_object('reportTypes', report_types.udp_report_types)::jsonb
FROM (
  SELECT  jsonb->>'providerId' as pId,
          ${myuniversity}_${mymodule}.udp_report_types(jsonb->>'providerId')
  FROM    ${myuniversity}_${mymodule}.counter_reports
  GROUP BY  pId
  ) AS report_types
WHERE NOT jsonb ? 'reportTypes' AND report_types.pId::text = usage_data_providers.id::text;