UPDATE  ${myuniversity}_${mymodule}.usage_data_providers
SET     jsonb = jsonb || json_build_object('reportErrorCodes', error_codes.udp_report_errors)::jsonb
FROM (
  SELECT  jsonb->>'providerId' as pId,
          ${myuniversity}_${mymodule}.udp_report_errors(jsonb->>'providerId')
  FROM    ${myuniversity}_${mymodule}.counter_reports
  WHERE   jsonb->>'failedReason' IS NOT NULL
  GROUP BY  pId
  ) AS error_codes
WHERE NOT jsonb ? 'reportErrorCodes' AND error_codes.pId::text = usage_data_providers.id::text;
