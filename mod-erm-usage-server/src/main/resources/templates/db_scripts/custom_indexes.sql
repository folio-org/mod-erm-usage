CREATE INDEX IF NOT EXISTS counter_reports_custom_getcsv_idx ON counter_reports
  USING btree ((jsonb ->> 'providerId'), (jsonb ->> 'reportName'), (jsonb ->> 'release'),
    (jsonb ->> 'yearMonth'));
CREATE INDEX IF NOT EXISTS counter_reports_custom_errorcodes_idx ON counter_reports
  USING btree(SUBSTRING(jsonb->>'failedReason','Number=([0-9]{1,4})'))
  WHERE jsonb ->> 'failedReason' IS NOT NULL;
CREATE INDEX IF NOT EXISTS usage_data_providers_custom_aggregatorid_idx ON usage_data_providers
  USING btree ((jsonb->'harvestingConfig'->'aggregator'->>'id'));
CREATE INDEX IF NOT EXISTS counter_reports_custom_reporttypes_idx ON counter_reports
  USING btree ((jsonb->>'reportName'))
  WHERE jsonb ->> 'reportName' IS NOT NULL;
