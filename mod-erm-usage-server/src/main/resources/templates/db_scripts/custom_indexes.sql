CREATE INDEX IF NOT EXISTS counter_reports_custom_getcsv_idx ON counter_reports
  USING btree ((jsonb ->> 'providerId'), (jsonb ->> 'reportName'), (jsonb ->> 'release'),
    (jsonb ->> 'yearMonth'));
CREATE INDEX IF NOT EXISTS usage_data_providers_custom_aggregatorid_idx ON usage_data_providers
  USING btree ((jsonb->'harvestingConfig'->'aggregator'->>'id'));