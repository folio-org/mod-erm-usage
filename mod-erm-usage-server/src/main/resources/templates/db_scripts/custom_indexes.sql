CREATE INDEX IF NOT EXISTS counter_reports_custom_getcsv_idx ON counter_reports
  USING btree ((jsonb ->> 'providerId'), (jsonb ->> 'reportName'), (jsonb ->> 'release'),
    (jsonb ->> 'yearMonth'));
