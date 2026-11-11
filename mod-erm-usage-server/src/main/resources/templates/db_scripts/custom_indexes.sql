DROP INDEX IF EXISTS counter_reports_custom_getcsv_idx;
CREATE INDEX IF NOT EXISTS counter_reports_custom_getcsv_idx ON counter_reports
  USING btree ((jsonb ->> 'providerId'), (jsonb ->> 'reportName'), (jsonb ->> 'release'),
    (jsonb ->> 'yearMonth'));

DROP INDEX IF EXISTS counter_reports_custom_errorcodes_idx;
CREATE INDEX IF NOT EXISTS counter_reports_custom_errorcodes_idx ON counter_reports
  (jsonb_path_query_array(jsonb, '$.apiExceptions[*].code'))
  WHERE jsonb->>'failedReason' IS NOT NULL;

DROP INDEX IF EXISTS counter_reports_custom_failed_idx;
CREATE INDEX IF NOT EXISTS counter_reports_custom_failed_idx ON counter_reports
  USING btree ((jsonb->>'providerId'))
  WHERE jsonb->>'failedReason' IS NOT NULL;

DROP INDEX IF EXISTS counter_reports_custom_reporttypes_idx;
CREATE INDEX IF NOT EXISTS counter_reports_custom_reporttypes_idx ON counter_reports
  USING btree ((jsonb->>'reportName'));

DROP INDEX IF EXISTS counter_reports_custom_reportreleases_idx;
CREATE INDEX IF NOT EXISTS counter_reports_custom_reportreleases_idx ON counter_reports
  USING btree ((jsonb->>'release'));
