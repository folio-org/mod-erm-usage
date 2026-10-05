-- delete associated counter-reports if usage-data-provider is deleted
CREATE OR REPLACE FUNCTION delete_counter_reports() RETURNS trigger AS $$
BEGIN
  DELETE FROM counter_reports WHERE jsonb->>'providerId' = OLD.jsonb->>'id';
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS delete_counter_reports ON usage_data_providers;
CREATE TRIGGER delete_counter_reports
AFTER DELETE ON usage_data_providers FOR EACH ROW
EXECUTE PROCEDURE delete_counter_reports();

DROP TRIGGER IF EXISTS resolve_aggregator_label_before_insert ON usage_data_providers;
DROP TRIGGER IF EXISTS resolve_aggregator_label_before_update ON usage_data_providers;
