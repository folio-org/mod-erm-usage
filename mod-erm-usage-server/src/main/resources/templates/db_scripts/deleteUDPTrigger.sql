-- delete associated counter-reports if usage-data-provider is deleted
CREATE OR REPLACE FUNCTION delete_counter_reports() RETURNS trigger AS $$
BEGIN
  DELETE FROM counter_reports WHERE jsonb->>'providerId' = OLD.jsonb->>'id';
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER delete_counter_reports
AFTER DELETE ON usage_data_providers FOR EACH ROW
EXECUTE PROCEDURE delete_counter_reports();