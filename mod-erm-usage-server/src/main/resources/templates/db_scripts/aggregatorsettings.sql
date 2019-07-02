-- update labels in usage_data_providers if aggregator label changes
CREATE OR REPLACE FUNCTION update_aggregator_label_references() RETURNS trigger AS $$
BEGIN
  UPDATE usage_data_providers
  SET jsonb = jsonb_set(jsonb, '{harvestingConfig,aggregator,name}', NEW.jsonb->'label', true)
  WHERE (jsonb->'harvestingConfig'->'aggregator'->'id' = NEW.jsonb->'id');
  RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_aggregator_label_references_after_update
AFTER UPDATE ON aggregator_settings FOR EACH ROW
WHEN (
  NEW.jsonb->'label' IS DISTINCT FROM OLD.jsonb->'label'
)
EXECUTE PROCEDURE update_aggregator_label_references();