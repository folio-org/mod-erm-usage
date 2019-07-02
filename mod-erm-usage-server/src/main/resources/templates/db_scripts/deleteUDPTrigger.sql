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

-- resolve label from aggregator if set/updated
CREATE OR REPLACE FUNCTION resolve_aggregator_label() RETURNS trigger AS $$
DECLARE
  _providerId       jsonb  := jsonb_extract_path(NEW.jsonb, 'harvestingConfig', 'aggregator', 'id');
  _aggregatorLabel  jsonb  := (SELECT jsonb->'label' FROM aggregator_settings WHERE (jsonb->'id' = _providerId));
BEGIN
  IF _aggregatorLabel IS NOT NULL THEN
    NEW.jsonb := jsonb_set(NEW.jsonb, '{harvestingConfig,aggregator,name}', _aggregatorLabel, true);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER resolve_aggregator_label_before_insert
BEFORE INSERT ON usage_data_providers FOR EACH ROW
WHEN (
  NEW.jsonb->'harvestingConfig'->'aggregator'->>'id' IS NOT NULL
)
EXECUTE PROCEDURE resolve_aggregator_label();

CREATE TRIGGER resolve_aggregator_label_before_update
BEFORE UPDATE ON diku_mod_erm_usage.usage_data_providers FOR EACH ROW
WHEN (
  NEW.jsonb->'harvestingConfig'->'aggregator'->>'id' IS NOT NULL AND
  NEW.jsonb->'harvestingConfig'->'aggregator'->>'id' IS DISTINCT FROM
  OLD.jsonb->'harvestingConfig'->'aggregator'->>'id'
)
EXECUTE PROCEDURE resolve_aggregator_label();