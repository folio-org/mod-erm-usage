-- 1. Deactivate aggregator UDPs (always satisfies the status/harvestingStatus constraint)
UPDATE usage_data_providers
SET jsonb = jsonb_set(jsonb, '{harvestingConfig,harvestingStatus}', '"inactive"')
WHERE jsonb #>> '{harvestingConfig,harvestVia}' = 'aggregator';

-- 2. Remove harvestVia and aggregator from all UDPs
UPDATE usage_data_providers
SET jsonb = jsonb #- '{harvestingConfig,harvestVia}' #- '{harvestingConfig,aggregator}'
WHERE jsonb->'harvestingConfig' ?| array['harvestVia', 'aggregator'];

-- 3. Drop triggers, functions, index and table (IF EXISTS)
DROP TRIGGER IF EXISTS resolve_aggregator_label_before_insert ON usage_data_providers;
DROP TRIGGER IF EXISTS resolve_aggregator_label_before_update ON usage_data_providers;
DROP FUNCTION IF EXISTS resolve_aggregator_label();
DROP TABLE IF EXISTS aggregator_settings CASCADE;
DROP FUNCTION IF EXISTS update_aggregator_label_references();
DROP INDEX IF EXISTS usage_data_providers_custom_aggregatorid_idx;
