ALTER TABLE usage_data_providers
DROP CONSTRAINT IF EXISTS usage_data_providers_harvestingstatus_constraint;

ALTER TABLE usage_data_providers
ADD CONSTRAINT usage_data_providers_harvestingstatus_constraint
CHECK (
  NOT (
    (jsonb ->> 'status' = 'inactive') AND
    (jsonb #>> '{harvestingConfig,harvestingStatus}' = 'active')
  )
);
