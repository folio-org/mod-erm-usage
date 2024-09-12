-- Replace number values stored in harvestingConfig.reportRelease with a string value
DO $$
BEGIN
    UPDATE ${myuniversity}_${mymodule}.usage_data_providers
    SET jsonb = jsonb_set(
        jsonb,
        '{harvestingConfig,reportRelease}',
        to_jsonb((jsonb->'harvestingConfig'->>'reportRelease'))
    )
    WHERE jsonb_typeof(jsonb->'harvestingConfig'->'reportRelease') = 'number';
END $$;
