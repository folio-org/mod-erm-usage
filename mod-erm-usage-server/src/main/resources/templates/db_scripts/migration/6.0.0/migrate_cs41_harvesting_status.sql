-- Set harvesting status to 'inactive' for UDPs with service type 'cs41', which is no longer supported by the harvester
DO $$
BEGIN
  UPDATE usage_data_providers
  SET jsonb = jsonb_set(jsonb, '{harvestingConfig,harvestingStatus}', '"inactive"')
  WHERE jsonb #>> '{harvestingConfig,sushiConfig,serviceType}' = 'cs41'
    AND jsonb #>> '{harvestingConfig,harvestingStatus}' IS DISTINCT FROM 'inactive';
END $$;
