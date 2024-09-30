DROP FUNCTION IF EXISTS update_latest_statistic_on_update();

SELECT update_udp_statistics(jsonb->>'id') FROM usage_data_providers;
