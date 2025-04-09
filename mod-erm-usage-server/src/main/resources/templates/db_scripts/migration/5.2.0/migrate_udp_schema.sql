-- Set udp attribute status to 'active' if it does not already exist
DO $$
BEGIN
  UPDATE usage_data_providers
  SET jsonb = jsonb || jsonb_build_object('status', 'active')
  WHERE NOT (jsonb ? 'status');
END $$;
