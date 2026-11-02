-- Remove the unused notes field from all UDPs
UPDATE usage_data_providers
SET jsonb = jsonb - 'notes'
WHERE jsonb ? 'notes';
