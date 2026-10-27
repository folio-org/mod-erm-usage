-- Back-fill apiExceptions of failed counter reports from their failedReason, the same way the harvester fills it for
-- new reports, then recompute the statistics of every usage data provider once.
-- The helper functions are dropped at the end.

-- returns the text as jsonb, or NULL if it isn't valid JSON
CREATE OR REPLACE FUNCTION api_exceptions_try_jsonb(t TEXT) RETURNS jsonb AS $$
BEGIN
  RETURN t::jsonb;
EXCEPTION WHEN others THEN
  RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- returns the text of a COUNTER exception property, JSON text if it isn't a string, NULL if it's missing or blank
CREATE OR REPLACE FUNCTION api_exceptions_text(value jsonb) RETURNS TEXT AS $$
  SELECT CASE
    WHEN value IS NULL OR jsonb_typeof(value) = 'null' THEN NULL
    WHEN jsonb_typeof(value) = 'string' THEN value #>> '{}'
    ELSE value::text
  END
$$ LANGUAGE sql IMMUTABLE;

-- returns an API exception, or NULL if there is no code; drops blank texts and a helpUrl that isn't a valid URI
CREATE OR REPLACE FUNCTION api_exceptions_build(code BIGINT, message TEXT, help_url TEXT, data TEXT)
RETURNS jsonb AS $$
  SELECT CASE WHEN code IS NOT NULL THEN jsonb_strip_nulls(jsonb_build_object(
    'code', code,
    'message', CASE WHEN message !~ '^\s*$' THEN message END,
    'helpUrl', CASE WHEN help_url ~ '^https?://([A-Za-z0-9._~:/?#@!$&''()*+,;=-]|%[0-9A-Fa-f]{2})+$' THEN help_url END,
    'data', CASE WHEN data !~ '^\s*$' THEN data END))
  END
$$ LANGUAGE sql IMMUTABLE;

-- maps a COUNTER 5 exception object to an API exception, or NULL if it has no code that is a whole number in int range
-- (or a string of up to 9 digits)
CREATE OR REPLACE FUNCTION api_exceptions_from_counter_exception(e jsonb) RETURNS jsonb AS $$
DECLARE
  code_value jsonb := e->'Code';
  code NUMERIC;
BEGIN
  IF jsonb_typeof(e) IS DISTINCT FROM 'object' THEN
    RETURN NULL;
  END IF;

  IF jsonb_typeof(code_value) = 'number' THEN
    code := code_value::text::numeric;
  ELSIF jsonb_typeof(code_value) = 'string' AND code_value #>> '{}' ~ '^[0-9]{1,9}$' THEN
    code := (code_value #>> '{}')::numeric;
  END IF;

  IF code IS NULL OR code <> trunc(code) OR code NOT BETWEEN -2147483648 AND 2147483647 THEN
    RETURN NULL;
  END IF;

  RETURN api_exceptions_build(
    code::bigint,
    api_exceptions_text(e->'Message'),
    CASE WHEN jsonb_typeof(e->'Help_URL') = 'string' THEN e->>'Help_URL' END,
    api_exceptions_text(e->'Data'));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- returns the API exceptions found in a failedReason as a non-empty array, or NULL if there is none
CREATE OR REPLACE FUNCTION api_exceptions_from_failed_reason(fr TEXT) RETURNS jsonb AS $$
DECLARE
  result jsonb := '[]'::jsonb;
  ex jsonb;
  chunk TEXT;
  json_start INT;
  json_text TEXT;
  parsed jsonb;
  code_text TEXT;
BEGIN
  IF fr IS NULL THEN
    RETURN NULL;
  END IF;

  IF strpos(fr, 'Exception{Number=') > 0 THEN
    -- COUNTER 4: "Report not valid: Exception{Number=…, Severity=…, Message=…, HelpUrl=…, Data=…}, Exception{…}"
    FOR chunk IN
      SELECT regexp_split_to_table(
        regexp_replace(substr(fr, strpos(fr, 'Exception{') + length('Exception{')), '\}\s*$', ''),
        '\}, Exception\{')
    LOOP
      ex := api_exceptions_build(
        substring(chunk FROM '^Number=([0-9]{1,9})(,|$)')::bigint,
        split_part(split_part(substring(chunk FROM ', Message=(.*)$'), ', HelpUrl=', 1), ', Data=', 1),
        split_part(substring(chunk FROM ', HelpUrl=(.*)$'), ', Data=', 1),
        substring(chunk FROM ', Data=(.*)$'));
      IF ex IS NOT NULL THEN
        result := result || ex;
      END IF;
    END LOOP;
  ELSE
    -- COUNTER 5: a JSON object or array, possibly after "Report not valid: " or "HTTP <status>: <message> - "
    json_start := LEAST(NULLIF(strpos(fr, '{'), 0), NULLIF(strpos(fr, '['), 0));
    IF json_start IS NOT NULL THEN
      json_text := substr(fr, json_start);
      parsed := api_exceptions_try_jsonb(json_text);
      IF parsed IS NOT NULL THEN
        result := result || COALESCE((
          SELECT jsonb_agg(x ORDER BY n)
          FROM (
            SELECT n, api_exceptions_from_counter_exception(c) AS x
            FROM jsonb_array_elements(
              CASE jsonb_typeof(parsed)
                WHEN 'array' THEN parsed
                WHEN 'object' THEN jsonb_build_array(parsed)
                ELSE '[]'::jsonb
              END) WITH ORDINALITY AS t(c, n)
          ) s
          WHERE x IS NOT NULL), '[]'::jsonb);
      ELSE
        -- cut-off body: take every complete exception object on its own
        result := result || COALESCE((
          SELECT jsonb_agg(x ORDER BY n)
          FROM (
            SELECT n, api_exceptions_from_counter_exception(api_exceptions_try_jsonb(m[1])) AS x
            FROM regexp_matches(json_text, '(\{[^{}]*"Code"[^{}]*\})', 'g') WITH ORDINALITY AS t(m, n)
          ) s
          WHERE x IS NOT NULL), '[]'::jsonb);
      END IF;
    END IF;
  END IF;

  -- every code the former error code pattern finds is migrated, at least with its code
  FOR code_text IN
    SELECT m[1] FROM regexp_matches(fr, '(?:Number=|"Code": ?)([0-9]{1,4})', 'g') AS t(m)
  LOOP
    IF NOT result @> jsonb_build_array(jsonb_build_object('code', code_text::int)) THEN
      result := result || jsonb_build_object('code', code_text::int);
    END IF;
  END LOOP;

  RETURN CASE WHEN jsonb_array_length(result) > 0 THEN result END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

DO $$
BEGIN
  -- avoid recomputing the statistics of a provider for every migrated report
  ALTER TABLE counter_reports DISABLE TRIGGER update_usage_data_providers_on_insert_or_update_or_delete;

  UPDATE counter_reports cr
  SET jsonb = cr.jsonb || jsonb_build_object('apiExceptions', x.api_exceptions)
  FROM (
    SELECT id, api_exceptions_from_failed_reason(jsonb->>'failedReason') AS api_exceptions
    FROM counter_reports
    WHERE jsonb->>'failedReason' IS NOT NULL
      AND jsonb_typeof(jsonb->'apiExceptions') IS DISTINCT FROM 'array'
  ) x
  WHERE cr.id = x.id AND x.api_exceptions IS NOT NULL;

  ALTER TABLE counter_reports ENABLE TRIGGER update_usage_data_providers_on_insert_or_update_or_delete;

  PERFORM update_udp_statistics(jsonb->>'id') FROM usage_data_providers;
END $$;

DROP FUNCTION IF EXISTS api_exceptions_from_failed_reason(TEXT);
DROP FUNCTION IF EXISTS api_exceptions_from_counter_exception(jsonb);
DROP FUNCTION IF EXISTS api_exceptions_build(BIGINT, TEXT, TEXT, TEXT);
DROP FUNCTION IF EXISTS api_exceptions_text(jsonb);
DROP FUNCTION IF EXISTS api_exceptions_try_jsonb(TEXT);
