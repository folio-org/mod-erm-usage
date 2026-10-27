-- Back-fills "apiExceptions" of failed counter reports from their "failedReason".
--
-- "failedReason" comes in these forms:
--   * COUNTER 4:  Report not valid: Exception{Number=3030, Severity=ERROR, Message=…}, Exception{…}
--   * COUNTER 5:  {"Code":3030,"Message":…} or [{…},{…}], possibly after "Report not valid: " or inside
--                 "HTTP <status>: <message> - <body>", where long bodies are cut off and are no valid JSON anymore
--   * anything else (HTML error pages, timeouts, parser messages), which stays without "apiExceptions"
--
-- Codes are copied as sent, without mapping between COUNTER releases. "failedReason" itself is never changed.
--
-- Structure of this script:
--   1. helper functions (api_exceptions_*), the entry point being "api_exceptions_from_failed_reason"
--   2. the migration itself (DO block): back-fill, then recompute the provider statistics once per provider
--   3. drop the helper functions again, they are only needed during the upgrade
--
-- Running the script again changes nothing: reports that already have "apiExceptions" are skipped.


-- 1. Helper functions -------------------------------------------------------------------------------------------------

-- Returns the text as jsonb, or NULL if it isn't valid JSON (e.g. a cut-off response body) instead of failing.
CREATE OR REPLACE FUNCTION api_exceptions_try_jsonb(t TEXT) RETURNS jsonb AS $$
BEGIN
  RETURN t::jsonb;
EXCEPTION WHEN others THEN
  RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Returns the value of a COUNTER exception property (Message, Data) as text: a string as is, any other JSON value
-- (e.g. a Data object) as JSON text, and NULL if the property is missing or JSON null.
CREATE OR REPLACE FUNCTION api_exceptions_text(value jsonb) RETURNS TEXT AS $$
  SELECT CASE
    WHEN value IS NULL OR jsonb_typeof(value) = 'null' THEN NULL
    WHEN jsonb_typeof(value) = 'string' THEN value #>> '{}'
    ELSE value::text
  END
$$ LANGUAGE sql IMMUTABLE;

-- Builds one API exception as stored in "apiExceptions" (see ramls/schemas/exception.json):
--   {"code": 3030, "message": "…", "helpUrl": "…", "data": "…"}
-- Returns NULL if there is no code, since code is required. Blank message and data are left out, and so is a helpUrl
-- that isn't a valid http(s) URI, because helpUrl is a java.net.URI in the generated model class and an invalid value
-- would make reading the report fail.
CREATE OR REPLACE FUNCTION api_exceptions_build(code BIGINT, message TEXT, help_url TEXT, data TEXT)
RETURNS jsonb AS $$
  SELECT CASE WHEN code IS NOT NULL THEN jsonb_strip_nulls(jsonb_build_object(
    'code', code,
    'message', CASE WHEN message !~ '^\s*$' THEN message END,
    'helpUrl', CASE WHEN help_url ~ '^https?://([A-Za-z0-9._~:/?#@!$&''()*+,;=-]|%[0-9A-Fa-f]{2})+$' THEN help_url END,
    'data', CASE WHEN data !~ '^\s*$' THEN data END))
  END
$$ LANGUAGE sql IMMUTABLE;

-- Maps one COUNTER 5 exception object, e.g. {"Code":3030,"Severity":"Error","Message":"…","Help_URL":"…","Data":"…"},
-- to an API exception. Like the harvester, it accepts a Code that is a whole number in int range or a string of up
-- to 9 digits, and returns NULL for anything else (no object, no or an invalid Code). Severity is not copied.
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

-- Entry point: returns all API exceptions found in a "failedReason" as a non-empty JSON array, in the order they appear,
-- or NULL if there is none. Never returns an empty array.
--
--   Step A  COUNTER 4: split the text into its Exception{…} parts and read the fields by their labels.
--   Step B  COUNTER 5: parse the JSON that starts at the first { or [. If it is cut off and can't be parsed, take
--           every complete {…} object that contains a "Code" on its own instead.
--   Step C  Safety net: every code the former error code pattern (?:Number=|"Code": ?)([0-9]{1,4}) finds in the text
--           is added as {"code": …} if steps A and B didn't migrate it already, e.g. the code of the cut-off object.
--           So no error code known before the upgrade gets lost.
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
    -- Step A, COUNTER 4: "Report not valid: Exception{Number=…, Severity=…, Message=…, HelpUrl=…, Data=…}, Exception{…}"
    -- (format of Counter4Utils.getErrorMessages in mod-erm-usage-counter, fields without a value are left out).
    -- Cut off everything up to the first "Exception{" and the final "}", then split at "}, Exception{", so each chunk
    -- is "Number=…, Severity=…, Message=…, HelpUrl=…, Data=…". A field's value runs up to the label of the next
    -- field, which is why Message and HelpUrl are cut at the labels that may follow them.
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
    -- Step B, COUNTER 5: a JSON object or array, possibly after "Report not valid: " or "HTTP <status>: <message> - ".
    -- The JSON starts at the first { or [ in the text.
    json_start := LEAST(NULLIF(strpos(fr, '{'), 0), NULLIF(strpos(fr, '['), 0));
    IF json_start IS NOT NULL THEN
      json_text := substr(fr, json_start);
      parsed := api_exceptions_try_jsonb(json_text);
      IF parsed IS NOT NULL THEN
        -- valid JSON: map each exception (a single object is treated like an array of one), keep their order and
        -- skip those that can't be mapped
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
        -- no valid JSON, e.g. a body cut off after 2000 characters: parse every complete, flat {…} object that
        -- contains a "Code" on its own; the cut-off last object is left to step C
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

  -- Step C, safety net: add every code the former error code pattern finds that isn't migrated yet, with its code only
  FOR code_text IN
    SELECT m[1] FROM regexp_matches(fr, '(?:Number=|"Code": ?)([0-9]{1,4})', 'g') AS t(m)
  LOOP
    IF NOT result @> jsonb_build_array(jsonb_build_object('code', code_text::int)) THEN
      result := result || jsonb_build_object('code', code_text::int);
    END IF;
  END LOOP;

  -- apiExceptions must not be an empty array, the property is left out instead
  RETURN CASE WHEN jsonb_array_length(result) > 0 THEN result END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- 2. Migration --------------------------------------------------------------------------------------------------------

-- This block does the actual work, using the helper functions above:
--   * Every failed report ("failedReason" is set) that has no "apiExceptions" yet gets the "apiExceptions" extracted from
--     its "failedReason". Reports where nothing can be extracted (NULL) are left untouched.
--   * The trigger that recomputes the statistics of a report's provider on every change of a report is switched off
--     meanwhile; otherwise the statistics of a provider would be recomputed once per migrated report.
--   * Afterwards the statistics of every provider, including reportErrorCodes, are recomputed exactly once, now
--     taking the error codes from "apiExceptions" (udp_report_errors in counterreports_triggers.sql).
-- All of it runs as one statement, so the trigger can't stay switched off if something fails.
DO $$
BEGIN
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


-- 3. Clean up ---------------------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS api_exceptions_from_failed_reason(TEXT);
DROP FUNCTION IF EXISTS api_exceptions_from_counter_exception(jsonb);
DROP FUNCTION IF EXISTS api_exceptions_build(BIGINT, TEXT, TEXT, TEXT);
DROP FUNCTION IF EXISTS api_exceptions_text(jsonb);
DROP FUNCTION IF EXISTS api_exceptions_try_jsonb(TEXT);
