-- return year-month of latest report available for a usage data provider
CREATE OR REPLACE FUNCTION latest_year_month(providerId TEXT) RETURNS TEXT AS $$
	SELECT MAX(jsonb->>'yearMonth') FROM counter_reports WHERE jsonb->>'providerId'=$1 AND jsonb->'failedAttempts' IS NULL;
$$ LANGUAGE sql;

-- return year-month of earliest report available for a usage data provider
CREATE OR REPLACE FUNCTION earliest_year_month(providerId TEXT) RETURNS TEXT AS $$
	SELECT MIN(jsonb->>'yearMonth') FROM counter_reports WHERE jsonb->>'providerId'=$1 AND jsonb->'failedAttempts' IS NULL;
$$ LANGUAGE sql;

-- returns the counter/sushi error codes of the usage data provider's counter reports
CREATE OR REPLACE FUNCTION udp_report_errors(providerId TEXT) RETURNS jsonb AS $$
  SELECT json_agg(error)::jsonb
  FROM (
    SELECT
      CASE
        WHEN error_code IS NOT NULL THEN error_code
        ELSE 'other'
      END
    AS error
    FROM (
      SELECT
        DISTINCT(SUBSTRING(jsonb->>'failedReason','(?:Number=|"Code": ?)([0-9]{1,4})')) as error_code,
        COUNT(jsonb->>'failedReason') as number_failed
      FROM  	counter_reports
      WHERE		jsonb->>'providerId'=$1 AND jsonb->>'failedReason' IS NOT NULL
      GROUP BY 	jsonb->>'providerId', error_code, jsonb->>'failedReason')
      AS sub
  ) AS sub2;
$$ LANGUAGE sql;

-- returns the counter reports types of the usage data provider's counter reports
CREATE OR REPLACE FUNCTION udp_report_types(providerId TEXT) RETURNS jsonb AS $$
  SELECT json_agg(name)::jsonb
  FROM (
    SELECT
      CASE
        WHEN name IS NOT NULL THEN name
        ELSE 'other'
      END
    AS name
    FROM (
      SELECT
        DISTINCT(jsonb->>'reportName') as name
      FROM  	counter_reports
      WHERE   jsonb->>'providerId'=$1
      GROUP BY 	jsonb->>'providerId', name)
      AS sub
  ) AS sub2;
$$ LANGUAGE sql;

-- trigger function to update latest report available of an usage data provider
CREATE OR REPLACE FUNCTION update_latest_statistic_on_update() RETURNS TRIGGER AS
$BODY$
DECLARE providerId TEXT;
DECLARE latest TEXT;
DECLARE earliest TEXT;
DECLARE error_codes jsonb;
DECLARE has_failed_report jsonb;
DECLARE report_types jsonb;
BEGIN
  IF (TG_OP = 'DELETE') THEN
    providerId := OLD.jsonb->>'providerId';
  ELSE
    providerId := NEW.jsonb->>'providerId';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext(providerId));
	SELECT latest_year_month(providerId) INTO latest;
	SELECT earliest_year_month(providerId) INTO earliest;
	SELECT udp_report_errors(providerId) INTO error_codes;
	SELECT udp_report_types(providerId) INTO report_types;
  IF error_codes IS NOT NULL THEN
    SELECT to_jsonb('yes'::TEXT) INTO has_failed_report;
  ELSE
    SELECT jsonb_build_array() INTO error_codes;
    SELECT to_jsonb('no'::TEXT) INTO has_failed_report;
  END IF;

  IF report_types IS NULL THEN
      SELECT jsonb_build_array() INTO report_types;
  END IF;

	UPDATE usage_data_providers SET
	  jsonb = jsonb || (
	    '{"latestReport": ' || COALESCE(to_jsonb(latest), 'null'::jsonb)::text ||
	    ', "earliestReport": ' || COALESCE(to_jsonb(earliest), 'null'::jsonb)::text ||
	    ', "reportErrorCodes": ' || error_codes ||
	    ', "hasFailedReport": ' || has_failed_report ||
	    ', "reportTypes": ' || report_types ||
	    '}')::jsonb	WHERE jsonb->>'id' = providerId;

	RETURN NULL;
END;
$BODY$ LANGUAGE plpgsql;

-- trigger to update latestReport, earliestReport, reportErrorCodes and hasFailedReport
-- of the usage data provider, on update/insert/delete
DROP TRIGGER IF EXISTS update_usage_data_providers_on_insert_or_update_or_delete ON counter_reports;
CREATE TRIGGER update_usage_data_providers_on_insert_or_update_or_delete
AFTER INSERT OR UPDATE OR DELETE ON counter_reports
FOR EACH ROW EXECUTE PROCEDURE update_latest_statistic_on_update();
