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
  SELECT json_agg(errors)::jsonb
  FROM (
    SELECT
      DISTINCT(
        COALESCE(SUBSTRING(jsonb->>'failedReason','(?:Number=|"Code": ?)([0-9]{1,4})'), 'other')
      ) AS errors
    FROM counter_reports
    WHERE jsonb->>'providerId'=$1 AND jsonb->>'failedReason' IS NOT NULL
    ORDER BY 1
  )
  AS sub
$$ LANGUAGE sql;

-- returns the counter reports types of the usage data provider's counter reports
CREATE OR REPLACE FUNCTION udp_report_types(providerId TEXT) RETURNS jsonb AS $$
  SELECT json_agg(reportNames)::jsonb
  FROM (
    SELECT
      DISTINCT(
        COALESCE(jsonb->>'reportName', 'other')
      ) AS reportNames
    FROM counter_reports
    WHERE jsonb->>'providerId'=$1
    ORDER BY 1
  )
  AS sub
$$ LANGUAGE sql;

-- returns the counter report release versions of the usage data provider's counter reports
CREATE OR REPLACE FUNCTION udp_report_releases(providerId TEXT) RETURNS jsonb AS $$
  SELECT COALESCE(jsonb_agg(DISTINCT jsonb->>'release'), '[]'::jsonb)
  FROM counter_reports
  WHERE jsonb->>'providerId' = $1
  ORDER BY 1;
$$ LANGUAGE sql;

-- function to update the usage data provider statistics
CREATE OR REPLACE FUNCTION update_udp_statistics(providerId TEXT) RETURNS VOID AS
$$
DECLARE latest TEXT;
DECLARE earliest TEXT;
DECLARE error_codes jsonb;
DECLARE has_failed_report jsonb;
DECLARE report_types jsonb;
DECLARE report_releases jsonb;
BEGIN
	SELECT latest_year_month(providerId) INTO latest;
	SELECT earliest_year_month(providerId) INTO earliest;
	SELECT udp_report_errors(providerId) INTO error_codes;
	SELECT udp_report_types(providerId) INTO report_types;
	SELECT udp_report_releases(providerId) INTO report_releases;
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
	    ', "reportReleases": ' || report_releases ||
	    '}')::jsonb	WHERE jsonb->>'id' = providerId;
END;
$$ LANGUAGE plpgsql;

-- trigger function to update the statistics of an usage data provider
CREATE OR REPLACE FUNCTION update_udp_statistics() RETURNS TRIGGER AS
$$
DECLARE providerId TEXT;
BEGIN
  IF (TG_OP = 'DELETE') THEN
    providerId := OLD.jsonb->>'providerId';
  ELSE
    providerId := NEW.jsonb->>'providerId';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext(providerId));
  PERFORM update_udp_statistics(providerId);

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- trigger to update usage data provider statistics, on update/insert/delete of reports
DROP TRIGGER IF EXISTS update_usage_data_providers_on_insert_or_update_or_delete ON counter_reports;
CREATE TRIGGER update_usage_data_providers_on_insert_or_update_or_delete
AFTER INSERT OR UPDATE OR DELETE ON counter_reports
FOR EACH ROW EXECUTE PROCEDURE update_udp_statistics();
