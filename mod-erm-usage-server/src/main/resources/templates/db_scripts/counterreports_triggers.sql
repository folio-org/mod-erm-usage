-- return year-month of latest report available for a usage data provider
CREATE OR REPLACE FUNCTION latest_year_month(
  providerId TEXT
  ) RETURNS TEXT AS
$$
DECLARE res TEXT;
DECLARE sp TEXT;
BEGIN
  SELECT setting INTO sp FROM pg_settings WHERE name = 'search_path';
  -- RAISE NOTICE 'current_user is currently %', current_user;
	-- RAISE NOTICE 'search_path is currently %', sp;
	SELECT jsonb->>'yearMonth' INTO res FROM counter_reports WHERE jsonb->>'providerId'=$1 AND jsonb->'failedAttempts' IS NULL ORDER BY jsonb->>'yearMonth' DESC LIMIT 1;
  -- RAISE EXCEPTION 'failed';
	IF res IS NULL THEN
		SELECT '' INTO res;
	END IF;
	-- RAISE NOTICE 'res is currently %', res;
	RETURN res;
END;
$$
LANGUAGE plpgsql;

-- return year-month of earliest report available for a usage data provider
CREATE OR REPLACE FUNCTION earliest_year_month(
  providerId TEXT
  ) RETURNS TEXT AS
$$
DECLARE res TEXT;
BEGIN
	SELECT jsonb->>'yearMonth' into res FROM counter_reports WHERE jsonb->>'providerId'=$1 AND jsonb->'failedAttempts' IS NULL ORDER BY jsonb->>'yearMonth' ASC LIMIT 1;
	IF res IS NULL THEN
		SELECT '' INTO res;
	END IF;
	RETURN res;
END;
$$
LANGUAGE plpgsql;

-- return yes/no depending whether the usage data provider has a failed report or not
CREATE OR REPLACE FUNCTION udp_has_failed_report(
  providerId TEXT
  ) RETURNS TEXT AS
$$
DECLARE c NUMERIC;
DECLARE res TEXT;
BEGIN
  SELECT count(*) INTO c FROM counter_reports WHERE jsonb->>'providerId'=$1 AND jsonb->>'failedAttempts' IS NOT NULL;
  IF c IS NULL THEN
    SELECT 'no' INTO res;
  ELSIF c = 0 THEN
    SELECT 'no' INTO res;
  ELSE
    SELECT 'yes' INTO res;
  END IF;
  RETURN res;
END;
$$
LANGUAGE plpgsql;

-- trigger function to update latest report available of an usage data provider, on update/insert
CREATE OR REPLACE FUNCTION update_latest_statistic_on_update() RETURNS TRIGGER AS
$BODY$
DECLARE latest TEXT;
DECLARE earliest TEXT;
BEGIN
	SELECT latest_year_month(NEW.jsonb->>'providerId') INTO latest;
	SELECT earliest_year_month(NEW.jsonb->>'providerId') INTO earliest;
	UPDATE usage_data_providers SET jsonb = jsonb_set(jsonb, '{latestReport}', to_jsonb(latest), TRUE) WHERE jsonb->>'id'=NEW.jsonb->>'providerId';
	UPDATE usage_data_providers SET jsonb = jsonb_set(jsonb, '{earliestReport}', to_jsonb(earliest), TRUE) WHERE jsonb->>'id'=NEW.jsonb->>'providerId';
	RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;

-- trigger function to update latest report available of an usage data provider, on delete
CREATE OR REPLACE FUNCTION update_latest_statistic_on_delete() RETURNS TRIGGER AS
$BODY$
DECLARE latest TEXT;
DECLARE earliest TEXT;
BEGIN
	SELECT latest_year_month(OLD.jsonb->>'providerId') INTO latest;
	SELECT earliest_year_month(OLD.jsonb->>'providerId') INTO earliest;
	UPDATE usage_data_providers SET jsonb = jsonb_set(jsonb, '{latestReport}', to_jsonb(latest), TRUE) WHERE jsonb->>'id'=OLD.jsonb->>'providerId';
	UPDATE usage_data_providers SET jsonb = jsonb_set(jsonb, '{earliestReport}', to_jsonb(earliest), TRUE) WHERE jsonb->>'id'=OLD.jsonb->>'providerId';
	RETURN OLD;
END;
$BODY$ LANGUAGE plpgsql;

-- trigger function to update if an usage data provider has a failed report, on update/insert
CREATE OR REPLACE FUNCTION update_udp_failed_report_on_update() RETURNS TRIGGER AS
$BODY$
DECLARE has_error TEXT;
BEGIN
	SELECT udp_has_failed_report(NEW.jsonb->>'providerId') INTO has_error;
	UPDATE usage_data_providers SET jsonb = jsonb_set(jsonb, '{hasFailedReport}', to_jsonb(has_error), TRUE) WHERE jsonb->>'id'=NEW.jsonb->>'providerId';
	RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;

-- trigger function to update if an usage data provider has a failed report, on delete
CREATE OR REPLACE FUNCTION update_udp_failed_report_on_delete() RETURNS TRIGGER AS
$BODY$
DECLARE has_error TEXT;
BEGIN
	SELECT udp_has_failed_report(OLD.jsonb->>'providerId') INTO has_error;
	UPDATE usage_data_providers SET jsonb = jsonb_set(jsonb, '{hasFailedReport}', to_jsonb(has_error), TRUE) WHERE jsonb->>'id'=OLD.jsonb->>'providerId';
	RETURN OLD;
END;
$BODY$ LANGUAGE plpgsql;

-- trigger to update latest report available of an usage data provider, on update/insert
DROP TRIGGER IF EXISTS update_provider_report_date_on_update ON counter_reports;

CREATE TRIGGER update_provider_report_date_on_update
AFTER INSERT OR UPDATE ON counter_reports
FOR EACH ROW
EXECUTE PROCEDURE update_latest_statistic_on_update();

-- trigger to update latest report available of an usage data provider, on delete
DROP TRIGGER IF EXISTS update_provider_report_date_on_delete ON counter_reports;

CREATE TRIGGER update_provider_report_date_on_delete
AFTER DELETE ON counter_reports
FOR EACH ROW
EXECUTE PROCEDURE update_latest_statistic_on_delete();

-- trigger to update if an usage data provider has a failed report, on update/insert
DROP TRIGGER IF EXISTS update_provider_failed_report_on_update ON counter_reports;

CREATE TRIGGER update_provider_failed_report_on_update
AFTER INSERT OR UPDATE ON counter_reports
FOR EACH ROW
EXECUTE PROCEDURE update_udp_failed_report_on_update();

-- trigger to update if an usage data provider has a failed report, on delete
DROP TRIGGER IF EXISTS update_provider_failed_report_on_delete ON counter_reports;

CREATE TRIGGER update_provider_failed_report_on_delete
AFTER DELETE ON counter_reports
FOR EACH ROW
EXECUTE PROCEDURE update_udp_failed_report_on_delete();
