DROP TRIGGER IF EXISTS update_provider_report_error_codes_on_delete ON ${myuniversity}_${mymodule}.counter_reports;
DROP TRIGGER IF EXISTS update_provider_report_error_codes_on_update ON ${myuniversity}_${mymodule}.counter_reports;
DROP TRIGGER IF EXISTS update_provider_report_date_on_delete ON ${myuniversity}_${mymodule}.counter_reports;
DROP TRIGGER IF EXISTS update_provider_report_date_on_update ON ${myuniversity}_${mymodule}.counter_reports;

DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.update_udp_error_codes_on_delete();
DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.update_udp_error_codes_on_update();
DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.update_latest_statistic_on_delete();
