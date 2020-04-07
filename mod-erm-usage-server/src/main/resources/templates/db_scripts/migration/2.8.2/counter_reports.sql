DROP TRIGGER IF EXISTS update_provider_failed_report_on_update ON ${myuniversity}_${mymodule}.counter_reports;

DROP TRIGGER IF EXISTS update_provider_failed_report_on_delete ON ${myuniversity}_${mymodule}.counter_reports;

DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.udp_has_failed_report(TEXT);

DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.update_udp_failed_report_on_update();

DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.update_udp_failed_report_on_delete();
