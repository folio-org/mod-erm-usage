# 2.7.0
* Renamed some permissions in accordance to other FOLIO modules (UIEUS-83 & UIEUS-126)
* Manage harvesting errors: filter providers by error type (UIEUS-117)
* Add indexes for full-text search (MODEUS-37)

# 2.6.0
* Update to RMB version 29.1.0
* Use new base docker image && new JAVA_OPTIONS (FOLIO-2358)
* Add database migration script
* Update jackson-databind version to 2.10.0
* Support upload of csv reports (UIEUS-105)
* Update UDP schema: add `hasFailedReport`

# 2.5.0
* Update RMB version to 26.2.4
* Update jackson-databind version to 2.9.9.3
* Some fixes in tests

# 2.4.1
* Remove hardcoded 'diku' from db_scripts and use different tenant for tests (MODEUS-32)

# 2.4.0
* Update jackson-databind to 2.9.9.1 CVE-2019-12814
* Add null checks when getting providers metadata during credentials CSV export
* Add tests for SQL triggers
* Add SQL triggers to autoupdate aggregator names stored in usage data providers when aggregator label changes
* Resolve aggregator label by querying the database (MODEUS-27)
* Remove `organizations-storage` from required interfaces (MODEUS-26)
* Add SQL trigger to delete counter-reports if provider gets deleted (MODEUS-29)

# 2.3.1
* Bump erm-usage-counter version to incorporate bugfix (MODEUS-24)

# 2.3.0
* Update models & sample data
* Upload Counter5 reports

# 2.2.0
* Add `createdDate`/`updatedDate` metadata to credentials csv export (MODEUS-18)
* Use latest `mod-erm-usage-counter` version
* Replace `mod-vendors` with `mod-organizations-storage` (MODEUS-19)
* Add reports to sample data

# 2.1.0
* Fix vendor permissions (MODEUS-14)
* Use RMBs PgUtil
* Use loadSample parameter in tenant init to load sample data (MODEUS-12)

# 2.0.0
* Added endpoint for exporting the sushi credentials that are associated with an aggregator (MODEUS-7)
* Added endpoint for exporting counter reports as CSV (MODEUS-5)
* Added endpoint for uploading counter reports from file
* Added database triggers to set `latestReport` and `earliestReport` properties
* Report content is stored as JSON object
* Store vendor and aggregator names in usage data provider
* Updated data model

# 1.0.0
* Move harvester to mod-erm-usage-harvester project

# 0.1.0
* CRUD usage data providers holding information to access counter statistics
* Ability to assign usage data providers to aggregators

## 0.0.3
* First release
