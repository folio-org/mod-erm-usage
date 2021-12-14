# 4.2.1
* [MODEUS-134](https://issues.folio.org/browse/MODEUS-134) Update to RMB 32.2.2 fixing remote exectution (CVE-2021-44228)
* [MODEUS-133](https://issues.folio.org/browse/MODEUS-133) Update to RMB 32.2.1 fixing remote exectution (CVE-2021-44228)

# 4.2.0
* Incorrect cell formatting in xlsx downloads (MODEUS-113)
* Support download of Standard Report Views (MODEUS-116)
* Replace ErmUsageFile POJO by RAML type (MODEUS-118)
* Upload of CSV report fails if the first line contains quotes (MODEUS-120)
* OutOfMemoryError when uploading CSV files (MODEUS-121)
* Update required attributes in UDP JSON schema  (MODEUS-123)
* Counter 5 SUSHI error codes not included in /counter-reports/error/codes (MODEUS-126)
* Schema upgrade test (MODEUS-127)
* Duplicate error codes in usage data provider (MODEUS-129)
* Add FOLIO info to Created_By value in exported files (MODEUS-130)
* Missing data in download files for reports with YOP (MODEUS-132)

# 4.1.0 (2021-06-11)
* Upgrade RMB to v33 (MODEUS-112)
* Multiple reports can be deleted with one request (MODEUS-111)

# 4.0.0 (2021-03-17)
* Upgrade to RMB 32 (MODEUS-101)
* Add endpoint to get all available report types (MODEUS-103)
* Counter reports can be marked as edited manually (MODEUS-99, MODEUS-104)
* Add available report types to udp (MODEUS-100)
* Custom reports can be specified by link (MODEUS-98)

# 3.0.2 (2020-11-04)
* Upgrade to RMB 31.1.5 and Vert.x 3.9.4 (MODEUS-95)

# 3.0.1 (2020-10-23)
* Bugfix: Module logging error "ERROR StatusLogger Unrecognized format specifier" (MODEUS-92)
* Upgrade to junit 4.13.1
* Bugfix: Some database migration scripts between Goldenrod & Honeysuckle not executed (MODEUS-93)

# 3.0.0
* Unsupported reports causing sql errors on upload (MODEUS-89)
* Report download seems to block the event loop (MODEUS-88)
* Remove deprecated /counter-reports/csv endpoint (MODEUS-62)
* Upgrade to RAML Module Builder 31.x (MODEUS-82)
* Update module to JDK 11 ([MODEUS-81])

# 2.10.0
* Add endpoint to manage custom-reports (MODEUS-58)
* Add property to UDP schema for storing last harvesting datetime (MODEUS-73)
* mod-erm-usage crashes when manually uploading certain COUNTER reports (MODEUS-76)
* Latest statistics sometimes has incorrect value (MODEUS-77)
* Add endpoint for downloading reports in their original format (MODEUS-78)

# 2.9.0
* Support xlsx download of udp credentials (MODEUS-63)
* Create endpoint for exporting counter reports in different formats (MODEUS-61)
* Add endpoint to manage custom-report files (MODEUS-59)
* Upgrade to RMB v30 (MODEUS-57)
* Complete documentation (MODEUS-49)
* Mapping from Counter 5 CSV to corresponding reports (MODEUS-54)
* Create missing indexes (MODEUS-56)
* Make downloading multi month CSV reports faster (MODEUS-52)
* Code refactoring
* Download stored reports for a range of several month (CSV, CoP 5) (UIEUS-162)

# 2.8.2
* Bugfix: Delete deprecated DB functions and triggers on migration (MODEUS-50)

# 2.8.1
* Bugfix: Update version of endpoint 'counter-reports'

# 2.8.0
* Ability to download COP5 csv reports for single month (UIEUS-106)
* Add endpoint to return sorted counter-reports (by year, month & report type) (UIEUS-134)

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
