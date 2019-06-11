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
