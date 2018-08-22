# mod-erm-usage

Copyright (C) 2018 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.


# Installation

```
git clone ...
cd mod-erm-usage
mvn clean install
```

# Run

### `mod-erm-usage-server`
```
cd mod-erm-usage-server
env \
DB_USERNAME=folio_admin \
DB_PASSWORD=folio_admin \
DB_HOST=localhost \
DB_PORT=5432 \
DB_DATABASE=okapi_modules \
java -jar target/mod-erm-usage-server-fat.jar
```

### `mod-erm-usage-harvester`

```
cd mod-erm-usage-harvester
java -jar target/mod-erm-usage-harvester-fat.jar -conf target/config.json
```

configuration via json file:
```json
{
  "okapiUrl": "http://localhost:9130",
  "tenantsPath": "/_/proxy/tenants",
  "reportsPath": "/counter-reports",
  "providerPath": "/usage-data-providers",
  "aggregatorPath": "/aggregator-settings",
  "moduleId": "mod-erm-usage-harvester-0.0.2-SNAPSHOT"
}
```


