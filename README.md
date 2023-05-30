# mod-erm-usage

Copyright (C) 2018-2023 The Open Library Foundation

This software is distributed under the terms of the Apache License, Version 2.0. See the
file "[LICENSE](LICENSE)" for more information.

![Development funded by European Regional Development Fund (EFRE)](assets/EFRE_2015_quer_RGB_klein.jpg)

# Installation

```
git clone ...
cd mod-erm-usage
mvn clean install
```

# Run plain jar

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

# Run via Docker

### Build docker image

```
$ docker build -t mod-erm-usage .
```

### Run docker image

```
$ docker run -p 8081:8081 -e DB_USERNAME=folio_admin -e DB_PASSWORD=folio_admin -e DB_HOST=172.17.0.1 -e DB_PORT=5432 -e DB_DATABASE=okapi_modules mod-erm-usage
```

### Register ModuleDescriptor

```
$ cd target
$ curl -w '\n' -X POST -D - -H "Content-type: application/json" -d @ModuleDescriptor.json http://localhost:9130/_/proxy/modules
```

### Register DeploymentDescriptor

Change _nodeId_ in _DockerDeploymentDescriptor.json_ to e.g. your hosts IP address (e.g. 10.0.2.15).
Then execute:

```
$ curl -w '\n' -X POST -D - -H "Content-type: application/json" -d @DockerDeploymentDescriptor.json http://localhost:9130/_/discovery/modules
```

### Activate module for tenant

```
$ curl -w '\n' -X POST -D - -H "Content-type: application/json" -d '{ "id": "mod-erm-usage-1.0.0"}' http://localhost:9130/_/proxy/tenants/diku/modules
```

## Additional information

### Issue tracker

See project [MODEUS](https://issues.folio.org/browse/MODEUS)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker).

### Other documentation

Other [modules](https://dev.folio.org/source-code/#server-side) are described, with further FOLIO
Developer documentation at [dev.folio.org](https://dev.folio.org/)

