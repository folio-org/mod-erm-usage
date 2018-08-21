#!/bin/sh

env DB_USERNAME=folio_admin DB_PASSWORD=folio_admin DB_HOST=192.168.56.103 DB_PORT=5432 DB_DATABASE=okapi_modules java -jar target/mod-erm-usage-fat.jar
