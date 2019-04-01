FROM image
ARG MODULE
ARG TAG
ENTRYPOINT ["/usr/local/bin/docker-entrypoint"]

ENV START_CMD="./run-java.sh" \
  OKAPI_TRY_COUNT=120 \
  REGISTER_SERVICE_TRY_COUNT=120 \
  INTRODUCE_MODULE_TRY_COUNT=120 \
  CURL_CONNECT_TIMEOUT=3 \
  SERVICE_ID=mod-erm-usage-2.1.0-SNAPSHOT \
  INSTALL_ID=mod-erm-usage-2.1.0-SNAPSHOT \
  SERVICE_URL=http://localhost:8081/ \
  SELF_DEPLOY=false

COPY target/ModuleDescriptor.json module-descriptor.json

COPY backend-modules/assets/docker-entrypoint /usr/local/bin/
