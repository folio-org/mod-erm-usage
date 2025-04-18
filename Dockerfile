FROM folioci/alpine-jre-openjdk21:latest

# Install latest patch versions of packages: https://pythonspeed.com/articles/security-updates-in-docker/
USER root
RUN apk upgrade --no-cache
USER folio

ENV VERTICLE_FILE mod-erm-usage-server-fat.jar

# Set the location of the verticles
ENV VERTICLE_HOME=/usr/verticles \
 DB_USERNAME=folio_admin \
 DB_PASSWORD=folio_admin \
 DB_HOST=172.17.0.1 \
 DB_PORT=5432 \
 DB_DATABASE=okapi_modules

# Copy your fat jar to the container
COPY mod-erm-usage-server/target/${VERTICLE_FILE} ${VERTICLE_HOME}/${VERTICLE_FILE}

# Expose this port locally in the container.
EXPOSE 8081
