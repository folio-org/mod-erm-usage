FROM folioci/openjdk8-jre:latest

ENV VERTICLE_FILE mod-erm-usage-harvester-fat.jar
ENV CONF_FILE config.json

# Copy your fat jar to the container
COPY ./${VERTICLE_FILE} ${VERTICLE_HOME}/${VERTICLE_FILE}
COPY ./${CONF_FILE} ${VERTICLE_HOME}/${CONF_FILE}

# Expose this port locally in the container.
EXPOSE 8081

ENTRYPOINT java -jar ${VERTICLE_HOME}/${VERTICLE_FILE} -conf ${VERTICLE_HOME}/${CONF_FILE}