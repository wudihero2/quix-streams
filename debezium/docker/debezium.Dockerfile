ARG DEBEZIUM_VERSION=${DEBEZIUM_VERSION}
ARG GRADLE_VERSION=${GRADLE_VERSION}

# [Stage] Download shared jars for Debezium

FROM gradle:${GRADLE_VERSION} AS loader

WORKDIR /gradle

COPY ./build.gradle build.gradle

RUN gradle --console verbose

# [Stage] Debezium Base Image

FROM quay.io/debezium/connect:${DEBEZIUM_VERSION}

ARG JMX_AGENT_VERSION=${JMX_AGENT_VERSION}
RUN mkdir /kafka/etc && cd /kafka/etc \
    && curl -so jmx_prometheus_javaagent.jar \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/$JMX_AGENT_VERSION/jmx_prometheus_javaagent-$JMX_AGENT_VERSION.jar

COPY docker/config.yml /kafka/etc/config.yml
COPY log4j.properties /kafka/config/log4j.properties

WORKDIR /kafka

COPY --from=loader /gradle/build/plugins ./connect
