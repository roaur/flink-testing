FROM quay.io/debezium/connect:2.4

USER root
# Install wget
RUN microdnf -y install wget

# Download the Prometheus JMX Exporter agent
RUN mkdir -p /kafka/libs && \
    wget -P /kafka/libs/ https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.19.0/jmx_prometheus_javaagent-0.19.0.jar

# Explicitly load the javaagent inside the Kafka startup arguments
COPY jmx_config.yml /kafka/etc/config.yml
ENV EXTRA_ARGS="-javaagent:/kafka/libs/jmx_prometheus_javaagent-0.19.0.jar=8090:/kafka/etc/config.yml"

USER kafka
