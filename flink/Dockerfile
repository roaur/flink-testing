FROM flink:1.18.0-scala_2.12

# Install Flink SQL Kafka connector and JSON format dependencies
# 1. Enable Flink Kafka Connector
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar

# 2. Enable Flink Prometheus Metrics
RUN mkdir -p /opt/flink/plugins/metrics-prometheus && \
    wget -P /opt/flink/plugins/metrics-prometheus/ https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.0/flink-metrics-prometheus-1.18.0.jar

# 3. Inject Prometheus Reporter Configurations
RUN echo "metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory" >> /opt/flink/conf/flink-conf.yaml && \
    echo "metrics.reporter.prom.port: 9249" >> /opt/flink/conf/flink-conf.yaml

EXPOSE 8081 9249

RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.18.0/flink-json-1.18.0.jar
