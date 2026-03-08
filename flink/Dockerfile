FROM flink:1.18.0-scala_2.12

# Install all dependencies in a single layer to avoid caching issues
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.18.0/flink-json-1.18.0.jar && \
    # Install Prometheus metrics reporter plugin
    mkdir -p /opt/flink/plugins/metrics-prometheus && \
    wget -O /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-1.18.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.0/flink-metrics-prometheus-1.18.0.jar && \
    # Configure flink-conf.yaml: update memory and append metrics config
    sed -i 's/^taskmanager\.memory\.process\.size:.*/taskmanager.memory.process.size: 8192m/' /opt/flink/conf/flink-conf.yaml && \
    echo "taskmanager.memory.managed.fraction: 0.05" >> /opt/flink/conf/flink-conf.yaml && \
    echo "metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory" >> /opt/flink/conf/flink-conf.yaml && \
    echo "metrics.reporter.prom.port: 9249" >> /opt/flink/conf/flink-conf.yaml

EXPOSE 8081 9249
