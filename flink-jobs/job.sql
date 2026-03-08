-- Set job parallelism to match number of Kafka partitions
SET 'parallelism.default' = '4';

-- Tune Flink's checkpoint and buffering behavior for throughput
SET 'execution.checkpointing.interval' = '30s';
SET 'state.backend.type' = 'hashmap';
SET 'state.checkpoints.dir' = 'file:///tmp/flink-checkpoints';
SET 'table.exec.source.idle-timeout' = '1s';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '500ms';
SET 'table.exec.mini-batch.size' = '5000';

-- 1. Create Source Table mapping to the Debezium Redpanda topic
CREATE TABLE raw_metrics_source (
    id INT,
    timestamp_ms BIGINT,
    job_id STRING,
    channel_id STRING,
    `value` DOUBLE,
    event_time AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp_ms / 1000)),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic-pattern' = 'postgres\.public\.raw_metrics',
    'scan.topic-partition-discovery.interval' = '5000',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'flink-consumer-group-1',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'properties.fetch.min.bytes' = '65536',
    'properties.fetch.max.wait.ms' = '100',
    'format' = 'json'
);

-- 2. Create the Sink Table mapping to the Enriched Redpanda topic
CREATE TABLE enriched_metrics_sink (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    channel_id STRING,
    avg_value DOUBLE,
    record_count BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'enriched_metrics',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.compression.type' = 'lz4',
    'properties.linger.ms' = '50',
    'properties.batch.size' = '524288',
    'format' = 'json'
);

-- 3. Execute the HOP window query
INSERT INTO enriched_metrics_sink
SELECT
    window_start,
    window_end,
    channel_id,
    AVG(`value`) AS avg_value,
    COUNT(*) AS record_count
FROM TABLE(
    HOP(TABLE raw_metrics_source, DESCRIPTOR(event_time), INTERVAL '1' SECONDS, INTERVAL '10' SECONDS)
)
GROUP BY
    window_start,
    window_end,
    channel_id;
