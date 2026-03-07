-- 1. Create Source Table mapping to the Debezium Redpanda topic
-- Debezium uses an envelope format that Flink natively supports using 'debezium-json'
CREATE TABLE raw_metrics_source (
    id INT,
    timestamp_ms BIGINT,
    job_id STRING,
    channel_id STRING,
    `value` DOUBLE,
    -- Create a strongly typed TIMESTAMP(3) column from the BIGINT ms timestamp for Flink Windowing
    event_time AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp_ms / 1000)),
    -- Define the watermark, allowing for up to 5 seconds of out-of-order data
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic-pattern' = 'postgres\.public\.raw_metrics',
    'scan.topic-partition-discovery.interval' = '5000',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'flink-consumer-group-1',
    'scan.startup.mode' = 'earliest-offset',
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
    'format' = 'json'
);
