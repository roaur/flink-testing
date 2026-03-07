-- We use an INSERT INTO statement which runs continuously as a Flink Job
-- The WINDOW function we use here is a HOP window (Sliding window)
-- It calculates the average over a 10-second period, sliding every 1 second
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
