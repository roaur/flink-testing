-- Create raw metrics table
CREATE TABLE IF NOT EXISTS public.raw_metrics (
    id SERIAL PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    job_id VARCHAR(50) NOT NULL,
    channel_id VARCHAR(50) NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

-- Set replica identity to full so Debezium can capture the before/after state properly
ALTER TABLE public.raw_metrics REPLICA IDENTITY FULL;

-- Explicitly create a publication that only publishes INSERT and UPDATE, skipping DELETEs
CREATE PUBLICATION dbz_publication FOR TABLE public.raw_metrics WITH (publish = 'insert, update');
