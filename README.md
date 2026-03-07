# Flink IoT Telemetry PoC

This repository contains a simple, high-throughput proof-of-concept for calculating streaming aggregations using Python, PostgreSQL, Redpanda, and Apache Flink.

## 🏗️ Architecture Overview

The pipeline executes through the following bounded steps:

1. **Python Data Generator (`uv` + `asyncpg`)**: Simulates 10 different concurrent 'jobs' that each own 300 unique 'channels' (totalling 3,000 unique keys). Using an asynchronous batching event loop, it generates a random walk value for every single channel, exactly once per second, resulting in a sustained **~3,000 records per second** write volume to the PostgreSQL database.
2. **PostgreSQL**: Acts as the transactional buffering layer, tracking raw metric data.
3. **Debezium CDC (Change Data Capture)**: Connects to the PostgreSQL Write-Ahead Log (WAL) to tail the database for new inserts in real-time. It serializes these changes and dumps them into a **Redpanda** topic (`postgres.public.raw_metrics`).
4. **Apache Flink SQL**: Subscribes to the Redpanda raw topic, securely handles the unboxing of the Debezium JSON payload, computes an intricate **10-second sliding moving average**, and persists the output straight back into a new Redpanda topic (`enriched_metrics`).

---

## 🚀 Running the Project

### 1. Start the Infrastructure
Launch Postgres, Redpanda, the Debezium Connect service, and the Apache Flink cluster.

```bash
docker-compose up -d --build
```

Wait around 30 seconds for the database and Kafka brokers to securely report as healthy.

### 2. Verify Data Flow
Debezium and the Flink SQL jobs are completely automated with initialization containers (`debezium-initializer` and `flink-job-submitter`).

Once the cluster safely boots up in ~45 seconds, the Debezium Postgres connector will be registered dynamically, and the `job.sql` script is submitted straight into the Flink JobManager.

You do not need to submit the jobs manually or perform any `curl` requests!

### 4. Verify the Metrics
Hop into the Redpanda console to observe the 10-second moving averages calculating in real-time. Navigate to your browser:
* **[http://localhost:8080](http://localhost:8080)**

You should see a topic created named `enriched_metrics`.

### 5. Access Monitoring and Observability
The following ports are exposed for monitoring and observability:
* **Grafana**: [http://localhost:3000](http://localhost:3000) (Credentials: `admin` / `admin`)
* **Prometheus**: [http://localhost:9090](http://localhost:9090)
* **Redpanda Console**: [http://localhost:8085](http://localhost:8085)
* **Flink Web UI**: [http://localhost:8081](http://localhost:8081)
* **Flink JobManager Metrics**: `localhost:9249`
* **Flink TaskManager Metrics**: `localhost:9250`
* **Debezium/Connect Metrics**: `localhost:8090`
* **Redpanda Broker Metrics**: `localhost:9644`

---

## 🧠 Flink SQL: Deep Dive

One of the predominant purposes of this project is serving as a sandbox for understanding **Flink SQL**. Flink SQL lets you define continuous, unbounded processing pipelines using declarative SQL statements, abstracted powerfully over Apache Flink's distributed stateful task managers.

### Mapping Data Streams to Tables
In a standard RDBMS, a table contains data resting statically on disk. In Flink SQL, a "table" is actually an abstract mapping sitting on top of an active Data Stream (like a Redpanda topic).

Take a look at how we define our source:

```sql
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
    'topic' = 'postgres.public.raw_metrics',
    'format' = 'debezium-json'
);
```

There are a few critical concepts occurring here:
1. **Computed Columns (`event_time`)**: Our raw inserts use a Unix millisecond BIGINT (`timestamp_ms`). Flink's time-based windowing strictly requires a `TIMESTAMP(3)` type. We use `AS` to define a derived column evaluating out this conversion per-row natively.
2. **Watermarks**: Real-world distributed systems experience network delay; events don't arrive strictly in chronological order. A watermark (`WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS`) tells Flink: *"Do not close a calculation window until you are absolutely confident you won't see an event older than 5 seconds ago."* It explicitly balances latency versus exactness.
3. **The Debezium Format**: Because we extracted the row using Debezium CDC, the Kafka payload actually contains an intricate envelope detailing `"before"` and `"after"` database states. Writing `'format' = 'debezium-json'` instructs Flink to automatically deserialize this envelope natively and extract just the new row without manual JSON parsing.

### How Sliding Windows Work

Our problem statement is calculating a **10-second rolling average for each unique channel**.
Because data continues unbounded forever, we can't just `GROUP BY channel_id`. We must `GROUP BY (channel_id + A window of time)`.

We use a **Hop (or Sliding) Window** instead of a tumbling window because we want the pipeline to emit *continuous updates* (every 1 second) characterizing the *trailing 10 seconds of data*.

```sql
INSERT INTO enriched_metrics_sink
SELECT
    window_start,
    window_end,
    channel_id,
    AVG(`value`) AS avg_value,
    COUNT(*) AS record_count
FROM TABLE(
    -- 1. Source Table
    -- 2. Time attribute to sort by
    -- 3. Slide Interval (How often to emit a calculation -> every 1 second)
    -- 4. Window Size (How much time the block should cover -> 10 seconds)
    HOP(TABLE raw_metrics_source, DESCRIPTOR(event_time), INTERVAL '1' SECONDS, INTERVAL '10' SECONDS)
)
GROUP BY
    window_start,
    window_end,
    channel_id;
```

When this `INSERT INTO` runs, Flink transforms the statement into a compiled execution graph, allocates state storage to hold the 10-second trailing state for the 3,000 unique `channel_id`s, and begins consuming. Every internal second ticks, and Flink emits out the computed aggregates securely into the `enriched_metrics_sink` table (terminating down into the Redpanda topic as JSON).
