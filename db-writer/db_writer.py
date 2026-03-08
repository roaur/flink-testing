import asyncio
import asyncpg
import time
import random
import os

# Database configuration
DB_USER = os.getenv("POSTGRES_USER", "pguser")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pgpassword")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "iotdb")

# Simulation config
NUM_JOBS = int(os.getenv("NUM_JOBS", "50"))
CHANNELS_PER_JOB = int(os.getenv("CHANNELS_PER_JOB", "1000"))
TOTAL_CHANNELS = NUM_JOBS * CHANNELS_PER_JOB
# To achieve ~50000 records/sec total, we need each channel to emit 1 record per second
# (50 jobs * 1000 channels/job * 1 record/sec = 50000 records/sec)

async def worker(pool, job_id, channels):
    """
    Worker simulating one 'job'. It continuously loops over its designated channels,
    generating a random walk value for each and writing them to the database in a batch
    every second to maximize throughput.
    """
    # Initialize random walk starting points
    values = {channel_id: random.uniform(20.0, 100.0) for channel_id in channels}

    print(f"[{job_id}] Started with {len(channels)} channels.")

    while True:
        start_time = time.time()

        # Prepare batch of records for this exact second
        records = []
        current_timestamp = int(time.time() * 1000) # Unix timestamp in milliseconds

        for channel_id in channels:
            # Brownian motion / random walk
            step = random.uniform(-1.0, 1.0)
            values[channel_id] += step

            records.append((current_timestamp, job_id, channel_id, values[channel_id]))

        gen_time = time.time()

        # Bulk insert the batch
        try:
            async with pool.acquire() as conn:
                await conn.copy_records_to_table(
                    "raw_metrics",
                    records=records,
                    columns=["timestamp_ms", "job_id", "channel_id", "value"]
                )
        except Exception as e:
            print(f"[{job_id}] Database Error: {e}")

        insert_time = time.time()

        # Sleep for exactly whatever is remaining of the 1-second interval
        elapsed = time.time() - start_time
        sleep_time = max(0, 1.0 - elapsed)

        # If we exceeded 1 second to write, log a warning
        if elapsed > 1.0:
            print(f"[{job_id}] Warning: Batch took {elapsed:.3f}s (Gen: {gen_time - start_time:.3f}s, DB: {insert_time - gen_time:.3f}s), exceeding 1s target.")

        await asyncio.sleep(sleep_time)


async def main():
    print("Initializing Database Connection Pool...")
    # Add a retry loop for initial connection since Postgres might take a moment to start
    pool = None
    for _ in range(10):
        try:
            pool = await asyncpg.create_pool(
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                host=DB_HOST,
                port=DB_PORT,
                min_size=1,
                max_size=NUM_JOBS + 2
            )
            print("Successfully connected to Database!")
            break
        except Exception as e:
            print(f"Waiting for database to be ready... ({e})")
            await asyncio.sleep(2)

    if not pool:
        print("Failed to initialize database pool after multiple retries. Exiting.")
        return

    # Generate the job and channel distribution
    tasks = []
    print(f"Starting simulation of {NUM_JOBS} jobs, generating ~{TOTAL_CHANNELS} records/second.")

    for i in range(NUM_JOBS):
        job_id = f"job_{i+1:02d}"
        channels = [f"channel_{j+1:04d}" for j in range(i * CHANNELS_PER_JOB, (i+1) * CHANNELS_PER_JOB)]
        tasks.append(worker(pool, job_id, channels))

    # Run all workers concurrently
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down generator.")
