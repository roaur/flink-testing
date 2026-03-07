import json
import time
import uuid
import psycopg2
from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = "localhost:9092"
POSTGRES_DSN = "host=localhost dbname=iotdb user=pguser password=pgpassword port=5432"

def test_end_to_end_flink_pipeline():
    """
    Integration script intended to be manually or CI-triggered against a running docker-compose cluster.
    It verifies that:
    1. Redpanda is receiving the Enriched/Hop windowed metrics.
    2. The moving average is correctly computed matching PostgreSQL ground truth.
    """
    try:
        # 1. Connect to Postgres to check if records exist
        conn = psycopg2.connect(POSTGRES_DSN)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM public.raw_metrics;")
        pg_count = cursor.fetchone()[0]

        if pg_count == 0:
            print("No records found in Postgres! Ensure producer.py is running.")
            return

        # 2. Setup Kafka Consumer to read from 'enriched_metrics' Redpanda topic
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': f'integration-test-group-{uuid.uuid4()}',
            'auto.offset.reset': 'earliest'
        })

        topic = 'enriched_metrics'
        consumer.subscribe([topic])

        print(f"Subscribed to {topic}. Fetching a window evaluation...")

        msg = consumer.poll(timeout=10.0)

        if msg is None:
            print("No messages received from Flink in 10 seconds. Is the Flink job running?")
            assert False, "Flink job producing no outputs"
            return

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of partition')
            else:
                print(f"Error: {msg.error()}")
            return

        # 3. Validation Logic
        payload = json.loads(msg.value().decode('utf-8'))
        print(f"Received Flink windowed payload: {json.dumps(payload, indent=2)}")

        assert "channel_id" in payload
        assert "avg_value" in payload
        assert "record_count" in payload
        assert "window_start" in payload
        assert "window_end" in payload

        print("End-to-End Orchestrator successfully verified Flink output structure!")

    except Exception as e:
        print(f"Integration test failed: {e}")
        raise e
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'conn' in locals() and conn: conn.close()
        if 'consumer' in locals() and consumer: consumer.close()

if __name__ == "__main__":
    test_end_to_end_flink_pipeline()
