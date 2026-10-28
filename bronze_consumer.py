import json
import os
import time
from kafka import KafkaConsumer, KafkaProducer
import psycopg2
from psycopg2.extras import Json

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:29092")
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5433")),
    "dbname": os.getenv("DB_NAME", "bronze_db"),
    "user": os.getenv("DB_USER", "shahd"),
    "password": os.getenv("DB_PASSWORD", "local_review_password"),
}
DB_SCHEMA = os.getenv("DB_SCHEMA", "bronze")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "solar-data-dlq")

TOPICS = [
    "solar-plant-1-generation",
    "solar-plant-1-weather",
    "solar-plant-2-generation",
    "solar-plant-2-weather",
]


def connect_db():
    for attempt in range(10):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError:
            if attempt == 9:
                raise
            time.sleep(2)


def setup_table(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")

        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = 'bronze_records'
            )
        """)
        old_table_exists = cur.fetchone()[0]

        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'bronze'
                  AND table_name = 'bronze_records'
            )
        """)
        new_table_exists = cur.fetchone()[0]

        if old_table_exists and not new_table_exists:
            cur.execute(
                "ALTER TABLE public.bronze_records SET SCHEMA bronze"
            )

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze.bronze_records (
                id BIGSERIAL PRIMARY KEY,
                topic TEXT NOT NULL,
                record_data JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                kafka_partition INTEGER,
                kafka_offset BIGINT
            )
        """)

        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
            bronze_records_kafka_message_unique
            ON bronze.bronze_records
                (topic, kafka_partition, kafka_offset)
            WHERE kafka_partition IS NOT NULL
              AND kafka_offset IS NOT NULL
        """)
    conn.commit()


def main():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_SERVER,
        group_id="bronze-layer-consumer-v4",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        acks="all",
        retries=5,
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False
        ).encode("utf-8"),
    )

    conn = connect_db()
    setup_table(conn)
    print("Bronze consumer started.")

    try:
        for message in consumer:
            try:
                raw_text = message.value.decode("utf-8")
                record = json.loads(raw_text)

                if not isinstance(record, dict):
                    raise ValueError("JSON record must be an object")

            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                producer.send(DLQ_TOPIC, {
                    "source_topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "error": str(exc),
                    "raw_value": message.value.decode("utf-8", errors="replace"),
                }).get(timeout=30)
                consumer.commit()
                print(f"Invalid message sent to DLQ: {message.topic}:{message.offset}")
                continue

            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO bronze.bronze_records
                            (topic, record_data, kafka_partition, kafka_offset)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (topic, kafka_partition, kafka_offset)
                        WHERE kafka_partition IS NOT NULL
                          AND kafka_offset IS NOT NULL
                        DO NOTHING
                    """, (
                        message.topic,
                        Json(record),
                        message.partition,
                        message.offset,
                    ))
                conn.commit()
                consumer.commit()
            except Exception:
                conn.rollback()
                raise

            print(f"Processed {message.topic}, offset={message.offset}")

    finally:
        consumer.close()
        producer.close()
        conn.close()


if __name__ == "__main__":
    main()
