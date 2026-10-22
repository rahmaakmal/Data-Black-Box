import json
import os
import psycopg2
from kafka import KafkaConsumer

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")

TOPICS = [
    "solar-plant-1-generation",
    "solar-plant-1-weather",
    "solar-plant-2-generation",
    "solar-plant-2-weather",
]

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "bronze_db"),
    "user": os.getenv("DB_USER", "shahd"),
    "password": os.getenv("DB_PASSWORD"),
}


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze_records (
            id BIGSERIAL PRIMARY KEY,
            topic TEXT NOT NULL,
            record_data JSONB NOT NULL,
            ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="bronze-layer-consumer",
    )

    print("Consumer started. Waiting for Kafka messages...")

    try:
        for message in consumer:
            cursor.execute(
                """
                INSERT INTO bronze_records (topic, record_data)
                VALUES (%s, %s)
                """,
                (message.topic, json.dumps(message.value)),
            )

            conn.commit()

            print(
                f"Saved record from {message.topic} "
                f"(partition={message.partition}, "
                f"offset={message.offset})"
            )

            consumer.commit()

    except KeyboardInterrupt:
        print("\nConsumer stopped.")

    finally:
        consumer.close()
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
