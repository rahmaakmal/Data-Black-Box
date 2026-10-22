import csv
import json
import os
from pathlib import Path
from kafka import KafkaProducer

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

# Test mode: send only 2 records from each file
TEST_LIMIT = 2

CSV_TO_TOPIC = {
    "Plant_1_Generation_Data.csv": "solar-plant-1-generation",
    "Plant_1_Weather_Sensor_Data.csv": "solar-plant-1-weather",
    "Plant_2_Generation_Data.csv": "solar-plant-2-generation",
    "Plant_2_Weather_Sensor_Data.csv": "solar-plant-2-weather",
}


def main():
    producer = KafkaProducer(
        bootstrap_servers=[os.getenv("KAFKA_SERVER", "localhost:9092")],
        value_serializer=lambda data: json.dumps(
            data, ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=5,
    )

    total_records = 0

    try:
        for filename, topic in CSV_TO_TOPIC.items():
            file_path = DATA_DIR / filename

            if not file_path.exists():
                raise FileNotFoundError(
                    f"CSV file not found: {file_path}"
                )

            print(f"\nReading: {filename}")
            print(f"Sending to topic: {topic}")

            file_records = 0

            with file_path.open(
                "r", encoding="utf-8-sig", newline=""
            ) as csv_file:
                reader = csv.DictReader(csv_file)

                if not reader.fieldnames:
                    raise ValueError(
                        f"CSV file has no header: {filename}"
                    )

                for row in reader:
                    future = producer.send(topic, value=row)
                    future.get(timeout=30)

                    file_records += 1

                    if file_records >= TEST_LIMIT:
                        break

            producer.flush()
            total_records += file_records

            print(f"Records sent: {file_records}")

        print(f"\nFinished! Total records sent: {total_records}")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
