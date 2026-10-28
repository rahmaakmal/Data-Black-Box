import csv
import json
import os
from pathlib import Path
from kafka import KafkaProducer

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
TEST_LIMIT = int(os.getenv("TEST_LIMIT", "2"))  # 0 = all records

CSV_TO_TOPIC = {
    "Plant_1_Generation_Data.csv": "solar-plant-1-generation",
    "Plant_1_Weather_Sensor_Data.csv": "solar-plant-1-weather",
    "Plant_2_Generation_Data.csv": "solar-plant-2-generation",
    "Plant_2_Weather_Sensor_Data.csv": "solar-plant-2-weather",
}


def main():
    producer = KafkaProducer(
        bootstrap_servers=[os.getenv("KAFKA_SERVER", "localhost:29092")],
        value_serializer=lambda data: json.dumps(
            data, ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
        batch_size=32768,
        compression_type="gzip",
    )

    total_records = 0
    try:
        for filename, topic in CSV_TO_TOPIC.items():
            file_path = DATA_DIR / filename
            if not file_path.exists():
                raise FileNotFoundError(f"CSV file not found: {file_path}")

            file_records = 0
            print(f"Reading {filename} -> {topic}")

            with file_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
                reader = csv.DictReader(csv_file)
                if not reader.fieldnames:
                    raise ValueError(f"CSV file has no header: {filename}")

                for row in reader:
                    producer.send(topic, value=row)
                    file_records += 1
                    if TEST_LIMIT > 0 and file_records >= TEST_LIMIT:
                        break

            producer.flush()
            total_records += file_records
            print(f"Records sent: {file_records}")

        print(f"Finished. Total records sent: {total_records}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
