# Data Black Box — Data Ingestion and Bronze Layer

## Overview
This project ingests solar plant CSV data into Kafka and stores valid records in PostgreSQL.

## Components
- **Kafka:** transports messages.
- **Data Generator:** reads CSV files and publishes records to Kafka.
- **Bronze Consumer:** saves valid records and routes invalid messages to a Dead Letter Queue (DLQ).
- **PostgreSQL:** stores raw records and Kafka metadata.

## Kafka Topics
- `solar-plant-1-generation`
- `solar-plant-1-weather`
- `solar-plant-2-generation`
- `solar-plant-2-weather`
- `solar-data-dlq` — invalid messages.

## Requirements
- Docker and Docker Compose.
- Four CSV files inside the `data/` directory.

## Configuration
Create your local environment file:

```bash
cp .env.example .env
```

Edit `.env` and set a strong `DB_PASSWORD`. Never commit `.env` to Git.

For an existing PostgreSQL volume, changing `DB_PASSWORD` in `.env` does not automatically change the database user's existing password.

## Start Services
Start Kafka, PostgreSQL, and the Bronze Consumer:

```bash
docker compose up -d --build kafka postgres bronze-consumer
```

The Data Generator is excluded from normal startup to avoid accidental repeated ingestion.

## Send Test Data
Send two records per CSV file:

```bash
docker compose --profile ingestion run --rm data-generator
```

The default `TEST_LIMIT` is `2`.

## Send All Data
Set the following in `.env`:

```env
TEST_LIMIT=0
```

Then run:

```bash
docker compose --profile ingestion run --rm data-generator
```

Sending all data may take time. Avoid running the generator repeatedly unless another ingestion run is intended.

## Bronze Storage
Valid records are stored in:

`bronze.bronze_records`

The table contains:
- `topic`
- `record_data` as JSONB
- `ingested_at`
- `kafka_partition`
- `kafka_offset`

A unique index on topic, partition, and offset prevents duplicate Kafka messages from being inserted.

## Dead Letter Queue
Invalid UTF-8 or invalid JSON messages are sent to `solar-data-dlq` with their source topic, partition, offset, error details, and raw message.

## Useful Commands

Check service status:

```bash
docker compose ps
```

View consumer logs:

```bash
docker compose logs -f bronze-consumer
```

Open PostgreSQL:

```bash
docker compose exec postgres psql -U shahd -d bronze_db
```

Check Bronze record counts:

```sql
SELECT topic, COUNT(*)
FROM bronze.bronze_records
GROUP BY topic
ORDER BY topic;
```

Read messages from the DLQ:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic solar-data-dlq \
  --from-beginning
```

## Important Notes
- Never use `docker compose down -v` unless deleting persistent database data is explicitly intended.
- `.env` contains local secrets and must not be committed.
- The default database password is for local development only.
- Coordinate schema changes and ingestion runs with the team before merging.
-
