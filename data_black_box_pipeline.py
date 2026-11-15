
from datetime import datetime

import logging
import docker
import psycopg2

from psycopg2 import sql
from airflow.sdk import dag, task


# Project PostgreSQL connection
DB_CONFIG = {
    "host": "arwa_shahd_integrated-postgres-1",
    "port": 5432,
    "dbname": "bronze_db",
    "user": "shahd",
    "password": "local_review_password",
}

# Existing Spark container
SPARK_CONTAINER = "arwa_shahd_integrated-spark-etl-1"

# Expected current table counts
EXPECTED_TABLES = {
    "silver.generation_clean": 136476,
    "silver.weather_clean": 6441,
    "silver.rejected_records": 0,
    "gold.daily_generation": 68,
    "gold.daily_weather": 68,
    "gold.daily_plant_performance": 68,
}


@dag(
    dag_id="data_black_box_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data-black-box", "integration", "spark"],
)
def data_black_box_pipeline():

    @task
    def check_bronze():
        conn = psycopg2.connect(**DB_CONFIG)

        try:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'bronze'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)

            tables = cursor.fetchall()
            total_records = 0

            for (table_name,) in tables:
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier("bronze"),
                        sql.Identifier(table_name),
                    )
                )

                count = cursor.fetchone()[0]
                total_records += count

                logging.info(
                    "Bronze table %s: %s records",
                    table_name,
                    count,
                )

            logging.info("Total Bronze records: %s", total_records)

            if total_records != 142917:
                raise ValueError(
                    f"Expected 142917 Bronze records, "
                    f"found {total_records}"
                )

            return total_records

        finally:
            conn.close()

    @task
    def run_spark_dry_run():
        client = docker.from_env()

        try:
            container = client.containers.get(SPARK_CONTAINER)

            if container.status != "running":
                container.reload()

            if container.status != "running":
                raise RuntimeError(
                    f"Spark container is not running: {SPARK_CONTAINER}"
                )

            command = [
                "/opt/spark/bin/spark-submit",
                "/app/silver_gold_etl_solar.py",
                "--report-path",
                "/app/reports/airflow_dry_run_report.json",
            ]

            logging.info("Starting Spark ETL in DRY-RUN mode")
            logging.info("Command: %s", " ".join(command))

            result = container.exec_run(
                cmd=command,
                stdout=True,
                stderr=True,
                demux=False,
            )

            output = result.output.decode(
                "utf-8",
                errors="replace",
            )

            logging.info("Spark output:\n%s", output)

            if result.exit_code != 0:
                raise RuntimeError(
                    f"Spark ETL failed with exit code "
                    f"{result.exit_code}"
                )

            logging.info("Spark DRY-RUN completed successfully")

            return result.exit_code

        finally:
            client.close()

    @task
    def validate_outputs(bronze_count, spark_exit_code):
        if spark_exit_code != 0:
            raise ValueError("Spark dry-run did not complete successfully")

        conn = psycopg2.connect(**DB_CONFIG)

        try:
            cursor = conn.cursor()

            for full_name, expected_count in EXPECTED_TABLES.items():
                schema, table = full_name.split(".")

                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                    )
                )

                actual_count = cursor.fetchone()[0]

                logging.info(
                    "%s: %s records (expected %s)",
                    full_name,
                    actual_count,
                    expected_count,
                )

                if actual_count != expected_count:
                    raise ValueError(
                        f"{full_name}: expected {expected_count}, "
                        f"found {actual_count}"
                    )

            logging.info(
                "PIPELINE VALIDATION PASSED. Bronze records: %s",
                bronze_count,
            )

        finally:
            conn.close()

    bronze_count = check_bronze()

    spark_result = run_spark_dry_run()

    validate_outputs(bronze_count, spark_result)


data_black_box_pipeline()

