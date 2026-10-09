from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from smart_water.config.settings import settings
from smart_water.utils.logger import get_logger


logger = get_logger(__name__)

INPUT_PATH = Path("data/gold/energy_analysis")
BATCH_SIZE = 500


def get_postgres_connection():
    """Create a PostgreSQL connection."""
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )


def create_table(connection):
    """Create the target PostgreSQL table if it does not exist."""

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS station_energy_analysis (
            station_id VARCHAR(20) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            date DATE NOT NULL,
            energy_kwh DOUBLE PRECISION,
            previous_energy_kwh DOUBLE PRECISION,
            energy_variation_kwh DOUBLE PRECISION,
            moving_avg_energy_kwh DOUBLE PRECISION,
            pump_status VARCHAR(20)
        );
    """

    with connection.cursor() as cursor:
        cursor.execute(create_table_sql)

    connection.commit()

    logger.info("PostgreSQL table is ready")


def load_data(connection, df):
    """Load Gold data into PostgreSQL in batches."""

    insert_sql = """
        INSERT INTO station_energy_analysis (
            station_id,
            timestamp,
            date,
            energy_kwh,
            previous_energy_kwh,
            energy_variation_kwh,
            moving_avg_energy_kwh,
            pump_status
        )
        VALUES %s
    """

    columns = [
        "station_id",
        "timestamp",
        "date",
        "energy_kwh",
        "previous_energy_kwh",
        "energy_variation_kwh",
        "moving_avg_energy_kwh",
        "pump_status",
    ]

    batch = []
    total_inserted = 0

    for row in df[columns].itertuples(index=False, name=None):

        batch.append(row)

        if len(batch) >= BATCH_SIZE:

            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    insert_sql,
                    batch,
                    page_size=BATCH_SIZE,
                )

            total_inserted += len(batch)

            logger.info(
                "Inserted %s rows",
                total_inserted,
            )

            batch.clear()

    if batch:

        with connection.cursor() as cursor:
            execute_values(
                cursor,
                insert_sql,
                batch,
                page_size=BATCH_SIZE,
            )

        total_inserted += len(batch)

    connection.commit()

    logger.info(
        "Successfully inserted %s rows into PostgreSQL",
        total_inserted,
    )

    return total_inserted


def main():

    connection = None

    try:

        logger.info(
            "Reading Gold dataset: %s",
            INPUT_PATH,
        )

        if not INPUT_PATH.exists():
            raise FileNotFoundError(
                f"Gold dataset not found: {INPUT_PATH}"
            )

        # Read partitioned Parquet directly with pandas/pyarrow
        df = pd.read_parquet(INPUT_PATH)

        expected_count = len(df)

        logger.info(
            "Gold dataset contains %s rows",
            expected_count,
        )

        if expected_count == 0:
            raise ValueError(
                "Gold dataset is empty"
            )

        logger.info(
            "Connecting to PostgreSQL"
        )

        connection = get_postgres_connection()

        logger.info(
            "PostgreSQL connection successful"
        )

        create_table(connection)

        # Make the load idempotent
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE station_energy_analysis;"
            )

        inserted_count = load_data(
            connection,
            df,
        )

        if inserted_count != expected_count:
            raise RuntimeError(
                f"Row count mismatch: "
                f"expected {expected_count}, "
                f"inserted {inserted_count}"
            )

        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM station_energy_analysis;
                """
            )

            postgres_count = cursor.fetchone()[0]

        if postgres_count != expected_count:

            raise RuntimeError(
                f"PostgreSQL verification failed: "
                f"expected {expected_count}, "
                f"found {postgres_count}"
            )

        logger.info(
            "Load completed successfully: %s rows verified",
            postgres_count,
        )

        print()
        print("=== POSTGRESQL LOAD SUMMARY ===")
        print(f"Gold rows:       {expected_count}")
        print(f"Inserted rows:   {inserted_count}")
        print(f"PostgreSQL rows: {postgres_count}")
        print("Status: SUCCESS")

    except Exception:

        if connection is not None:
            connection.rollback()

        logger.exception(
            "Gold to PostgreSQL load failed"
        )

        raise

    finally:

        if connection is not None:
            connection.close()

        logger.info(
            "PostgreSQL connection closed"
        )


if __name__ == "__main__":
    main()