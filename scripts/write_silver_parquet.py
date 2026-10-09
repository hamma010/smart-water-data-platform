from pathlib import Path

import pandas as pd

from smart_water.utils.logger import get_logger


logger = get_logger(__name__)


SILVER_CSV = Path(
    "data/processed/sensor_readings_silver.csv"
)

SILVER_PARQUET_DIR = Path(
    "data/silver/sensor_readings"
)


def write_partitioned_parquet(
    df: pd.DataFrame,
) -> None:

    df = df.copy()

    # Convert timestamp to datetime
    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce",
    )

    # Create partition column
    df["date"] = df["timestamp"].dt.date

    logger.info(
        "Created date partition column"
    )

    # Create output directory
    SILVER_PARQUET_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Write one Parquet file per date
    for date_value, daily_df in df.groupby("date"):

        partition_dir = (
            SILVER_PARQUET_DIR
            / f"date={date_value}"
        )

        partition_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_file = (
            partition_dir
            / "sensor_readings.parquet"
        )

        daily_df.to_parquet(
            output_file,
            index=False,
            engine="pyarrow",
        )

        logger.info(
            "Written partition %s: %s rows",
            date_value,
            len(daily_df),
        )


def main() -> None:

    logger.info(
        "Reading Silver CSV: %s",
        SILVER_CSV,
    )

    df = pd.read_csv(
        SILVER_CSV
    )

    logger.info(
        "Loaded %s rows",
        len(df),
    )

    write_partitioned_parquet(df)

    logger.info(
        "Partitioned Parquet dataset created successfully"
    )


if __name__ == "__main__":
    main()