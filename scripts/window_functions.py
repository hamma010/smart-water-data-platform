from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lag,
    avg,
    round as spark_round,
)
from pyspark.sql.window import Window

from smart_water.utils.logger import get_logger


logger = get_logger(__name__)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("SmartWaterWindowFunctions")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():
    spark = create_spark_session()

    input_path = "data/silver/sensor_readings_spark"

    logger.info("Reading Silver Parquet dataset")

    df = spark.read.parquet(input_path)

    logger.info("Loaded %s rows", df.count())

    # ------------------------------------------------------------
    # 1. Define Window
    # ------------------------------------------------------------

    station_window = (
        Window
        .partitionBy("station_id")
        .orderBy("timestamp")
    )

    # ------------------------------------------------------------
    # 2. Previous energy consumption
    # ------------------------------------------------------------

    df = df.withColumn(
        "previous_energy_kwh",
        lag("energy_kwh", 1).over(station_window)
    )

    # ------------------------------------------------------------
    # 3. Energy variation
    # ------------------------------------------------------------

    df = df.withColumn(
        "energy_variation_kwh",
        spark_round(
            col("energy_kwh") - col("previous_energy_kwh"),
            2
        )
    )

    # ------------------------------------------------------------
    # 4. Moving average
    # ------------------------------------------------------------

    moving_window = (
        station_window
        .rowsBetween(-3, 0)
    )

    df = df.withColumn(
        "moving_avg_energy_kwh",
        spark_round(
            avg("energy_kwh").over(moving_window),
            2
        )
    )

    # ------------------------------------------------------------
    # 5. Display results
    # ------------------------------------------------------------

    print("\n=== WINDOW FUNCTIONS ===")

    df.select(
        "station_id",
        "timestamp",
        "energy_kwh",
        "previous_energy_kwh",
        "energy_variation_kwh",
        "moving_avg_energy_kwh",
    ).orderBy(
        "station_id",
        "timestamp"
    ).show(
        30,
        truncate=False
    )

    # ------------------------------------------------------------
    # 6. Save result
    # ------------------------------------------------------------

    output_path = "data/gold/energy_analysis"

    (
        df.select(
            "station_id",
            "timestamp",
            "date",
            "energy_kwh",
            "previous_energy_kwh",
            "energy_variation_kwh",
            "moving_avg_energy_kwh",
            "pump_status",
        )
        .write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(output_path)
    )

    logger.info(
        "Energy analysis saved to: %s",
        output_path
    )

    spark.stop()

    logger.info("Spark session stopped")


if __name__ == "__main__":
    main()