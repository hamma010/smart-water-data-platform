from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

from smart_water.utils.logger import get_logger


logger = get_logger(__name__)


def create_spark_session() -> SparkSession:
    """
    Create and configure the Spark session.
    """
    spark = (
        SparkSession.builder
        .appName("SmartWaterStationJoin")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():
    spark = create_spark_session()

    logger.info("Spark session created")

    # ============================================================
    # 1. Paths
    # ============================================================

    sensor_path = "data/silver/sensor_readings_spark"
    stations_path = "data/reference/stations.csv"

    # ============================================================
    # 2. Read sensor data
    # ============================================================

    logger.info("Reading sensor data")

    sensors_df = spark.read.parquet(sensor_path)

    logger.info("Sensor rows: %s", sensors_df.count())

    # ============================================================
    # 3. Read station reference data
    # ============================================================

    logger.info("Reading station reference data")

    stations_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(stations_path)
    )

    logger.info("Station reference rows: %s", stations_df.count())

    # ============================================================
    # 4. Display source data
    # ============================================================

    print("\n=== SENSOR DATA ===")

    sensors_df.show(
        5,
        truncate=False
    )

    print("\n=== STATIONS ===")

    stations_df.show(
        truncate=False
    )

    # ============================================================
    # 5. Create an incomplete station reference table
    # ============================================================
    #
    # We remove ST005 intentionally.
    # This allows us to understand the difference between
    # INNER JOIN and LEFT JOIN.
    #

    stations_incomplete_df = stations_df.filter(
        col("station_id") != "ST005"
    )

    print("\n=== INCOMPLETE STATIONS ===")

    stations_incomplete_df.show(
        truncate=False
    )

    # ============================================================
    # 6. INNER JOIN
    # ============================================================
    #
    # Only matching station_id values are kept.
    #

    inner_df = sensors_df.join(
        stations_incomplete_df,
        on="station_id",
        how="inner"
    )

    logger.info(
        "INNER JOIN rows: %s",
        inner_df.count()
    )

    print("\n=== INNER JOIN ===")

    inner_df.select(
        "station_id",
        "station_name",
        "city",
        "region",
        "flow_m3_h",
        "energy_kwh",
        "pump_status"
    ).show(
        10,
        truncate=False
    )

    # ============================================================
    # 7. LEFT JOIN
    # ============================================================
    #
    # All sensor records are kept.
    # If a station does not exist in the reference table,
    # the station information becomes NULL.
    #

    left_df = sensors_df.join(
        stations_incomplete_df,
        on="station_id",
        how="left"
    )

    logger.info(
        "LEFT JOIN rows: %s",
        left_df.count()
    )

    print("\n=== LEFT JOIN ===")

    left_df.select(
        "station_id",
        "station_name",
        "city",
        "region",
        "flow_m3_h",
        "energy_kwh",
        "pump_status"
    ).show(
        10,
        truncate=False
    )

    # ============================================================
    # 8. BROADCAST JOIN
    # ============================================================
    #
    # The stations table is very small.
    # We can broadcast it to Spark executors.
    #

    broadcast_df = sensors_df.join(
        broadcast(stations_df),
        on="station_id",
        how="left"
    )

    logger.info(
        "BROADCAST JOIN rows: %s",
        broadcast_df.count()
    )

    print("\n=== BROADCAST JOIN ===")

    broadcast_df.select(
        "station_id",
        "station_name",
        "city",
        "region",
        "flow_m3_h",
        "energy_kwh",
        "pump_status"
    ).show(
        10,
        truncate=False
    )

    # ============================================================
    # 9. Compare JOIN results
    # ============================================================

    print("\n=== JOIN COUNTS ===")

    print(
        f"INNER JOIN:      {inner_df.count()} rows"
    )

    print(
        f"LEFT JOIN:       {left_df.count()} rows"
    )

    print(
        f"BROADCAST JOIN:  {broadcast_df.count()} rows"
    )

    # ============================================================
    # 10. Explain Broadcast Join execution plan
    # ============================================================

    print("\n=== BROADCAST JOIN EXECUTION PLAN ===")

    broadcast_df.explain(
        mode="formatted"
    )

    # ============================================================
    # 11. Finish
    # ============================================================

    logger.info("Station enrichment completed")

    spark.stop()

    logger.info("Spark session stopped")


if __name__ == "__main__":
    main()