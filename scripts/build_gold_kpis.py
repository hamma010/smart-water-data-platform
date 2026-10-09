
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    sum as spark_sum,
    count,
    round as spark_round,
    max as spark_max,
)
from smart_water.utils.logger import get_logger

logger = get_logger(__name__)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("SmartWaterGoldKPIs")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():
    spark = create_spark_session()

    input_path = "data/silver/sensor_readings_spark"
    output_path = "data/gold/station_kpis"

    logger.info("Reading Silver Parquet dataset")

    # 1. Lire les données Silver
    df = spark.read.parquet(input_path)

    logger.info("Silver rows: %s", df.count())

    # 2. Filtrer les pompes en fonctionnement
    running_df = df.filter(
        col("pump_status") == "RUNNING"
    )

    logger.info(
        "Running measurements: %s",
        running_df.count()
    )

    # 3. Calculer les indicateurs par station
    gold_df = (
        running_df
        .groupBy("station_id")
        .agg(
            count("*").alias("measurement_count"),
            spark_round(
                avg("flow_m3_h"), 2
            ).alias("avg_flow_m3_h"),
            spark_round(
                avg("pressure_bar"), 2
            ).alias("avg_pressure_bar"),
            spark_round(
                spark_sum("energy_kwh"), 2
            ).alias("total_energy_kwh"),
            spark_round(
                avg("temperature_c"), 2
            ).alias("avg_temperature_c"),
            spark_round(
                spark_max("flow_m3_h"), 2
            ).alias("max_flow_m3_h"),
        )
    )

    logger.info("Gold KPI aggregation completed")

    # 4. Afficher les résultats
    print("\n=== GOLD STATION KPIs ===")

    gold_df.orderBy(
        col("station_id")
    ).show(truncate=False)

    # 5. Enregistrer la couche Gold
    (
        gold_df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    logger.info(
        "Gold dataset saved to: %s",
        output_path
    )

    spark.stop()

    logger.info("Spark session stopped")


if __name__ == "__main__":
    main()