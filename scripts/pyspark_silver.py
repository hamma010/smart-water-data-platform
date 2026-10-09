from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date

from smart_water.utils.logger import get_logger


logger = get_logger(__name__)


def create_spark_session() -> SparkSession:

    spark = (
        SparkSession.builder
        .appName("SmartWaterSilverPipeline")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():

    spark = create_spark_session()

    logger.info(
        "Spark session created"
    )

    input_file = (
        "data/processed/"
        "sensor_readings_silver.csv"
    )

    output_path = (
        "data/silver/"
        "sensor_readings_spark"
    )

    logger.info(
        "Reading Silver CSV: %s",
        input_file,
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_file)
    )

    logger.info(
        "Loaded %s rows",
        df.count(),
    )

    print("Schema:")
    df.printSchema()

    print("Sample data:")
    df.show(5, truncate=False)

    # Convert timestamp
    df = df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp")),
    )

    # Create date partition column
    df = df.withColumn(
        "date",
        to_date(col("timestamp")),
    )

    print("Updated schema:")
    df.printSchema()

    print("Data with partition column:")
    df.show(5, truncate=False)

    logger.info(
        "Writing partitioned Parquet dataset"
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(output_path)
    )

    logger.info(
        "Partitioned Parquet dataset created: %s",
        output_path,
    )

    spark.stop()

    logger.info(
        "Spark session stopped"
    )


if __name__ == "__main__":
    main()