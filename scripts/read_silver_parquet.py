from pyspark.sql import SparkSession
from smart_water.utils.logger import get_logger


logger = get_logger(__name__)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("ReadSmartWaterSilver")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():

    spark = create_spark_session()

    input_path = "data/silver/sensor_readings_spark"

    logger.info(
        "Reading Silver Parquet dataset: %s",
        input_path,
    )

    df = spark.read.parquet(input_path)

    logger.info(
        "Loaded %s rows",
        df.count(),
    )

    print("\n=== SCHEMA ===")
    df.printSchema()

    print("\n=== SAMPLE DATA ===")
    df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()