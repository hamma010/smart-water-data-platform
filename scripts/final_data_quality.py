from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    when,
)

from smart_water.utils.logger import get_logger
from smart_water.utils.exceptions import DataQualityError


logger = get_logger(__name__)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("SmartWaterFinalDataQuality")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def check_null_values(df):
    """
    Check null values in critical columns.
    """

    critical_columns = [
        "station_id",
        "timestamp",
        "flow_m3_h",
        "pressure_bar",
        "temperature_c",
        "energy_kwh",
        "voltage_v",
        "current_a",
        "pump_status",
    ]

    null_checks = []

    for column_name in critical_columns:

        null_count = df.filter(
            col(column_name).isNull()
        ).count()

        null_checks.append(
            (column_name, null_count)
        )

    print("\n=== NULL CHECK ===")

    for column_name, null_count in null_checks:

        status = "PASS" if null_count == 0 else "FAIL"

        print(
            f"{column_name:20} | "
            f"nulls={null_count:5} | "
            f"{status}"
        )

    return all(
        null_count == 0
        for _, null_count in null_checks
    )


def check_negative_values(df):
    """
    Check negative values in numeric columns.
    """

    checks = {
        "flow_m3_h": col("flow_m3_h") < 0,
        "pressure_bar": col("pressure_bar") < 0,
        "energy_kwh": col("energy_kwh") < 0,
        "current_a": col("current_a") < 0,
    }

    print("\n=== NEGATIVE VALUES CHECK ===")

    all_passed = True

    for column_name, condition in checks.items():

        invalid_count = df.filter(condition).count()

        status = "PASS" if invalid_count == 0 else "FAIL"

        print(
            f"{column_name:20} | "
            f"invalid={invalid_count:5} | "
            f"{status}"
        )

        if invalid_count > 0:
            all_passed = False

    return all_passed


def check_temperature(df):
    """
    Temperature must be between 0 and 100 degrees Celsius.
    """

    invalid_count = df.filter(
        (col("temperature_c") < 0)
        | (col("temperature_c") > 100)
    ).count()

    status = "PASS" if invalid_count == 0 else "FAIL"

    print("\n=== TEMPERATURE CHECK ===")

    print(
        f"temperature_c       | "
        f"invalid={invalid_count:5} | "
        f"{status}"
    )

    return invalid_count == 0


def check_voltage(df):
    """
    Check electrical voltage range.
    """

    invalid_count = df.filter(
        (col("voltage_v") < 300)
        | (col("voltage_v") > 500)
    ).count()

    status = "PASS" if invalid_count == 0 else "FAIL"

    print("\n=== VOLTAGE CHECK ===")

    print(
        f"voltage_v           | "
        f"invalid={invalid_count:5} | "
        f"{status}"
    )

    return invalid_count == 0


def check_pump_status(df):
    """
    Check allowed pump status values.
    """

    invalid_count = df.filter(
        ~col("pump_status").isin(
            "RUNNING",
            "STOPPED"
        )
    ).count()

    status = "PASS" if invalid_count == 0 else "FAIL"

    print("\n=== PUMP STATUS CHECK ===")

    print(
        f"pump_status         | "
        f"invalid={invalid_count:5} | "
        f"{status}"
    )

    return invalid_count == 0


def check_duplicates(df):
    """
    Check duplicate sensor records.
    """

    total_count = df.count()

    distinct_count = df.dropDuplicates(
        [
            "station_id",
            "timestamp",
        ]
    ).count()

    duplicate_count = total_count - distinct_count

    status = "PASS" if duplicate_count == 0 else "FAIL"

    print("\n=== DUPLICATE CHECK ===")

    print(
        f"duplicates          | "
        f"count={duplicate_count:5} | "
        f"{status}"
    )

    return duplicate_count == 0


def check_row_count(df):
    """
    Check that the dataset is not empty.
    """

    row_count = df.count()

    status = "PASS" if row_count > 0 else "FAIL"

    print("\n=== ROW COUNT CHECK ===")

    print(
        f"rows                | "
        f"count={row_count:5} | "
        f"{status}"
    )

    return row_count > 0


def main():

    spark = create_spark_session()

    input_path = "data/gold/energy_analysis"

    logger.info(
        "Reading Gold dataset: %s",
        input_path
    )

    df = spark.read.parquet(input_path)

    logger.info(
        "Loaded %s rows",
        df.count()
    )

    print("\n" + "=" * 70)
    print("       SMART WATER - FINAL DATA QUALITY")
    print("=" * 70)

    # ------------------------------------------------------------
    # Run quality checks
    # ------------------------------------------------------------

    results = []

    results.append(
        check_row_count(df)
    )

    results.append(
        check_null_values(df)
    )

    results.append(
        check_negative_values(df)
    )

    results.append(
        check_temperature(df)
    )

    results.append(
        check_voltage(df)
    )

    results.append(
        check_pump_status(df)
    )

    results.append(
        check_duplicates(df)
    )

    # ------------------------------------------------------------
    # Final result
    # ------------------------------------------------------------

    print("\n" + "=" * 70)
    print("                    FINAL RESULT")
    print("=" * 70)

    if all(results):

        print("STATUS: PASS")
        print("All data quality checks passed.")

        logger.info(
            "Final data quality validation PASSED"
        )

    else:

        print("STATUS: FAIL")
        print("One or more data quality checks failed.")

        logger.error(
            "Final data quality validation FAILED"
        )

        spark.stop()

        raise DataQualityError(
            "Final data quality validation failed."
        )

    print("=" * 70)

    spark.stop()

    logger.info("Spark session stopped")


if __name__ == "__main__":
    main()