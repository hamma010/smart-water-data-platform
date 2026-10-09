import pandas as pd

from smart_water.utils.logger import get_logger


logger = get_logger(__name__)

def check_missing_values(
    df: pd.DataFrame,
) -> dict:

    missing = (
        df.isnull()
        .sum()
        .to_dict()
    )

    logger.info(
        "Missing values checked"
    )

    return missing
def check_duplicates(
    df: pd.DataFrame,
) -> int:

    duplicates = df.duplicated().sum()

    logger.info(
        "Found %s duplicates",
        duplicates,
    )

    return duplicates
def check_negative_flow(
    df: pd.DataFrame,
) -> int:

    invalid = (
        df["flow_m3_h"] < 0
    ).sum()

    return int(invalid)
def check_negative_pressure(
    df: pd.DataFrame,
) -> int:

    invalid = (
        df["pressure_bar"] < 0
    ).sum()

    return int(invalid)
def check_extreme_temperature(
    df: pd.DataFrame,
) -> int:

    invalid = (
        df["temperature_c"] > 100
    ).sum()

    return int(invalid)
def generate_quality_report(
    df: pd.DataFrame,
) -> dict:

    report = {
        "total_rows": len(df),

        "missing_values":
            check_missing_values(df),

        "duplicates":
            check_duplicates(df),

        "negative_flow":
            check_negative_flow(df),

        "negative_pressure":
            check_negative_pressure(df),

        "extreme_temperature":
            check_extreme_temperature(df),
    }

    logger.info(
        "Quality report generated"
    )

    return report
