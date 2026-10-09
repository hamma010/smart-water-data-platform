import random

import pandas as pd

from smart_water.utils.logger import get_logger


logger = get_logger(__name__)


def inject_missing_values(
    df: pd.DataFrame,
    percentage: float = 0.01,
) -> pd.DataFrame:

    df = df.copy()

    columns = [
        "flow_m3_h",
        "pressure_bar",
        "temperature_c",
        "energy_kwh",
        "voltage_v",
        "current_a",
    ]

    total_values = len(df) * len(columns)
    number_of_errors = int(total_values * percentage)

    for _ in range(number_of_errors):
        row_index = random.randrange(len(df))
        column = random.choice(columns)

        df.loc[row_index, column] = None

    logger.info(
        "Injected %s missing values",
        number_of_errors,
    )

    return df
def inject_invalid_values(
    df: pd.DataFrame,
    percentage: float = 0.005,
) -> pd.DataFrame:

    df = df.copy()

    number_of_errors = int(len(df) * percentage)

    for _ in range(number_of_errors):

        row_index = random.randrange(len(df))

        error_type = random.choice(
            [
                "negative_flow",
                "negative_pressure",
                "extreme_temperature",
                "extreme_pressure",
            ]
        )

        if error_type == "negative_flow":
            df.loc[row_index, "flow_m3_h"] = -random.uniform(1, 100)

        elif error_type == "negative_pressure":
            df.loc[row_index, "pressure_bar"] = -random.uniform(1, 5)

        elif error_type == "extreme_temperature":
            df.loc[row_index, "temperature_c"] = random.uniform(100, 150)

        elif error_type == "extreme_pressure":
            df.loc[row_index, "pressure_bar"] = random.uniform(10, 20)

    logger.info(
        "Injected %s invalid sensor values",
        number_of_errors,
    )

    return df
def inject_duplicates(
    df: pd.DataFrame,
    percentage: float = 0.005,
) -> pd.DataFrame:

    number_of_duplicates = int(len(df) * percentage)

    if number_of_duplicates == 0:
        return df

    duplicated_rows = df.sample(
        n=number_of_duplicates,
        random_state=42,
    )

    df = pd.concat(
        [df, duplicated_rows],
        ignore_index=True,
    )

    logger.info(
        "Injected %s duplicate rows",
        number_of_duplicates,
    )

    return df
def inject_data_quality_issues(
    df: pd.DataFrame,
) -> pd.DataFrame:

    logger.info(
        "Starting data quality issue injection"
    )

    df = inject_missing_values(df)
    df = inject_invalid_values(df)
    df = inject_duplicates(df)

    logger.info(
        "Data quality issue injection completed"
    )

    return df