import pandas as pd

from smart_water.utils.logger import get_logger


logger = get_logger(__name__)

def remove_duplicates(
    df: pd.DataFrame,
) -> pd.DataFrame:

    before = len(df)

    df = df.drop_duplicates().copy()

    removed = before - len(df)

    logger.info(
        "Removed %s duplicate rows",
        removed,
    )

    return df
def remove_duplicates(
    df: pd.DataFrame,
) -> pd.DataFrame:

    before = len(df)

    df = df.drop_duplicates().copy()

    removed = before - len(df)

    logger.info(
        "Removed %s duplicate rows",
        removed,
    )

    return df
def convert_timestamp(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df = df.copy()

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce",
    )

    invalid_timestamps = df["timestamp"].isna().sum()

    logger.info(
        "Invalid timestamps after conversion: %s",
        invalid_timestamps,
    )

    return df
def remove_invalid_timestamps(
    df: pd.DataFrame,
) -> pd.DataFrame:

    before = len(df)

    df = df.dropna(
        subset=["timestamp"]
    ).copy()

    removed = before - len(df)

    logger.info(
        "Removed %s rows with invalid timestamps",
        removed,
    )

    return df
def remove_invalid_flow(
    df: pd.DataFrame,
) -> pd.DataFrame:

    before = len(df)

    df = df[
        df["flow_m3_h"].isna()
        | (df["flow_m3_h"] >= 0)
    ].copy()

    removed = before - len(df)

    logger.info(
        "Removed %s rows with invalid flow",
        removed,
    )

    return df
def remove_invalid_pressure(
    df: pd.DataFrame,
) -> pd.DataFrame:

    before = len(df)

    df = df[
        df["pressure_bar"].isna()
        | (df["pressure_bar"] >= 0)
    ].copy()

    removed = before - len(df)

    logger.info(
        "Removed %s rows with invalid pressure",
        removed,
    )

    return df
def remove_invalid_temperature(
    df: pd.DataFrame,
) -> pd.DataFrame:

    before = len(df)

    df = df[
        df["temperature_c"].isna()
        | (
            (df["temperature_c"] >= 0)
            & (df["temperature_c"] <= 100)
        )
    ].copy()

    removed = before - len(df)

    logger.info(
        "Removed %s rows with invalid temperature",
        removed,
    )

    return df
def remove_missing_critical_values(
    df: pd.DataFrame,
) -> pd.DataFrame:

    before = len(df)

    critical_columns = [
        "station_id",
        "timestamp",
        "pump_status",
    ]

    df = df.dropna(
        subset=critical_columns
    ).copy()

    removed = before - len(df)

    logger.info(
        "Removed %s rows with missing critical values",
        removed,
    )

    return df
def clean_sensor_data(
    df: pd.DataFrame,
) -> pd.DataFrame:

    logger.info(
        "Starting sensor data cleaning"
    )

    df = remove_duplicates(df)

    df = convert_timestamp(df)

    df = remove_invalid_timestamps(df)

    df = remove_invalid_flow(df)

    df = remove_invalid_pressure(df)

    df = remove_invalid_temperature(df)

    df = remove_missing_critical_values(df)

    logger.info(
        "Sensor data cleaning completed: %s rows",
        len(df),
    )

    return df
