from pathlib import Path

import pandas as pd

from smart_water.utils.logger import get_logger


logger = get_logger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[3]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def save_raw_data(
    df: pd.DataFrame,
    filename: str,
) -> Path:
    """
    Save raw sensor data as a CSV file.

    Args:
        df: Raw sensor data.
        filename: Name of the output file.

    Returns:
        Path of the created file.
    """

    RAW_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = RAW_DATA_DIR / filename

    df.to_csv(
        output_path,
        index=False,
    )

    logger.info(
        "Raw data saved: %s rows -> %s",
        len(df),
        output_path,
    )

    return output_path