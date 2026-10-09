from pathlib import Path

import pandas as pd

from smart_water.processing.cleaner import (
    clean_sensor_data,
)


raw_dir = Path("data/raw")
silver_dir = Path("data/processed")

silver_dir.mkdir(
    parents=True,
    exist_ok=True,
)

latest_file = sorted(
    raw_dir.glob("*.csv")
)[-1]

print(f"Reading RAW file: {latest_file}")

df = pd.read_csv(latest_file)

print(f"RAW rows: {len(df)}")

clean_df = clean_sensor_data(df)

print(f"SILVER rows: {len(clean_df)}")

output_file = (
    silver_dir / "sensor_readings_silver.csv"
)

clean_df.to_csv(
    output_file,
    index=False,
)

print(
    f"Silver data saved to: {output_file}"
)