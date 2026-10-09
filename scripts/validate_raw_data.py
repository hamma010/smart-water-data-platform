from pathlib import Path
import pandas as pd

from smart_water.quality.validator import (
    generate_quality_report,
)


raw_dir = Path("data/raw")

latest_file = sorted(
    raw_dir.glob("*.csv")
)[-1]

print(f"Reading: {latest_file}")

df = pd.read_csv(latest_file)

report = generate_quality_report(df)

print()

for key, value in report.items():
    print(key)
    print(value)
    print()