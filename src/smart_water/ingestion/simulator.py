from smart_water.ingestion.data_quality import (
    inject_data_quality_issues,
)
from smart_water.utils.logger import get_logger
from datetime import datetime, timedelta
from smart_water.ingestion.storage import save_raw_data
import random

import pandas as pd
logger = get_logger(__name__)


STATIONS = {
    "ST001": {
        "name": "Station Tunis Nord",
        "city": "Tunis",
        "pump_count": 3,
    },
    "ST002": {
        "name": "Station Ariana",
        "city": "Ariana",
        "pump_count": 2,
    },
    "ST003": {
        "name": "Station Ben Arous",
        "city": "Ben Arous",
        "pump_count": 3,
    },
    "ST004": {
        "name": "Station Sousse",
        "city": "Sousse",
        "pump_count": 4,
    },
    "ST005": {
        "name": "Station Sfax",
        "city": "Sfax",
        "pump_count": 3,
    },
}


def generate_sensor_reading(
    station_id: str,
    timestamp: datetime,
) -> dict:

    pump_status = random.choices(
        ["RUNNING", "STOPPED"],
        weights=[0.85, 0.15],
    )[0]

    if pump_status == "RUNNING":
        flow = random.uniform(80, 180)
        pressure = random.uniform(3.5, 6.5)
        temperature = random.uniform(35, 65)
        current = random.uniform(30, 70)
        energy = random.uniform(20, 50)
        voltage = random.uniform(395, 415)

    else:
        flow = 0.0
        pressure = random.uniform(0, 0.3)
        temperature = random.uniform(25, 40)
        current = random.uniform(0, 2)
        energy = random.uniform(0, 2)
        voltage = random.uniform(395, 415)

    return {
        "station_id": station_id,
        "timestamp": timestamp,
        "flow_m3_h": round(flow, 2),
        "pressure_bar": round(pressure, 2),
        "temperature_c": round(temperature, 2),
        "energy_kwh": round(energy, 2),
        "voltage_v": round(voltage, 2),
        "current_a": round(current, 2),
        "pump_status": pump_status,
    }


def generate_sensor_data(
    days: int = 7,
    interval_minutes: int = 15,
) -> pd.DataFrame:

    start_time = datetime.now() - timedelta(days=days)

    records = []

    current_time = start_time

    while current_time <= datetime.now():

        for station_id in STATIONS:
            reading = generate_sensor_reading(
                station_id=station_id,
                timestamp=current_time,
            )

            records.append(reading)

        current_time += timedelta(minutes=interval_minutes)

    return pd.DataFrame(records)


if __name__ == "__main__":
    df = generate_sensor_data()

    logger.info(
        "Generated clean dataset with %s rows",
        len(df),
    )

    df = inject_data_quality_issues(df)

    filename = (
        f"sensor_readings_"
        f"{datetime.now():%Y%m%d_%H%M%S}.csv"
    )

    output_path = save_raw_data(
        df=df,
        filename=filename,
    )

    print()
    print(f"Final rows: {len(df)}")
    print(f"Saved to: {output_path}")