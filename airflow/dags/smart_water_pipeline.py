from datetime import datetime
from pathlib import Path
import subprocess
import sys

from airflow.sdk import DAG, task

PROJECT_DIR = Path("/opt/airflow/project")
GOLD_DIR = PROJECT_DIR / "data" / "gold" / "energy_analysis"
DBT_DIR = PROJECT_DIR / "dbt"
DBT_PROFILES_DIR = (
    PROJECT_DIR / "airflow" / "dbt_profiles"
)


with DAG(
    dag_id="smart_water_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["smart-water", "data-engineering"],
) as dag:

    @task
    def validate_inputs():
        if not GOLD_DIR.exists():
            raise FileNotFoundError(
                f"Gold data directory not found: {GOLD_DIR}"
            )

        if not (DBT_DIR / "dbt_project.yml").exists():
            raise FileNotFoundError(
                f"dbt project not found: {DBT_DIR}"
            )

        if not (
            DBT_PROFILES_DIR / "profiles.yml"
        ).exists():
            raise FileNotFoundError(
                "dbt profiles.yml not found"
            )

        print("Gold data and dbt configuration verified.")

    @task
    def load_postgres():
        script = PROJECT_DIR / "scripts" / "load_gold_to_postgres.py"

        if not script.exists():
            raise FileNotFoundError(
                f"Loader script not found: {script}"
            )

        subprocess.run(
            [sys.executable, str(script)],
            cwd=str(PROJECT_DIR),
            check=True,
        )

    @task
    def run_dbt():
        subprocess.run(
            [
                "dbt",
                "run",
                "--project-dir", str(DBT_DIR),
                "--profiles-dir", str(DBT_PROFILES_DIR),
            ],
            cwd=str(PROJECT_DIR),
            check=True,
        )

    @task
    def test_dbt():
        subprocess.run(
            [
                "dbt",
                "test",
                "--project-dir", str(DBT_DIR),
                "--profiles-dir", str(DBT_PROFILES_DIR),
            ],
            cwd=str(PROJECT_DIR),
            check=True,
        )

    validation = validate_inputs()
    loading = load_postgres()
    transformation = run_dbt()
    testing = test_dbt()

    validation >> loading >> transformation >> testing
