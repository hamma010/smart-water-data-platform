# Smart Water Data Platform

An end-to-end data engineering project for processing, validating, and analyzing smart water pumping station data.

## Overview

Water utilities collect operational data from pumping stations, including water flow, pressure, temperature, energy consumption, and pump status. Transforming this raw data into reliable, analytics-ready datasets can help monitor operations and identify opportunities to improve efficiency.

**Smart Water Data Platform** is a data engineering portfolio project that simulates this use case by building a data pipeline to ingest, clean, transform, and analyze sensor readings.

The project demonstrates core data engineering skills using Python, PySpark, PostgreSQL, dbt, and Apache Airflow.

## Business Objectives

* Build a structured pipeline for smart water sensor data.
* Improve data quality through validation and cleaning.
* Transform raw readings into analytics-ready datasets.
* Analyze energy consumption by pumping station.
* Store processed data in PostgreSQL for downstream analytics.
* Prepare an orchestrated workflow using Apache Airflow.

## Architecture

```text
Synthetic Sensor Data
        |
        v
Python Data Generation
        |
        v
Data Validation & Cleaning
        |
        v
PySpark Transformations
        |
        v
Silver / Gold Data Layers
        |
        v
PostgreSQL
        |
        v
dbt Transformations & Tests
        |
        v
Analytics-Ready Data

Apache Airflow: Workflow Orchestration
```

## Technology Stack

| Technology     | Purpose                                           |
| -------------- | ------------------------------------------------- |
| Python         | Data generation, validation, and pipeline scripts |
| Pandas         | Data manipulation and cleaning                    |
| PySpark        | Distributed data processing and transformations   |
| Parquet        | Storage of processed datasets                     |
| PostgreSQL     | Relational storage for analytical data            |
| dbt            | SQL-based transformations and data testing        |
| Apache Airflow | Workflow orchestration                            |
| Docker         | Containerized development environment             |
| Git & GitHub   | Version control and project documentation         |

## Data Pipeline

### 1. Data Generation

Synthetic sensor readings are generated to simulate data collected from water pumping stations.

The dataset includes:

* `station_id`
* `timestamp`
* `flow_m3_h`
* `pressure_bar`
* `temperature_c`
* `energy_kwh`
* `voltage_v`
* `current_a`
* `pump_status`

Synthetic data makes the project reproducible without requiring access to confidential operational data.

### 2. Data Quality and Cleaning

The pipeline validates and cleans the generated dataset by identifying invalid measurements and handling data quality issues.

Examples include:

* Invalid flow measurements
* Invalid pressure values
* Invalid temperature values
* Timestamp validation
* Missing critical values

The cleaning process produces a more reliable dataset for downstream transformations.

### 3. PySpark Processing

PySpark is used to transform the cleaned data and prepare analytical datasets.

The project follows a layered approach:

* **Bronze:** Raw sensor data.
* **Silver:** Validated and cleaned sensor readings.
* **Gold:** Aggregated data designed for analytical use.

The Gold layer includes station-level energy analysis.

### 4. PostgreSQL Integration

Processed Gold data is loaded into PostgreSQL to make analytical results available through SQL.

The project includes a loading script:

`scripts/load_gold_to_postgres.py`

The target analytical table is:

`station_energy_analysis`

### 5. dbt Transformations and Testing

The dbt project organizes SQL transformations and data tests, helping make analytical models easier to maintain and validate.

### 6. Apache Airflow Orchestration

An Airflow DAG is defined to organize the workflow:

1. Validate input data and configuration.
2. Load Gold data into PostgreSQL.
3. Execute dbt transformations.
4. Run dbt tests.

The intended execution order is defined through task dependencies. End-to-end execution must be verified in the Docker environment.

## Project Structure

```text
smart-water-data-platform/
├── airflow/
│   ├── dags/
│   │   └── smart_water_pipeline.py
│   └── Dockerfile
├── dbt/
│   └── dbt_project.yml
├── data/
│   ├── raw/
│   ├── processed/
│   ├── silver/
│   └── gold/
├── scripts/
│   └── load_gold_to_postgres.py
├── src/
│   ├── config/
│   ├── ingestion/
│   ├── processing/
│   ├── quality/
│   ├── smart_water/
│   └── utils/
├── tests/
├── docker-compose.yml
├── pyproject.toml
├── .gitignore
└── README.md
```

*Note: The structure above describes the main project components; the exact folders may evolve as development continues.*

## Current Results

The pipeline development has produced the following results:

* Generated **3,365 synthetic sensor readings**.
* Cleaned the dataset to **3,352 valid readings**.
* Produced a cleaned Silver dataset.
* Generated Gold analytical data.
* Loaded **3,352 rows** into the PostgreSQL `station_energy_analysis` table.

These results are from the development runs; a complete automated execution through Airflow and Docker still needs to be validated.

## Getting Started

### Prerequisites

Install or prepare the following tools:

* Python 3.11 or compatible version
* Git
* Docker Desktop
* Java 17, if running Spark locally

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/smart-water-data-platform.git
cd smart-water-data-platform
```

Replace `<your-username>` with your GitHub username.

### 2. Create a Python environment

Using `uv`:

```bash
uv venv
```

Activate the environment in Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Install the project and its dependencies according to the project's `pyproject.toml` and setup instructions.

### 3. Configure environment variables

Create a local `.env` file for configuration such as database connection settings.

Do not commit real passwords, credentials, or secret configuration files to GitHub.

### 4. Start the containerized services

After verifying the Docker Compose configuration:

```bash
docker compose up --build -d
```

Inspect the service status:

```bash
docker compose ps
```

### 5. Access the services

If the configured services start successfully:

* Airflow UI: `http://localhost:8080`
* PostgreSQL: `localhost:5432`

Use the credentials defined in your local configuration.

### 6. Validate the pipeline

Before considering the workflow production-ready, verify that the Airflow DAG executes successfully, the PostgreSQL connection works, and the dbt models and tests pass.

## Future Improvements

* Replace synthetic data with a real-time sensor ingestion pipeline using MQTT.
* Add incremental processing instead of full data reloads.
* Improve automated data quality checks.
* Add unit and integration tests.
* Implement structured logging and error handling.
* Add CI/CD using GitHub Actions.
* Build a Power BI dashboard for energy consumption and pumping station KPIs.
* Add monitoring, documentation, and reproducible deployment instructions.

## Key Skills Demonstrated

Python · SQL · PySpark · ETL/ELT · Data Quality · Parquet · PostgreSQL · dbt · Apache Airflow · Docker · Git

## Author

**Mohamed Benhmida**

Aspiring Data Engineer focused on building reliable data pipelines, transforming operational data into analytical datasets, and developing practical data engineering solutions.

[LinkedIn](https://www.linkedin.com/in/mohamed-ben-hmida-13158b157/)
