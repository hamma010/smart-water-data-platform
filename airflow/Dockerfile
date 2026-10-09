FROM apache/airflow:3.3.2-python3.12

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    psycopg2-binary \
    dbt-postgres
