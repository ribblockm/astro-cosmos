FROM quay.io/astronomer/astro-runtime:12.7.1

WORKDIR /usr/local/airflow/

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-duckdb && deactivate
