from airflow import DAG
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from airflow.utils.dates import days_ago
import requests
import json
from datetime import datetime
import os
import duckdb
from deltalake import DeltaTable, write_deltalake

# Constants
API_URL = "https://api.openbrewerydb.org/breweries"
BRONZE_PATH = "/usr/local/airflow/include/bronze"
SILVER_PATH = "/usr/local/airflow/include/silver"
GOLD_PATH = "/usr/local/airflow/include/gold"
DBT_PROJECT_DIR = "/usr/local/airflow/dags/dbt_cosmos"

# Ensure directories exist
for path in [BRONZE_PATH, SILVER_PATH, GOLD_PATH]:
    os.makedirs(path, exist_ok=True)

# --- Initialize DuckDB connection ---
conn = duckdb.connect("/usr/local/airflow/include/duckdb.db")

# --- Load DuckDB extensions ---
conn.execute("INSTALL 'json'")
conn.execute("LOAD 'json'")
conn.execute("INSTALL 'delta'")
conn.execute("LOAD 'delta'")


def fetch_api_data():
    print("Fetching data from API...")
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()
        # Save raw data for reference
        with open(
            f"{BRONZE_PATH}/raw_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            "w",
        ) as f:
            json.dump(data, f)
        return data
    else:
        raise Exception(f"API request failed with status code {response.status_code}")


def create_raw_table(data):
    print("Creating raw table from json...")
    # Create a temporary table from the JSON data
    query = "CREATE TEMPORARY TABLE temp_raw_data AS SELECT * FROM read_json(?);"
    conn.execute(query, [json.dumps(data)])

    df = conn.query("SELECT * FROM temp_raw_data").df()
    write_deltalake("/usr/local/airflow/include/raw_delta_table", df, mode="overwrite")

    # Clean up temporary table
    conn.execute("DROP TABLE temp_raw_data")


# Define the DAG
with DAG(
    dag_id="breweries_data_pipeline",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": 300,  # 5 minutes
    },
) as dag:
    # Task to fetch raw data
    fetch_task = PythonOperator(
        task_id="fetch_breweries_data",
        python_callable=fetch_api_data,
    )

    raw_table_task = PythonOperator(
        task_id="create_raw_table", python_callable=create_raw_table
    )

    # dbt configuration
    profile_config = ProfileConfig(
        profile_name="dbt_cosmos",
        target_name="dev",
        profiles_yml_filepath=os.path.join(DBT_PROJECT_DIR, "profiles.yml"),
    )

    execution_config = ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
    )

    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECT_DIR,
    )

    # dbt task group for transformations
    dbt_transform = DbtTaskGroup(
        group_id="transform_data",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
    )

    # Define task dependencies
    fetch_task >> raw_table_task >> dbt_transform
