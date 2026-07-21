from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "soumitra",
    "retries": 1,
}

with DAG(
    dag_id="coingecko_etl_pipeline",
    default_args=default_args,
    description="Orchestrates Bronze ingestion and Silver transformation on Databricks",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["coingecko", "etl"],
) as dag:

    ingest_bronze = DatabricksRunNowOperator(
        task_id="ingest_bronze",
        databricks_conn_id="databricks_default",
        job_id= "583455561545656"
    )

    transform_silver = DatabricksRunNowOperator(
        task_id = "transform_silver",
        databricks_conn_id="databricks_default",
        job_id="783176890600322"
    )


    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/coingecko_dbt && DBT_PROFILES_DIR=/opt/airflow/.dbt dbt run",
    )


    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/coingecko_dbt && DBT_PROFILES_DIR=/opt/airflow/.dbt dbt test",
    )

    ingest_bronze >> transform_silver >> dbt_run >> dbt_test




