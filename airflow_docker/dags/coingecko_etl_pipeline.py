from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

default_args = {
    "owner": "soumitra",
    "retries": 1,
}

with DAG(
    dag_id="coingecko_etl_pipeline",
    default_args=default_args,
    description="Orchestrates Bronze ingestion and Silver transformation on Databricks",
    schedule=None,
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

    ingest_bronze >> transform_silver




