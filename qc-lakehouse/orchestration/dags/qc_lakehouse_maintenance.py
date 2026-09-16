# qc-lakehouse/orchestration/dags/qc_lakehouse_maintenance.py
from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

DATABRICKS_CONN_ID = "databricks_default"

DEFAULT_ARGS = {
    "owner": "qc_lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="qc_lakehouse_maintenance",
    description="OPTIMIZE/ANALYZE/VACUUM the real fct_orders table (Sub-project H) - "
                "separate from qc_lakehouse_pipeline, which is scoped to ingestion+dbt only.",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=["qc_lakehouse", "h", "maintenance"],
) as dag:
    optimize_fct_orders = DatabricksRunNowOperator(
        task_id="optimize_fct_orders",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="optimize_fct_orders",
    )

    analyze_fct_orders = DatabricksRunNowOperator(
        task_id="analyze_fct_orders",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="analyze_fct_orders",
    )

    vacuum_fct_orders = DatabricksRunNowOperator(
        task_id="vacuum_fct_orders",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="vacuum_fct_orders",
    )

    optimize_fct_orders >> analyze_fct_orders >> vacuum_fct_orders
