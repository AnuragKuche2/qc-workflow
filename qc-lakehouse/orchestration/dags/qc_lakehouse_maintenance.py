# qc-lakehouse/orchestration/dags/qc_lakehouse_maintenance.py
from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from alerting import alert_on_failure

DATABRICKS_CONN_ID = "databricks_default"
GOLD_TABLES = (
    "fct_orders", "fct_deliveries", "dim_customer", "dim_restaurant",
    "dim_rider", "dim_zone", "dim_date",
)
OPERATIONS = ("optimize", "analyze", "vacuum")
JOB_NAMES = {
    "optimize": "optimize_gold_table",
    "analyze": "analyze_gold_table",
    "vacuum": "vacuum_gold_table",
}

DEFAULT_ARGS = {
    "owner": "qc_lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="qc_lakehouse_maintenance",
    description="OPTIMIZE/ANALYZE/VACUUM every gold-layer table (Sub-project H, extended "
                "2026-09-18) - one parallel chain per table, fanning into a single report "
                "task regardless of per-table outcome. Separate from qc_lakehouse_pipeline, "
                "which is scoped to ingestion+dbt only and stays manual-trigger-only.",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    catchup=False,
    tags=["qc_lakehouse", "h", "maintenance"],
) as dag:
    report = EmptyOperator(task_id="maintenance_report", trigger_rule="all_done")

    for table in GOLD_TABLES:
        previous_task = None
        for operation in OPERATIONS:
            task = DatabricksRunNowOperator(
                task_id=f"{operation}_{table}",
                databricks_conn_id=DATABRICKS_CONN_ID,
                job_name=JOB_NAMES[operation],
                python_params=["--operation", operation, "--table", table],
            )
            if previous_task is not None:
                previous_task >> task
            previous_task = task
        previous_task >> report
