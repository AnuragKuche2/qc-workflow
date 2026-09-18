# qc-lakehouse/orchestration/dags/qc_lakehouse_maintenance.py
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.python import PythonOperator
from alerting import alert_on_failure

DATABRICKS_CONN_ID = "databricks_default"
# Deliberately duplicated literal, not an oversight to "DRY up" later: orchestration/'s tests
# run under .venv-airflow, which has no perf_lab/qc_lakehouse package dependencies, so there
# is no import path back to perf_lab/run_maintenance.py's own GOLD_TABLES - this tuple must be
# kept in sync by hand if the gold layer's table list ever changes.
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


def check_maintenance_results(**context) -> None:
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()
    failed_tasks = [
        ti.task_id for ti in task_instances
        if ti.state == "failed" and ti.task_id != "maintenance_report"
    ]
    if failed_tasks:
        raise AirflowException(f"Maintenance failed for: {failed_tasks}")
    print("maintenance_report: all table chains succeeded")


with DAG(
    dag_id="qc_lakehouse_maintenance",
    description="OPTIMIZE/ANALYZE/VACUUM every gold-layer table (Sub-project H, extended "
                "2026-09-18) - one independent chain per table (no cross-table task "
                "dependencies), fanning into a single report task regardless of per-table "
                "outcome. Databricks executes each of the 3 underlying jobs serially "
                "(max_concurrent_runs: 1 in databricks.yml, deliberate) - the fan-out is an "
                "orchestration-shape/dependency-graph decision here, not a wall-clock-"
                "parallelism guarantee. Separate from qc_lakehouse_pipeline, which is scoped "
                "to ingestion+dbt only and stays manual-trigger-only.",
    default_args=DEFAULT_ARGS,
    schedule="@weekly",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["qc_lakehouse", "h", "maintenance"],
) as dag:
    report = PythonOperator(
        task_id="maintenance_report",
        python_callable=check_maintenance_results,
        trigger_rule="all_done",
    )

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
