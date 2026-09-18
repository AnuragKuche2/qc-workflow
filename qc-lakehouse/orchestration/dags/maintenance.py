# qc-lakehouse/orchestration/dags/maintenance.py
"""Gold-table maintenance (OPTIMIZE/ANALYZE/VACUUM), wired as the final stage of
qc_lakehouse_pipeline after marts has built and passed its tests. Uses Airflow's dynamic
task mapping (.expand()) to apply each operation across every gold table, instead of a
hand-unrolled per-table loop - the actual tool built for "same operation, many entities."

Barrier semantics between operations (all OPTIMIZE instances complete before any ANALYZE
instance starts, and so on), not per-table independent chains: simpler, and matches how
batch maintenance commonly runs in practice - see
docs/superpowers/specs/2026-09-18-qc-lakehouse-medallion-dag-redesign.md's "Non-goals".
Databricks executes each of the 3 underlying jobs serially regardless
(max_concurrent_runs: 1 in databricks.yml, deliberate - Free Edition quota: max 5
concurrent job tasks account-wide, a quota overrun shuts down all workspace compute for
the day) - the barrier is an orchestration-shape decision here, not a wall-clock-
parallelism guarantee.
"""
from __future__ import annotations

from airflow.exceptions import AirflowException
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import TaskGroup

DATABRICKS_CONN_ID = "databricks_default"

# Deliberately duplicated literal, not an oversight to "DRY up" later: orchestration/'s
# tests run under .venv-airflow, which has no import path to perf_lab/run_maintenance.py's
# own GOLD_TABLES - this tuple must be kept in sync by hand if the gold layer's table list
# ever changes.
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


def build_maintenance_tasks(dag):
    """Builds a `maintenance` TaskGroup inside `dag` containing three dynamically-mapped
    OPTIMIZE -> ANALYZE -> VACUUM stages (one mapped instance per GOLD_TABLES entry per
    stage) and returns the final (VACUUM) mapped task, for the caller to chain `>> report`.
    """
    with TaskGroup(group_id="maintenance", dag=dag) as _group:
        previous = None
        final_task = None
        for operation in OPERATIONS:
            task = DatabricksRunNowOperator.partial(
                task_id=operation,
                databricks_conn_id=DATABRICKS_CONN_ID,
                job_name=JOB_NAMES[operation],
            ).expand(
                python_params=[
                    ["--operation", operation, "--table", table] for table in GOLD_TABLES
                ]
            )
            if previous is not None:
                previous >> task
            previous = task
            final_task = task
    return final_task


def check_maintenance_results(**context) -> None:
    """report's python_callable. Scans every task instance in the WHOLE dag run (not just
    the maintenance group) and raises if anything other than report itself failed. This is
    deliberately broad, not a bug: the pipeline's earlier stages (generation, each dbt
    build) all use the default trigger_rule="all_success", so if any of them had failed,
    report's own upstream (the maintenance group) would never have started and report
    would never reach this callable at all - by the time this code runs, only the
    maintenance group's mapped tasks can plausibly be in a mixed success/failed state."""
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()
    failed = []
    for ti in task_instances:
        if ti.task_id == "report" or ti.state != "failed":
            continue
        map_index = getattr(ti, "map_index", -1)
        if map_index is not None and map_index != -1:
            failed.append(f"{ti.task_id}[{map_index}]")
        else:
            failed.append(ti.task_id)
    if failed:
        raise AirflowException(f"Maintenance failed for: {failed}")
    print("report: all pipeline stages succeeded")
