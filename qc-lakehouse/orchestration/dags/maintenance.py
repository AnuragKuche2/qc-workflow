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
    """report's python_callable. Scans every task in the WHOLE dag run (not just the
    maintenance group) and raises if anything other than report itself failed. This is
    deliberately broad, not a bug: `upstream_failed` is a member of Airflow's
    State.finished, so when an earlier stage fails (generation, any dbt build), every
    downstream task - including the whole maintenance TaskGroup - transitions to
    upstream_failed rather than being skipped or left pending. report's
    trigger_rule="all_done" is satisfied by upstream_failed just like success/failed, so
    report still runs this callable even when an earlier stage failed; its upstream never
    "never started." Because report is the DAG's only leaf task, Airflow derives the whole
    DAG run's overall state from report's own state - so this whole-run scan is not an
    unreachable safety net, it's the only mechanism that turns an earlier-stage failure into
    a red DAG run at all. Narrowing this scan to just the maintenance group would silently
    turn every non-maintenance failure into a reported SUCCESS (this project has already
    been bitten by exactly that bug once - see the deleted old maintenance DAG's test
    comments about an EmptyOperator leaf with trigger_rule="all_done").

    IMPORTANT: `context["dag_run"]` in Airflow's Task SDK (3.x) is a lightweight Pydantic
    data object for inter-process communication, not the classic `airflow.models.DagRun`
    ORM object - it has no `.get_task_instances()` (confirmed live: this exact call raised
    `AttributeError: 'DagRun' object has no attribute 'get_task_instances'` against a real
    run of this DAG, since tasks execute in an isolated process with no direct DB access by
    design in Airflow 3.x). The supported replacement is `context["ti"].get_task_states(...)`,
    which round-trips through the real Task Execution API
    (airflow.api_fastapi.execution_api.routes.task_instances.get_task_instance_states) and
    returns `{run_id: {task_key: state}}`, where a mapped task's key is
    f"{task_id}_{map_index}" (e.g. "maintenance.optimize_3"), not the `task_id[map_index]`
    formatting an earlier version of this function synthesized by hand."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    task_states = ti.get_task_states(dag_id=dag_run.dag_id, run_ids=[dag_run.run_id])
    states_for_this_run = task_states.get(dag_run.run_id, {})
    failed = [
        task_key for task_key, state in states_for_this_run.items()
        if state == "failed" and task_key != "report"
    ]
    if failed:
        raise AirflowException(f"Pipeline failed at: {failed}")
    print("report: all pipeline stages succeeded")
