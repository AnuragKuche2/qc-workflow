from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback
from alerting import alert_on_failure
from maintenance import build_maintenance_tasks, check_maintenance_results

logger = logging.getLogger(__name__)

DATABRICKS_CONN_ID = "databricks_default"

DEFAULT_ARGS = {
    "owner": "qc_lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_on_failure,
}

# Budget recalculated for the merged medallion pipeline (previously 65 minutes for just
# generate -> dbt_run -> dbt_test). Real observed Databricks job run_duration figures
# (queried 2026-09-18 via `databricks jobs list-runs`, worst of last 3 runs per job) inform
# each per-stage allowance below, with generous headroom since Free Edition serverless
# compute has real contention variance:
#   generate_reference_data:      observed max ~143s  -> budget 15 min
#   generate_fact_data:           observed max ~1546s -> budget 30 min
#   dbt_build_test_staging:       (was dbt_run, whole-project max ~143s, now scoped to 13
#                                  of 23 models plus interleaved tests) -> budget 10 min
#   dbt_build_test_intermediate:  (2 of 23 models)                     -> budget  5 min
#   dbt_build_test_marts:         (7 of 23 models, all table materializations, plus tests
#                                  that were previously a separate ~137s dbt_test stage)
#                                                                       -> budget 10 min
#   maintenance.optimize (7 tables, serialized by max_concurrent_runs: 1; single-table
#                          observed max ~448s for the largest table)   -> budget 15 min
#   maintenance.analyze  (single-table observed max ~61s)              -> budget 10 min
#   maintenance.vacuum   (single-table observed max ~436s)             -> budget 15 min
# Total: 15+30+10+5+10+15+10+15 = 110 minutes.
PIPELINE_DEADLINE = timedelta(minutes=110)

# DeadlineReference.DAGRUN_QUEUED_AT, not DAGRUN_LOGICAL_DATE: a manually-triggered or
# @weekly-scheduled DAG run still needs `queued_at` (always populated) rather than
# `logical_date` (NULL for a manual trigger, and the deadline evaluator silently skips a
# null-resolving reference - no error, the trigger still exits 0, the callback then can
# never fire). Confirmed live against this project's own Airflow 3.3.1 instance - see
# git history for the throwaway-probe-DAG verification this comment refers to.


def log_deadline_missed(**kwargs):
    """Deadline-miss callback. A real system would page/alert here (PagerDuty, Slack, etc.) -
    this project has no live alerting channel for deadline misses specifically (distinct
    from alert_on_failure, which does have one), so it logs a structured warning instead."""
    logger.warning(
        "Deadline missed for dag_id=qc_lakehouse_pipeline: %s",
        kwargs,
    )


with DAG(
    dag_id="qc_lakehouse_pipeline",
    description="Medallion-shaped pipeline: ingest bronze, build+test each silver/gold "
                "dbt layer (fail-fast - a layer's test failure blocks every downstream "
                "layer), maintain gold tables (OPTIMIZE/ANALYZE/VACUUM, dynamically mapped "
                "across every gold table), report.",
    default_args=DEFAULT_ARGS,
    schedule="@weekly",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=PIPELINE_DEADLINE,
        callback=SyncCallback(log_deadline_missed),
    ),
    tags=["qc_lakehouse", "e1", "h"],
) as dag:
    generate_reference_data = DatabricksRunNowOperator(
        task_id="generate_reference_data",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="generate_reference_data",
    )

    generate_fact_data = DatabricksRunNowOperator(
        task_id="generate_fact_data",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="generate_fact_data",
    )

    dbt_build_test_staging = DatabricksRunNowOperator(
        task_id="dbt_build_test_staging",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="dbt_build_test_staging",
    )

    dbt_build_test_intermediate = DatabricksRunNowOperator(
        task_id="dbt_build_test_intermediate",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="dbt_build_test_intermediate",
    )

    dbt_build_test_marts = DatabricksRunNowOperator(
        task_id="dbt_build_test_marts",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="dbt_build_test_marts",
    )

    report = PythonOperator(
        task_id="report",
        python_callable=check_maintenance_results,
        trigger_rule="all_done",
    )

    final_maintenance_task = build_maintenance_tasks(dag)

    (
        generate_reference_data
        >> generate_fact_data
        >> dbt_build_test_staging
        >> dbt_build_test_intermediate
        >> dbt_build_test_marts
        >> dag.get_task("maintenance.optimize")
    )
    final_maintenance_task >> report
