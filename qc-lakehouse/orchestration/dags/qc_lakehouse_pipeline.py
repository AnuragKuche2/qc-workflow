from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback

logger = logging.getLogger(__name__)

DATABRICKS_CONN_ID = "databricks_default"

DEFAULT_ARGS = {
    "owner": "qc_lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Budget for the whole pipeline (sum of the per-job budgets the design called for:
# 15 + 30 + 10 + 10 minutes). Airflow 3.0 removed the SLA feature entirely
# (`DAG.sla_miss_callback` and the per-task `sla` kwarg both still exist as parameters but
# are dead - passing either is now a silent no-op that only emits a DeprecationWarning; see
# airflow.sdk.definitions.dag.DAG._validate_sla_miss_callback and
# airflow.sdk.bases.operator.BaseOperator.__init__ in the installed apache-airflow==3.3.1).
# Its replacement, Deadline Alerts (stable as of Airflow >=3.1), is DAG-level only - there is
# no per-task equivalent in the installed version - so this single DAG-level deadline
# preserves the design's "SLA with a logged warning" intent as closely as the current API
# allows, covering the pipeline's full sequential run instead of each job individually.
PIPELINE_DEADLINE = timedelta(minutes=65)


def log_deadline_missed(**kwargs):
    """Deadline-miss callback. A real system would page/alert here (PagerDuty, Slack, etc.) -
    this project has no live alerting channel, so it logs a structured warning instead, making
    visible what a production system would act on rather than silently having no SLA story."""
    logger.warning(
        "Deadline missed for dag_id=qc_lakehouse_pipeline: %s",
        kwargs,
    )


with DAG(
    dag_id="qc_lakehouse_pipeline",
    description="Trigger QC Lakehouse's ingestion (B1) and dbt (C) Databricks Jobs, in order.",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=PIPELINE_DEADLINE,
        callback=SyncCallback(log_deadline_missed),
    ),
    tags=["qc_lakehouse", "e1"],
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

    dbt_run = DatabricksRunNowOperator(
        task_id="dbt_run",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="dbt_run",
    )

    dbt_test = DatabricksRunNowOperator(
        task_id="dbt_test",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="dbt_test",
    )

    generate_reference_data >> generate_fact_data >> dbt_run >> dbt_test
