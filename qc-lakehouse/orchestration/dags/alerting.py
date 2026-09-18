# qc-lakehouse/orchestration/dags/alerting.py
"""on_failure_callback for both qc_lakehouse DAGs: writes a structured failure record to
qc_dev.ops.alerts via stateless calls to the Databricks SQL Statement Execution API -
deliberately NOT Databricks Connect. This session live-verified (2026-09-17/18, the
generate_benchmark_orders.py and apply_layouts.py incidents) that Databricks Connect sessions
die under duration/idle limits; a failure callback must be fast and must never hold a
long-lived Spark session open, so it uses the databricks_default connection's existing token
via bare `requests` instead - no new Python dependency, no session to keep alive.
"""
from __future__ import annotations

import logging
import os

import requests
from airflow.sdk.bases.hook import BaseHook

DATABRICKS_CONN_ID = "databricks_default"
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "ca865a4ef1668613")


def build_create_schema_statement() -> str:
    return "CREATE SCHEMA IF NOT EXISTS qc_dev.ops"


def build_create_table_statement() -> str:
    return (
        "CREATE TABLE IF NOT EXISTS qc_dev.ops.alerts ("
        "dag_id STRING, task_id STRING, run_id STRING, try_number INT, "
        "exception STRING, alerted_at TIMESTAMP)"
    )


def build_insert_statement(
    dag_id: str, task_id: str, run_id: str, try_number: int, exception_str: str,
) -> tuple[str, list[dict]]:
    statement = (
        "INSERT INTO qc_dev.ops.alerts VALUES "
        "(:dag_id, :task_id, :run_id, :try_number, :exception_str, current_timestamp())"
    )
    parameters = [
        {"name": "dag_id", "value": dag_id, "type": "STRING"},
        {"name": "task_id", "value": task_id, "type": "STRING"},
        {"name": "run_id", "value": run_id, "type": "STRING"},
        {"name": "try_number", "value": str(try_number), "type": "LONG"},
        {"name": "exception_str", "value": exception_str, "type": "STRING"},
    ]
    return statement, parameters


def _execute_statement(
    host: str, token: str, statement: str, parameters: list[dict] | None = None,
) -> None:
    payload = {"warehouse_id": WAREHOUSE_ID, "statement": statement, "wait_timeout": "10s"}
    if parameters is not None:
        payload["parameters"] = parameters

    response = requests.post(
        f"https://{host}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=15,
    )
    response.raise_for_status()

    # raise_for_status() only catches HTTP-level failures. The Statement Execution API
    # returns HTTP 200 with {"status": {"state": "FAILED", "error": {...}}} for SQL-level
    # errors (e.g. SCHEMA_NOT_FOUND), so that case must be checked explicitly or it's
    # silently swallowed.
    state = response.json().get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(f"Statement did not succeed (state={state}): {response.text}")


def alert_on_failure(context: dict) -> None:
    """Airflow on_failure_callback signature: receives the task instance context dict."""
    task_instance = context["task_instance"]
    exception_str = str(context.get("exception", "unknown error"))

    connection = BaseHook.get_connection(DATABRICKS_CONN_ID)
    host = connection.host
    token = connection.password or connection.extra_dejson.get("token")

    try:
        _execute_statement(host, token, build_create_schema_statement())
        _execute_statement(host, token, build_create_table_statement())
        statement, parameters = build_insert_statement(
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            try_number=task_instance.try_number,
            exception_str=exception_str,
        )
        _execute_statement(host, token, statement, parameters)
    except Exception as exc:  # noqa: BLE001 - a broken alert must never mask the real failure
        logging.getLogger(__name__).error(
            "alerting: failed to write failure record to qc_dev.ops.alerts: %r", exc,
        )
