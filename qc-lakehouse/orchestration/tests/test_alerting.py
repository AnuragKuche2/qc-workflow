# qc-lakehouse/orchestration/tests/test_alerting.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from alerting import (
    build_create_schema_statement,
    build_create_table_statement,
    build_insert_statement,
)


def test_build_create_schema_statement_targets_the_ops_schema():
    statement = build_create_schema_statement()
    assert statement == "CREATE SCHEMA IF NOT EXISTS qc_dev.ops"


def test_build_create_table_statement_targets_the_alerts_table():
    statement = build_create_table_statement()
    assert "CREATE TABLE IF NOT EXISTS qc_dev.ops.alerts" in statement
    assert "dag_id STRING" in statement
    assert "task_id STRING" in statement
    assert "run_id STRING" in statement
    assert "try_number INT" in statement
    assert "exception STRING" in statement
    assert "alerted_at TIMESTAMP" in statement


def test_build_insert_statement_uses_named_placeholders_not_raw_values():
    statement, _parameters = build_insert_statement(
        dag_id="qc_lakehouse_maintenance",
        task_id="optimize_dim_customer",
        run_id="manual__2026-09-18T00:00:00+00:00",
        try_number=2,
        exception_str="DatabricksApiError: boom",
    )
    assert "INSERT INTO qc_dev.ops.alerts" in statement
    assert "current_timestamp()" in statement
    # Placeholders, not interpolated values - the API's own parameter binding fills these in.
    assert ":dag_id" in statement
    assert ":task_id" in statement
    assert ":run_id" in statement
    assert ":try_number" in statement
    assert ":exception_str" in statement
    assert "qc_lakehouse_maintenance" not in statement
    assert "optimize_dim_customer" not in statement
    assert "DatabricksApiError: boom" not in statement


def test_build_insert_statement_returns_all_five_parameters_with_correct_types():
    _statement, parameters = build_insert_statement(
        dag_id="qc_lakehouse_maintenance",
        task_id="optimize_dim_customer",
        run_id="manual__2026-09-18T00:00:00+00:00",
        try_number=2,
        exception_str="DatabricksApiError: boom",
    )
    assert isinstance(parameters, list)
    by_name = {p["name"]: p for p in parameters}
    assert set(by_name) == {"dag_id", "task_id", "run_id", "try_number", "exception_str"}

    assert by_name["dag_id"] == {
        "name": "dag_id", "value": "qc_lakehouse_maintenance", "type": "STRING",
    }
    assert by_name["task_id"] == {
        "name": "task_id", "value": "optimize_dim_customer", "type": "STRING",
    }
    assert by_name["run_id"] == {
        "name": "run_id",
        "value": "manual__2026-09-18T00:00:00+00:00",
        "type": "STRING",
    }
    assert by_name["try_number"] == {"name": "try_number", "value": "2", "type": "LONG"}
    assert by_name["exception_str"] == {
        "name": "exception_str", "value": "DatabricksApiError: boom", "type": "STRING",
    }


def test_build_insert_statement_does_not_escape_the_exception_value():
    # Escaping is no longer build_insert_statement's job - the Statement Execution API's own
    # named-parameter binding handles this safely, so a raw single quote should pass through
    # untouched into the parameters list rather than being doubled.
    _statement, parameters = build_insert_statement(
        dag_id="d", task_id="t", run_id="r", try_number=1,
        exception_str="it's broken",
    )
    by_name = {p["name"]: p for p in parameters}
    assert by_name["exception_str"]["value"] == "it's broken"
