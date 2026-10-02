# qc-lakehouse/orchestration/tests/test_alerting.py
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from alerting import (
    _execute_statement,
    build_create_schema_statement,
    build_create_table_statement,
    build_insert_statement,
)


def _fake_response(status_code=200, json_body=None):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_body or {}
    response.text = str(json_body)
    response.raise_for_status.return_value = None
    return response


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


def test_execute_statement_succeeds_immediately_without_polling():
    post_response = _fake_response(json_body={"status": {"state": "SUCCEEDED"}})
    with patch("alerting.requests.post", return_value=post_response) as mock_post, \
         patch("alerting.requests.get") as mock_get:
        _execute_statement("host", "token", "SELECT 1")
    mock_post.assert_called_once()
    mock_get.assert_not_called()


def test_execute_statement_polls_through_pending_to_success_on_a_cold_warehouse():
    # PENDING/RUNNING on the initial response are normal for a cold/starting serverless SQL
    # warehouse, not failures - the statement should be polled, not rejected outright.
    post_response = _fake_response(
        json_body={"status": {"state": "PENDING"}, "statement_id": "abc123"},
    )
    poll_running = _fake_response(json_body={"status": {"state": "RUNNING"}})
    poll_succeeded = _fake_response(json_body={"status": {"state": "SUCCEEDED"}})

    with patch("alerting.requests.post", return_value=post_response), \
         patch("alerting.requests.get", side_effect=[poll_running, poll_succeeded]) as mock_get, \
         patch("alerting.time.sleep") as mock_sleep:
        _execute_statement("host", "token", "SELECT 1")

    assert mock_get.call_count == 2
    mock_get.assert_called_with(
        "https://host/api/2.0/sql/statements/abc123",
        headers={"Authorization": "Bearer token"},
        timeout=15,
    )
    mock_sleep.assert_called()


def test_execute_statement_raises_on_a_genuine_terminal_failure_after_polling():
    post_response = _fake_response(
        json_body={"status": {"state": "RUNNING"}, "statement_id": "abc123"},
    )
    poll_failed = _fake_response(json_body={"status": {"state": "FAILED", "error": "boom"}})

    with patch("alerting.requests.post", return_value=post_response), \
         patch("alerting.requests.get", return_value=poll_failed), \
         patch("alerting.time.sleep"), pytest.raises(RuntimeError, match="FAILED"):
        _execute_statement("host", "token", "SELECT 1")


def test_execute_statement_raises_if_still_not_terminal_after_the_poll_budget():
    post_response = _fake_response(
        json_body={"status": {"state": "PENDING"}, "statement_id": "abc123"},
    )
    still_pending = _fake_response(json_body={"status": {"state": "PENDING"}})

    with patch("alerting.requests.post", return_value=post_response), \
         patch("alerting.requests.get", return_value=still_pending) as mock_get, \
         patch("alerting.time.sleep"), pytest.raises(RuntimeError, match="PENDING"):
        _execute_statement("host", "token", "SELECT 1")

    # Bounded, not an infinite/unbounded poll loop.
    assert mock_get.call_count > 0
