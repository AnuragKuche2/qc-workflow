# qc-lakehouse/orchestration/tests/test_alerting.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from alerting import build_create_table_statement, build_insert_statement


def test_build_create_table_statement_targets_the_alerts_table():
    statement = build_create_table_statement()
    assert "CREATE TABLE IF NOT EXISTS qc_dev.ops.alerts" in statement
    assert "dag_id STRING" in statement
    assert "task_id STRING" in statement
    assert "run_id STRING" in statement
    assert "try_number INT" in statement
    assert "exception STRING" in statement
    assert "alerted_at TIMESTAMP" in statement


def test_build_insert_statement_includes_every_field():
    statement = build_insert_statement(
        dag_id="qc_lakehouse_maintenance",
        task_id="optimize_dim_customer",
        run_id="manual__2026-09-18T00:00:00+00:00",
        try_number=2,
        exception_str="DatabricksApiError: boom",
    )
    assert "INSERT INTO qc_dev.ops.alerts" in statement
    assert "'qc_lakehouse_maintenance'" in statement
    assert "'optimize_dim_customer'" in statement
    assert "'manual__2026-09-18T00:00:00+00:00'" in statement
    assert "2" in statement
    assert "DatabricksApiError: boom" in statement
    assert "current_timestamp()" in statement


def test_build_insert_statement_escapes_single_quotes_in_the_exception():
    # A raw exception message is untrusted text - an unescaped single quote would break out
    # of the SQL string literal and either error or (worse) alter the statement.
    statement = build_insert_statement(
        dag_id="d", task_id="t", run_id="r", try_number=1,
        exception_str="it's broken",
    )
    assert "it''s broken" in statement
    assert "it's broken" not in statement
