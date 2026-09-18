# Maintenance DAG Orchestration Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `qc_lakehouse_maintenance` from a single-table, manual-trigger-only,
purely-sequential 3-task DAG into a real production-shaped orchestration: parallel
per-table `optimize -> analyze -> vacuum` chains across all 7 gold tables, fanning into
one report task via `trigger_rule`, on a daily schedule, with failure alerting.

**Architecture:** `perf_lab/run_maintenance.py` gains a `--table` flag (defaulting to
`fct_orders` for backward compatibility) alongside its existing `--operation` flag, so the
same 3 Databricks Jobs (renamed to be table-agnostic) can run any operation against any gold
table. The Airflow DAG builds one 3-task chain per table via `DatabricksRunNowOperator`'s
`python_params` override, all 7 chains running in parallel, converging into a single
`maintenance_report` `EmptyOperator` task with `trigger_rule="all_done"` (runs regardless of
which upstream chains succeeded or failed). A new `on_failure_callback` (in a new
`orchestration/dags/alerting.py` module) writes a structured failure record to
`qc_dev.ops.alerts` via a single stateless SQL Statement Execution API call (not Databricks
Connect - a callback must be fast and must never hold a long-lived Spark session, per this
session's own live-verified lesson that Databricks Connect sessions die under duration/idle
limits).

**Tech Stack:** Apache Airflow 3.3.1, `apache-airflow-providers-databricks`,
Databricks SQL Statement Execution API (via the Airflow `databricks_default` connection's
existing token, using bare `requests` - no new Python dependency), pytest.

**Spec:** No separate design spec file - this is a Bounded change per
`superpowers:brainstorming`'s classification (all 3 files already exist in this repo), agreed
in-chat with the user on 2026-09-18. `PROJECTINFO.md`'s "Roadmap to completion" item 2
("Orchestration hardening") is the tracking reference.

## Global Constraints

- VACUUM must never explicitly go below Delta's 7-day default retention (existing
  project-wide constraint, already respected by `run_maintenance.py` - do not add a `RETAIN`
  clause when extending it).
- Every Airflow import must use this project's already-verified Airflow 3.3.1 non-deprecated
  paths: `EmptyOperator` from `airflow.providers.standard.operators.empty` (not
  `airflow.operators.empty`), `BaseHook` from `airflow.sdk.bases.hook` (not
  `airflow.hooks.base`).
- `DatabricksRunNowOperator`'s parameter-override argument for a `spark_python_task`-type job
  is `python_params` (a plain list of strings, fully replacing the job's declared
  `parameters:` for that run) - verified live against the installed
  `apache-airflow-providers-databricks` version via `inspect.signature`. Do not use
  `job_parameters` (that's for job-level named parameters, a different mechanism this bundle
  doesn't use) or a nonexistent `--python-params` CLI flag.
- `qc_lakehouse_pipeline` (the data-generation DAG) is explicitly OUT of scope for scheduling
  in this plan - it stays manual-trigger-only, since scheduling it would silently regenerate/
  overwrite bronze data and burn real Databricks compute unattended.
- The Databricks SQL warehouse ID for direct SQL calls is `ca865a4ef1668613` (this project's
  existing default, matching `perf_lab/_shared.py`'s `WAREHOUSE_ID` constant - duplicated here
  rather than imported, since `orchestration/` has no import path to the `qc_lakehouse`/
  `perf_lab` packages, matching this DAG file's existing pattern of duplicating
  `DATABRICKS_CONN_ID = "databricks_default"` rather than importing it).

---

### Task 1: Parameterize `run_maintenance.py` by table, rename the 3 Databricks Jobs

**Files:**
- Modify: `qc-lakehouse/perf_lab/run_maintenance.py`
- Modify: `qc-lakehouse/databricks.yml:174-220` (the `optimize_fct_orders`/`analyze_fct_orders`/`vacuum_fct_orders` job definitions)
- Test: `qc-lakehouse/tests/test_run_maintenance.py` (new file)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `parse_args(argv: list[str]) -> tuple[str, str]` returning `(operation, table)`,
  and `GOLD_TABLES: tuple[str, ...]` (the 7 valid table short names) - Task 3's DAG file
  duplicates this same table list (no import path exists between `orchestration/` and
  `perf_lab/`, matching this project's established pattern), so the exact 7 names must match
  verbatim: `("fct_orders", "fct_deliveries", "dim_customer", "dim_restaurant", "dim_rider",
  "dim_zone", "dim_date")`.

- [ ] **Step 1: Write the failing tests**

Create `qc-lakehouse/tests/test_run_maintenance.py`:

```python
# qc-lakehouse/tests/test_run_maintenance.py
import pytest

from perf_lab.run_maintenance import GOLD_TABLES, parse_args


def test_parse_args_defaults_table_to_fct_orders():
    assert parse_args(["--operation", "optimize"]) == ("optimize", "fct_orders")


def test_parse_args_reads_an_explicit_table():
    assert parse_args(["--operation", "vacuum", "--table", "dim_customer"]) == (
        "vacuum",
        "dim_customer",
    )


def test_parse_args_reads_flags_in_either_order():
    assert parse_args(["--table", "dim_zone", "--operation", "analyze"]) == (
        "analyze",
        "dim_zone",
    )


def test_parse_args_requires_a_valid_operation():
    with pytest.raises(ValueError, match="--operation"):
        parse_args(["--table", "fct_orders"])
    with pytest.raises(ValueError, match="--operation"):
        parse_args(["--operation", "not-a-real-operation"])


def test_parse_args_rejects_an_unknown_table():
    with pytest.raises(ValueError, match="--table"):
        parse_args(["--operation", "optimize", "--table", "not_a_real_table"])


def test_gold_tables_has_all_seven_gold_layer_tables():
    assert GOLD_TABLES == (
        "fct_orders", "fct_deliveries", "dim_customer", "dim_restaurant",
        "dim_rider", "dim_zone", "dim_date",
    )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd qc-lakehouse && .venv/bin/python -m pytest tests/test_run_maintenance.py -v`
Expected: `ImportError: cannot import name 'GOLD_TABLES' from 'perf_lab.run_maintenance'`

- [ ] **Step 3: Implement `parse_args` and `GOLD_TABLES`**

Replace the full contents of `qc-lakehouse/perf_lab/run_maintenance.py` with:

```python
# qc-lakehouse/perf_lab/run_maintenance.py
"""OPTIMIZE/ANALYZE/VACUUM for any gold-layer table, triggered by 3 table-agnostic Databricks
Jobs (optimize_gold_table/analyze_gold_table/vacuum_gold_table) that all point at this same
script with different --operation/--table arguments. VACUUM never goes below Delta's 7-day
default retention (project-wide constraint) - no explicit RETAIN clause is passed, so Delta's
own 7-day default applies.

Originally hardcoded to fct_orders only, triggered by 3 identically-shaped but
fct_orders-specific jobs (optimize_fct_orders/analyze_fct_orders/vacuum_fct_orders). Extended
(2026-09-18, orchestration hardening) to take --table so the maintenance DAG can run one
parallel chain per gold table instead of only maintaining the single biggest one - the jobs
themselves were renamed to be table-agnostic since a job named "optimize_fct_orders" running
against dim_customer would be a misleading name.

Runs on serverless Spark (not the SQL warehouse) because it executes as a Databricks Job
triggered by Airflow, unlike apply_winning_layout.py's one-time interactive warehouse run."""
from __future__ import annotations

import sys

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session, is_running_on_databricks

CATALOG_SCHEMA = "qc_dev.gold"
GOLD_TABLES = (
    "fct_orders", "fct_deliveries", "dim_customer", "dim_restaurant",
    "dim_rider", "dim_zone", "dim_date",
)
DEFAULT_TABLE = "fct_orders"

OPERATION_TEMPLATES = {
    "optimize": "OPTIMIZE {table}",
    "analyze": "ANALYZE TABLE {table} COMPUTE STATISTICS",
    "vacuum": "VACUUM {table}",  # no RETAIN clause - Delta's 7-day default applies
}


def parse_args(argv: list[str]) -> tuple[str, str]:
    """(operation, table_short_name) from `--operation X --table Y` CLI flags (either order).
    --operation is required (no valid default - a bare invocation must fail loudly, not
    silently no-op). --table defaults to DEFAULT_TABLE, preserving the 3 renamed Databricks
    Jobs' existing default parameters (which only pass --operation) as still valid,
    backward-compatible invocations against fct_orders."""
    operation = None
    table = DEFAULT_TABLE
    i = 0
    while i < len(argv):
        if argv[i] == "--operation":
            operation = argv[i + 1]
            i += 2
        elif argv[i] == "--table":
            table = argv[i + 1]
            i += 2
        else:
            i += 1
    if operation not in OPERATION_TEMPLATES:
        raise ValueError(f"--operation must be one of {list(OPERATION_TEMPLATES)}, got {operation!r}")
    if table not in GOLD_TABLES:
        raise ValueError(f"--table must be one of {GOLD_TABLES}, got {table!r}")
    return operation, table


def main() -> None:
    operation, table = parse_args(sys.argv[1:])
    full_table = f"{CATALOG_SCHEMA}.{table}"

    settings = None if is_running_on_databricks() else load_settings()
    spark = build_databricks_session(settings)

    statement = OPERATION_TEMPLATES[operation].format(table=full_table)
    print(f"run-maintenance: {statement}")
    spark.sql(statement).collect()
    print(f"run-maintenance: OK - {operation} completed on {full_table}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd qc-lakehouse && .venv/bin/python -m pytest tests/test_run_maintenance.py -v`
Expected: 6 passed.

- [ ] **Step 5: Rename the 3 Databricks Jobs in `databricks.yml` to be table-agnostic**

In `qc-lakehouse/databricks.yml`, replace the `optimize_fct_orders`, `analyze_fct_orders`, and
`vacuum_fct_orders` job blocks (currently at lines 174-220) with:

```yaml
    optimize_gold_table:
      name: optimize_gold_table
      tasks:
        - task_key: main
          spark_python_task:
            python_file: ./perf_lab/run_maintenance.py
            parameters: ["--operation", "optimize", "--table", "fct_orders"]
          environment_key: qc_lakehouse_env
      environments:
        - environment_key: qc_lakehouse_env
          spec:
            environment_version: "3"
            dependencies:
              - ./dist/*.whl
              - python-dotenv

    analyze_gold_table:
      name: analyze_gold_table
      tasks:
        - task_key: main
          spark_python_task:
            python_file: ./perf_lab/run_maintenance.py
            parameters: ["--operation", "analyze", "--table", "fct_orders"]
          environment_key: qc_lakehouse_env
      environments:
        - environment_key: qc_lakehouse_env
          spec:
            environment_version: "3"
            dependencies:
              - ./dist/*.whl
              - python-dotenv

    vacuum_gold_table:
      name: vacuum_gold_table
      tasks:
        - task_key: main
          spark_python_task:
            python_file: ./perf_lab/run_maintenance.py
            parameters: ["--operation", "vacuum", "--table", "fct_orders"]
          environment_key: qc_lakehouse_env
      environments:
        - environment_key: qc_lakehouse_env
          spec:
            environment_version: "3"
            dependencies:
              - ./dist/*.whl
              - python-dotenv
```

The `--table fct_orders` default is only a schema-validation placeholder (matching
`generate_benchmark_orders`'s existing default-parameter convention in this same file) -
every real Airflow invocation overrides both flags explicitly via `python_params` (Task 3).

- [ ] **Step 6: Validate and deploy the bundle**

Run: `cd qc-lakehouse && databricks bundle validate`
Expected: `Validation OK!`

Run: `cd qc-lakehouse && databricks bundle deploy`
Expected: output includes `Created jobs.optimize_gold_table`, `Created jobs.analyze_gold_table`,
`Created jobs.vacuum_gold_table`, and `Deleted jobs.optimize_fct_orders`,
`Deleted jobs.analyze_fct_orders`, `Deleted jobs.vacuum_fct_orders` (the bundle deploy removes
the old, now-unreferenced job names since they're gone from `databricks.yml`).

- [ ] **Step 7: Commit**

```bash
cd /Users/anuragk/randomproject
git add qc-lakehouse/perf_lab/run_maintenance.py qc-lakehouse/tests/test_run_maintenance.py qc-lakehouse/databricks.yml
git commit -m "Parameterize run_maintenance.py by table, rename the 3 maintenance Jobs

Extends --operation with --table (defaulting to fct_orders for backward
compatibility) so the same 3 Databricks Jobs can maintain any gold table, not
just fct_orders. Renamed the jobs from *_fct_orders to *_gold_table since a
table-agnostic job keeping a table-specific name would be misleading once the
maintenance DAG (next task) runs them against all 7 gold tables in parallel.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 2: Add the failure-alerting callback module

**Files:**
- Create: `qc-lakehouse/orchestration/dags/alerting.py`
- Test: `qc-lakehouse/orchestration/tests/test_alerting.py` (new file)

**Interfaces:**
- Consumes: nothing from other tasks (this task is independent of Task 1).
- Produces: `alert_on_failure(context: dict) -> None` - an Airflow `on_failure_callback`,
  consumed by Task 3's DAG file as `default_args["on_failure_callback"]`. Also produces
  `build_create_table_statement() -> str` and `build_insert_statement(dag_id: str, task_id:
  str, run_id: str, try_number: int, exception_str: str) -> str`, the two pure/testable
  SQL-building functions `alert_on_failure` calls internally.

- [ ] **Step 1: Write the failing tests**

Create `qc-lakehouse/orchestration/tests/test_alerting.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd qc-lakehouse && .venv/bin/python -m pytest orchestration/tests/test_alerting.py -v`
Expected: `ModuleNotFoundError: No module named 'alerting'`

- [ ] **Step 3: Implement `alerting.py`**

Create `qc-lakehouse/orchestration/dags/alerting.py`:

```python
# qc-lakehouse/orchestration/dags/alerting.py
"""on_failure_callback for both qc_lakehouse DAGs: writes a structured failure record to
qc_dev.ops.alerts via a single stateless call to the Databricks SQL Statement Execution API -
deliberately NOT Databricks Connect. This session live-verified (2026-09-17/18, the
generate_benchmark_orders.py and apply_layouts.py incidents) that Databricks Connect sessions
die under duration/idle limits; a failure callback must be fast and must never hold a
long-lived Spark session open, so it uses the databricks_default connection's existing token
via bare `requests` instead - no new Python dependency, no session to keep alive.
"""
from __future__ import annotations

import requests
from airflow.sdk.bases.hook import BaseHook

DATABRICKS_CONN_ID = "databricks_default"
WAREHOUSE_ID = "ca865a4ef1668613"


def build_create_table_statement() -> str:
    return (
        "CREATE TABLE IF NOT EXISTS qc_dev.ops.alerts ("
        "dag_id STRING, task_id STRING, run_id STRING, try_number INT, "
        "exception STRING, alerted_at TIMESTAMP)"
    )


def build_insert_statement(
    dag_id: str, task_id: str, run_id: str, try_number: int, exception_str: str,
) -> str:
    escaped_exception = exception_str.replace("'", "''")
    return (
        "INSERT INTO qc_dev.ops.alerts VALUES ("
        f"'{dag_id}', '{task_id}', '{run_id}', {try_number}, "
        f"'{escaped_exception}', current_timestamp())"
    )


def _execute_statement(host: str, token: str, statement: str) -> None:
    response = requests.post(
        f"https://{host}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}"},
        json={"warehouse_id": WAREHOUSE_ID, "statement": statement, "wait_timeout": "10s"},
        timeout=15,
    )
    response.raise_for_status()


def alert_on_failure(context: dict) -> None:
    """Airflow on_failure_callback signature: receives the task instance context dict."""
    task_instance = context["task_instance"]
    exception_str = str(context.get("exception", "unknown error"))

    connection = BaseHook.get_connection(DATABRICKS_CONN_ID)
    host = connection.host
    token = connection.password or connection.extra_dejson.get("token")

    try:
        _execute_statement(host, token, build_create_table_statement())
        _execute_statement(
            host, token,
            build_insert_statement(
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                run_id=task_instance.run_id,
                try_number=task_instance.try_number,
                exception_str=exception_str,
            ),
        )
    except Exception as exc:  # noqa: BLE001 - a broken alert must never mask the real failure
        print(f"alerting: failed to write failure record to qc_dev.ops.alerts: {exc!r}")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd qc-lakehouse && .venv/bin/python -m pytest orchestration/tests/test_alerting.py -v`
Expected: 3 passed.

- [ ] **Step 5: Lint**

Run: `cd qc-lakehouse && .venv/bin/python -m ruff check orchestration/dags/alerting.py orchestration/tests/test_alerting.py`
Expected: `All checks passed!`

- [ ] **Step 6: Commit**

```bash
cd /Users/anuragk/randomproject
git add qc-lakehouse/orchestration/dags/alerting.py qc-lakehouse/orchestration/tests/test_alerting.py
git commit -m "Add on_failure_callback alerting for the qc_lakehouse DAGs

Writes a structured failure record (dag_id/task_id/run_id/try_number/exception)
to qc_dev.ops.alerts via a single stateless SQL Statement Execution API call -
not Databricks Connect, since this session live-verified that Connect sessions
die under duration/idle limits and a failure callback must be fast and never
hold a long-lived session open.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 3: Redesign the maintenance DAG - parallel per-table fan-out/fan-in, daily schedule, wired-in alerting

**Files:**
- Modify: `qc-lakehouse/orchestration/dags/qc_lakehouse_maintenance.py`
- Modify: `qc-lakehouse/orchestration/tests/test_dag.py:106-182` (all maintenance-DAG tests)

**Interfaces:**
- Consumes: Task 1's `GOLD_TABLES` tuple (duplicated literally per the Global Constraints
  note - no import path exists) and the 3 renamed job names (`optimize_gold_table`,
  `analyze_gold_table`, `vacuum_gold_table`); Task 2's `alert_on_failure` (imported directly:
  `from alerting import alert_on_failure`, since `alerting.py` lives alongside this DAG file
  in `orchestration/dags/`).
- Produces: the redesigned `qc_lakehouse_maintenance` DAG - 22 tasks (7 tables x 3 operations
  + 1 report task), consumed only by Databricks/Airflow itself, not by any later task in this
  plan.

- [ ] **Step 1: Write the failing tests**

First, add an explicit import path for `alerting.py` near the top of
`qc-lakehouse/orchestration/tests/test_dag.py` (do not rely on `DagBag`'s import of the DAG
module having already added `DAGS_DIR` to `sys.path` as an implicit side effect - that's
fragile and version-dependent; Task 2's own test file already does this explicitly, so this
mirrors that pattern):

```python
import sys

sys.path.insert(0, str(DAGS_DIR))
```

Add this line immediately after the existing `DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"`
line (line 5), and add `import sys` alongside the existing `from pathlib import Path` at the
top of the file.

Then, replace the four maintenance-DAG-specific tests
(`test_maintenance_dag_has_exactly_three_tasks`,
`test_maintenance_dag_dependency_chain_is_sequential`,
`test_maintenance_dag_has_no_schedule`, `test_maintenance_dag_job_names_match_databricks_yml`
- currently at lines 116-182) with:

```python
GOLD_TABLES = (
    "fct_orders", "fct_deliveries", "dim_customer", "dim_restaurant",
    "dim_rider", "dim_zone", "dim_date",
)
OPERATIONS = ("optimize", "analyze", "vacuum")


def test_maintenance_dag_has_one_chain_per_table_plus_a_report_task():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    assert dag is not None
    expected_task_ids = {
        f"{operation}_{table}" for table in GOLD_TABLES for operation in OPERATIONS
    }
    expected_task_ids.add("maintenance_report")
    assert set(dag.task_ids) == expected_task_ids


def test_maintenance_dag_each_table_chain_is_sequential_and_independent():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    for table in GOLD_TABLES:
        optimize = dag.get_task(f"optimize_{table}")
        analyze = dag.get_task(f"analyze_{table}")
        vacuum = dag.get_task(f"vacuum_{table}")
        assert optimize.downstream_task_ids == {f"analyze_{table}"}
        assert analyze.downstream_task_ids == {f"vacuum_{table}"}
        assert vacuum.downstream_task_ids == {"maintenance_report"}
        # Independent of every other table's chain - optimize_fct_orders must not depend on
        # or block dim_customer's chain, proving these run in parallel, not sequentially.
        assert optimize.upstream_task_ids == set()


def test_maintenance_dag_report_task_fans_in_from_every_chain_with_all_done():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    report = dag.get_task("maintenance_report")
    expected_upstream = {f"vacuum_{table}" for table in GOLD_TABLES}
    assert report.upstream_task_ids == expected_upstream
    assert report.trigger_rule == "all_done"


def test_maintenance_dag_has_a_daily_schedule():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    assert dag.schedule == "@daily"
    assert dag.timetable.can_be_scheduled is True


def test_maintenance_dag_every_task_has_the_alert_callback():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    from alerting import alert_on_failure

    for task_id in dag.task_ids:
        task = dag.get_task(task_id)
        assert task.on_failure_callback == alert_on_failure


def test_maintenance_dag_job_names_and_table_params_match_databricks_yml():
    # Extends the pre-existing job-name/databricks.yml cross-check (task-5 review, Important
    # finding #3) to the renamed jobs, and adds a new assertion this redesign needs: every
    # task's python_params must pass the correct --table for its own chain, not silently
    # reuse the job's fct_orders default every time.
    import yaml

    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")

    with BUNDLE_PATH.open() as f:
        bundle = yaml.safe_load(f)
    jobs = bundle["resources"]["jobs"]

    expected_job_names = {"optimize_gold_table", "analyze_gold_table", "vacuum_gold_table"}
    for job_name in expected_job_names:
        assert job_name in jobs, f"{job_name} is missing from databricks.yml's resources.jobs"
        assert jobs[job_name]["name"] == job_name

    for table in GOLD_TABLES:
        for operation in OPERATIONS:
            task = dag.get_task(f"{operation}_{table}")
            assert task.job_name == f"{operation}_gold_table"
            assert task.job_name in jobs
            assert task.python_params == ["--operation", operation, "--table", table]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd qc-lakehouse && .venv/bin/python -m pytest orchestration/tests/test_dag.py -v`
Expected: multiple FAILs (`test_maintenance_dag_has_one_chain_per_table_plus_a_report_task`
fails because the DAG still only has the old 3 tasks, etc.) - not import errors, since
`test_dag.py` itself still imports fine at this point.

- [ ] **Step 3: Implement the redesigned DAG**

Replace the full contents of `qc-lakehouse/orchestration/dags/qc_lakehouse_maintenance.py`
with:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd qc-lakehouse && .venv/bin/python -m pytest orchestration/tests/test_dag.py -v`
Expected: all tests pass, including the untouched pipeline-DAG tests above them in the same
file (confirms this change didn't break `qc_lakehouse_pipeline`'s own tests).

- [ ] **Step 5: Lint**

Run: `cd qc-lakehouse && .venv/bin/python -m ruff check orchestration/dags/qc_lakehouse_maintenance.py orchestration/tests/test_dag.py`
Expected: `All checks passed!`

- [ ] **Step 6: Restart the local Airflow container and confirm the DAG parses with no import errors**

```bash
cd qc-lakehouse
docker compose -f orchestration/docker-compose.yml restart airflow
```

Then, after giving it a few seconds to reload the dag-processor:

```bash
docker compose -f orchestration/docker-compose.yml exec -T airflow airflow dags list
```

Expected: `qc_lakehouse_maintenance` and `qc_lakehouse_pipeline` both listed, no import-error
output.

- [ ] **Step 7: Commit**

```bash
cd /Users/anuragk/randomproject
git add qc-lakehouse/orchestration/dags/qc_lakehouse_maintenance.py qc-lakehouse/orchestration/tests/test_dag.py
git commit -m "Redesign qc_lakehouse_maintenance: parallel per-table chains, daily schedule, alerting

Was a single fct_orders-only, manual-trigger-only, 3-task sequential chain.
Now: one optimize->analyze->vacuum chain per gold table (7 tables, running
in parallel - independent tables have no reason to serialize), fanning into
a single maintenance_report task via trigger_rule=all_done (runs regardless
of which chains succeeded or failed), on a daily schedule, with every task
alerting on failure via the new alerting.alert_on_failure callback.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```
