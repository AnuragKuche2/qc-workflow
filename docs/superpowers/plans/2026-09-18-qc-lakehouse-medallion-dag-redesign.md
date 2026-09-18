# QC Lakehouse Medallion-Shaped Orchestration Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge `qc_lakehouse_pipeline` and `qc_lakehouse_maintenance` into one Airflow DAG whose task graph visibly matches the medallion architecture (bronze ingest -> build+test each silver/gold layer -> maintain gold -> report), with real per-layer fail-fast gating and dynamic task mapping for gold-table maintenance, instead of two DAGs that trigger opaque jobs with no gating between layers.

**Architecture:** Three new Databricks Jobs (`dbt_build_test_staging`/`_intermediate`/`_marts`, each running `dbt build --select <layer>`) replace the single `dbt_run`+`dbt_test` pair. A new `orchestration/dags/maintenance.py` module builds the OPTIMIZE/ANALYZE/VACUUM stage using Airflow dynamic task mapping (`.expand()`) inside a `TaskGroup`, replacing the hand-unrolled 7-table loop. `qc_lakehouse_pipeline.py` is rewritten to chain all of this sequentially and the old `qc_lakehouse_maintenance.py` is deleted.

**Tech Stack:** Apache Airflow 3.3.1 (`apache-airflow-providers-databricks`), dbt-databricks 1.12.5, Databricks Asset Bundles (`databricks.yml`), pytest.

**Spec:** `docs/superpowers/specs/2026-09-18-qc-lakehouse-medallion-dag-redesign.md`

## Global Constraints

- `max_concurrent_runs: 1` stays on `optimize_gold_table`/`analyze_gold_table`/`vacuum_gold_table` in `databricks.yml` - Databricks Free Edition allows a maximum of 5 concurrent job tasks account-wide; exceeding it shuts down all workspace compute for the rest of the day (`datapipelineplan.md` ADR-001/C16). Do not remove or raise this.
- Import `TaskGroup` from `airflow.sdk`, not `airflow.utils.task_group` (the latter still works under the installed `apache-airflow==3.3.1` but emits `DeprecatedImportWarning` - confirmed live this session).
- The DAG-level `DeadlineAlert` must use `DeadlineReference.DAGRUN_QUEUED_AT`, never `DAGRUN_LOGICAL_DATE` - live-verified this session that `LOGICAL_DATE` creates zero `Deadline` rows for a manually-triggered run (this DAG's trigger path has no `logical_date`).
- `orchestration/dags/alerting.py` is not modified by this plan. `alert_on_failure` stays wired via `DEFAULT_ARGS["on_failure_callback"]`, which Airflow applies to every task automatically, including mapped task instances.
- Each new dbt Databricks Job runs `dbt build --select <layer>` (build+test interleaved, dbt's own fail-fast), never `dbt run` followed by a separate `dbt test`.
- `GOLD_TABLES` in the new `orchestration/dags/maintenance.py` must be byte-identical to the tuple already in `perf_lab/run_maintenance.py`: `("fct_orders", "fct_deliveries", "dim_customer", "dim_restaurant", "dim_rider", "dim_zone", "dim_date")`. `orchestration/`'s `.venv-airflow` has no import path to `perf_lab/`, so this is a deliberate hand-kept duplicate, not an oversight to "DRY up."
- The merged DAG keeps `dag_id="qc_lakehouse_pipeline"`, `schedule="@weekly"`, `start_date=datetime(2026, 1, 1, tzinfo=UTC)`, `catchup=False` - confirmed with the user: this preserves the "at least one scheduled DAG" signal from the prior orchestration-hardening work.
- Run orchestration tests via `.venv-airflow/bin/python -m pytest orchestration/tests/ -q` from `qc-lakehouse/`. Current baseline before this plan: 23 passed.
- Run the main test suite via `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home .venv/bin/python -m pytest -q` from `qc-lakehouse/`. Current baseline before this plan: 122 passed, 36 failed (pre-existing, unrelated local-Spark-socket failures in `test_fact_entities.py`/`test_fact_writer.py` - do not try to fix these, just confirm the count doesn't grow).

---

### Task 1: Replace `dbt_run`/`dbt_test` with three per-layer `dbt build` jobs in `databricks.yml`

**Files:**
- Modify: `qc-lakehouse/databricks.yml`

**Interfaces:**
- Produces: three new job names referenced by Task 3's DAG rewrite: `dbt_build_test_staging`, `dbt_build_test_intermediate`, `dbt_build_test_marts`.

This task has no Python to unit-test; verification is `databricks bundle validate` plus a live `dbt ls` sanity check (both run as part of this task, see Step 3).

- [ ] **Step 1: Remove the `dbt_run` and `dbt_test` job blocks**

Open `qc-lakehouse/databricks.yml`. Delete the entire `dbt_run:` job block and the entire `dbt_test:` job block (each currently spans from its `dbt_run:`/`dbt_test:` line down through its `environments:` section, ending just before the next job's `# NOTE: adapted from the brief's draft...` comment or `optimize_gold_table:` line).

- [ ] **Step 2: Add the three per-layer jobs in their place**

Insert this in place of the deleted blocks (same indentation level as the other jobs under `resources: jobs:`):

```yaml
    dbt_build_test_staging:
      name: dbt_build_test_staging
      tasks:
        # dbt_task has no field to reference a Databricks secret directly, and
        # {{secrets/scope/key}} is not resolved inside dbt_task commands (confirmed live - see
        # scripts/resolve_pii_salt.py's docstring). This task bridges dbutils.secrets.get -> a
        # task value the dbt_task below can reference via --vars.
        # CAVEAT: the resolved value is NOT protected end-to-end - the dbt_task below echoes
        # its own resolved shell command (including the substituted value) into its run
        # output/logs in plaintext, visible to anyone with read/API access to that job run.
        # Known, accepted limitation for this single-user Free Edition workspace - see
        # scripts/resolve_pii_salt.py for details.
        - task_key: resolve_secrets
          spark_python_task:
            python_file: ./scripts/resolve_pii_salt.py
          environment_key: qc_lakehouse_secrets_env
        - task_key: main
          depends_on:
            - task_key: resolve_secrets
          dbt_task:
            project_directory: ./dbt/qc_lakehouse
            commands:
              - "dbt build --select staging --vars '{\"pii_hash_salt\": \"{{tasks.resolve_secrets.values.pii_hash_salt}}\"}'"
            warehouse_id: ${var.warehouse_id}
            catalog: qc_dev
            schema: silver
          environment_key: qc_lakehouse_dbt_env
      environments:
        - environment_key: qc_lakehouse_secrets_env
          spec:
            environment_version: "3"
        - environment_key: qc_lakehouse_dbt_env
          spec:
            environment_version: "3"
            dependencies:
              - dbt-databricks>=1.12.5

    dbt_build_test_intermediate:
      name: dbt_build_test_intermediate
      tasks:
        # Same bridging task as dbt_build_test_staging's - duplicated rather than shared
        # because task values only propagate within one job run's own task DAG, and these are
        # separate top-level jobs (the pipeline DAG triggers them independently in sequence).
        # Same plaintext-in-logs CAVEAT as dbt_build_test_staging - see that job's comment.
        - task_key: resolve_secrets
          spark_python_task:
            python_file: ./scripts/resolve_pii_salt.py
          environment_key: qc_lakehouse_secrets_env
        - task_key: main
          depends_on:
            - task_key: resolve_secrets
          dbt_task:
            project_directory: ./dbt/qc_lakehouse
            commands:
              - "dbt build --select intermediate --vars '{\"pii_hash_salt\": \"{{tasks.resolve_secrets.values.pii_hash_salt}}\"}'"
            warehouse_id: ${var.warehouse_id}
            catalog: qc_dev
            schema: silver
          environment_key: qc_lakehouse_dbt_env
      environments:
        - environment_key: qc_lakehouse_secrets_env
          spec:
            environment_version: "3"
        - environment_key: qc_lakehouse_dbt_env
          spec:
            environment_version: "3"
            dependencies:
              - dbt-databricks>=1.12.5

    dbt_build_test_marts:
      name: dbt_build_test_marts
      tasks:
        # Same bridging task as dbt_build_test_staging's - see that job's comments for the
        # duplication rationale and the plaintext-in-logs CAVEAT.
        - task_key: resolve_secrets
          spark_python_task:
            python_file: ./scripts/resolve_pii_salt.py
          environment_key: qc_lakehouse_secrets_env
        - task_key: main
          depends_on:
            - task_key: resolve_secrets
          dbt_task:
            project_directory: ./dbt/qc_lakehouse
            commands:
              - "dbt build --select marts --vars '{\"pii_hash_salt\": \"{{tasks.resolve_secrets.values.pii_hash_salt}}\"}'"
            warehouse_id: ${var.warehouse_id}
            catalog: qc_dev
            schema: silver
          environment_key: qc_lakehouse_dbt_env
      environments:
        - environment_key: qc_lakehouse_secrets_env
          spec:
            environment_version: "3"
        - environment_key: qc_lakehouse_dbt_env
          spec:
            environment_version: "3"
            dependencies:
              - dbt-databricks>=1.12.5
```

- [ ] **Step 3: Verify**

```bash
cd qc-lakehouse
databricks bundle validate
```
Expected: `Validation OK!` (already confirmed reproducible in this environment).

Then re-confirm the selectors resolve to the right models (already verified once this session - re-run to prove the checked-in file matches):
```bash
set -a && . ./.env && set +a
DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt ls --project-dir dbt/qc_lakehouse --target qc_dev --select staging --resource-type model
DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt ls --project-dir dbt/qc_lakehouse --target qc_dev --select intermediate --resource-type model
DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt ls --project-dir dbt/qc_lakehouse --target qc_dev --select marts --resource-type model
```
Expected: staging lists 13 `stg_*` models, intermediate lists `int_order_economics`/`int_order_matching`, marts lists the 5 `dim_*` + 2 `fct_*` models.

- [ ] **Step 4: Commit**

```bash
git add qc-lakehouse/databricks.yml
git commit -m "Replace dbt_run/dbt_test with three per-layer dbt build+test jobs

dbt build interleaves model build and test per dbt's own graph order, so a
staging test failure now stops before intermediate/marts are attempted -
the previous dbt_run/dbt_test pair built everything before testing anything."
```

---

### Task 2: Add `orchestration/dags/maintenance.py` with dynamic-task-mapped gold-table maintenance

**Files:**
- Create: `qc-lakehouse/orchestration/dags/maintenance.py`
- Test: `qc-lakehouse/orchestration/tests/test_maintenance.py`

**Interfaces:**
- Consumes: nothing from other tasks in this plan.
- Produces (used by Task 3): `GOLD_TABLES: tuple[str, ...]`, `OPERATIONS: tuple[str, ...] = ("optimize", "analyze", "vacuum")`, `JOB_NAMES: dict[str, str]` (mapping each operation to its Databricks Job name: `optimize_gold_table`/`analyze_gold_table`/`vacuum_gold_table` - unchanged from the current `qc_lakehouse_maintenance.py`), `build_maintenance_tasks(dag) -> BaseOperator` (builds a `TaskGroup(group_id="maintenance")` containing three dynamically-mapped `DatabricksRunNowOperator` stages and returns the final VACUUM mapped task, for the caller to chain `>> report`), `check_maintenance_results(**context) -> None` (raises `AirflowException` if any task instance in the whole DAG run - other than `report` itself - has `state == "failed"`; formats mapped-task failures as `f"{task_id}[{map_index}]"` so a failure message names the actual table).

- [ ] **Step 1: Write the failing tests**

Create `qc-lakehouse/orchestration/tests/test_maintenance.py`:

```python
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from airflow import DAG
from airflow.exceptions import AirflowException

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))

from maintenance import (  # noqa: E402
    GOLD_TABLES,
    JOB_NAMES,
    OPERATIONS,
    build_maintenance_tasks,
    check_maintenance_results,
)


def _build_test_dag():
    with DAG(
        dag_id="test_maintenance_only",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        catchup=False,
    ) as dag:
        final_task = build_maintenance_tasks(dag)
    return dag, final_task


def test_gold_tables_and_operations_are_the_expected_seven_tables_three_ops():
    assert GOLD_TABLES == (
        "fct_orders", "fct_deliveries", "dim_customer", "dim_restaurant",
        "dim_rider", "dim_zone", "dim_date",
    )
    assert OPERATIONS == ("optimize", "analyze", "vacuum")
    assert JOB_NAMES == {
        "optimize": "optimize_gold_table",
        "analyze": "analyze_gold_table",
        "vacuum": "vacuum_gold_table",
    }


def test_build_maintenance_tasks_creates_one_mapped_task_per_operation():
    dag, _ = _build_test_dag()
    for operation in OPERATIONS:
        task = dag.get_task(f"maintenance.{operation}")
        assert task.job_name == JOB_NAMES[operation]
        # A mapped operator's expand input carries one python_params list per table.
        expanded = task.expand_input.value["python_params"]
        assert expanded == [
            ["--operation", operation, "--table", table] for table in GOLD_TABLES
        ]


def test_build_maintenance_tasks_chains_optimize_then_analyze_then_vacuum():
    dag, final_task = _build_test_dag()
    optimize = dag.get_task("maintenance.optimize")
    analyze = dag.get_task("maintenance.analyze")
    vacuum = dag.get_task("maintenance.vacuum")
    assert optimize.downstream_task_ids == {"maintenance.analyze"}
    assert analyze.downstream_task_ids == {"maintenance.vacuum"}
    assert final_task.task_id == "maintenance.vacuum"


def test_check_maintenance_results_raises_with_map_index_when_a_mapped_task_failed():
    class _FakeTI:
        def __init__(self, task_id, state, map_index=-1):
            self.task_id = task_id
            self.state = state
            self.map_index = map_index

    class _FakeDagRun:
        def get_task_instances(self):
            return [
                _FakeTI("generate_reference_data", "success"),
                _FakeTI("maintenance.optimize", "success", map_index=0),
                _FakeTI("maintenance.optimize", "failed", map_index=1),
                _FakeTI("report", "running"),
            ]

    with pytest.raises(AirflowException, match=r"maintenance\.optimize\[1\]"):
        check_maintenance_results(dag_run=_FakeDagRun())


def test_check_maintenance_results_passes_when_everything_succeeded():
    class _FakeTI:
        def __init__(self, task_id, state, map_index=-1):
            self.task_id = task_id
            self.state = state
            self.map_index = map_index

    class _FakeDagRun:
        def get_task_instances(self):
            return [
                _FakeTI("generate_reference_data", "success"),
                _FakeTI("maintenance.vacuum", "success", map_index=6),
                _FakeTI("report", "running"),
            ]

    check_maintenance_results(dag_run=_FakeDagRun())  # must not raise
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd qc-lakehouse
.venv-airflow/bin/python -m pytest orchestration/tests/test_maintenance.py -v
```
Expected: `ModuleNotFoundError: No module named 'maintenance'` (the file doesn't exist yet).

- [ ] **Step 3: Write `orchestration/dags/maintenance.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd qc-lakehouse
.venv-airflow/bin/python -m pytest orchestration/tests/test_maintenance.py -v
```
Expected: 5 passed. If `expand_input.value` is not the correct public attribute for reading a mapped operator's expand kwargs under this installed Airflow version, inspect the actual `MappedOperator` object live (`python -c "from airflow.models.mappedoperator import MappedOperator; help(MappedOperator)"` or equivalent) and adjust the test to the confirmed real attribute - do not guess past a first failure here, this project has a documented history of Airflow-version API surprises.

- [ ] **Step 5: Commit**

```bash
git add qc-lakehouse/orchestration/dags/maintenance.py qc-lakehouse/orchestration/tests/test_maintenance.py
git commit -m "Add maintenance.py: dynamic-task-mapped OPTIMIZE/ANALYZE/VACUUM

Replaces the hand-unrolled 7-table x 3-operation Python loop with Airflow's
dynamic task mapping (.expand()) inside a TaskGroup - the actual tool built
for applying the same operation across many entities."
```

---

### Task 3: Rewrite `qc_lakehouse_pipeline.py` as the merged medallion DAG; delete the old maintenance DAG

**Files:**
- Modify: `qc-lakehouse/orchestration/dags/qc_lakehouse_pipeline.py`
- Delete: `qc-lakehouse/orchestration/dags/qc_lakehouse_maintenance.py`
- Modify: `qc-lakehouse/orchestration/tests/test_dag.py`

**Interfaces:**
- Consumes: `maintenance.build_maintenance_tasks(dag)` and `maintenance.check_maintenance_results` from Task 2; the three job names `dbt_build_test_staging`/`_intermediate`/`_marts` from Task 1.

- [ ] **Step 1: Write the failing tests**

Replace the entire contents of `qc-lakehouse/orchestration/tests/test_dag.py` with:

```python
import sys
from pathlib import Path

import pytest
from airflow.exceptions import AirflowException

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))
BUNDLE_PATH = Path(__file__).resolve().parents[2] / "databricks.yml"

GOLD_TABLES = (
    "fct_orders", "fct_deliveries", "dim_customer", "dim_restaurant",
    "dim_rider", "dim_zone", "dim_date",
)
OPERATIONS = ("optimize", "analyze", "vacuum")


def _dagbag():
    from airflow.models import DagBag

    # Airflow 3.3.1's DagBag.__init__ no longer accepts `include_examples` (verified via
    # inspect.signature(DagBag.__init__) against the installed version) - it isn't needed
    # here anyway since dag_folder is scoped to this project's own single-DAG directory.
    return DagBag(dag_folder=str(DAGS_DIR))


def test_dag_imports_without_errors():
    dagbag = _dagbag()
    assert dagbag.import_errors == {}


def test_maintenance_dag_no_longer_exists():
    dagbag = _dagbag()
    assert dagbag.get_dag("qc_lakehouse_maintenance") is None


def test_pipeline_dag_has_the_expected_medallion_shaped_tasks():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert dag is not None
    expected_task_ids = {
        "generate_reference_data",
        "generate_fact_data",
        "dbt_build_test_staging",
        "dbt_build_test_intermediate",
        "dbt_build_test_marts",
        "report",
    }
    for operation in OPERATIONS:
        expected_task_ids.add(f"maintenance.{operation}")
    assert set(dag.task_ids) == expected_task_ids


def test_pipeline_dag_dependency_chain_is_sequential_through_each_medallion_layer():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    ref = dag.get_task("generate_reference_data")
    fact = dag.get_task("generate_fact_data")
    staging = dag.get_task("dbt_build_test_staging")
    intermediate = dag.get_task("dbt_build_test_intermediate")
    marts = dag.get_task("dbt_build_test_marts")
    optimize = dag.get_task("maintenance.optimize")
    vacuum = dag.get_task("maintenance.vacuum")
    report = dag.get_task("report")

    assert ref.downstream_task_ids == {"generate_fact_data"}
    assert fact.downstream_task_ids == {"dbt_build_test_staging"}
    assert staging.downstream_task_ids == {"dbt_build_test_intermediate"}
    assert intermediate.downstream_task_ids == {"dbt_build_test_marts"}
    assert marts.downstream_task_ids == {"maintenance.optimize"}
    assert optimize.downstream_task_ids == {"maintenance.analyze"}
    assert vacuum.downstream_task_ids == {"report"}
    assert report.downstream_task_ids == set()


def test_pipeline_dag_has_a_weekly_schedule():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert dag.schedule == "@weekly"
    assert dag.timetable.can_be_scheduled is True


def test_pipeline_dag_every_task_has_the_alert_callback():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    from alerting import alert_on_failure

    for task_id in dag.task_ids:
        task = dag.get_task(task_id)
        assert task.on_failure_callback == [alert_on_failure]


def test_all_tasks_have_retries_configured():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    for task_id in dag.task_ids:
        task = dag.get_task(task_id)
        assert task.retries == 2


def test_dag_has_deadline_alert_configured():
    # Airflow 3.0 removed the SLA feature in favor of Deadline Alerts - see the
    # DAGRUN_QUEUED_AT test below for the full reasoning, carried over unchanged from the
    # pre-redesign version of this file.
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert dag.deadline is not None
    assert len(dag.deadline) == 1


def test_deadline_alert_uses_queued_at_not_logical_date():
    from airflow.sdk.definitions.deadline import DagRunQueuedAtDeadline

    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert isinstance(dag.deadline[0].reference, DagRunQueuedAtDeadline)


def test_report_task_fans_in_from_vacuum_with_all_done():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    report = dag.get_task("report")
    assert report.upstream_task_ids == {"maintenance.vacuum"}
    assert report.trigger_rule == "all_done"
    # NOTE: DagBag loads DAG files under a generated module name, distinct from a plain
    # `import maintenance` - so the callable object it attaches to the task is never `is`
    # a separately-imported one, even though it's the same source function. Comparing by
    # name is therefore the correct check here, not object identity.
    assert report.python_callable.__name__ == "check_maintenance_results"


def test_pipeline_dag_job_names_and_layer_selectors_match_databricks_yml():
    import yaml

    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")

    with BUNDLE_PATH.open() as f:
        bundle = yaml.safe_load(f)
    jobs = bundle["resources"]["jobs"]

    for job_name in (
        "generate_reference_data", "generate_fact_data",
        "dbt_build_test_staging", "dbt_build_test_intermediate", "dbt_build_test_marts",
    ):
        assert job_name in jobs, f"{job_name} is missing from databricks.yml's resources.jobs"

    dag_job_names = {
        dag.get_task("generate_reference_data").job_name,
        dag.get_task("generate_fact_data").job_name,
        dag.get_task("dbt_build_test_staging").job_name,
        dag.get_task("dbt_build_test_intermediate").job_name,
        dag.get_task("dbt_build_test_marts").job_name,
    }
    assert dag_job_names == {
        "generate_reference_data", "generate_fact_data",
        "dbt_build_test_staging", "dbt_build_test_intermediate", "dbt_build_test_marts",
    }

    for job_name in ("optimize_gold_table", "analyze_gold_table", "vacuum_gold_table"):
        assert jobs[job_name].get("max_concurrent_runs") == 1


def test_check_maintenance_results_raises_when_a_table_task_failed():
    from maintenance import check_maintenance_results

    class _FakeTI:
        def __init__(self, task_id, state, map_index=-1):
            self.task_id = task_id
            self.state = state
            self.map_index = map_index

    class _FakeDagRun:
        def get_task_instances(self):
            return [
                _FakeTI("maintenance.optimize", "success", map_index=0),
                _FakeTI("maintenance.analyze", "failed", map_index=2),
                _FakeTI("report", "running"),
            ]

    with pytest.raises(AirflowException, match=r"maintenance\.analyze\[2\]"):
        check_maintenance_results(dag_run=_FakeDagRun())
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd qc-lakehouse
.venv-airflow/bin/python -m pytest orchestration/tests/test_dag.py -v
```
Expected: multiple failures - `dbt_run`/`dbt_test`/old maintenance-DAG assertions are gone from the test file already, so failures should instead come from the DAG still being in its pre-redesign shape (e.g. `test_pipeline_dag_has_the_expected_medallion_shaped_tasks` fails because `dag.task_ids` still contains `dbt_run`/`dbt_test`, not the new task ids).

- [ ] **Step 3: Delete the old maintenance DAG file**

```bash
git rm qc-lakehouse/orchestration/dags/qc_lakehouse_maintenance.py
```

- [ ] **Step 4: Rewrite `qc_lakehouse_pipeline.py`**

Replace its entire contents with:

```python
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
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd qc-lakehouse
.venv-airflow/bin/python -m pytest orchestration/tests/ -v
```
Expected: all tests pass, including `test_maintenance.py` from Task 2 and `test_alerting.py` (untouched). If `dag.get_task("maintenance.optimize")` doesn't resolve the way expected (e.g. the TaskGroup's task-id prefixing works differently than assumed), inspect `dag.task_ids` directly in a Python shell against the live-loaded DagBag and adjust - do not guess past a first failure.

- [ ] **Step 6: Commit**

```bash
git add qc-lakehouse/orchestration/dags/qc_lakehouse_pipeline.py qc-lakehouse/orchestration/tests/test_dag.py
git commit -m "Merge qc_lakehouse_maintenance into qc_lakehouse_pipeline

One medallion-shaped DAG: bronze ingest -> build+test staging -> build+test
intermediate -> build+test marts -> maintain gold (dynamically mapped) ->
report. Replaces two DAGs that triggered opaque jobs with no per-layer
gating - a staging test failure now blocks every downstream stage instead
of being discovered only after everything already built."
```

---

### Task 4: Update `README.md` and `PROJECTINFO.md` to describe the merged DAG

**Files:**
- Modify: `README.md`
- Modify: `PROJECTINFO.md`

**Interfaces:**
- Consumes: the final task/job names from Tasks 1-3 (no code interfaces - this is documentation only).

- [ ] **Step 1: Update the README's Mermaid diagram and Airflow section**

In `README.md`, replace the `AIRFLOW` subgraph block:

```
    subgraph AIRFLOW["Airflow orchestration (triggers only, never transforms)"]
        DAG1["qc_lakehouse_pipeline DAG\ngenerate -> dbt run -> dbt test"]
        DAG2["qc_lakehouse_maintenance DAG\nOPTIMIZE -> ANALYZE -> VACUUM"]
    end

    DAG1 -->|"triggers real\nDatabricks Jobs"| GEN
    DAG1 -->|"triggers real\nDatabricks Jobs"| STG
    DAG2 -->|"triggers real\nDatabricks Jobs"| GOLD
```

with:

```
    subgraph AIRFLOW["Airflow orchestration (triggers only, never transforms)"]
        DAG1["qc_lakehouse_pipeline DAG (@weekly)\ngenerate -> build+test staging ->\nbuild+test intermediate -> build+test marts ->\nmaintain gold (dynamically mapped) -> report"]
    end

    DAG1 -->|"triggers real\nDatabricks Jobs"| GEN
    DAG1 -->|"triggers real\nDatabricks Jobs"| STG
    DAG1 -->|"triggers real\nDatabricks Jobs"| GOLD
```

Then update the "Apache Airflow" bullet under "Tech stack" from:

```
**Apache Airflow** - two DAGs orchestrating real Databricks Jobs, idempotent task design, no
transform logic in the scheduler.
```

to:

```
**Apache Airflow** - one medallion-shaped DAG orchestrating real Databricks Jobs, with
per-layer build+test gating (a staging test failure blocks every downstream layer) and
dynamic task mapping for gold-table maintenance, idempotent task design, no transform logic
in the scheduler.
```

- [ ] **Step 2: Update `PROJECTINFO.md`'s roadmap item 2**

Find the "2. **Orchestration hardening..." roadmap bullet. Add this sentence to the end of
its paragraph (before the numbered list item ends):

```
 Update (2026-09-18): the two DAGs this produced (`qc_lakehouse_pipeline`,
`qc_lakehouse_maintenance`) were subsequently merged into one medallion-shaped DAG -
having two DAGs meant maintenance ran on its own schedule with no dependency on whether
the pipeline that produces the gold tables it maintains had ever succeeded, and the
pipeline DAG's `dbt_run`/`dbt_test` pair built the entire dbt project before testing any
of it. See `docs/superpowers/specs/2026-09-18-qc-lakehouse-medallion-dag-redesign.md`.
```

- [ ] **Step 3: Verify by reading both files back**

Confirm no other reference to `qc_lakehouse_maintenance` or `dbt_run`/`dbt_test` (as job
names) remains in either file.

```bash
grep -n "qc_lakehouse_maintenance\|dbt_run\|dbt_test" README.md PROJECTINFO.md
```
Expected: no matches (or only matches that are clearly historical narrative referring to
what existed before, not current-state claims - review any hit by hand).

- [ ] **Step 4: Commit**

```bash
git add README.md PROJECTINFO.md
git commit -m "Update docs for the merged medallion-shaped Airflow DAG"
```

---

## Post-implementation live verification (not an SDD task - done by the orchestrating session after all 4 tasks pass final review)

This project's established practice is to live-validate orchestration changes, not just
unit-test them. Before offering the finishing-a-development-branch menu:

1. `databricks bundle deploy` from `qc-lakehouse/` to push the new/changed jobs.
2. Trigger `qc_lakehouse_pipeline` for real (Airflow UI or CLI) and watch it run to
   completion, confirming in the Grid view that the `maintenance` TaskGroup shows mapped
   instances (not 21 flat tasks) and that `report` correctly reflects the run's outcome.
3. If time allows, deliberately break one thing (e.g. a bad `--select` typo temporarily)
   to confirm a staging-layer failure actually blocks intermediate/marts/maintenance
   rather than silently continuing - this is the core behavior this whole redesign exists
   to fix, so it should be observed once, live, not just asserted by a mocked unit test.
