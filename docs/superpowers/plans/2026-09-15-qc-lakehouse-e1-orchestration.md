# QC Lakehouse Sub-project E1 (Orchestration) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dockerized Airflow (SQLite + SequentialExecutor) triggers 4 Databricks Jobs -
`generate_reference_data`, `generate_fact_data` (Sub-project B1's existing generation
scripts), `dbt_run`, `dbt_test` (Sub-project C's dbt project against the `qc_dev` target) -
via `DatabricksRunNowOperator`, in a single manually-triggered DAG with retries and SLAs.

**Architecture:** A Databricks Asset Bundle (`databricks.yml`) deploys the 4 jobs to serverless
compute - the two Python generation scripts run as `spark_python_task`s with the
`qc_lakehouse` wheel as a library dependency; dbt run/test use dbt's native Databricks Jobs
task type. Airflow, in a single Docker container (`airflow standalone`, SQLite +
`SequentialExecutor` - no Postgres container, resolving a real conflict with this project's
"no local Postgres" constraint), only triggers and polls those jobs; it never runs Spark or
dbt itself. A new isolated `.venv-airflow` (mirroring Sub-project A's `.venv-databricks`
precedent) holds `apache-airflow`/`apache-airflow-providers-databricks` locally, used only to
structurally validate the DAG file via `pytest` without needing Docker running.

**Tech Stack:** Databricks Asset Bundles (Databricks CLI), Apache Airflow +
`apache-airflow-providers-databricks`, Docker, `uv`.

**Spec:** `docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md`, section 9.

## Global Constraints

- This plan implements **E1 only**: Airflow triggering B1 (reference-data + W1a fact-table
  generation) and C (dbt run/test). **B2/W1b** (streaming ingestion) and **E2** (a future
  addition to this same DAG to trigger/monitor B2 once it exists) are explicitly out of scope.
- Fixed job-name contract, used identically in `databricks.yml`'s job resource names and the
  Airflow DAG's `DatabricksRunNowOperator(job_name=...)` calls: `generate_reference_data`,
  `generate_fact_data`, `dbt_run`, `dbt_test`.
- Airflow: SQLite + `SequentialExecutor`, single Docker container, no Postgres container.
- DAG trigger: `schedule=None` (manual trigger only) - the generator is deterministic (fixed
  seed), so a cron schedule would just regenerate identical data and burn Free Edition credits
  for no benefit.
- Every Databricks Job task runs on serverless compute - never specify `new_cluster` or
  `existing_cluster_id` anywhere in `databricks.yml`, matching the project-wide "no
  provisioned clusters" constraint.
- Compute always happens on Databricks; the Airflow container never imports `pyspark` or runs
  `dbt` itself - it only calls the Databricks Jobs API and polls run status.
- `apache-airflow`/`apache-airflow-providers-databricks` live in a new isolated
  `.venv-airflow`, never added to the main `pyproject.toml` dependencies - Airflow's own
  dependency constraints are notorious for conflicting with an unrelated project's pinned
  `pyspark`/`dbt-databricks` versions, the same reason Sub-project A isolated
  `databricks-connect` into `.venv-databricks` rather than the main venv.
- **Exact version/schema currency is not guaranteed here and must be confirmed live during
  implementation, not assumed correct from this plan alone** - the same category of
  platform-vs-docs gotcha Sub-project W1a (Python-UDF sandbox failure) and Sub-project C (dbt
  double-limit parse error) both hit. Concretely: the Databricks Asset Bundle YAML field names
  in Task 3, the `apache/airflow` Docker image tag in Task 6, and the exact
  `apache-airflow`/`apache-airflow-providers-databricks` versions `uv` resolves in Task 2 are
  all best-effort as of this plan's writing. If `databricks bundle validate` rejects a field
  name, run `databricks bundle schema` to get the current schema and fix the field name to
  match - that is expected schema drift, not a logic bug to work around.
- Never use an em dash character anywhere (code, YAML, comments, commit messages) - plain
  hyphen only.
- Never add `Co-Authored-By`/`Claude-Session` trailers to any commit.

---

### Task 1: Databricks-Job-aware Spark session detection

Addresses the open risk flagged in the design spec: `databricks_session.py` currently always
builds a Databricks Connect (external-client) session, which is unusual to use from *inside* a
Databricks Job already running on Databricks serverless compute. Resolve it defensively now,
locally and testably, before anything in this plan depends on live Databricks Jobs behavior.

**Files:**
- Modify: `qc-lakehouse/src/qc_lakehouse/databricks_session.py`
- Test: `qc-lakehouse/tests/test_databricks_session.py`

**Interfaces:**
- Produces: `is_running_on_databricks() -> bool` in `qc_lakehouse.databricks_session` -
  consumed by `build_databricks_session` in this same task, and available for any later task
  that needs the same detection.
- `build_databricks_session(settings: Settings)` keeps its existing signature and return type
  (a Spark session) - callers in `scripts/generate_reference_data.py` and
  `scripts/generate_fact_data.py` need no changes.

- [ ] **Step 1: Write the failing test for the detection helper**

Add to `qc-lakehouse/tests/test_databricks_session.py`:

```python
from qc_lakehouse.databricks_session import ensure_schema_exists, is_running_on_databricks


def test_is_running_on_databricks_false_when_env_var_unset(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert is_running_on_databricks() is False


def test_is_running_on_databricks_true_when_env_var_set(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4")
    assert is_running_on_databricks() is True
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_databricks_session.py -v`
Expected: `FAIL` - `ImportError: cannot import name 'is_running_on_databricks'`

- [ ] **Step 3: Implement the detection helper and wire it into build_databricks_session**

Replace the full contents of `qc-lakehouse/src/qc_lakehouse/databricks_session.py`:

```python
from __future__ import annotations

import os
from typing import Protocol

from qc_lakehouse.config import Settings


class SqlRunner(Protocol):
    def sql(self, query: str): ...


def ensure_schema_exists(session: SqlRunner, catalog: str, schema: str) -> None:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def is_running_on_databricks() -> bool:
    """True when this process is already executing on Databricks (e.g. as a Databricks Job
    task), detected via DATABRICKS_RUNTIME_VERSION - a variable Databricks sets automatically
    on every job/cluster runtime and that never exists on a local machine."""
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def build_databricks_session(settings: Settings):
    """Build a Spark session appropriate to where this process is running.

    Locally (e.g. a dev machine or CI), builds a Databricks Connect serverless session for
    the workspace in `settings` - the workspace host is passed explicitly via
    `.host(settings.databricks_host)` so the session always connects to the workspace named
    by `settings`, regardless of what Databricks profile or environment variables happen to
    be ambient.

    When already running ON Databricks (detected via `is_running_on_databricks()` - true
    inside a Databricks Job task on serverless compute), Databricks Connect is unnecessary
    and unusual to use recursively against the same workspace the process is already in -
    build a native Spark session instead via plain `SparkSession.builder.getOrCreate()`.
    """
    if is_running_on_databricks():
        from pyspark.sql import SparkSession

        session = SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksSession

        session = (
            DatabricksSession.builder.host(settings.databricks_host).serverless(True).getOrCreate()
        )

    ensure_schema_exists(session, settings.databricks_catalog, settings.databricks_schema)
    return session
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_databricks_session.py -v`
Expected: `PASS` - all tests green (the existing `test_ensure_schema_exists_issues_create_schema_if_not_exists` test must still pass unchanged).

- [ ] **Step 5: Run the full existing suite and lint to confirm no regression**

Run: `uv run ruff check src/qc_lakehouse/databricks_session.py tests/test_databricks_session.py && uv run pytest -q`
Expected: ruff clean, all tests pass (same count as before plus the 2 new tests).

- [ ] **Step 6: Commit**

```bash
git add qc-lakehouse/src/qc_lakehouse/databricks_session.py qc-lakehouse/tests/test_databricks_session.py
git commit -m "Detect when running as a Databricks Job and skip Databricks Connect

build_databricks_session always built a Databricks Connect (external-client)
session, which is unusual to use from inside a Databricks Job already
running on Databricks serverless compute. Added is_running_on_databricks()
(via the DATABRICKS_RUNTIME_VERSION env var Databricks sets automatically)
and a native SparkSession.builder.getOrCreate() fallback for that case,
resolving the open risk flagged in the Sub-project E1 design spec before
any job actually depends on this behavior."
```

---

### Task 2: Isolated `.venv-airflow` for local Airflow tooling

**Files:**
- Modify: `qc-lakehouse/Makefile`
- Modify: `qc-lakehouse/.gitignore`

**Interfaces:**
- Produces: `.venv-airflow/bin/python` with `apache-airflow` and
  `apache-airflow-providers-databricks` installed - consumed by Task 5's DAG structural test
  and the `test-orchestration` Makefile target this task creates.

- [ ] **Step 1: Add `.venv-airflow/` to gitignore**

Add to `qc-lakehouse/.gitignore`, after the `.venv-databricks/` line:

```
.venv-airflow/
```

- [ ] **Step 2: Add install-airflow and test-orchestration Makefile targets**

Add to `qc-lakehouse/Makefile`, after the existing `install-dbt`/`smoke-dbt` targets and before
`smoke-all`:

```makefile
# Isolated venv for Airflow tooling, same reasoning as install-databricks's
# .venv-databricks: Airflow's own dependency constraints are notorious for conflicting with
# an unrelated project's pinned pyspark/dbt-databricks versions, so it never goes in the main
# venv. Airflow itself runs in Docker (see orchestration/docker-compose.yml) - this venv
# exists only so the DAG file can be structurally validated locally via pytest without
# needing Docker running.
install-airflow:
	uv venv .venv-airflow --python 3.12
	uv pip install --python .venv-airflow/bin/python apache-airflow apache-airflow-providers-databricks

# Structural DAG validation only (DagBag import + task/dependency checks) - lives outside the
# main `tests/` dir and the main `make check` because apache-airflow is not installed in the
# main venv. See orchestration/tests/test_dag.py.
test-orchestration:
	.venv-airflow/bin/python -m pytest orchestration/tests -q
```

Also update `.PHONY` at the top of the Makefile to add `install-airflow test-orchestration`.

- [ ] **Step 3: Run the install and verify the venv works**

Run: `make install-airflow && .venv-airflow/bin/python -c "import airflow; import airflow.providers.databricks; print('ok')"`
Expected: prints `ok` with no import errors. If `apache-airflow` fails to resolve for Python
3.12 with a dependency conflict, check `apache-airflow`'s published constraints file for the
resolved version (e.g. `https://raw.githubusercontent.com/apache/airflow/constraints-<version>/constraints-3.12.txt`) and install with `uv pip install --python .venv-airflow/bin/python "apache-airflow==<version>" apache-airflow-providers-databricks --constraint <constraints-url>` instead - this is exactly the kind of version-pinning-by-live-trial Sub-project A did for `databricks-connect`.

- [ ] **Step 4: Commit**

```bash
git add qc-lakehouse/Makefile qc-lakehouse/.gitignore
git commit -m "Add isolated .venv-airflow for local Airflow tooling

Mirrors .venv-databricks's isolation reasoning from Sub-project A -
Airflow's own dependency constraints would conflict with the main venv's
pinned pyspark/dbt-databricks versions. This venv exists only for
structural DAG validation via pytest (orchestration/tests/); Airflow
itself runs in Docker."
```

---

### Task 3: Databricks Asset Bundle - generation jobs

**Files:**
- Create: `qc-lakehouse/databricks.yml`
- Modify: `qc-lakehouse/.gitignore`
- Modify: `qc-lakehouse/Makefile`

**Interfaces:**
- Produces: two deployed Databricks Jobs named `generate_reference_data` and
  `generate_fact_data` (fixed names per Global Constraints) - consumed by Task 5's Airflow DAG
  via `DatabricksRunNowOperator(job_name=...)`.

- [ ] **Step 1: Add dist/ and .databricks/ to gitignore**

Add to `qc-lakehouse/.gitignore`:

```
dist/
.databricks/
```

- [ ] **Step 2: Write the bundle config for the two generation jobs**

Create `qc-lakehouse/databricks.yml`:

```yaml
bundle:
  name: qc_lakehouse

artifacts:
  qc_lakehouse_wheel:
    type: whl
    build: uv build --wheel
    path: .

resources:
  jobs:
    generate_reference_data:
      name: generate_reference_data
      tasks:
        - task_key: main
          spark_python_task:
            python_file: ./scripts/generate_reference_data.py
          libraries:
            - whl: ./dist/*.whl
          environment_key: qc_lakehouse_env

    generate_fact_data:
      name: generate_fact_data
      tasks:
        - task_key: main
          spark_python_task:
            python_file: ./scripts/generate_fact_data.py
          libraries:
            - whl: ./dist/*.whl
          environment_key: qc_lakehouse_env

environments:
  - environment_key: qc_lakehouse_env
    spec:
      client: "2"
      dependencies:
        - python-dotenv

targets:
  dev:
    mode: development
    default: true
```

`environment_key`/`environments:` with no `new_cluster`/`existing_cluster_id` on either task
is what selects serverless compute - do not add a cluster spec to either job.

- [ ] **Step 3: Add bundle-validate and bundle-deploy Makefile targets**

Add to `qc-lakehouse/Makefile`:

```makefile
bundle-validate:
	databricks bundle validate

bundle-deploy:
	databricks bundle deploy -t dev
```

Update `.PHONY` to add `bundle-validate bundle-deploy`.

- [ ] **Step 4: Validate the bundle**

Run: `make bundle-validate`
Expected: validates cleanly. If it rejects a field name (e.g. `spark_python_task`,
`environment_key`, or the top-level `environments:` list), run `databricks bundle schema` to
get the current bundle JSON schema and fix the field names in `databricks.yml` to match - see
Global Constraints on schema currency.

- [ ] **Step 5: Deploy to the dev target and run the early-validation smoke test**

Run: `make bundle-deploy`
Expected: deploys successfully, uploading the built wheel and both job definitions.

Run: `databricks bundle run generate_reference_data -t dev`
Expected: the job succeeds. **This is the critical early validation of Task 1's fix** - it
proves `build_databricks_session` correctly detects it's running inside a Databricks Job and
builds a native Spark session rather than a broken recursive Databricks Connect session. If
the job fails with a Databricks-Connect-related error, re-check Task 1's
`is_running_on_databricks()` logic and `DATABRICKS_RUNTIME_VERSION`'s actual presence/value in
a Databricks Job task's environment (confirm via the job run's logs) before assuming any other
cause.

Note: this actually writes real reference data to `qc_dev.bronze_source`, same as running
`scripts/generate_reference_data.py` locally - not a throwaway smoke test.

- [ ] **Step 6: Commit**

```bash
git add qc-lakehouse/databricks.yml qc-lakehouse/.gitignore qc-lakehouse/Makefile
git commit -m "Add Databricks Asset Bundle with the two generation jobs

generate_reference_data and generate_fact_data deploy as spark_python_task
jobs on serverless compute, with the qc_lakehouse wheel as a library
dependency. Live-validated generate_reference_data via databricks bundle
run - confirms Task 1's Databricks-Job session detection works correctly
when the script actually runs as a Databricks Job, not just in theory."
```

---

### Task 4: Databricks Asset Bundle - dbt jobs

**Files:**
- Modify: `qc-lakehouse/databricks.yml`

**Interfaces:**
- Produces: two deployed Databricks Jobs named `dbt_run` and `dbt_test` (fixed names per
  Global Constraints) - consumed by Task 5's Airflow DAG.

- [ ] **Step 1: Add the dbt jobs to the bundle config**

Add to the `resources: jobs:` block in `qc-lakehouse/databricks.yml`, alongside the two
existing jobs:

```yaml
    dbt_run:
      name: dbt_run
      tasks:
        - task_key: main
          dbt_task:
            project_directory: ./dbt/qc_lakehouse
            commands:
              - dbt deps
              - dbt run --target qc_dev
          environment_key: qc_lakehouse_env

    dbt_test:
      name: dbt_test
      tasks:
        - task_key: main
          dbt_task:
            project_directory: ./dbt/qc_lakehouse
            commands:
              - dbt test --target qc_dev
          environment_key: qc_lakehouse_env
```

If `databricks bundle validate` (Step 2) rejects `dbt_task` or `project_directory`, check
whether the current Databricks Jobs dbt task type instead expects a SQL warehouse reference
(`warehouse_id`) alongside the commands - the dbt task type's exact required fields are one of
the schema-currency risks called out in Global Constraints.

- [ ] **Step 2: Validate and deploy**

Run: `make bundle-validate && make bundle-deploy`
Expected: both new jobs validate and deploy cleanly.

- [ ] **Step 3: Smoke-run both dbt jobs**

Run: `databricks bundle run dbt_run -t dev`
Expected: succeeds - this is the same `dbt run --target qc_dev` Sub-project C already proved
works, now running as a scheduled Databricks Job instead of from a local shell.

Run: `databricks bundle run dbt_test -t dev`
Expected: succeeds - all of Sub-project C's tests pass, same as `make dbt-test` does locally.

- [ ] **Step 4: Commit**

```bash
git add qc-lakehouse/databricks.yml
git commit -m "Add dbt_run and dbt_test Databricks Jobs to the bundle

Uses dbt's native Databricks Jobs task type against the qc_dev target -
the same dbt run/test Sub-project C already proved locally, now deployable
and triggerable as Databricks Jobs. Live-validated both via
databricks bundle run."
```

---

### Task 5: Airflow DAG

**Files:**
- Create: `qc-lakehouse/orchestration/dags/qc_lakehouse_pipeline.py`
- Test: `qc-lakehouse/orchestration/tests/test_dag.py`

**Interfaces:**
- Consumes: the 4 fixed Databricks Job names from Global Constraints (`generate_reference_data`,
  `generate_fact_data`, `dbt_run`, `dbt_test`), deployed by Tasks 3 and 4.
- Produces: a DAG with `dag_id="qc_lakehouse_pipeline"` and exactly those 4 task IDs, in a
  strict sequential chain - consumed by Task 6's Docker Compose setup (mounted as the DAGs
  folder) and Task 7's live end-to-end trigger.

- [ ] **Step 1: Write the failing structural tests**

Create `qc-lakehouse/orchestration/tests/test_dag.py`:

```python
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


def _dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_dag_imports_without_errors():
    dagbag = _dagbag()
    assert dagbag.import_errors == {}


def test_pipeline_dag_has_exactly_four_tasks():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert dag is not None
    assert set(dag.task_ids) == {
        "generate_reference_data", "generate_fact_data", "dbt_run", "dbt_test",
    }


def test_pipeline_dag_dependency_chain_is_sequential():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    ref = dag.get_task("generate_reference_data")
    fact = dag.get_task("generate_fact_data")
    run = dag.get_task("dbt_run")
    test = dag.get_task("dbt_test")
    assert ref.downstream_task_ids == {"generate_fact_data"}
    assert fact.downstream_task_ids == {"dbt_run"}
    assert run.downstream_task_ids == {"dbt_test"}
    assert test.downstream_task_ids == set()


def test_pipeline_dag_has_no_schedule():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_pipeline")
    assert dag.timetable.summary in ("Never", "None") or dag.schedule_interval is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv-airflow/bin/python -m pytest orchestration/tests -v`
Expected: `FAIL` - `orchestration/dags` doesn't exist yet, or `get_dag` returns `None`.

- [ ] **Step 3: Write the DAG**

Create `qc-lakehouse/orchestration/dags/qc_lakehouse_pipeline.py`:

```python
from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

logger = logging.getLogger(__name__)

DATABRICKS_CONN_ID = "databricks_default"

DEFAULT_ARGS = {
    "owner": "qc_lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def log_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """SLA-miss handler. A real system would page/alert here (PagerDuty, Slack, etc.) - this
    project has no live alerting channel, so it logs a structured warning instead, making
    visible what a production system would act on rather than silently having no SLA story."""
    logger.warning(
        "SLA missed for dag_id=%s task_ids=%s",
        dag.dag_id,
        [t.task_id for t in task_list],
    )


with DAG(
    dag_id="qc_lakehouse_pipeline",
    description="Trigger QC Lakehouse's ingestion (B1) and dbt (C) Databricks Jobs, in order.",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    sla_miss_callback=log_sla_miss,
    tags=["qc_lakehouse", "e1"],
) as dag:
    generate_reference_data = DatabricksRunNowOperator(
        task_id="generate_reference_data",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="generate_reference_data",
        sla=timedelta(minutes=15),
    )

    generate_fact_data = DatabricksRunNowOperator(
        task_id="generate_fact_data",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="generate_fact_data",
        sla=timedelta(minutes=30),
    )

    dbt_run = DatabricksRunNowOperator(
        task_id="dbt_run",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="dbt_run",
        sla=timedelta(minutes=10),
    )

    dbt_test = DatabricksRunNowOperator(
        task_id="dbt_test",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="dbt_test",
        sla=timedelta(minutes=10),
    )

    generate_reference_data >> generate_fact_data >> dbt_run >> dbt_test
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv-airflow/bin/python -m pytest orchestration/tests -v`
Expected: `PASS` - all 4 tests green. If `test_pipeline_dag_has_no_schedule` fails because the
installed Airflow version exposes scheduling differently than either branch checks, inspect
`dag.schedule_interval` and `dag.timetable` directly in a REPL
(`.venv-airflow/bin/python -c "..."`) and adjust the assertion to match what a `schedule=None`
DAG actually reports on the installed version - this is a test-assertion detail, not a reason
to add a schedule.

- [ ] **Step 5: Lint the orchestration code**

Run: `.venv-airflow/bin/python -m pip install ruff --quiet && .venv-airflow/bin/python -m ruff check orchestration/`

(Alternatively, if the main venv's `ruff` binary is on `PATH` and version drift isn't a
concern: `uv run ruff check orchestration/` from the main venv works too, since `ruff` itself
has no Airflow dependency.)

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add qc-lakehouse/orchestration/dags/qc_lakehouse_pipeline.py qc-lakehouse/orchestration/tests/test_dag.py
git commit -m "Add the qc_lakehouse_pipeline Airflow DAG

Four DatabricksRunNowOperator tasks in a strict sequential chain
(generate_reference_data >> generate_fact_data >> dbt_run >> dbt_test),
manually triggered only (schedule=None - the generator is deterministic,
so a cron schedule would just regenerate identical data), with retries
and a logged SLA-miss stub on every task. Structurally validated via
DagBag in .venv-airflow."
```

---

### Task 6: Dockerized Airflow

**Files:**
- Create: `qc-lakehouse/orchestration/docker-compose.yml`
- Create: `qc-lakehouse/orchestration/.env.example`
- Modify: `qc-lakehouse/.gitignore`

**Interfaces:**
- Produces: a running Airflow instance (webserver on `localhost:8080`) with
  `orchestration/dags/` mounted and `apache-airflow-providers-databricks` installed -
  consumed by Task 7's live end-to-end trigger.

- [ ] **Step 1: Add orchestration's own env file to gitignore**

Add to `qc-lakehouse/.gitignore`:

```
orchestration/.env
```

- [ ] **Step 2: Write the Docker Compose file**

Create `qc-lakehouse/orchestration/docker-compose.yml`:

```yaml
services:
  airflow:
    image: apache/airflow:2.10.4-python3.12
    environment:
      AIRFLOW__CORE__EXECUTOR: SequentialExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:////opt/airflow/airflow.db
      # Local-dev-only mechanism (Airflow's own docs call out this env var as unsuitable for
      # production images) for adding the Databricks provider without a custom Dockerfile -
      # fine here since this container is never deployed anywhere, only run locally.
      _PIP_ADDITIONAL_REQUIREMENTS: apache-airflow-providers-databricks
      DATABRICKS_HOST: "${DATABRICKS_HOST}"
    env_file:
      - .env
    volumes:
      - ./dags:/opt/airflow/dags
      - airflow_data:/opt/airflow
    ports:
      - "8080:8080"
    command: standalone

volumes:
  airflow_data:
```

Confirm `apache/airflow:2.10.4-python3.12` is still a published tag at implementation time
(`docker pull apache/airflow:2.10.4-python3.12`); if not, use the latest stable
`apache/airflow:<version>-python3.12` tag from Docker Hub instead - see Global Constraints on
version currency.

- [ ] **Step 3: Write the env template**

Create `qc-lakehouse/orchestration/.env.example`:

```
# Copy to orchestration/.env and fill in - orchestration/.env is gitignored.
# Same Databricks workspace host as the main project's .env.example - reused so the
# Airflow connection (set up in Step 4 below) and DatabricksRunNowOperator both target the
# same workspace this project's Databricks Jobs (databricks.yml) are deployed to.
DATABRICKS_HOST=
```

- [ ] **Step 4: Bring Airflow up and configure the Databricks connection**

Run: `cp orchestration/.env.example orchestration/.env` and fill in the real
`DATABRICKS_HOST` (same value as the main `.env`).

Run: `cd orchestration && docker compose up -d`
Expected: container starts; `airflow standalone` prints an auto-generated admin password to
its logs (`docker compose logs airflow | grep password`) - use it to log in at
`http://localhost:8080`.

Configure the `databricks_default` Airflow connection (the `databricks_conn_id` the DAG uses),
either via the UI (Admin -> Connections -> add `databricks_default`, type `Databricks`, host =
`DATABRICKS_HOST`, and an OAuth token from `databricks auth token` for the same profile used
by `databricks auth login`) or via the CLI:

```bash
docker compose exec airflow airflow connections add databricks_default \
  --conn-type databricks \
  --conn-host "$DATABRICKS_HOST" \
  --conn-extra "{\"token\": \"$(databricks auth token -o json | jq -r .access_token)\"}"
```

Expected: connection saved with no error.

- [ ] **Step 5: Confirm the DAG is visible and error-free in the running instance**

Run: `docker compose exec airflow airflow dags list-import-errors`
Expected: empty output (no import errors) - confirms the container's Airflow, not just
`.venv-airflow`, can also import the DAG cleanly (validates `_PIP_ADDITIONAL_REQUIREMENTS`
actually installed the Databricks provider).

- [ ] **Step 6: Commit**

```bash
git add qc-lakehouse/orchestration/docker-compose.yml qc-lakehouse/orchestration/.env.example qc-lakehouse/.gitignore
git commit -m "Add Dockerized Airflow (SQLite + SequentialExecutor)

Single-container airflow standalone, no Postgres - resolves the conflict
between the project's 'no local Postgres' constraint and Airflow's usual
docker-compose pattern, and is right-sized for a 4-task manual-trigger
DAG with no concurrency needs. Databricks provider installed via
_PIP_ADDITIONAL_REQUIREMENTS (local-dev-only, never deployed anywhere)."
```

---

### Task 7: Live end-to-end validation and documentation

**Files:**
- Modify: `qc-lakehouse/README.md`

**Interfaces:**
- None - this task validates the whole system built in Tasks 1-6 and documents it; no new
  code interfaces.

- [ ] **Step 1: Trigger the DAG for real, end to end**

With the Airflow container from Task 6 still running and the `databricks_default` connection
configured:

Run: `docker compose exec airflow airflow dags trigger qc_lakehouse_pipeline`

Watch it through to completion via the UI (`http://localhost:8080`) or:
`docker compose exec airflow airflow dags list-runs -d qc_lakehouse_pipeline`

Expected: all 4 tasks succeed in order (`generate_reference_data`, `generate_fact_data`,
`dbt_run`, `dbt_test`), each one showing a real Databricks Jobs run ID in its task logs.

- [ ] **Step 2: Trigger it a second time to confirm idempotent reruns hold through Airflow**

Run: `docker compose exec airflow airflow dags trigger qc_lakehouse_pipeline`

Expected: succeeds again with the same row counts as Step 1 (the generation scripts overwrite
deterministically from a fixed seed, and dbt's models are full-refresh views/tables - Sub-projects
B1/W1a and C already proved this holds when run directly; this step proves it still holds when
triggered through Airflow -> Databricks Jobs, not just via a direct local run).

- [ ] **Step 3: Simulate a failure to confirm retries actually engage**

Temporarily rename the `generate_reference_data` job in `databricks.yml` to an intentionally
wrong name (e.g. `generate_reference_data_typo`), leaving the DAG's `job_name` unchanged so it
can't find the job, then `make bundle-deploy` and trigger the DAG once more.

Expected: the `generate_reference_data` task fails, retries twice (per
`DEFAULT_ARGS["retries"] = 2`) with a 2-minute delay between attempts (visible in the task's
try-number history in the UI), then marks the task and downstream tasks as failed.

Revert the job name in `databricks.yml` back to `generate_reference_data`, `make bundle-deploy`
again, and confirm a subsequent trigger succeeds cleanly - do not leave the intentional typo in
place.

- [ ] **Step 4: Document Sub-project E1 in the README**

In `qc-lakehouse/README.md`, update the `## What's not here yet` section (currently lists
"Airflow/Docker orchestration" as not-yet-built) to remove that item, and add a new section
after the existing `## dbt medallion transformation (Sub-project C)` section:

```markdown
## Orchestration (Sub-project E1)

Dockerized Airflow triggers 4 Databricks Jobs - `generate_reference_data`,
`generate_fact_data` (Sub-project B1), `dbt_run`, `dbt_test` (Sub-project C) - via the
Databricks Jobs API, in a single sequential DAG. Compute always happens on Databricks;
Airflow only triggers and polls.

Setup:

```bash
make install-airflow      # isolated .venv-airflow, for local DAG validation only
make bundle-validate       # validates databricks.yml
make bundle-deploy         # deploys the 4 jobs to the qc_dev workspace, serverless compute
cd orchestration && cp .env.example .env   # fill in DATABRICKS_HOST
docker compose up -d       # brings up Airflow at localhost:8080
```

Then configure the `databricks_default` Airflow connection (see
`docs/superpowers/plans/2026-09-15-qc-lakehouse-e1-orchestration.md`, Task 6) and trigger
`qc_lakehouse_pipeline` from the UI or `airflow dags trigger qc_lakehouse_pipeline`.

The DAG is manually-triggered only (no cron schedule) - the generator is deterministic, so a
recurring schedule would just regenerate identical data. Each task has 2 retries and a
logged SLA-miss warning (no live alerting channel exists for this portfolio project, so the
callback logs what a production system would page on).

Not yet built: B2/W1b (streaming ingestion) and E2 (this DAG's future extension to trigger
that streaming job) - see the design spec's section 9/10 open items.
```

- [ ] **Step 5: Final check - run the whole existing test suite once more**

Run: `uv run ruff check . && uv run pytest -q`
Expected: unchanged pass count from before this plan (Tasks 1-7 added no changes to the main
`tests/` suite beyond Task 1's 2 new tests) - confirms nothing in this plan regressed the
existing B1/W1a/C code paths.

- [ ] **Step 6: Commit**

```bash
git add qc-lakehouse/README.md
git commit -m "Document Sub-project E1 orchestration in README

Setup instructions for the Dockerized Airflow + Databricks Jobs pipeline,
and marks 'Airflow/Docker orchestration' as no longer missing. Live-verified
end to end: both a clean run and an idempotent rerun of the full DAG
succeeded, and induced task failure confirmed retries actually engage."
```
