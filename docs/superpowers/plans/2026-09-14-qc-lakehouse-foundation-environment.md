# QC Lakehouse - Sub-project A: Foundation & Environment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `qc-lakehouse` project's toolchain - a uv-managed Python 3.12 venv, local Spark (dev-loop only), Databricks Free Edition serverless compute, and dbt-databricks - each proven with a real, green smoke test, so every later sub-project (B onward) has a working foundation to build on.

**Architecture:** A single Python package (`src/qc_lakehouse`) holds small, dependency-injectable modules (`config`, `spark_local`, `databricks_session`) with unit tests that don't require a JVM or a live Databricks workspace. Three standalone smoke-test scripts, wired to `make smoke-local` / `make smoke-databricks` / `make smoke-dbt`, prove the real integrations (local Delta round-trip, Databricks serverless Delta round-trip, dbt run against Databricks) work end to end. dbt gets a hand-written minimal project (not `dbt init`) so its files are exact and reviewable.

**Tech Stack:** Python 3.12, `uv`, PySpark 3.5.x + `delta-spark` (local dev-loop only), `databricks-sdk` + `databricks-connect` (serverless), Databricks CLI (OAuth U2M), `dbt-databricks`, `pytest`, `ruff`, `python-dotenv`, `pyyaml`.

**Spec:** `docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md` (section 5 is this sub-project's detailed design; sections 1-4 and 6 are overall project context - not implemented here).

## Global Constraints

- Databricks Free Edition only: serverless compute, no provisioned/all-purpose clusters - nothing in this plan may assume a provisioned cluster exists.
- No local Postgres / local Hive metastore. Local Spark uses its default in-memory catalog; it is a dev/test tool only, never the pipeline's source of truth.
- dbt targets Databricks via `dbt-databricks` against a serverless SQL warehouse - never `dbt-spark` against local Spark.
- Python tooling is `uv` end to end (venv creation, dependency add/sync, running scripts/tests) - no bare `pip`/`python -m venv`.
- Idempotency and "effectively-once" (MERGE-based writes, checkpointing) are real-pipeline constraints that start applying in Sub-project B. This plan's smoke tests write small throwaway tables with `mode("overwrite")` on purpose - they exist only to prove connectivity, not to model production write patterns.
- Everything created here lives under a new `qc-lakehouse/` directory at the repo root, alongside the existing `docs/`, `QC-datapipelineplan.md`, and `yt_transcript_*.md` (untouched).

---

### Task 1: Repo skeleton & git hygiene

**Files:**
- Create: `qc-lakehouse/.gitignore`
- Create: `qc-lakehouse/.env.example`
- Create: `qc-lakehouse/README.md`
- Create: `qc-lakehouse/src/qc_lakehouse/__init__.py`
- Create: `qc-lakehouse/conf/.gitkeep`
- Create: `qc-lakehouse/dbt/.gitkeep`
- Create: `qc-lakehouse/scripts/.gitkeep`
- Create: `qc-lakehouse/tests/test_repo_structure.py`

**Interfaces:**
- Produces: the `qc-lakehouse/` directory tree every later task writes into. No Python interfaces yet.

- [ ] **Step 1: Create the directory tree and placeholder files**

```bash
mkdir -p qc-lakehouse/src/qc_lakehouse qc-lakehouse/conf qc-lakehouse/dbt qc-lakehouse/scripts qc-lakehouse/tests
touch qc-lakehouse/src/qc_lakehouse/__init__.py qc-lakehouse/conf/.gitkeep qc-lakehouse/dbt/.gitkeep qc-lakehouse/scripts/.gitkeep
```

- [ ] **Step 2: Write `qc-lakehouse/.gitignore`**

```gitignore
.venv/
.env
__pycache__/
*.pyc
.pytest_cache/
.ruff_cache/
warehouse/
dbt/qc_lakehouse/target/
dbt/qc_lakehouse/dbt_packages/
dbt/qc_lakehouse/logs/
uv.lock.tmp
```

- [ ] **Step 3: Write `qc-lakehouse/.env.example` (header only for now - Task 4 adds the real variables)**

```dotenv
# Copy this file to .env and fill in real values. .env is gitignored - never commit it.
# Variables are added incrementally as later tasks need them.
```

- [ ] **Step 4: Write `qc-lakehouse/README.md` (stub - Task 8 fills in the full walkthrough)**

```markdown
# QC Lakehouse

Foundation & Environment setup is in progress. Full setup instructions land in Task 8
of `docs/superpowers/plans/2026-09-14-qc-lakehouse-foundation-environment.md`.

Design spec: `docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md`.
```

- [ ] **Step 5: Write the failing structure test**

```python
# qc-lakehouse/tests/test_repo_structure.py
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

REQUIRED_PATHS = [
    "conf",
    "src/qc_lakehouse",
    "dbt",
    "scripts",
    "tests",
    ".env.example",
    ".gitignore",
    "README.md",
]


def test_required_top_level_paths_exist():
    missing = [p for p in REQUIRED_PATHS if not (REPO_ROOT / p).exists()]
    assert missing == [], f"missing required paths: {missing}"
```

This test can't run yet (no pytest installed, no venv) - that's expected. It becomes the
first test `make check` runs once Task 3 installs pytest.

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add .
git commit -m "Add qc-lakehouse repo skeleton"
```

---

### Task 2: uv-managed venv pinned to Python 3.12

**Files:**
- Create: `qc-lakehouse/pyproject.toml`
- Create: `qc-lakehouse/.python-version` (generated by `uv python pin`)
- Create: `qc-lakehouse/Makefile`

**Interfaces:**
- Produces: a `.venv` at `qc-lakehouse/.venv`, and a `make venv` target every later task's Makefile additions assume exists.

- [ ] **Step 1: Write `qc-lakehouse/pyproject.toml`**

```toml
[project]
name = "qc-lakehouse"
version = "0.1.0"
description = "QC Lakehouse - Databricks + dbt hero quick-commerce data pipeline"
requires-python = ">=3.12,<3.13"
dependencies = []

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]

[tool.ruff]
line-length = 100
target-version = "py312"
```

- [ ] **Step 2: Pin the Python version and create the venv**

```bash
cd qc-lakehouse
uv python pin 3.12
uv venv
```

Expected: `.python-version` now contains `3.12`, and `.venv/` exists.

- [ ] **Step 3: Write `qc-lakehouse/Makefile` with the first target**

```makefile
.PHONY: venv

venv:
	uv python pin 3.12
	uv venv
```

- [ ] **Step 4: Verify the venv runs the pinned interpreter**

Run: `cd qc-lakehouse && uv run python -c "import sys; assert sys.version_info[:2] == (3, 12); print('ok')"`
Expected: prints `ok`

- [ ] **Step 5: Commit**

```bash
cd qc-lakehouse
git add pyproject.toml .python-version Makefile
git commit -m "Add uv-managed venv pinned to Python 3.12"
```

---

### Task 3: Baseline dev tooling (ruff, pytest, python-dotenv) + `make check`

**Files:**
- Modify: `qc-lakehouse/pyproject.toml` (uv rewrites the `dependencies`/`[dependency-groups]` tables automatically via `uv add`)
- Modify: `qc-lakehouse/Makefile`
- Create: `qc-lakehouse/tests/test_environment_sanity.py`

**Interfaces:**
- Produces: `make check` (ruff + pytest), used as a gate by every later task.

- [ ] **Step 1: Add baseline dependencies**

```bash
cd qc-lakehouse
uv add --dev ruff pytest
uv add python-dotenv
```

- [ ] **Step 2: Write a trivial sanity test (this is the first test that can actually run)**

```python
# qc-lakehouse/tests/test_environment_sanity.py
import sys


def test_python_version_is_312():
    assert sys.version_info[:2] == (3, 12)
```

- [ ] **Step 3: Run the full test suite and confirm both tests pass**

Run: `cd qc-lakehouse && uv run pytest -q`
Expected: `2 passed` (the Task 1 structure test plus this sanity test)

- [ ] **Step 4: Run ruff and confirm it's clean**

Run: `cd qc-lakehouse && uv run ruff check .`
Expected: `All checks passed!`

- [ ] **Step 5: Add `install-baseline` and `check` targets to the Makefile**

```makefile
.PHONY: venv install-baseline check

venv:
	uv python pin 3.12
	uv venv

install-baseline:
	uv add --dev ruff pytest
	uv add python-dotenv

check:
	uv run ruff check .
	uv run pytest -q
```

- [ ] **Step 6: Verify `make check` works standalone**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, then `2 passed`

- [ ] **Step 7: Commit**

```bash
cd qc-lakehouse
git add pyproject.toml uv.lock Makefile tests/test_environment_sanity.py
git commit -m "Add baseline dev tooling (ruff, pytest, python-dotenv) and make check"
```

---

### Task 4: Environment settings loader (`config.py`)

**Files:**
- Create: `qc-lakehouse/src/qc_lakehouse/config.py`
- Create: `qc-lakehouse/tests/test_config.py`
- Modify: `qc-lakehouse/.env.example`

**Interfaces:**
- Consumes: nothing from earlier tasks beyond the venv/pytest setup.
- Produces: `qc_lakehouse.config.Settings` (frozen dataclass: `databricks_host: str`, `databricks_http_path: str`, `databricks_catalog: str`, `databricks_schema: str`), `qc_lakehouse.config.load_settings(env_file: Path | None = None) -> Settings`, `qc_lakehouse.config.MissingSettingsError(RuntimeError)` with a `.missing_keys: list[str]` attribute. Tasks 5, 6, and 8 depend on these exact names.

- [ ] **Step 1: Write the failing tests**

```python
# qc-lakehouse/tests/test_config.py
import pytest

from qc_lakehouse.config import MissingSettingsError, load_settings

REQUIRED_KEYS = (
    "DATABRICKS_HOST",
    "DATABRICKS_HTTP_PATH",
    "DATABRICKS_CATALOG",
    "DATABRICKS_SCHEMA",
)


def test_load_settings_reads_all_required_keys(tmp_path, monkeypatch):
    for key in REQUIRED_KEYS:
        monkeypatch.delenv(key, raising=False)

    env_file = tmp_path / ".env"
    env_file.write_text(
        "DATABRICKS_HOST=adb-123.cloud.databricks.com\n"
        "DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123\n"
        "DATABRICKS_CATALOG=qc_lakehouse\n"
        "DATABRICKS_SCHEMA=dev\n"
    )

    settings = load_settings(env_file=env_file)

    assert settings.databricks_host == "adb-123.cloud.databricks.com"
    assert settings.databricks_http_path == "/sql/1.0/warehouses/abc123"
    assert settings.databricks_catalog == "qc_lakehouse"
    assert settings.databricks_schema == "dev"


def test_load_settings_raises_on_missing_keys(tmp_path, monkeypatch):
    for key in REQUIRED_KEYS:
        monkeypatch.delenv(key, raising=False)

    env_file = tmp_path / ".env"
    env_file.write_text("DATABRICKS_HOST=adb-123.cloud.databricks.com\n")

    with pytest.raises(MissingSettingsError) as exc_info:
        load_settings(env_file=env_file)

    assert "DATABRICKS_HTTP_PATH" in str(exc_info.value)
    assert "DATABRICKS_CATALOG" in str(exc_info.value)
    assert "DATABRICKS_SCHEMA" in str(exc_info.value)
    assert exc_info.value.missing_keys == [
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_CATALOG",
        "DATABRICKS_SCHEMA",
    ]
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'qc_lakehouse.config'`

- [ ] **Step 3: Write the implementation**

```python
# qc-lakehouse/src/qc_lakehouse/config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

REQUIRED_KEYS = (
    "DATABRICKS_HOST",
    "DATABRICKS_HTTP_PATH",
    "DATABRICKS_CATALOG",
    "DATABRICKS_SCHEMA",
)


class MissingSettingsError(RuntimeError):
    def __init__(self, missing_keys: list[str]) -> None:
        self.missing_keys = missing_keys
        super().__init__(
            f"Missing required environment variables: {', '.join(missing_keys)}"
        )


@dataclass(frozen=True)
class Settings:
    databricks_host: str
    databricks_http_path: str
    databricks_catalog: str
    databricks_schema: str


def load_settings(env_file: Path | None = None) -> Settings:
    if env_file is not None:
        load_dotenv(dotenv_path=env_file, override=True)
    else:
        load_dotenv(override=False)

    missing = [key for key in REQUIRED_KEYS if not os.environ.get(key)]
    if missing:
        raise MissingSettingsError(missing)

    return Settings(
        databricks_host=os.environ["DATABRICKS_HOST"],
        databricks_http_path=os.environ["DATABRICKS_HTTP_PATH"],
        databricks_catalog=os.environ["DATABRICKS_CATALOG"],
        databricks_schema=os.environ["DATABRICKS_SCHEMA"],
    )
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_config.py -v`
Expected: `2 passed`

- [ ] **Step 5: Add the real variables to `.env.example`**

```dotenv
# Copy this file to .env and fill in real values. .env is gitignored - never commit it.

# Your Databricks Free Edition workspace URL, e.g. adb-1234567890123456.7.azuredatabricks.net
# (no https:// prefix, no trailing slash). Set once via `databricks auth login`, reused here.
DATABRICKS_HOST=

# HTTP Path of a serverless SQL warehouse - Databricks UI: SQL Warehouses -> your warehouse ->
# Connection details -> HTTP Path. Looks like /sql/1.0/warehouses/xxxxxxxxxxxxxxxx
DATABRICKS_HTTP_PATH=

# Unity Catalog catalog and schema this project writes to. The schema is created automatically
# (see databricks_session.ensure_schema_exists) if it doesn't exist; the catalog must already
# exist - Free Edition workspaces come with a default catalog you can reuse.
DATABRICKS_CATALOG=
DATABRICKS_SCHEMA=
```

- [ ] **Step 6: Run `make check` to confirm nothing else broke**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `4 passed`

- [ ] **Step 7: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/config.py tests/test_config.py .env.example
git commit -m "Add environment settings loader (config.py)"
```

---

### Task 5: Local Spark dev-loop session builder + smoke test

**Files:**
- Create: `qc-lakehouse/conf/spark-local.conf`
- Create: `qc-lakehouse/src/qc_lakehouse/spark_local.py`
- Create: `qc-lakehouse/tests/test_spark_local.py`
- Create: `qc-lakehouse/scripts/smoke_local.py`
- Modify: `qc-lakehouse/Makefile`

**Interfaces:**
- Consumes: nothing from Task 4 (local Spark is deliberately independent of Databricks settings).
- Produces: `qc_lakehouse.spark_local.parse_spark_conf(conf_path: Path) -> dict[str, str]`, `qc_lakehouse.spark_local.build_local_spark_session(conf_path: Path = DEFAULT_CONF_PATH) -> pyspark.sql.SparkSession`, `qc_lakehouse.spark_local.DEFAULT_CONF_PATH: Path`.

- [ ] **Step 1: Install JDK 17 and the Spark/Delta dependencies**

```bash
brew install openjdk@17
cd qc-lakehouse
uv add pyspark delta-spark
```

Expected: `brew install` succeeds (or reports already installed); `uv add` updates `pyproject.toml`
and `uv.lock`.

- [ ] **Step 2: Write `qc-lakehouse/conf/spark-local.conf`**

```properties
# Delta Lake wiring for the local dev-loop Spark session only. Not used for real pipeline
# runs - those happen on Databricks. No metastore config on purpose: local Spark uses its
# default in-memory catalog (see spec section 3, "No local Postgres / local Hive metastore").
spark.sql.extensions                 io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog      org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.sql.warehouse.dir              ./warehouse
spark.sql.shuffle.partitions         4
spark.driver.memory                  2g
```

- [ ] **Step 3: Write the failing unit test for the pure conf parser**

```python
# qc-lakehouse/tests/test_spark_local.py
from qc_lakehouse.spark_local import parse_spark_conf


def test_parse_spark_conf_reads_key_value_pairs(tmp_path):
    conf_file = tmp_path / "spark-local.conf"
    conf_file.write_text(
        "# comment line, ignored\n"
        "\n"
        "spark.sql.warehouse.dir       ./warehouse\n"
        "spark.sql.shuffle.partitions  4\n"
    )

    conf = parse_spark_conf(conf_file)

    assert conf == {
        "spark.sql.warehouse.dir": "./warehouse",
        "spark.sql.shuffle.partitions": "4",
    }
```

- [ ] **Step 4: Run the test and confirm it fails**

Run: `cd qc-lakehouse && uv run pytest tests/test_spark_local.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'qc_lakehouse.spark_local'`

- [ ] **Step 5: Write the implementation**

```python
# qc-lakehouse/src/qc_lakehouse/spark_local.py
from __future__ import annotations

from pathlib import Path

DEFAULT_CONF_PATH = Path(__file__).resolve().parents[2] / "conf" / "spark-local.conf"


def parse_spark_conf(conf_path: Path) -> dict[str, str]:
    conf: dict[str, str] = {}
    for raw_line in conf_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        key, value = parts
        conf[key] = value.strip()
    return conf


def build_local_spark_session(conf_path: Path = DEFAULT_CONF_PATH):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    conf = parse_spark_conf(conf_path)
    builder = SparkSession.builder.appName("qc-lakehouse-local")
    for key, value in conf.items():
        builder = builder.config(key, value)

    return configure_spark_with_delta_pip(builder).getOrCreate()
```

- [ ] **Step 6: Run the test and confirm it passes**

Run: `cd qc-lakehouse && uv run pytest tests/test_spark_local.py -v`
Expected: `1 passed`

- [ ] **Step 7: Write the local smoke-test script**

```python
# qc-lakehouse/scripts/smoke_local.py
"""Smoke test: local Spark + Delta round-trip. Requires a real JDK and starts a JVM, so it's
a separate Makefile target rather than a pytest test."""
from __future__ import annotations

import shutil
from pathlib import Path

from qc_lakehouse.spark_local import build_local_spark_session

WAREHOUSE_DIR = Path("warehouse")
TABLE_PATH = str(WAREHOUSE_DIR / "smoke_local_table")


def main() -> None:
    if WAREHOUSE_DIR.exists():
        shutil.rmtree(WAREHOUSE_DIR)

    spark = build_local_spark_session()
    try:
        df = spark.createDataFrame([(1, "ok"), (2, "ok")], ["id", "status"])
        df.write.format("delta").mode("overwrite").save(TABLE_PATH)

        result = spark.read.format("delta").load(TABLE_PATH)
        rows = {row["id"]: row["status"] for row in result.collect()}

        assert rows == {1: "ok", 2: "ok"}, f"unexpected rows: {rows}"
        print("smoke-local: OK - wrote and read back 2 rows via local Delta table")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 8: Add `install-spark` and `smoke-local` targets to the Makefile**

```makefile
.PHONY: venv install-baseline check install-spark smoke-local

install-spark:
	uv add pyspark delta-spark

smoke-local:
	uv run python scripts/smoke_local.py
```

(Append these two targets and their `.PHONY` names to the existing Makefile from Task 3 -
don't remove the earlier targets.)

- [ ] **Step 9: Run the smoke test for real**

Run: `cd qc-lakehouse && make smoke-local`
Expected: last line of output is `smoke-local: OK - wrote and read back 2 rows via local Delta table`

- [ ] **Step 10: Run `make check` to confirm the fast suite is still green**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `5 passed`

- [ ] **Step 11: Commit**

```bash
cd qc-lakehouse
git add conf/spark-local.conf src/qc_lakehouse/spark_local.py tests/test_spark_local.py scripts/smoke_local.py Makefile pyproject.toml uv.lock
git commit -m "Add local Spark dev-loop session builder and smoke test"
```

---

### Task 6: Databricks CLI auth + serverless session builder + smoke test

**Files:**
- Create: `qc-lakehouse/src/qc_lakehouse/databricks_session.py`
- Create: `qc-lakehouse/tests/test_databricks_session.py`
- Create: `qc-lakehouse/scripts/smoke_databricks.py`
- Modify: `qc-lakehouse/Makefile`
- Modify: `qc-lakehouse/README.md`

**Interfaces:**
- Consumes: `qc_lakehouse.config.Settings` and `qc_lakehouse.config.load_settings` from Task 4.
- Produces: `qc_lakehouse.databricks_session.ensure_schema_exists(session, catalog: str, schema: str) -> None`, `qc_lakehouse.databricks_session.build_databricks_session(settings: Settings) -> databricks.connect.DatabricksSession`. Task 8's README references the `databricks auth login` step this task documents.

- [ ] **Step 1: Install the Databricks CLI and log in**

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks auth login --host https://<your-free-edition-workspace-url>
```

Expected: a browser window opens for OAuth login; on success the CLI prints a confirmation
and writes a default profile to `~/.databrickscfg`. Replace `<your-free-edition-workspace-url>`
with the host you'll also put in `.env`'s `DATABRICKS_HOST`.

- [ ] **Step 2: Add the Databricks Python dependencies**

```bash
cd qc-lakehouse
uv add databricks-sdk databricks-connect
```

- [ ] **Step 3: Write the failing unit test for the injectable schema-ensure helper**

```python
# qc-lakehouse/tests/test_databricks_session.py
from qc_lakehouse.databricks_session import ensure_schema_exists


class FakeSession:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def sql(self, query: str) -> None:
        self.queries.append(query)


def test_ensure_schema_exists_issues_create_schema_if_not_exists():
    session = FakeSession()

    ensure_schema_exists(session, catalog="qc_lakehouse", schema="dev")

    assert session.queries == ["CREATE SCHEMA IF NOT EXISTS `qc_lakehouse`.`dev`"]
```

- [ ] **Step 4: Run the test and confirm it fails**

Run: `cd qc-lakehouse && uv run pytest tests/test_databricks_session.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'qc_lakehouse.databricks_session'`

- [ ] **Step 5: Write the implementation**

```python
# qc-lakehouse/src/qc_lakehouse/databricks_session.py
from __future__ import annotations

from typing import Protocol

from qc_lakehouse.config import Settings


class SqlRunner(Protocol):
    def sql(self, query: str): ...


def ensure_schema_exists(session: SqlRunner, catalog: str, schema: str) -> None:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def build_databricks_session(settings: Settings):
    from databricks.connect import DatabricksSession

    session = DatabricksSession.builder.serverless(True).getOrCreate()
    ensure_schema_exists(session, settings.databricks_catalog, settings.databricks_schema)
    return session
```

- [ ] **Step 6: Run the test and confirm it passes**

Run: `cd qc-lakehouse && uv run pytest tests/test_databricks_session.py -v`
Expected: `1 passed`

- [ ] **Step 7: Write the Databricks smoke-test script**

```python
# qc-lakehouse/scripts/smoke_databricks.py
"""Smoke test: Databricks serverless compute + Unity Catalog Delta round-trip."""
from __future__ import annotations

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session


def main() -> None:
    settings = load_settings()
    spark = build_databricks_session(settings)

    table = (
        f"`{settings.databricks_catalog}`.`{settings.databricks_schema}`"
        ".smoke_databricks_table"
    )

    df = spark.createDataFrame([(1, "ok"), (2, "ok")], ["id", "status"])
    df.write.format("delta").mode("overwrite").saveAsTable(table)

    result = spark.sql(f"SELECT id, status FROM {table} ORDER BY id")
    rows = {row["id"]: row["status"] for row in result.collect()}

    assert rows == {1: "ok", 2: "ok"}, f"unexpected rows: {rows}"
    print(
        f"smoke-databricks: OK - wrote and read back 2 rows via {table} on serverless compute"
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 8: Add `install-databricks` and `smoke-databricks` targets to the Makefile**

```makefile
.PHONY: venv install-baseline check install-spark smoke-local install-databricks smoke-databricks

install-databricks:
	uv add databricks-sdk databricks-connect

smoke-databricks:
	uv run python scripts/smoke_databricks.py
```

(Append to the existing Makefile - keep all earlier targets.)

- [ ] **Step 9: Fill in your real `.env` and run the smoke test**

```bash
cd qc-lakehouse
cp .env.example .env   # if you haven't already; then fill in the four DATABRICKS_* values
make smoke-databricks
```

Expected: last line of output is
`smoke-databricks: OK - wrote and read back 2 rows via ... on serverless compute`

- [ ] **Step 10: Document the auth step in the README**

Append to `qc-lakehouse/README.md`:

```markdown
## Databricks auth

1. Install the CLI: `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`
2. `databricks auth login --host https://<your-free-edition-workspace-url>` (opens a browser,
   OAuth U2M - no token to store or rotate).
3. Copy `.env.example` to `.env` and fill in `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`
   (SQL Warehouses -> your warehouse -> Connection details), `DATABRICKS_CATALOG`,
   `DATABRICKS_SCHEMA`.
```

- [ ] **Step 11: Run `make check` to confirm the fast suite is still green**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `6 passed`

- [ ] **Step 12: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/databricks_session.py tests/test_databricks_session.py scripts/smoke_databricks.py Makefile README.md pyproject.toml uv.lock
git commit -m "Add Databricks serverless session builder and smoke test"
```

---

### Task 7: dbt-databricks scaffold + smoke test

**Files:**
- Create: `qc-lakehouse/dbt/qc_lakehouse/dbt_project.yml`
- Create: `qc-lakehouse/dbt/qc_lakehouse/profiles.yml`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/example/hello_dbt.sql`
- Create: `qc-lakehouse/tests/test_dbt_project_files.py`
- Modify: `qc-lakehouse/Makefile`

**Interfaces:**
- Consumes: the same four `DATABRICKS_*` environment variable names Task 4 defined (dbt's `env_var()` jinja reads them from the process environment, not from `.env` directly - the Makefile target sources `.env` first).
- Produces: a runnable dbt project at `dbt/qc_lakehouse`, and `make smoke-dbt`.

- [ ] **Step 1: Add the dbt dependency**

```bash
cd qc-lakehouse
uv add dbt-databricks
uv add --dev pyyaml
```

- [ ] **Step 2: Write `qc-lakehouse/dbt/qc_lakehouse/dbt_project.yml`**

```yaml
name: "qc_lakehouse"
version: "1.0.0"
config-version: 2
profile: "qc_lakehouse"

model-paths: ["models"]
target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  qc_lakehouse:
    example:
      +materialized: view
```

- [ ] **Step 3: Write `qc-lakehouse/dbt/qc_lakehouse/profiles.yml`**

```yaml
qc_lakehouse:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: "{{ env_var('DATABRICKS_CATALOG') }}"
      schema: "{{ env_var('DATABRICKS_SCHEMA') }}"
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      auth_type: oauth
      threads: 4
```

`auth_type: oauth` triggers dbt-databricks's own OAuth U2M flow (browser-based, one-time) - no
token lives in this file or in `.env`. Note this is a *separate* token cache from the
Databricks CLI's own login (Task 6 Step 1) - `databricks-cli` is not a valid `auth_type` value
for this adapter and was corrected during implementation after `dbt debug` rejected it
outright. The two OAuth sessions (CLI, dbt) are independent; each needs its own one-time
browser login, but both then persist across future runs.

- [ ] **Step 4: Write `qc-lakehouse/dbt/qc_lakehouse/models/example/hello_dbt.sql`**

```sql
select 1 as ok
```

- [ ] **Step 5: Write the failing tests for the project files**

```python
# qc-lakehouse/tests/test_dbt_project_files.py
from pathlib import Path

import yaml

DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt" / "qc_lakehouse"


def test_dbt_project_yml_is_valid_and_named_correctly():
    content = yaml.safe_load((DBT_PROJECT_DIR / "dbt_project.yml").read_text())
    assert content["name"] == "qc_lakehouse"
    assert content["profile"] == "qc_lakehouse"


def test_profiles_yml_targets_oauth_auth():
    content = yaml.safe_load((DBT_PROJECT_DIR / "profiles.yml").read_text())
    dev_output = content["qc_lakehouse"]["outputs"]["dev"]
    assert dev_output["type"] == "databricks"
    assert dev_output["auth_type"] == "oauth"
```

These files already exist from Steps 2-4, so this step is "add tests that lock in the
contract" rather than red-green - still run them to confirm they pass, not just trust them.

- [ ] **Step 6: Run the tests**

Run: `cd qc-lakehouse && uv run pytest tests/test_dbt_project_files.py -v`
Expected: `2 passed`

- [ ] **Step 7: Add `install-dbt` and `smoke-dbt` targets to the Makefile**

```makefile
.PHONY: venv install-baseline check install-spark smoke-local install-databricks smoke-databricks install-dbt smoke-dbt

install-dbt:
	uv add dbt-databricks
	uv add --dev pyyaml

smoke-dbt:
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt debug --project-dir dbt/qc_lakehouse
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --select hello_dbt
```

`set -a && . ./.env && set +a` exports every variable from `.env` into the shell before
running `dbt`, since dbt's `env_var()` jinja reads the process environment, not `.env` files
directly.

- [ ] **Step 8: Run the smoke test for real**

Run: `cd qc-lakehouse && make smoke-dbt`
Expected: `dbt debug` reports "All checks passed!"; `dbt run` reports `Completed successfully`
with 1 of 1 model (`hello_dbt`) run.

- [ ] **Step 9: Run `make check` to confirm the fast suite is still green**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `8 passed`

- [ ] **Step 10: Commit**

```bash
cd qc-lakehouse
git add dbt/qc_lakehouse tests/test_dbt_project_files.py Makefile pyproject.toml uv.lock
git commit -m "Add dbt-databricks scaffold and smoke test"
```

---

### Task 8: README walkthrough + full clean-clone validation

**Files:**
- Modify: `qc-lakehouse/README.md`
- Modify: `qc-lakehouse/Makefile`

**Interfaces:**
- Consumes: every Makefile target and script from Tasks 1-7.
- Produces: `make smoke-all`, and a README a stranger can follow start to finish.

- [ ] **Step 1: Add `smoke-all` to the Makefile**

```makefile
.PHONY: smoke-all

smoke-all: smoke-local smoke-databricks smoke-dbt
```

(Append - keep every earlier target.)

- [ ] **Step 2: Rewrite `qc-lakehouse/README.md` with the full walkthrough**

```markdown
# QC Lakehouse

Foundation & Environment for the QC Lakehouse project - a quick-commerce delivery data
pipeline with Databricks and dbt as co-hero technologies. This README covers Sub-project A
only: getting the toolchain running. See
`docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md` for the full
architecture and later sub-projects (B onward).

## Prerequisites

- macOS with Homebrew
- A Databricks Free Edition account (https://databricks.com/free) with a serverless SQL
  warehouse (Free Edition provisions one by default)
- `uv` installed (https://docs.astral.sh/uv/getting-started/installation/)

## Setup, in order

```bash
cd qc-lakehouse
make venv               # create the uv-managed .venv, pinned to Python 3.12
make install-baseline   # ruff, pytest, python-dotenv
make check               # confirms the fast test suite is green so far

brew install openjdk@17 # required by local Spark, not installable via uv
make install-spark
make smoke-local         # local Spark + Delta round-trip, no Databricks needed yet
```

## Databricks auth

1. Install the CLI: `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`
2. `databricks auth login --host https://<your-free-edition-workspace-url>` (opens a browser,
   OAuth U2M - no token to store or rotate).
3. Copy `.env.example` to `.env` and fill in `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`
   (SQL Warehouses -> your warehouse -> Connection details), `DATABRICKS_CATALOG`,
   `DATABRICKS_SCHEMA`.

```bash
make install-databricks
make smoke-databricks    # writes/reads a Delta table on serverless compute via Unity Catalog

make install-dbt
make smoke-dbt           # dbt debug + a trivial dbt run, same Databricks connection
```

## Everything at once (after the one-time auth step above)

```bash
make smoke-all
```

## What's not here yet

Real data generation, dbt medallion models, Airflow/Docker orchestration, and the AI/RAG
layer are separate sub-projects (B-H) - see the design spec.
```

- [ ] **Step 3: Full clean-clone validation - actually run every command in order**

Run, in a fresh shell, from `qc-lakehouse/`:

```bash
make venv
make install-baseline
make check
brew install openjdk@17
make install-spark
make smoke-local
make install-databricks
make smoke-databricks
make install-dbt
make smoke-dbt
make check
```

Expected: every command exits 0; `make check` at the end reports `8 passed`; both smoke test
scripts print their `OK` lines; `dbt run` reports 1 of 1 `hello_dbt` model completed
successfully. This is the acceptance test for the whole sub-project - if any step fails,
Sub-project A is not done.

- [ ] **Step 4: Commit**

```bash
cd qc-lakehouse
git add README.md Makefile
git commit -m "Add full setup README and smoke-all target"
```
