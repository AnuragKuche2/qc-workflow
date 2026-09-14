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

Two things to know about this step:

- **`make install-databricks` builds a second virtual environment**, `.venv-databricks`,
  alongside the default `.venv`. `databricks-connect` cannot be installed into the same
  environment as `pyspark` (they pin conflicting internal versions), so
  `install-databricks` creates `.venv-databricks` and installs `databricks-sdk` and
  `databricks-connect` there instead of adding them to the main `.venv`. `smoke-databricks`
  therefore runs with `.venv-databricks/bin/python` directly rather than `uv run` - that's
  expected, not a bug. You don't need to activate or manage `.venv-databricks` yourself;
  every `make` target that needs it already points at the right interpreter.
- **You will be asked to log in twice, in two different browser windows.** The
  `databricks auth login` you ran above authenticates the Databricks CLI and Databricks
  Connect (used by `smoke-databricks`). dbt-databricks's `auth_type: oauth` keeps its own,
  separate OAuth token cache and does not read the CLI's login - the first time
  `make smoke-dbt` runs `dbt debug` or `dbt run`, a second browser window opens for a dbt
  login. This is normal: log in again with the same Databricks account and the token is
  cached for subsequent runs.

## Everything at once (after the one-time auth steps above)

```bash
make smoke-all
```

## What's not here yet

Real data generation, dbt medallion models, Airflow/Docker orchestration, and the AI/RAG
layer are separate sub-projects (B-H) - see the design spec.
