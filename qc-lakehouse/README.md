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
make install             # installs everything already declared in pyproject.toml/uv.lock
make check               # confirms the fast test suite is green so far

brew install openjdk@17 # required by local Spark, not installable via uv
export JAVA_HOME=$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home
make smoke-local         # local Spark + Delta round-trip, no Databricks needed yet
```

`openjdk@17` is Homebrew keg-only - it is never symlinked into `/opt/homebrew`, so `java` and
`JAVA_HOME` are not set up just by installing it. Add the `export JAVA_HOME=...` line above to
your shell profile (`~/.zshrc` on a default macOS setup) rather than only running it once in
your current terminal - a one-off `export` will not persist across new terminal sessions, and
`make smoke-local` will fail with "JAVA_HOME is not set and no 'java' command could be found"
the next time you open a fresh shell.

`make install` runs `uv sync`, which installs exactly what `pyproject.toml`/`uv.lock` already
declare with no mutation - the right choice for a clean clone. The `install-baseline`/
`install-spark`/`install-dbt` Makefile targets still exist (they use `uv add`) but are only
useful later, if you genuinely want to add a new dependency to the project.

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

make smoke-dbt           # dbt debug + a trivial dbt run, same Databricks connection
```

(`dbt-databricks` is already installed into `.venv` by `make install` above - there's no
separate `make install-dbt` step needed here; that target only exists for adding a new
dependency later.)

Two things to know about this step:

- **`make install-databricks` builds a second virtual environment**, `.venv-databricks`,
  alongside the default `.venv`. `databricks-connect` cannot be installed into the same
  environment as `pyspark` (they pin conflicting internal versions), so
  `install-databricks` creates `.venv-databricks` and installs `databricks-sdk` and
  `databricks-connect` there instead of adding them to the main `.venv`. `smoke-databricks`
  therefore runs with `.venv-databricks/bin/python` directly rather than `uv run` - that's
  expected, not a bug. You don't need to activate or manage `.venv-databricks` yourself;
  every `make` target that needs it already points at the right interpreter.
  `install-databricks` pins exact `databricks-sdk`/`databricks-connect` versions in the
  Makefile (matching what's confirmed working against Free Edition's serverless runtime), so
  rebuilding `.venv-databricks` reproduces the same versions rather than drifting to whatever
  resolves newest.
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

## Reference data generator (Sub-project B spine)

Ported from a Databricks notebook (see
`docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md`, section 6).
Generates the quick-commerce world's starting state - cities, zones, restaurants, riders,
menu items, customers, payout tiers, and a 90-day demand curve - and writes it directly to
Delta tables in `qc_dev.bronze_source` (not `workspace.dev`, which stays reserved for
Sub-project A's own throwaway smoke-test tables).

```bash
make generate-reference-data
```

This does not use Auto Loader - reference/dimension data is a one-time seed of the
starting world, not a stream of arriving files, so Auto Loader's incremental-ingestion
value doesn't apply here. Auto Loader is introduced in a later widen phase, for the
fact tables (orders, order_events, courier_shifts, gps_pings) this generator does not
produce.

Idempotent: every write is `mode("overwrite")` from a fixed seed
(`GeneratorConfig.seed`), so rerunning reproduces the same data rather than
accumulating duplicates.
