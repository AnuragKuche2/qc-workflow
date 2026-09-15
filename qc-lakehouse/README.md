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
make install             # base install: just python-dotenv + dev tooling (ruff, pytest)
make install-spark       # adds pyspark + delta-spark, needed for tests/smoke-local/generation
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
declare with no mutation - the right choice for a clean clone. Its base dependency set is
deliberately minimal (just `python-dotenv`) - `pyspark`, `delta-spark`, and `dbt-databricks`
live in `pyproject.toml`'s `[project.optional-dependencies]` groups instead of the base
`dependencies` list, so the `qc_lakehouse` wheel built for Databricks Jobs
(`databricks.yml`) doesn't pull in a `pyspark` pin that conflicts with serverless compute's
own immutable package constraints. That means **`make install` alone is not enough for the
full local dev loop**: `make install-spark` (installs the `spark` extra) is required before
`make check`/`make smoke-local`/the generation scripts will work, and `make install-dbt`
(installs the `dbt` extra) is required before the dbt steps below. Both use `uv sync
--extra <name> --extra <other>`, syncing **both** extras together rather than just their own -
`uv sync --extra X` alone defaults to exact mode, which would remove the *other* extra's
packages, so running `install-spark` then `install-dbt` (or vice versa) would otherwise leave
the venv without the first one's packages. Because of this, either target alone installs
exactly what's already declared, with no mutation, and reproduces the full
base+spark+dbt environment - running the other one afterward is a no-op. The
`install-baseline` target (which does use `uv add`, and does mutate `pyproject.toml`/
`uv.lock`) is separate and only useful if you genuinely want to add a new base dependency to
the project.

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

make install-dbt         # installs dbt-databricks into .venv
make smoke-dbt           # dbt debug + a trivial dbt run, same Databricks connection
```

`dbt-databricks` is not part of `make install`'s base dependency set - `make install-dbt`
installs it into the main `.venv` from the `dbt` optional-dependency group before `smoke-dbt`
(or any other dbt step) will work.

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

`make generate-reference-data` creates the `qc_dev` catalog and `bronze_source` schema
automatically if they don't already exist - nothing needs to be pre-created by hand.

This does not use Auto Loader - reference/dimension data is a one-time seed of the
starting world, not a stream of arriving files, so Auto Loader's incremental-ingestion
value doesn't apply here. Auto Loader is introduced in a later widen phase, for the
fact tables (orders, order_events, courier_shifts, gps_pings) this generator does not
produce.

Idempotent: every write is `mode("overwrite")` from a fixed seed
(`GeneratorConfig.seed`), so rerunning reproduces the same data rather than
accumulating duplicates.

## Money-chain fact tables (Sub-project B widen step W1a)

Ported design from `docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md`,
section 6.6. Generates `orders`, `order_items`, `match_attempts`, `payments`, `refunds` from
the already-written `demand_hourly` table and reference-layer profile tables, and writes them
to `qc_dev.bronze_source`. Requires `make generate-reference-data` to have been run first.

```bash
make generate-fact-data
```

Two deliberate, isolated defects are present in the generated data (see
`src/qc_lakehouse/generator/defects.py`), matching the superseded old plan's defect catalog:
`refunds.refund_amount_raw` is a STRING, with a `money_text_defect_rate` fraction using
accounting-negative parens formatting (e.g. `"(20.47)"`); `orders.delivery_notes` gets
casing/whitespace mangling on a `text_noise_defect_rate` fraction of non-null notes. Both are
there deliberately, for Sub-project C's dbt staging models to clean.

No Auto Loader here either - still a from-scratch generation run, direct-write to Delta.
Idempotent the same way the reference layer is: `mode("overwrite")` from a fixed seed.

## dbt medallion transformation (Sub-project C)

Full bronze -> silver -> gold dbt project against `qc_dev.bronze_source`, per
`docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md` section 8.

- **Silver** (`qc_dev.silver`): 14 staging models (1:1 with each bronze source, type
  casting), plus 2 intermediate models for cross-table logic. Cleans both of W1a's
  deliberate defects (`stg_orders`'s text-noise mangling, `stg_refunds`'s money-as-text
  formatting - both flagged via a `was_*_defect` boolean column for auditability) and masks
  customer PII (`stg_customers`: email one-way hashed, phone partially masked).
- **Gold** (`qc_dev.gold`): a proper dimensional model - `dim_customer`, `dim_restaurant`,
  `dim_rider`, `dim_zone`, `dim_date`, plus `fct_orders` (order economics) and
  `fct_deliveries` (match/courier-assignment operations - deliberately NOT a full
  delivery-SLA fact, since no delivery-completion timestamp exists in bronze data yet).
- **Testing**: generic dbt tests (not_null/unique/relationships/accepted_values) on every
  model, plus 3 custom singular SQL tests re-implementing the money-chain invariants
  `fact_writer.py` already checks in Python - the same invariants verified independently by
  two different tools at two different layers.

```bash
make dbt-run    # builds every silver + gold model against qc_dev
make dbt-test   # runs every generic + custom test
make dbt-docs   # generates the browsable docs site + lineage DAG
```

Requires `make generate-reference-data` and `make generate-fact-data` to have been run first
(dbt only reads `qc_dev.bronze_source`, never writes to it).
