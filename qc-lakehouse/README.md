# QC Lakehouse

A quick-commerce delivery data pipeline with Databricks and dbt as co-hero technologies. This
README covers Sub-projects A (toolchain setup), B/W1a (reference and fact data generation), C
(dbt medallion transformation), E1 (Airflow orchestration), and H (performance, maintenance &
cost lab). See
`docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md` for the full
architecture and the sub-projects not yet built here.

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

Sub-projects D (AI review-issue layer), G (text-to-SQL agent), and F (Polish) are not yet
built - see the design spec (build order A -> B -> C -> E -> H -> D -> G -> F; H, the
performance/cost lab, is done - see "Performance, maintenance & cost lab" below). Streaming
ingestion and its Airflow trigger (Sub-project B2/W1b, E2) aren't built either - see the "Not
yet built" note in "Airflow DAG orchestration" below for details.

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

`stg_customers`'s email hash is salted with `pii_hash_salt`, which has no hardcoded default -
locally it comes from the `PII_HASH_SALT` env var (`.env`), and dbt fails loudly if that isn't
set (see `dbt_project.yml`). Never commit a real value for it.

## Airflow DAG orchestration (Sub-project E1)

Dockerized Airflow triggers 5 Databricks Jobs - `generate_reference_data`, `generate_fact_data`
(Sub-project B1), and the three per-layer dbt jobs `dbt_build_test_staging`,
`dbt_build_test_intermediate`, `dbt_build_test_marts` (Sub-project C) - via the Databricks Jobs
API, in a single sequential DAG (`orchestration/dags/qc_lakehouse_pipeline.py`) that finishes
with a `maintenance` TaskGroup (see "Performance, maintenance & cost lab" below). Compute always
happens on Databricks; Airflow only triggers and polls.

`databricks.yml` deploys `dbt_build_test_staging`, `dbt_build_test_intermediate`, and
`dbt_build_test_marts` as Databricks Jobs (dbt's native `dbt_task` type, serverless, against the
same `qc_dev` catalog as `make dbt-run`/`make dbt-test` above - `dbt_task` generates its own
profile from `warehouse_id`/`catalog`/`schema` rather than passing `--target qc_dev`, so the two
reach the same place by different mechanisms, not literally the same dbt target) - each runs
`dbt build --select <layer>` (build+test interleaved per layer), triggerable standalone via
`databricks bundle run dbt_build_test_staging -t dev` (and similarly for the other two layers),
and end-to-end live-verified as steps of the `qc_lakehouse_pipeline` DAG.

Because the `dbt_task` type has no field to reference a Databricks secret directly, and
`{{secrets/scope/key}}` isn't resolved inside its `commands`, each of those three jobs runs a
small `resolve_secrets` task first (`scripts/resolve_pii_salt.py`) that reads the
`pii_hash_salt` secret from the `qc_lakehouse` Databricks secret scope and republishes it as a
task value, which the dbt task then passes through via `--vars`. This is the same
`pii_hash_salt` explained in the root `.env.example` (salts `stg_customers`' `email_hash` -
changing it rewrites every existing hash) - the Databricks Secret and the local `PII_HASH_SALT`
env var should hold the same value, so hashes computed locally and on a Databricks Job match.

**Known limitation, not a fully-protected secret**: the dbt task echoes its own resolved shell
command - including the substituted salt value - into that task's run output/logs in
plaintext. Anyone with read/API access to one of the dbt Databricks Jobs' runs can see the raw
value there. This is accepted as reasonable for this project's single-user Free Edition
workspace, but is a real exposure surface, not a secret-management best practice - re-examine
before reusing this pattern anywhere with more than one reader of job run history.

Setup:

```bash
make install-airflow        # isolated .venv-airflow, for local DAG validation only
databricks secrets create-scope qc_lakehouse
databricks secrets put-secret qc_lakehouse pii_hash_salt   # same value as .env's PII_HASH_SALT
make bundle-validate        # validates databricks.yml
make bundle-deploy          # deploys the 5 jobs to the qc_dev workspace, serverless compute
cd orchestration && cp .env.example .env   # fill in DATABRICKS_HOST
docker compose up -d        # brings up Airflow at localhost:8080
```

The secret scope must exist before `resolve_secrets` runs - without it, `dbt_build_test_staging`
(the first dbt Databricks Job) fails two tasks into the DAG (after both generation jobs already
succeeded) with an opaque `dbutils.secrets.get` error, rather than failing fast at setup time.

Then configure the `databricks_default` Airflow connection (Admin -> Connections in the UI, or
`airflow connections add`: connection type `Databricks`, host your workspace URL, and a
personal access token or OAuth token in the `token` extra), unpause `qc_lakehouse_pipeline`, and
trigger it from the UI or `docker compose exec airflow airflow dags trigger
qc_lakehouse_pipeline`. A `databricks auth token` U2M OAuth token is short-lived - if the
container stays up for a long time (this pipeline's own live validation run took long enough to
hit this), re-run the same connection setup with a fresh token rather than debugging a
`403 Invalid Token` failure as something else.

The DAG runs on an `@weekly` schedule and is also manually triggerable the same way as above.
Each task has 2 retries with a 2-minute delay, and the DAG has a logged deadline-miss warning
(Airflow 3's replacement for the removed SLA feature - no live alerting channel exists for this
portfolio project, so the callback logs what a production system would page on). The deadline
uses `DeadlineReference.DAGRUN_QUEUED_AT`, not `DAGRUN_LOGICAL_DATE` - a manually-triggered run
has no logical date, and a deadline reference that resolves against a null timestamp silently
never fires (see the code comment in `qc_lakehouse_pipeline.py` for the live-verified detail).

`databricks.yml`'s `dev` target intentionally does not use bundle `mode: development` - that
mode prefixes every deployed job's display name with `[dev <username>]`, which breaks the DAG's
`DatabricksRunNowOperator(job_name=...)` exact-name lookups. Live-verified end to end: a clean
run and an idempotent rerun of the full DAG both succeeded with matching row counts, and an
induced job-name failure confirmed the 2 retries actually engage (2-minute delay between
attempts) before the task and its downstream dependents correctly fail.

Not yet built: B2/W1b (streaming ingestion) and E2 (this DAG's future extension to trigger that
streaming job) - see the design spec's section 9/10 open items.

## Performance, maintenance & cost lab (Sub-project H)

A deliberately large `orders` table was generated in an isolated `qc_dev.perf_bench` schema
and compared across 4 physical layouts (no clustering, partition-by-date, `ZORDER BY zone_id`,
Delta Liquid Clustering) - see
`docs/superpowers/reports/2026-09-16-h-perf-cost-report.md` for the full comparison and
reasoning. The generator initially targeted 500x the baseline order volume, but a live
Databricks Connect session error stopped the first (2026-09-15) attempt partway through, after
2 of 18 planned chunks had already landed durably (73,723,047 rows, ~51.7x baseline). **The
layout comparison and recommendation below are measured against that original 51.7x-scale
run** - it was judged sufficient for a meaningful comparison, and the layouts were never
re-benchmarked at a larger scale. Separately, a resumable-generation capability (added later;
see `perf_lab/generate_benchmark_orders.py`'s own module docstring for full detail, including
its caveat that windowed/resumed generation is not distributionally equivalent to a single
contiguous run) was used across 9 total invocations to extend the raw `orders_bench` source
table to its real final scale of **799,389,745 rows (~561x baseline)**, reaching the full
90-day window (2026-06-01 through 2026-08-30, exclusive end). This later extension grew the raw
generation table only - it did not re-run or change the layout comparison itself; see the cost
report's addendum for the full split between the two numbers. **Delta Liquid Clustering on
`zone_id`** won - lowest total bytes scanned (420,340,485) across the benchmark query set,
ahead of ZORDER (421,585,617), partition-by-date (552,189,357), and no clustering
(616,350,363) - and was applied to the real `fct_orders` table (`ALTER TABLE ... CLUSTER BY
(zone_id)`, durably kept across future `dbt run`s via `liquid_clustered_by` in
`fct_orders.sql`'s own dbt config, not just the one-time migration script).

`OPTIMIZE`/`ANALYZE`/`VACUUM` are now real Databricks Jobs (`optimize_gold_table`/
`analyze_gold_table`/`vacuum_gold_table`), run across all 7 gold tables as the final stage of
the single `qc_lakehouse_pipeline` DAG rather than a separate DAG: a `maintenance` TaskGroup
(`orchestration/dags/maintenance.py`) uses Airflow's dynamic task mapping (`.expand()`) to fan
each of the 3 operations out across all 7 `GOLD_TABLES` entries, with a barrier between
operations (every OPTIMIZE instance completes before any ANALYZE instance starts, and so on -
see that file's module docstring for the full reasoning). It runs whenever
`qc_lakehouse_pipeline` runs - on its `@weekly` schedule, or via a manual
`airflow dags trigger qc_lakehouse_pipeline` - using the same `job_name` lookup mechanism as the
rest of the DAG (not `databricks bundle run`, which resolves job names differently - see E1's
`mode: development` bug above for why this distinction matters).

The benchmark's own `qc_dev.perf_bench` schema currently still exists and holds this data
(`orders_bench_baseline` at its original ~51.7x scale, and `orders_bench` extended to
~561x) - it was not dropped after the winning layout was applied to `fct_orders`. Whether to
drop it now is a separate decision that has not been made.
