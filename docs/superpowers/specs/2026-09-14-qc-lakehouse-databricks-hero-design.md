# QC Lakehouse (Databricks-Hero) - Design

**Status:** Approved for sub-project A. Sub-projects B-F are decomposed below but not yet individually designed.

## 1. Background

An earlier planning pass (`QC-datapipelineplan.md` at the repo root) produced a detailed
"local-primary" architecture: PySpark + Delta Lake on disk is the authoritative engine, and
Databricks Free Edition is exercised only as a late-phase parity check (Phase 14) for three
features OSS Spark lacks (Auto Loader, Unity Catalog, Photon).

That plan is **superseded by this document** for architecture purposes. The goal has changed:
Databricks is now the primary/hero compute, not a parity target. `QC-datapipelineplan.md`
remains useful as a source of domain content (the quick-commerce data generator design,
defect catalog, dbt model list) but its "local-primary" framing, phase ordering, and ADRs
tied to that framing (ADR-004/013, the Postgres Hive metastore, host-native Airflow) do not
apply to the rebuilt architecture.

Source material: `yt_transcript_01.md` / `yt_transcript_02.md` (a Zomato-style "AI Data
Analytics / End-to-End AI Data Engineering" tutorial) define the quick-commerce delivery
domain - restaurants, orders, couriers, GPS pings, reviews, payments/refunds - reused here
as the dataset shape.

## 2. Goal

Build an open-source-flavored, portfolio-quality quick-commerce delivery data pipeline with
**two co-heroes: Databricks and dbt.** Databricks is the compute engine - ingestion and AI/RAG
enrichment execute as Databricks jobs on serverless compute, using Databricks-proprietary
features (Unity Catalog, Auto Loader, Delta Live Tables/Lakeflow, Photon, Vector Search, AI
Functions) wherever they add real value. dbt (`dbt-databricks`) is the transformation engine -
it owns the entire bronze->silver->gold medallion build, including tests, documentation, and
lineage, rather than being one option among several ways the pipeline could shape data. A
pipeline is not "done" until both stories are demonstrable: a Databricks-native ingestion/AI
story and a dbt-native transformation story with real tests and docs. Local tooling exists
only to develop and smoke-test code before it runs for real on Databricks/dbt. Orchestration
is external: a Dockerized Airflow instance triggers Databricks Jobs and dbt runs rather than
Databricks Workflows or dbt Cloud orchestrating themselves.

Build order follows a thin-spine-then-widen approach: get one table flowing end-to-end
(generate -> ingest -> transform -> orchestrate) before adding breadth or the AI/RAG layer.

## 3. Constraints

- **Databricks Free Edition** (databricks.com/free) - no cost, serverless-only compute
  (no provisioned/all-purpose clusters), Unity Catalog included. Design must not assume
  provisioned clusters are available.
- Local machine: macOS (darwin), zsh.
- Python tooling: `uv` (venv + package management), Python 3.12.
- No local Postgres / local Hive metastore. Local Spark is a lightweight dev/test tool only,
  using Spark's default local catalog.
- Orchestration: Docker + Airflow, external to Databricks, triggering Databricks Jobs via the
  Databricks Airflow provider / Jobs API. Compute always happens on Databricks, never in the
  Airflow containers.
- dbt targets Databricks (`dbt-databricks`) against a serverless SQL warehouse - not
  `dbt-spark` against local Spark.
- AI/RAG layer uses Databricks Vector Search (not FAISS) where Free Edition supports it, plus
  the Anthropic Claude API for enrichment, matching the original plan's tiered-model approach
  (Haiku for full-volume enrichment, Opus for an eval slice).

## 4. Sub-project decomposition

Each sub-project below is independently shippable and gets its own design/spec/plan cycle
when its turn comes. This document only fully specifies **Sub-project A**.

| # | Sub-project | Depends on | One-line scope |
|---|---|---|---|
| A | Foundation & Environment | - | venv, incremental deps, Databricks Free Edition + Unity Catalog setup, dbt-databricks connectivity, local + Databricks + dbt smoke tests |
| B | Data generation & ingestion (bronze) | A | Quick-commerce synthetic generator; Auto Loader batch + streaming into Unity Catalog bronze |
| C | Transformation (dbt-databricks medallion) | B | Full dbt project against Databricks serverless SQL warehouse: bronze->silver->gold, tests, docs, lineage - this is dbt's hero showcase |
| D | AI/RAG layer | C | Claude-powered review enrichment, embeddings, Databricks Vector Search / AI Functions |
| E | Orchestration | B, C (D once it exists) | Dockerized Airflow triggering Databricks Jobs for ingestion/dbt/AI tasks, retries/SLAs |
| F | Polish | A-E | CI (GitHub Actions), Free Edition credit/cost guardrails, README/narrative, optional Streamlit dashboard |

MVP/Level 1 = a thin slice through A -> B -> C -> E: one bronze table, one dbt model,
one Airflow DAG that successfully triggers a Databricks job. D layers on afterward.

## 5. Sub-project A: Foundation & Environment - detailed design

### 5.1 Repo layout

```
qc-lakehouse/
  pyproject.toml          # uv-managed, pinned Python 3.12
  .python-version
  .env.example            # DATABRICKS_HOST, DATABRICKS_CATALOG, etc - .env itself gitignored
  Makefile                # make venv / make smoke-local / make smoke-databricks / make smoke-dbt
  conf/spark-local.conf   # minimal local Spark+Delta conf, no metastore wiring
  src/qc_lakehouse/       # shared python package (config, session helpers)
  dbt/qc_lakehouse/       # dbt project scaffold (profiles.yml via env vars, no models yet)
  tests/
  README.md
```

### 5.2 Incremental install sequence

Each step ends in something runnable, so the environment is validated incrementally rather
than all at once:

1. `uv venv` pinned to Python 3.12. Verify with a trivial `python -c "print('ok')"`.
2. Baseline dev tooling via `uv pip install`: `ruff`, `pytest`, `python-dotenv`. Confirms the
   venv/uv loop works before anything data-related is added.
3. Local Spark (dev/test tool only): `brew install openjdk@17`, then
   `uv pip install pyspark delta-spark`. Smoke test writes/reads a small local Delta table
   using `spark.sql.warehouse.dir=./warehouse` and Spark's default in-memory catalog - no
   metastore setup.
4. Databricks tooling: `uv pip install databricks-sdk databricks-connect` (version matched to
   the Free Edition workspace's runtime), plus the Databricks CLI via the official installer
   script (not pip). Auth via `databricks auth login` (OAuth U2M, browser-based - no
   long-lived token to manage).
5. Databricks smoke test: a script using
   `DatabricksSession.builder.serverless(True).getOrCreate()` that creates a Unity Catalog
   catalog/schema if needed, then writes and reads back a Delta table on serverless compute.
6. dbt tooling: `uv pip install dbt-databricks`, then an empty `dbt/qc_lakehouse` project
   scaffolded via `dbt init` with `profiles.yml` pointed at the same Databricks serverless SQL
   warehouse (credentials from the same `.env` used in step 4/5, no separate auth story).
   Smoke test is `dbt debug` passing, plus a single trivial seed or `select 1` model run via
   `dbt run` to prove dbt can actually execute against Databricks end to end - not just
   authenticate.

### 5.3 Success criteria

- `make smoke-local`, `make smoke-databricks`, and `make smoke-dbt` all run green from a clean
  clone in under a few minutes.
- `.env.example` documents every required variable (shared between Databricks Connect and
  dbt's `profiles.yml`); `.env` itself is gitignored.
- Databricks auth and dbt connectivity are both reproducible by another machine following the
  README (no hardcoded tokens or workspace-specific paths).

### 5.4 Explicitly out of scope for A

Real data generation, real dbt models (the bronze->silver->gold medallion build), Airflow/
Docker, and the AI/RAG layer are not part of A. A proves the toolchain and all three
execution paths (local Spark dev-loop, Databricks compute, dbt-against-Databricks) work; it
does not build pipeline or transformation logic - that's B and C.

## 6. Open questions for later sub-projects (not blocking A)

- Exact `databricks-connect` version pin depends on the Free Edition workspace's current
  serverless runtime version at implementation time - confirm during A rather than pinning
  here.
- Whether Free Edition's Vector Search availability is sufficient for Sub-project D, or
  whether that sub-project needs a fallback (e.g. a Databricks-native alternative to FAISS
  using plain Delta + embeddings columns) - defer investigation to D's design.
