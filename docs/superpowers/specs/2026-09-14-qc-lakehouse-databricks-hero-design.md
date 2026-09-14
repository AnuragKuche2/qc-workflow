# QC Lakehouse (Databricks-Hero) - Design

**Status:** Approved for sub-project A. Sub-projects B-H are decomposed below but not yet individually designed.

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
Databricks Workflows or dbt Cloud orchestrating themselves, and it only triggers the AI/RAG
review-enrichment layer once ingestion and transformation are already flowing end to end -
AI enrichment reads from already-built gold marts, it does not gate them.

The explicit ambition is to end up with a more complete version of what
`yt_transcript_01.md` / `yt_transcript_02.md` build (a Zomato-style "AI Data Analytics" data
pipeline): the same quick-commerce domain, but with Databricks and dbt as genuine co-heroes,
a structured AI review-issue layer, and a natural-language (text-to-SQL) analytics agent on
top - capabilities the tutorial's own pipeline does not have.

Build order follows a thin-spine-then-widen approach: get one table flowing end-to-end
(generate -> ingest -> transform -> orchestrate) before adding breadth, the AI review layer,
or the text-to-SQL agent.

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
- **Idempotency:** every ingestion and transformation task must be safely re-runnable with the
  same input and produce the same result - MERGE-based upserts keyed on a natural/business
  key, never blind `INSERT`/append-only writes for anything Airflow might retry. This applies
  to B (bronze ingestion) and C (dbt incremental models) equally.
- **Effectively-once, not literal exactly-once:** true exactly-once delivery does not exist in
  distributed systems. The achievable and correct target is *effectively-once results under
  retries*, via Structured Streaming checkpoints (B) plus idempotent MERGE writes (B, C) plus
  Airflow tasks that are safe to retry without operator intervention (E). Any doc/README
  language must say "effectively-once," not "exactly-once."

## 4. Sub-project decomposition

Each sub-project below is independently shippable and gets its own design/spec/plan cycle
when its turn comes. This document only fully specifies **Sub-project A**.

| # | Sub-project | Depends on | One-line scope |
|---|---|---|---|
| A | Foundation & Environment | - | venv, incremental deps, Databricks Free Edition + Unity Catalog setup, dbt-databricks connectivity, local + Databricks + dbt smoke tests |
| B | Data generation & ingestion (bronze) | A | Quick-commerce synthetic generator; Auto Loader batch + streaming into Unity Catalog bronze |
| C | Transformation (dbt-databricks medallion) | B | Full dbt project against Databricks serverless SQL warehouse: bronze->silver->gold, tests, docs, lineage - this is dbt's hero showcase |
| D | AI review-issue layer | C | Claude-powered review enrichment: classifies each review into structured issue categories (late delivery, food temperature, food quality/taste, wrong order, packaging, courier behavior, other/none), aggregable by restaurant, city, and cuisine; embeddings + Databricks Vector Search / AI Functions for retrieval |
| E | Orchestration | B, C (D, G once they exist) | Dockerized Airflow triggering Databricks Jobs for ingestion/dbt/AI/text-to-SQL tasks, retries/SLAs |
| F | Polish | A-E | CI (GitHub Actions), Free Edition credit/cost guardrails, README/narrative, optional Streamlit dashboard |
| G | Text-to-SQL analytics agent | C (D optional, enriches answerable questions) | Custom Claude-based agent: natural-language question -> generated SQL -> executed against gold marts (and D's review-issue marts once they exist) -> answer. Own prompting/schema-context/validation, not Databricks Genie |
| H | Performance, maintenance & cost optimization lab | B, C | Benchmark partitioning vs Z-order vs Liquid Clustering on a fan-out-scale table; scheduled `OPTIMIZE`/`ANALYZE`/`VACUUM` as maintenance tasks in E; cost-based comparison report per layout using real Databricks cost signals, not assertions |

MVP/Level 1 = a thin slice through A -> B -> C -> E: one bronze table, one dbt model,
one Airflow DAG that successfully triggers a Databricks job, built idempotently from the
start. D, G, and H layer on afterward, in any order, once C's gold marts exist and carry
real data volume.

**H's scope in more detail** (deferred to H's own design pass, noted now so it isn't lost):
- Layout comparison on a table with enough rows that file-skipping differences are actually
  observable (the old plan's ADR-016 found that at small scale, `OPTIMIZE` compacts a table
  to one file and every layout measures identically - the benchmark table needs a deliberate
  fan-out, not the pipeline's normal MVP-scale data).
- Compare: no clustering (baseline) vs. partition-by-date vs. `ZORDER BY` on high-cardinality
  predicate columns vs. Delta Liquid Clustering - Databricks' current recommended replacement
  for partitioning+Z-order on new tables.
- `OPTIMIZE`, `ANALYZE` (stats for the query optimizer), and `VACUUM` (with an explicit,
  safe retention policy - never below Delta's 7-day default without a stated reason) become
  scheduled maintenance tasks orchestrated by Airflow (E), not one-off manual commands.
- Cost evaluation uses real Databricks signals (e.g. `system.billing.usage`, query-level
  bytes-scanned) rather than proxies - pending confirmation that Free Edition exposes these
  system tables (see open questions).

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
- How Sub-project G's agent gets schema context (static schema dump vs. live `information_schema`
  lookups vs. a fixed set of vetted query templates it fills in) and how generated SQL is
  validated/sandboxed before execution against real gold marts - defer to G's design.
- Whether Databricks Free Edition exposes `system.billing.usage` / `system.query.history` (or
  equivalents) for real cost measurement in Sub-project H, or whether a fallback (bytes-scanned
  from query metrics, DBU-seconds estimated from job run duration) is needed - confirm during
  H's design, since Free Edition's system-table access may differ from a paid workspace.
