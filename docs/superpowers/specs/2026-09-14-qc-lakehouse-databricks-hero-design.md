# QC Lakehouse (Databricks-Hero) - Design

**Status:** Approved for sub-project A (built and merged). Sub-project B's spine (reference-data layer only) is designed in section 6, pending review. Sub-projects C-H are decomposed below but not yet individually designed. Build order is fixed: A -> B -> C -> E -> H -> D -> G -> F (see section 4).

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
| B | Data generation & ingestion (bronze) | A | Quick-commerce synthetic generator; Auto Loader batch + streaming into Unity Catalog bronze. **Spine (section 6) is reference-data only, direct-write to Delta, no Auto Loader** - fact tables (orders, events, GPS pings) and Auto Loader/streaming are a later widen step within B |
| C | Transformation (dbt-databricks medallion) | B | Full dbt project against Databricks serverless SQL warehouse: bronze->silver->gold, tests, docs, lineage - this is dbt's hero showcase |
| D | AI review-issue layer | C | Claude-powered review enrichment: classifies each review into structured issue categories (late delivery, food temperature, food quality/taste, wrong order, packaging, courier behavior, other/none), aggregable by restaurant, city, and cuisine; embeddings + Databricks Vector Search / AI Functions for retrieval |
| E | Orchestration | B, C (D, G once they exist) | Dockerized Airflow triggering Databricks Jobs for ingestion/dbt/AI/text-to-SQL tasks, retries/SLAs |
| F | Polish | A-E | CI (GitHub Actions), Free Edition credit/cost guardrails, README/narrative, optional Streamlit dashboard |
| G | Text-to-SQL analytics agent | C (D optional, enriches answerable questions) | Custom Claude-based agent: natural-language question -> generated SQL -> executed against gold marts (and D's review-issue marts once they exist) -> answer. Own prompting/schema-context/validation, not Databricks Genie |
| H | Performance, maintenance & cost optimization lab | B, C | Benchmark partitioning vs Z-order vs Liquid Clustering on a fan-out-scale table; scheduled `OPTIMIZE`/`ANALYZE`/`VACUUM` as maintenance tasks in E; cost-based comparison report per layout using real Databricks cost signals, not assertions |

MVP/Level 1 = a thin slice through A -> B -> C -> E: one bronze table, one dbt model,
one Airflow DAG that successfully triggers a Databricks job, built idempotently from the
start.

**Chosen build order: A -> B -> C -> E -> H -> D -> G -> F.** Data-engineering-first: the
core pipeline and its orchestration and performance/cost work (A-E, H) are built and proven
before the AI review layer (D) and text-to-SQL agent (G). Two consequences of this order:

- E is built against B/C only (its "(D, G once they exist)" scope does not apply yet). Once
  D and G land, each gets a small follow-up touch to E's Airflow DAG to add their
  triggers/retries - not a separate sub-project, just a short addition folded into D's and
  G's own implementation steps.
- F (Polish) moves to last, after G. This means F's "depends on A-E" scope is satisfied in
  full (A-G are all done by then), so F becomes a single final polish pass - CI, cost
  guardrails, README, optional dashboard - covering the whole system including the AI layer
  and text-to-SQL agent, rather than needing to be split into two passes.

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

## 6. Sub-project B: Data generation & ingestion (bronze) - detailed design (spine only)

### 6.1 Background

A prior planning pass (independent of this spec, done directly in a Databricks notebook -
`QuickcommerceV2N1.html`, exported from the workspace) already built and validated the
reference-data half of the generator: cities, zones, restaurants, riders, menu items,
customers, rider payout tiers, and a demand curve (daily/hourly order counts, event spikes,
day-of-week seasonality). That notebook does **not** generate orders, `order_events`,
`courier_shifts`, or `gps_pings` - a comment in it says explicitly "notebook 02 reads these
tables."

The notebook's domain logic and math were reviewed and found to be genuinely strong: correct
largest-remainder allocation for exact-sum splits, `Decimal`-based money handling, proper
latitude-adjusted geographic math, deliberately shuffled Zipf popularity (so restaurant
popularity is uncorrelated with `restaurant_id`, which matters for H's later file-layout
benchmarks), and generator-only columns split from source-visible columns. It is, however,
notebook-shaped: global mutable state built across cell-execution order, zero automated
tests, `print`-based sanity checks that never fail the run even when they detect a real
problem, and hardcoded config at module scope.

**Sub-project B's spine is a faithful port of this reference-data layer into tested
`qc_lakehouse` code**, not a rewrite of the domain logic and not an expansion to the
order/event/GPS layer - that is explicitly deferred (see 6.4).

### 6.2 Scope decisions

- **Reference layer only.** Orders, `order_events`, `courier_shifts`, `gps_pings` are out of
  scope for B's spine - they are the next widen step (a follow-up sub-project or a later part
  of B), and they are exactly the tables that will actually justify Auto Loader and streaming.
- **No Auto Loader for this layer.** Reference/dimension data is a one-time seed of the
  world's starting state, not a stream of arriving files - Auto Loader's incremental
  file-discovery value doesn't apply here. These tables are written directly to Delta
  (`spark.createDataFrame(...).write.saveAsTable(...)`), matching what the notebook already
  does. Auto Loader is introduced later, for the fact tables, where it earns its keep.
- **Runs via Databricks Connect from local**, reusing `build_databricks_session()` from
  Sub-project A (Task 6) exactly as-is - no new deployment mechanism. When Sub-project E
  (Orchestration) exists, Airflow triggers this same script as a job step; B does not need to
  solve Databricks Job packaging/deployment itself.
- **Faithful port + targeted fixes**, not a rewrite:
  - Domain logic, distributions, and constants are carried over as-is - they are already
    correct and were already empirically tuned (several comments in the source notebook
    record specific earlier bugs and their fixes, e.g. rider counts, SLA ladder minutes,
    festival date placement).
  - Fixed: every pure function (no Spark dependency) gets real `pytest` coverage -
    `allocate`, `stream_seed`, `zipf`, `haversine_km`, `offset_km`, `destination`,
    `spiral_point`, `sample_around`, `zone_density`, `pick`, `day_factors`,
    `orders_by_hour`, `events_on`.
  - Fixed: the notebook's `print`-based sanity checks (hour/day conservation, referential
    integrity, rider capacity) become real `assert` statements that fail the run - the
    notebook's own comment says a conservation failure "would be losing cents in the
    payouts," which is a hard-failure condition, not a print-and-hope one.
  - Fixed: `CATALOG`/`SCHEMA`/`DAYS`/`SEED`/etc. move from hardcoded module-level constants
    to a `GeneratorConfig` dataclass, constructed with the notebook's current values as
    defaults but overridable.
  - **Not fixed (deferred):** positional tuple/list entity records (e.g. `restaurants.append([...])`,
    accessed later as `r[14]`, `r[:14]`) stay as-is. A dataclass/namedtuple refactor is real
    and worth doing, but it touches every generation function and is explicitly out of scope
    for this port - revisit if/when the positional-index fragility actually causes a bug.
- **Where data lands:** catalog `qc_dev`, schema `bronze_source` - matches the notebook's own
  naming, and keeps real pipeline data cleanly separate from Sub-project A's throwaway
  `workspace.dev` smoke-test tables (which must never be treated as pipeline data, per A's
  Global Constraints).

### 6.3 File structure

```
qc-lakehouse/src/qc_lakehouse/generator/
  __init__.py
  math_utils.py     # pure functions: allocate, stream_seed, zipf, haversine_km, offset_km,
                     # destination, spiral_point, sample_around, zone_density, pick,
                     # day_factors, orders_by_hour, events_on - zero Spark dependency
  config.py          # GeneratorConfig dataclass (DAYS, ORDERS_PER_DAY, SEED, START_DATE,
                      # N_CITIES, ZONES_PER_CITY, N_RESTAURANTS, N_RIDERS, N_CUSTOMERS,
                      # MENU_ITEMS_PER_RESTAURANT) + static catalogs (CITIES, ZONE_NAMES,
                      # CUISINES, BRAND_PREFIX, BRAND_CORE, MENUS, VEHICLE_MIX, TIER_MIX,
                      # TIER_RATES, EVENTS)
  entities.py         # non-Spark entity builders: build_cities, build_zones,
                       # build_restaurants, build_riders, build_menu_items,
                       # build_payout_tiers, build_demand_curve
  customers.py          # the one Spark-dependent generation piece (the ~300k-row customer
                         # DataFrame pipeline: broadcast-join zone bucketing, hash-based
                         # deterministic field derivation)
  schemas.py             # StructType definitions, one per table
  writer.py               # orchestrates: build everything, write to qc_dev.bronze_source,
                           # run the (now real) integrity/conservation assertions

qc-lakehouse/scripts/generate_reference_data.py   # entrypoint: load_settings() +
                                                    # build_databricks_session() + call
                                                    # generator.writer, mirrors the shape of
                                                    # scripts/smoke_databricks.py from A

qc-lakehouse/tests/test_math_utils.py              # real pytest coverage for every pure
                                                     # function in math_utils.py - runs in
                                                     # the fast make check suite, no Spark,
                                                     # no live Databricks needed
```

### 6.4 Explicitly out of scope for B's spine

Orders, `order_events`, `courier_shifts`, `gps_pings`, Auto Loader (batch or streaming), and
any dbt/transformation work are not part of this pass. This is the reference/dimension layer
only. The fact-table layer (which is where Auto Loader, streaming, and higher data volume
actually apply) is deliberately deferred to a widen step once this spine is proven working
end to end - matching the project's thin-spine-then-widen philosophy.

### 6.5 Success criteria

- `qc-lakehouse/scripts/generate_reference_data.py` run against live Databricks Free Edition
  produces all reference tables in `qc_dev.bronze_source`, matching the notebook's original
  row counts and summary output.
- The integrity/conservation checks that used to be `print` statements now genuinely fail the
  run (non-zero exit / raised exception) if violated - proven by at least one test that
  deliberately breaks an invariant and confirms the run fails.
- `make check` gains real, fast, Spark-free unit test coverage for every pure function in
  `math_utils.py`.

## 7. Open questions for later sub-projects (not blocking A or B)

- ~~Exact `databricks-connect` version pin depends on the Free Edition workspace's current
  serverless runtime version at implementation time - confirm during A rather than pinning
  here.~~ **Resolved during A:** pinned to `databricks-connect==19.1.*` and
  `databricks-sdk==0.139.*` in `qc-lakehouse/Makefile`'s `install-databricks` target - these
  are the exact versions confirmed working against the Free Edition serverless runtime via
  the real smoke test (Task 6). A future version bump should be a deliberate, tested change.
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
