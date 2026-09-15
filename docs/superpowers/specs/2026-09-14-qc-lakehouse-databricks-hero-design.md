# QC Lakehouse (Databricks-Hero) - Design

**Status:** Sub-project A built and merged. **B1** (spine, section 6) and **W1a** (money-chain fact tables, section 6.6) built and merged. **C** (dbt medallion, section 8) built and merged. **E1** (Airflow triggering B1+C via Databricks Jobs, section 9) is designed, pending implementation. **B2**/W1b (order_events/courier_shifts/gps_pings + Auto Loader, **streaming**) and **E2** (Airflow triggering/monitoring B2 once it exists, **streaming**) are deferred - not yet designed. D, G, H, F are decomposed below but not yet individually designed. Build order is fixed: A -> B -> C -> E -> H -> D -> G -> F (see section 4).

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

## 6.6 Sub-project B, widen step W1a: money-chain fact tables - detailed design

### 6.6.1 Background

Sub-project B's spine (section 6) built and live-verified the reference/dimension layer only
(`cities`, `zones`, `restaurants`, `riders`, `menu_items`, `customers`, `rider_payout_tiers`,
`demand_daily`, `demand_hourly`, plus the two `_gen_*` generator-only profile tables) in
`qc_dev.bronze_source`. This section widens B with the first fact-table slice: the
**commercial transaction layer** - `orders`, `order_items`, `match_attempts`, `payments`,
`refunds` - without yet modeling physical fulfillment (courier assignment timing, GPS,
delivery-event lifecycle). That's W1b, a separate future design pass.

An older superseded planning pass (`QC-datapipelineplan.md`) contains a real, if partial,
defect catalog and domain ideas for this layer - notably defect **#18** (money stored as text,
with accounting-style negatives in parens, e.g. `"(20.47)"`, which breaks a naive `CAST` under
ANSI mode) and defect **#17** (text casing/whitespace noise on free-text fields). Both are
adopted here, adapted to this project's own table shapes (the old plan's own field lists for
these tables were themselves incomplete - it references an external generator source that
isn't part of this repo - so the schemas below are designed fresh, not ported).

### 6.6.2 Scope decisions

- **Five tables only**: `orders`, `order_items`, `match_attempts`, `payments`, `refunds`.
  `order_events`, `courier_shifts`, `gps_pings` are W1b (separate design pass) - that's where
  Auto Loader and streaming are introduced, once there's a genuinely high-volume,
  genuinely-benefits-from-incremental-ingestion table to justify them.
- **No Auto Loader here either.** Same reasoning as the spine: this is still a from-scratch
  generation run, not a stream of arriving files. Direct-write to Delta.
- **Order volume comes from the already-built demand curve, not a new count.** For every
  `(day, hour, order_count)` row in `demand_hourly`, generate exactly that many orders with
  `placed_at` timestamps jittered within that hour. This is literally what `demand_hourly`
  exists for (the source notebook's own comment: "Does NOT generate orders; notebook 02 reads
  these tables").
- **Restaurant and customer selection reuse the generator-only profile tables**: restaurant
  chosen per order weighted by `_gen_restaurant_profile.popularity_weight` (Zipf), customer
  chosen weighted by `_gen_customer_profile.order_propensity` (lognormal). This is the payoff
  for keeping those tables separate from the source-visible ones in the spine.
- **Reads reference tables back from Delta, not in-process Python objects.** This is a
  separate script run from the spine's generator - `spark.table("qc_dev.bronze_source.zones")`
  etc., not passing `zones: list[tuple]` across a process boundary. The spine's `writer.py`
  keeps building everything in one in-memory pass; this widen step is architecturally a
  different, later run.
- **Generation is Spark-native throughout, not Python loops.** Order volume is driven by
  `demand_hourly`'s counts and can reach roughly 1.3-2M+ rows (baseline `orders_per_day=15_000`
  x `days=90`, plus event-day spikes) - at that scale, driver-side Python loops (the style
  `entities.py` uses for `restaurants`/`riders`, which only reach low thousands) would be slow
  and wouldn't use the serverless compute the project pays for. `fact_entities.py` follows the
  spine's `customers.py` pattern instead: deterministic hash-based field derivation
  (`F.hash(col, salt)`, never `F.rand()`) and broadcast joins against the small reference
  tables, all as Spark DataFrame pipelines. Tested the same way `customers.py` is - via
  `qc_lakehouse.spark_local.build_local_spark_session()`, not plain `pytest` - since this code
  is Spark-dependent by design, not incidentally.
- **Deliberate defects, isolated in their own module** (`defects.py`), so they're
  independently unit-testable and their probability is controlled by explicit
  `GeneratorConfig` fields, not hardcoded:
  - `refunds.refund_amount_raw`: stored as a `STRING`, not `DECIMAL` - a
    `money_text_defect_rate` fraction of rows use accounting-negative-in-parens format
    (`"(12.50)"`), the rest are plain (`"12.50"`). Placed on `refunds` specifically because a
    refund is naturally a credit/negative-flavored amount - realistic, not arbitrary.
  - `orders.delivery_notes`: a free-text field; a `text_noise_defect_rate` fraction of rows get
    random casing/whitespace mangling (upper/lower/leading-space/trailing-space).
- **Idempotent by the same pattern as the spine**: full-table `mode("overwrite")` regeneration
  from a fixed seed - not incremental MERGE. Same justification as spine section 6.2's writer:
  deterministic regeneration from a fixed seed is safe to retry, unlike an accumulating append.
- **Same real assert-based integrity checks as the spine**, extended for this layer (see 6.6.4)
  - continuing the "assert, not print, and run before any write" discipline.

### 6.6.3 Table shapes

All five tables land in `qc_dev.bronze_source`, same catalog/schema as the spine.

**`orders`** (one row per generated order):
`order_id` (long, PK), `order_ref` (string, e.g. `"O-0000001"`), `customer_id` (long, FK
customers), `restaurant_id` (long, FK restaurants), `zone_id` (long, FK zones - the customer's
delivery zone), `placed_at` (timestamp), `order_status` (string: `DELIVERED` / `CANCELLED` /
`UNFULFILLED`), `subtotal` (`DECIMAL(18,2)` - sum of `order_items.line_total`),
`delivery_fee` (`DECIMAL(18,2)` - snapshotted from `zones.base_delivery_fee` at order time),
`commission_pct` (`DECIMAL(5,4)` - snapshotted from `restaurants.commission_pct`),
`commission_amount` (`DECIMAL(18,2)` - `subtotal * commission_pct`, quantized once),
`order_total` (`DECIMAL(18,2)` - `subtotal + delivery_fee`, clean, no defect),
`delivery_notes` (string, nullable - the text-noise-defect field).

**`order_items`** (one row per line item, `menu_items_per_order` roughly 1-4, weighted toward
1-2): `order_item_id` (long, PK), `order_id` (long, FK), `menu_item_id` (long, FK menu_items),
`quantity` (int), `unit_price` (`DECIMAL(18,2)` - snapshotted from `menu_items.price` at order
time, since menu prices drift over the generation window), `line_total` (`DECIMAL(18,2)` -
`quantity * unit_price`, quantized once).

**`match_attempts`** (one or more rows per order - the courier-offer process; UNFULFILLED
orders have zero `ACCEPTED` rows, DELIVERED/CANCELLED orders have exactly one):
`match_id` (long, PK), `order_id` (long, FK), `rider_id` (long, FK riders - the candidate
offered), `attempt_number` (int, 1-based), `offered_at` (timestamp), `response` (string:
`ACCEPTED` / `DECLINED` / `TIMEOUT`), `responded_at` (timestamp).

**`payments`** (one row per order that reaches a payable state - i.e. not `UNFULFILLED`):
`payment_id` (long, PK), `order_id` (long, FK), `amount` (`DECIMAL(18,2)` - equals
`orders.order_total` for `SUCCESS` rows), `method` (string: `card` / `upi` / `wallet` / `cod`),
`status` (string: `SUCCESS` / `FAILED`, a small `payment_failure_rate` fraction), `paid_at`
(timestamp).

**`refunds`** (one row for every `CANCELLED` order, plus a `refund_rate` fraction of
`DELIVERED` orders - quality-issue refunds): `refund_id` (long, PK), `order_id` (long, FK),
`payment_id` (long, FK), `refund_amount_raw` (STRING - the money-text-defect field), `reason`
(string: `CANCELLED` / `QUALITY_ISSUE` / `LATE_DELIVERY` / `MISSING_ITEMS`), `refunded_at`
(timestamp).

### 6.6.4 New `GeneratorConfig` fields and integrity checks

`GeneratorConfig` (already defined in the spine, section 6.3) gains: `cancel_rate: float =
0.06`, `unfulfilled_rate: float = 0.025`, `refund_rate: float = 0.028` (fraction of
`DELIVERED` orders that also get a quality-issue refund), `payment_failure_rate: float =
0.01`, `money_text_defect_rate: float = 0.15`, `text_noise_defect_rate: float = 0.08`.

New real (assert-based, pre-write) checks, continuing the spine's pattern: order counts per
`(day, hour)` must exactly match `demand_hourly`; every `order_item`/`match_attempt`/
`payment`/`refund` must reference a real `order_id` (and `menu_item_id`/`rider_id`/
`payment_id` respectively); every matched order (`DELIVERED`/`CANCELLED`) has exactly one
`ACCEPTED` `match_attempt`, every `UNFULFILLED` order has zero; `orders.subtotal` equals the
sum of its `order_items.line_total`; every `SUCCESS` payment's `amount` equals its order's
`order_total`.

### 6.6.5 File structure

```
qc_lakehouse/generator/
  defects.py         # pure functions: format_money_with_accounting_negative(amount, rng) -> str,
                      # mangle_text_casing_and_whitespace(text, rng) -> str - no rate logic here,
                      # callers decide whether to apply based on GeneratorConfig's rate fields
  fact_entities.py     # build_orders, build_order_items, build_match_attempts, build_payments,
                        # build_refunds - each a Spark DataFrame pipeline (like customers.py,
                        # not entities.py's Python loops - see 6.6.2), reading demand_hourly +
                        # reference tables back from Delta via spark.table(...)
  fact_writer.py         # write_fact_tables(spark, config) -> dict[str, int]: runs the 6.6.4
                          # integrity checks before any write, then writes all five tables -
                          # sibling to the spine's writer.py, not an addition to it

qc-lakehouse/scripts/generate_fact_data.py   # entrypoint: load_settings() + build_databricks_session()
                                               # + GeneratorConfig() + call the fact writer,
                                               # mirrors generate_reference_data.py's shape
```

A new `fact_writer.py` (sibling to the spine's `writer.py`, not an addition to it - the spine's
`writer.py` is already ~137 lines for 3 checks + 11-table orchestration, and this widen step
adds 5 more checks + 5-table orchestration, which would make one file unwieldy) reads
reference tables from Delta, builds the five fact tables via `fact_entities.py`, runs the
checks in 6.6.4 before any write, then writes all five - `write_fact_tables(spark, config) ->
dict[str, int]`, same shape as the spine's `write_reference_tables`.

### 6.6.6 Explicitly out of scope for W1a

`order_events`, `courier_shifts`, `gps_pings`, Auto Loader (batch or streaming), and any
dbt/transformation work remain out of scope - deferred to W1b and Sub-project C respectively.

### 6.6.7 Success criteria

- `qc-lakehouse/scripts/generate_fact_data.py` run against live Databricks produces all five
  fact tables in `qc_dev.bronze_source`.
- Total order count across all `orders` rows exactly matches the sum of `demand_hourly.orders`.
- All new integrity checks (6.6.4) genuinely fail the run when violated (same proof-by-test
  discipline as the spine).
- Both defect types are present in the generated data and documented clearly enough that
  Sub-project C's dbt staging models can reference them when writing cleaning logic.
- `make check` gains real, fast, Spark-free unit test coverage for `defects.py`'s pure
  functions.

## 8. Sub-project C: Transformation (dbt-databricks medallion) - detailed design

### 8.1 Background

Sub-project B (done) writes 14 tables to `qc_dev.bronze_source`: 9 reference/dimension-ish
tables (cities, zones, restaurants, riders, menu_items, rider_payout_tiers, customers,
demand_daily, demand_hourly) plus 5 money-chain fact tables from widen step W1a (orders,
order_items, match_attempts, payments, refunds). Two deliberate data-quality defects live in
that bronze data specifically for this sub-project to clean: `refunds.refund_amount_raw` is a
STRING (sometimes accounting-parens formatted, e.g. `"(20.47)"`), and `orders.delivery_notes`
has casing/whitespace mangling on a fraction of non-null rows. This is dbt's hero showcase per
section 4's decomposition table - a full bronze->silver->gold medallion pipeline with real
tests, docs, and lineage against a live Databricks serverless SQL warehouse.

### 8.2 Scope decisions

- **Medallion mapping onto dbt model types**: bronze is unchanged (declared as dbt `sources:`
  against `qc_dev.bronze_source`, never modified). Silver is split into two dbt model types for
  readability/testability, both landing in `qc_dev.silver`: `stg_*` (one per bronze source,
  1:1 grain, type casts + defect cleaning, no joins) and `int_*` (cross-table business logic).
  Gold is a proper dimensional model (`dim_*`/`fct_*`) landing in `qc_dev.gold`.
- **Catalog/schema separation**: dbt gets its own env vars (not `DATABRICKS_CATALOG`/
  `DATABRICKS_SCHEMA`, which stay pointed at `workspace.dev` for Sub-project A's Python-side
  smoke tests) plus a custom `generate_schema_name` macro so `+schema: silver`/`+schema: gold`
  config in `dbt_project.yml` lands models directly in `qc_dev.silver`/`qc_dev.gold` without
  dbt's default schema-prefixing behavior.
- **Materialization**: staging and intermediate models as views (cheap, no storage cost -
  they're cleaning/composition lenses, nothing queries them directly except downstream
  models). Gold marts as tables (queried repeatedly - by this project now, by Sub-project G's
  text-to-SQL agent later - so paying the write cost once beats recomputing joins per query).
  No incremental materialization: bronze itself is fully regenerated via `mode("overwrite")`
  each generator run, not incrementally appended, so incremental dbt models would have no
  natural watermark to key off - full-refresh table/view materializations honestly match how
  the data actually arrives.
- **PII handling**: `customers.py`'s own code comment defers pseudonymization to this
  sub-project ("pseudonymization at Silver has to be real work with a real column mask") -
  `stg_customers` masks `email`/`phone` rather than passing them through raw. This is the one
  requirement not driven by the two named defects but discovered from B's own code.
- **`fct_deliveries` scope**: no delivery-completion timestamp exists anywhere in current
  bronze data (`match_attempts.responded_at` is the courier accepting the job, not completing
  delivery), so true delivery-SLA measurement (actual vs. `zones.sla_target_minutes`) is not
  possible yet. `fct_deliveries` is scoped to what's honestly measurable now: match acceptance
  rate, attempts-per-order, time-to-accept. `sla_target_minutes` is still carried through as a
  dimension attribute so real SLA-adherence measurement is a small addition once W1b lands,
  not a redesign. See section 9 for the related open item on Sub-project D's missing review
  data.
- **Testing depth**: generic tests (`not_null`/`unique`/`relationships`/`accepted_values`) on
  every model, PLUS custom singular SQL tests re-implementing the money-chain invariants
  `fact_writer.py` already checks in Python (`check_subtotal_matches_line_items`,
  `check_payment_amount_matches_order_total`, `check_exactly_one_accepted_match_per_matched_order`)
  - now verified independently over the *gold* layer by a different tool, proving the
  transformation didn't silently break something the generator got right.

### 8.3 Project structure & model list

```
dbt/qc_lakehouse/models/
  staging/
    stg_cities.sql, stg_zones.sql, stg_restaurants.sql, stg_riders.sql,
    stg_menu_items.sql, stg_rider_payout_tiers.sql, stg_customers.sql,
    stg_demand_daily.sql, stg_demand_hourly.sql,
    stg_orders.sql, stg_order_items.sql, stg_match_attempts.sql,
    stg_payments.sql, stg_refunds.sql
    _staging__sources.yml       (source declarations against qc_dev.bronze_source)
    _staging__models.yml        (descriptions + generic tests)
  intermediate/
    int_order_matching.sql      (resolves each order's one ACCEPTED match_attempt, or none)
    int_order_economics.sql     (orders + order_items agg + payments + refunds, one row/order)
    _intermediate__models.yml
  marts/
    dim_customer.sql, dim_restaurant.sql, dim_rider.sql, dim_zone.sql, dim_date.sql
    fct_orders.sql              (grain: order_id - subtotal, fees, commission, refunds, net revenue)
    fct_deliveries.sql          (grain: order_id - match attempts, acceptance, time-to-accept)
    _marts__models.yml
    tests/                      (custom singular SQL tests, one file per invariant)
```

`stg_orders` and `stg_refunds` each add a `was_<defect>_flag` boolean column alongside the
cleaned value, so the cleaning itself is auditable rather than silently invisible.
`dim_restaurant` sources only `restaurants` - never `_gen_restaurant_profile` (generator-only
internals the real pipeline must never touch, per section 6's own schema design). `dim_zone`
folds city attributes (name, country, timezone) in directly rather than a separate `dim_city`
(3 cities total - not worth a standalone dimension at this scale).

### 8.4 Testing & docs strategy

Every model gets a `description:` in its `.yml` (column-level too, especially the two
defect-cleaning columns). `dbt docs generate` produces the browsable docs site + lineage DAG,
wired as a new `make dbt-docs` target alongside the existing `make smoke-dbt`.

### 8.5 Explicitly out of scope for C

Review/comment data and sentiment analysis (Sub-project D's problem - no such data exists yet,
see section 9). True delivery-SLA measurement (needs W1b's delivery-completion events).
Orchestrating dbt runs on a schedule (Sub-project E). Text-to-SQL access to gold marts
(Sub-project G).

### 8.6 Success criteria

- `dbt run` against live Databricks builds all staging/intermediate/mart models into
  `qc_dev.silver`/`qc_dev.gold` without error.
- `dbt test` passes: every generic test plus every custom singular test (the re-implemented
  money-chain invariants) passes against the real, live-generated data.
- Both defects are demonstrably cleaned: `stg_refunds.refund_amount` is a correct
  `decimal(18,2)` for every row regardless of the source string's formatting;
  `stg_orders.delivery_notes` casing/whitespace is normalized for every previously-mangled row.
  Both verified against the flag columns, not just spot-checked.
- `dbt docs generate` produces a working docs site with a visible lineage graph from bronze
  sources through to gold marts.
- `make check`-equivalent dbt coverage: `dbt test` runs cleanly as part of this sub-project's
  own CI-able check command.

## 9. Sub-project E (E1): Orchestration - Dockerized Airflow triggering Databricks Jobs - detailed design

**Labeling convention (applies project-wide from this point on):** each sub-project's parts
are labeled `<Letter><N>`, and any part involving streaming is tagged explicitly rather than
left implicit. **B1** = the spine reference-data generator (done). **B2** = W1b,
`order_events`/`courier_shifts`/`gps_pings` via Auto Loader (pending, **streaming**). **E1** =
this section's scope - Airflow triggering the existing B1 (batch) and C jobs. **E2** = a future
addition to this same DAG, once B2 exists, to trigger/monitor that streaming job (not yet
scoped, **streaming**, deferred until B2 is built - not a separate sub-project, per section 4's
existing note about D/G getting similar small follow-up additions to E later).

**E1 scope boundary:** section 4's one-liner for E says "triggering Databricks Jobs for
ingestion/dbt/AI/text-to-SQL tasks" - but D (AI) and G (text-to-SQL) don't exist yet. Per
section 4's own build-order note, D and G each get a small follow-up addition to this DAG once
they're built, rather than this pass trying to anticipate their shape. E1 is scoped to
ingestion (B1) + dbt (C) triggers only.

### Architecture

- **Databricks Asset Bundles (DABs)** define 4 Databricks Jobs, deployed via
  `databricks bundle deploy` to the Free Edition workspace, each running on serverless compute:
  1. `generate_reference_data` - runs the existing `scripts/generate_reference_data.py` (B1)
  2. `generate_fact_data` - runs `scripts/generate_fact_data.py` (B1/W1a)
  3. `dbt_run` - dbt's native Databricks Jobs task type, `qc_dev` target
  4. `dbt_test` - same, `dbt test`
- **Airflow**, in Docker (SQLite + `SequentialExecutor` - see the metastore decision below),
  runs a single DAG (`qc_lakehouse_pipeline`) with 4 tasks using `DatabricksRunNowOperator`
  (from `apache-airflow-providers-databricks`) - Airflow only triggers and polls; all compute
  stays on Databricks, matching section 3's constraint ("compute always happens on Databricks,
  never in the Airflow containers").
- Task order: `generate_reference_data >> generate_fact_data >> dbt_run >> dbt_test` (fact data
  reads reference tables; dbt reads bronze written by both).
- Trigger: manual only (`schedule_interval=None`). The generator is deterministic (same seed ->
  same data every run), so a recurring cron schedule would just regenerate identical rows each
  time and burn Free Edition credits for no benefit - the DAG exists to demonstrate real
  retries/SLAs/dependencies when triggered, not to run unattended on a clock.

### Airflow metastore decision

Section 3 bans local Postgres/Hive metastore, but the standard Airflow docker-compose needs a
Postgres container for its own scheduler metadata. Resolved as: **SQLite +
`SequentialExecutor`** - Airflow's officially-supported lightweight local mode, no Postgres
container at all. Fully respects the constraint, and is right-sized for a 4-task manual-trigger
DAG that has no concurrency needs.

### Components & auth

- **DAB config** (`qc-lakehouse/databricks.yml`) declares the 4 jobs, targeting
  `qc_dev.bronze_source`/`silver`/`gold` - the same catalog/schema `GeneratorConfig` and
  `dbt_project.yml` already use. Jobs run under the workspace's own OAuth identity; no `.env`
  file is needed inside the job itself.
- **Airflow's Databricks auth**: the `apache-airflow-providers-databricks` connection needs its
  own OAuth credentials (an Airflow connection, reusing `DATABRICKS_HOST`, configured once via
  Airflow's connection UI/CLI - not baked into DAG code).

### Error handling: retries + SLAs

- **Retries**: each task gets `retries=2`, `retry_delay=timedelta(minutes=2)` - a transient
  Databricks Jobs API hiccup or a flaky serverless cold-start retries automatically without
  operator intervention (matches section 3's "effectively-once... tasks safe to retry"
  constraint). Retrying `DatabricksRunNowOperator` is safe because the underlying scripts
  already overwrite deterministically from a fixed seed - a retry never duplicates data.
- **SLAs**: each task gets an `sla` timedelta (first-pass estimates: 15 min for
  `generate_reference_data`, 30 min for `generate_fact_data` at the default ~1.35M-order scale,
  10 min each for the dbt tasks) and an `sla_miss_callback` that logs a structured warning. No
  real alerting channel (Slack/PagerDuty) exists for a portfolio project, so the callback is a
  clearly-labeled stub - it logs what a real system would page on.
- **Failure propagation**: default Airflow behavior, no `trigger_rule` override - a failed task
  blocks its downstream tasks, since `dbt_run` genuinely shouldn't run against incomplete
  bronze data.

### Testing

- **DAG structural test** (pytest, same suite discipline as the rest of the project): load the
  DAG via `airflow.models.DagBag`, assert it imports without errors, has exactly 4 tasks, and
  the dependency chain matches. Catches DAG-definition bugs without needing a live Airflow
  instance.
- **DAB validation**: `databricks bundle validate` as a `make` target, catching job-config
  errors before deploy.
- **Live end-to-end validation** (same pattern as B/C): manually trigger the full DAG once
  against the real Free Edition workspace, confirm all 4 tasks succeed via the Databricks Jobs
  API run status, then trigger it a second time to confirm the idempotent-rerun story holds
  through Airflow too (not just when the scripts are run directly, as W1a already proved).

## 10. Open questions for later sub-projects (not blocking A or B)

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
- **Found during C's brainstorming:** no review/comment/user-generated-text data exists
  anywhere in the pipeline. `orders.delivery_notes` is NOT a substitute - it's a fixed pool of
  6 canned delivery instructions set by the customer at order time, not organic post-delivery
  feedback. Sub-project D (AI review-issue layer) depends on this data existing but nothing
  currently generates it - D's own design needs to include a synthetic review-generation step
  (likely a new widen step in B, or self-contained within D), not assume the data is already
  there.
- **Found during C's brainstorming:** true delivery-SLA measurement (actual delivery time vs.
  `zones.sla_target_minutes`) is not possible with current bronze data - there is no
  delivery-completion timestamp anywhere (`match_attempts.responded_at` is the courier
  *accepting the job*, not completing delivery). C's gold layer (`fct_deliveries`) is
  deliberately scoped to what's honestly measurable now (match acceptance rate,
  attempts-per-order, time-to-accept) rather than fabricating a fake `delivered_at`. Once W1b
  adds real delivery-completion events, true SLA-adherence measurement (actual vs.
  `sla_target_minutes`, already carried through as a dimension attribute) becomes a small
  natural addition to the existing dbt project, not a redesign.
- **Found during E's brainstorming:** `generate_reference_data.py`/`generate_fact_data.py`
  build their Spark session via `databricks.connect.DatabricksSession` (external-client-style
  connection). Running that *from inside* a Databricks Job that's already executing on
  Databricks serverless compute is unusual - Databricks Connect is designed for connecting to
  Databricks from outside it, not for a job to connect to itself. It may work as-is via the
  job's own OAuth identity, or may need a small fallback: detect "running as a Databricks Job"
  (Databricks sets `DATABRICKS_RUNTIME_VERSION` automatically) and use a plain
  `SparkSession.builder.getOrCreate()` instead of Databricks Connect in that case. Validate
  early in E1's implementation, the same way W1a's UDF-sandbox surprise and C's dbt
  double-limit quirk were caught early rather than assumed away.
