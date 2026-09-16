# QC Lakehouse Sub-project H (Performance, Maintenance & Cost Lab) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate a deliberately large (~500x current volume) `orders` table, compare 4
physical layouts (no-clustering baseline, partition-by-date, Z-order, Delta Liquid
Clustering) with real Databricks cost signals, apply the winning layout to the real
`fct_orders` table, and add a manually-triggered `OPTIMIZE`/`ANALYZE`/`VACUUM` maintenance
DAG for it.

**Architecture:** An isolated `qc_dev.perf_bench` schema holds the throwaway benchmark data,
completely separate from the real pipeline. Generation and layout-application (`OPTIMIZE`/
`ZORDER`/Liquid Clustering) run via serverless Spark (Databricks Jobs, same pattern as the
existing generator scripts) - not the SQL warehouse, which is capped at one `2X-Small`
instance on Free Edition and carries a real quota-lockout risk for the heaviest operations.
Only the benchmark queries and the cost report query the SQL warehouse, since
`system.query.history` (needed for bytes-scanned cost signals) only tracks warehouse-executed
queries - a much smaller, safer surface than routing everything through it. Generation writes
in date-chunked batches with a checkpoint-based bailout (not a retry-after-failure pattern),
since Databricks' quota enforcement doesn't give a cheap, catchable rejection.

**Tech Stack:** PySpark (Databricks serverless compute), `databricks-sdk`'s
`WorkspaceClient.statement_execution` (SQL warehouse queries), Databricks Asset Bundles,
Airflow (existing E1 infrastructure).

**Spec:** `docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md`, section 10.

## Global Constraints

- **Isolated schema**: all benchmark tables live in `qc_dev.perf_bench` - never touch
  `bronze_source`/`silver`/`gold`. Dropped entirely once the winning layout is applied to
  production (final task).
- **Benchmark table is `orders` only** - not the full money-chain. `order_items` is computed
  in-memory to derive `subtotal`/`order_total` (required by `finalize_orders`) but never
  persisted at scale.
- **Target scale: 500x current order volume** via `orders_per_day=7_500_000` (500x the
  existing `15_000` baseline), keeping `days=90` and all other `GeneratorConfig` fields
  (customer/restaurant/rider/zone counts) at their existing values - this is a
  physical-layout benchmark, not a business-realism exercise, so extreme per-entity order
  density is an acceptable tradeoff. **500x is a target, not a guarantee** - the checkpointed
  generation (Task 1) may bail out at a lower scale; every later task must work correctly at
  whatever scale was actually reached, never assume 500x succeeded.
- **`zone_id` is the fixed Z-order/clustering column** for all 3 non-baseline layouts -
  confirmed via the generator's own existing code comment in `config.py` (zones are ordered
  inner-to-outer by design specifically to create hot-zone skew "the file-layout work
  (Sub-project H) will address").
- **Compute routing (binding, not a suggestion)**: generation (Task 1) and layout application
  including `OPTIMIZE`/`ZORDER BY`/`CLUSTER BY` (Task 2) run via serverless Spark
  (`build_databricks_session`, same as `scripts/generate_reference_data.py`) - never the SQL
  warehouse. Benchmark queries (Task 3) and the cost report (Task 4) query via
  `databricks.sdk.WorkspaceClient().statement_execution` against the one SQL warehouse
  (`ca865a4ef1668613`, confirmed live and already used by dbt's `dbt_run`/`dbt_test` jobs) -
  this is the only compute path that populates `system.query.history`, which both tasks need.
- **Never below Delta's 7-day default VACUUM retention** without a stated reason (project-wide
  constraint, spec section 3) - applies only to the real `fct_orders` table (Task 5); the
  throwaway `qc_dev.perf_bench` tables are never vacuumed, only dropped outright.
- **New Airflow DAG (`qc_lakehouse_maintenance`) is separate from E1's `qc_lakehouse_pipeline`**
  - E1's own scope boundary is ingestion+dbt triggers only. Manual-trigger only
  (`schedule=None`), matching E1 - automatic scheduling is explicitly deferred as a future
  upgrade, out of scope for this plan.
- **Already confirmed live, do not re-derive**: `system.billing.usage` and
  `system.query.history` are both queryable and populated on this Free Edition workspace
  (2,450 billing rows spanning 2026-02-02 to 2026-09-15; 2,392 query-history rows in the most
  recent day alone) - no fallback cost-measurement mechanism is needed.
- **Exact version/schema currency is not guaranteed** - the same category of platform-vs-docs
  gotcha every prior live-run sub-project has hit (W1a's UDF sandbox failure, C's dbt
  double-limit quirk, E1's 6 distinct issues). Specifically: `databricks-sdk`'s
  `statement_execution` API method names/parameters below are a best-effort baseline from the
  pinned `databricks-sdk==0.139.*` - if a call signature doesn't match, check
  `python -c "from databricks.sdk import WorkspaceClient; help(WorkspaceClient().statement_execution)"`
  and adjust; Liquid Clustering's exact SQL syntax (`CLUSTER BY` vs. `CLUSTER BY AUTO`) should
  be confirmed against the current Databricks Runtime via `DESCRIBE TABLE EXTENDED` if
  `ALTER TABLE ... CLUSTER BY` errors.
- Never use an em dash character anywhere (code, YAML, comments, commit messages) - plain
  hyphen only.
- Never add `Co-Authored-By`/`Claude-Session` trailers to any commit.

---

### Task 1: Checkpointed benchmark-orders generation

**Files:**
- Create: `qc-lakehouse/perf_lab/__init__.py` (empty)
- Create: `qc-lakehouse/perf_lab/generate_benchmark_orders.py`
- Test: `qc-lakehouse/tests/test_generate_benchmark_orders.py`

**Interfaces:**
- Produces: `chunk_date_ranges(start_date: str, days: int, chunk_days: int) -> list[tuple[str, str]]`
  - a pure function returning `(chunk_start_iso, chunk_end_exclusive_iso)` pairs - consumed by
    this task's own `main()` and unit-tested directly.
- Produces: `should_bail_out(chunk_elapsed_seconds: list[float], threshold: float = 1.75) -> bool`
  - pure function, `True` when the most recent elapsed time exceeds `threshold` times the
    average of all prior chunks' elapsed times (never true before at least 2 prior chunks
    exist) - consumed by `main()`, unit-tested directly.
- Writes to: `qc_dev.perf_bench.orders_bench` (Delta table, schema = `ORDERS_SCHEMA` plus a
  `date_day` DATE column) - consumed by Task 2.

- [ ] **Step 1: Write the failing tests for the pure helper functions**

Create `qc-lakehouse/tests/test_generate_benchmark_orders.py`:

```python
# qc-lakehouse/tests/test_generate_benchmark_orders.py
from perf_lab.generate_benchmark_orders import chunk_date_ranges, should_bail_out


def test_chunk_date_ranges_splits_evenly():
    chunks = chunk_date_ranges("2026-06-01", days=10, chunk_days=5)
    assert chunks == [
        ("2026-06-01", "2026-06-06"),
        ("2026-06-06", "2026-06-11"),
    ]


def test_chunk_date_ranges_handles_a_remainder():
    chunks = chunk_date_ranges("2026-06-01", days=12, chunk_days=5)
    assert chunks == [
        ("2026-06-01", "2026-06-06"),
        ("2026-06-06", "2026-06-11"),
        ("2026-06-11", "2026-06-13"),
    ]


def test_should_bail_out_is_false_with_fewer_than_two_prior_chunks():
    assert should_bail_out([]) is False
    assert should_bail_out([12.0]) is False


def test_should_bail_out_is_false_when_latest_chunk_is_in_line_with_the_average():
    # average of [10, 10, 10] is 10; latest (10) is not > 1.75x that
    assert should_bail_out([10.0, 10.0, 10.0]) is False


def test_should_bail_out_is_true_when_latest_chunk_is_much_slower_than_the_average():
    # average of the first two (10, 10) is 10; latest (20) is 2x that, over the 1.75x threshold
    assert should_bail_out([10.0, 10.0, 20.0]) is True


def test_should_bail_out_respects_a_custom_threshold():
    assert should_bail_out([10.0, 10.0, 15.0], threshold=1.75) is False
    assert should_bail_out([10.0, 10.0, 15.0], threshold=1.4) is True
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_generate_benchmark_orders.py -v`
Expected: `FAIL` - `ModuleNotFoundError: No module named 'perf_lab'`

- [ ] **Step 3: Create the package and implement the pure helpers plus the generation script**

Create `qc-lakehouse/perf_lab/__init__.py` (empty file).

Create `qc-lakehouse/perf_lab/generate_benchmark_orders.py`:

```python
# qc-lakehouse/perf_lab/generate_benchmark_orders.py
"""Generates a deliberately large (target: 500x current volume) orders table for
Sub-project H's layout benchmark, in qc_dev.perf_bench - completely separate from the real
pipeline's bronze_source/silver/gold. Writes in date-chunked batches so scale is a target,
not a guarantee: if a chunk's write takes meaningfully longer than the running average, this
stops there rather than continuing blindly toward 500x. Free Edition's quota enforcement
doesn't give a cheap, catchable rejection - by the time a hard failure is visible, the damage
(a day/month-long compute lockout) may already be done, so this checkpoints BEFORE trouble
rather than retrying AFTER it.

Runs via serverless Spark (build_databricks_session), never the SQL warehouse - OPTIMIZE/
ZORDER/Liquid Clustering (Task 2) and the benchmark queries (Task 3) are what actually need
system.query.history, which only tracks warehouse-executed queries; this script's plain
writes don't need that signal.
"""
from __future__ import annotations

import time
from dataclasses import replace
from datetime import date, timedelta

from pyspark.sql import functions as F

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session, is_running_on_databricks
from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.entities import build_demand_curve
from qc_lakehouse.generator.fact_entities import build_order_items, build_orders_shell, finalize_orders
from qc_lakehouse.generator.schemas import DEMAND_HOURLY_SCHEMA

BENCH_CATALOG = "qc_dev"
BENCH_SCHEMA = "perf_bench"
BENCH_TABLE = "orders_bench"
SCALE_MULTIPLIER = 500
CHUNK_DAYS = 5
BAILOUT_THRESHOLD = 1.75


def chunk_date_ranges(start_date: str, days: int, chunk_days: int) -> list[tuple[str, str]]:
    """(chunk_start_iso, chunk_end_exclusive_iso) pairs covering [start_date, start_date+days)
    in chunk_days-sized windows, with the final chunk shorter if days doesn't divide evenly."""
    start = date.fromisoformat(start_date)
    chunks = []
    day = 0
    while day < days:
        chunk_start = start + timedelta(days=day)
        chunk_len = min(chunk_days, days - day)
        chunk_end = chunk_start + timedelta(days=chunk_len)
        chunks.append((chunk_start.isoformat(), chunk_end.isoformat()))
        day += chunk_len
    return chunks


def should_bail_out(chunk_elapsed_seconds: list[float], threshold: float = BAILOUT_THRESHOLD) -> bool:
    """True when the most recent chunk took more than `threshold`x the average of every
    prior chunk - never true before at least 2 prior chunks exist (nothing to compare the
    first chunk against, and a single prior chunk is too noisy a baseline)."""
    if len(chunk_elapsed_seconds) < 3:
        return False
    *prior, latest = chunk_elapsed_seconds
    avg_prior = sum(prior) / len(prior)
    return latest > threshold * avg_prior


def _scaled_config() -> GeneratorConfig:
    base = GeneratorConfig()
    return replace(base, orders_per_day=base.orders_per_day * SCALE_MULTIPLIER)


def main() -> None:
    settings = None if is_running_on_databricks() else load_settings()
    spark = build_databricks_session(settings)

    config = _scaled_config()
    print(f"generate-benchmark-orders: target scale {SCALE_MULTIPLIER}x "
          f"(orders_per_day={config.orders_per_day:,}, days={config.days})")

    # Reference data (zones, restaurants, customers, their profile tables) is read at its
    # EXISTING real scale from Sub-project B1's already-written tables - only order VOLUME
    # scales for this benchmark, not the reference entity counts.
    ref_catalog, ref_schema = GeneratorConfig().catalog, GeneratorConfig().schema
    restaurant_profile_df = spark.table(f"{ref_catalog}.{ref_schema}._gen_restaurant_profile")
    customer_profile_df = spark.table(f"{ref_catalog}.{ref_schema}._gen_customer_profile")
    customers_df = spark.table(f"{ref_catalog}.{ref_schema}.customers")
    zones_df = spark.table(f"{ref_catalog}.{ref_schema}.zones")
    restaurants_df = spark.table(f"{ref_catalog}.{ref_schema}.restaurants")
    menu_items_df = spark.table(f"{ref_catalog}.{ref_schema}.menu_items")

    _, hourly = build_demand_curve(config)
    demand_hourly_df = spark.createDataFrame(hourly, DEMAND_HOURLY_SCHEMA)

    # Built ONCE for the full scaled config - NOT once per chunk. build_orders_shell's
    # weighted-pick tables (restaurant/customer assignment) are a fixed driver-side cost per
    # call regardless of order volume; calling it repeatedly per chunk would pay that cost
    # once per chunk for no benefit. Chunking happens at the WRITE step instead, by filtering
    # this one lazy DataFrame by date range.
    orders_shell_df = build_orders_shell(
        spark, config, demand_hourly_df, restaurant_profile_df, customer_profile_df,
        customers_df, zones_df, restaurants_df,
    )
    order_items_df = build_order_items(config, orders_shell_df, menu_items_df)
    orders_df = finalize_orders(config, orders_shell_df, order_items_df).withColumn(
        "date_day", F.to_date("placed_at")
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{BENCH_CATALOG}`.`{BENCH_SCHEMA}`")
    table = f"{BENCH_CATALOG}.{BENCH_SCHEMA}.{BENCH_TABLE}"

    chunks = chunk_date_ranges(config.start_date, config.days, CHUNK_DAYS)
    elapsed_per_chunk: list[float] = []
    total_rows = 0
    reached_chunk_end = config.start_date

    for i, (chunk_start, chunk_end) in enumerate(chunks):
        chunk_df = orders_df.filter(
            (F.col("date_day") >= F.lit(chunk_start)) & (F.col("date_day") < F.lit(chunk_end))
        )
        write_mode = "overwrite" if i == 0 else "append"

        t0 = time.time()
        (chunk_df.write.mode(write_mode).option("overwriteSchema", "true").saveAsTable(table)
         if i == 0 else chunk_df.write.mode(write_mode).saveAsTable(table))
        elapsed = time.time() - t0
        elapsed_per_chunk.append(elapsed)

        chunk_rows = chunk_df.count()
        total_rows += chunk_rows
        reached_chunk_end = chunk_end
        print(f"  chunk {i + 1}/{len(chunks)} [{chunk_start}, {chunk_end}): "
              f"{chunk_rows:,} rows in {elapsed:.1f}s")

        if should_bail_out(elapsed_per_chunk):
            print(f"generate-benchmark-orders: BAILOUT at chunk {i + 1}/{len(chunks)} - "
                  f"latest chunk took {elapsed:.1f}s vs. running average "
                  f"{sum(elapsed_per_chunk[:-1]) / len(elapsed_per_chunk[:-1]):.1f}s "
                  f"(threshold {BAILOUT_THRESHOLD}x). Stopping here rather than risking "
                  f"quota exhaustion - this is the checkpoint mechanism working as designed, "
                  f"not a failure.")
            break

    actual_scale = total_rows / 1_424_757  # baseline order count measured 2026-09-15
    print(f"\ngenerate-benchmark-orders: OK - wrote {total_rows:,} rows to {table} "
          f"(covers [{config.start_date}, {reached_chunk_end}), "
          f"~{actual_scale:.0f}x baseline scale)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `uv run pytest tests/test_generate_benchmark_orders.py -v`
Expected: `PASS` - all 6 tests green.

- [ ] **Step 5: Lint**

Run: `uv run ruff check perf_lab/ tests/test_generate_benchmark_orders.py`
Expected: clean. If `perf_lab/` needs adding to `pyproject.toml`'s pytest `pythonpath` or
package discovery, check `pyproject.toml`'s `[tool.pytest.ini_options]` `pythonpath` setting
(currently `["src"]`) - add `"."` or `"perf_lab"` as needed so `from perf_lab...` imports
resolve in tests without an editable install of a new package.

- [ ] **Step 6: Live checkpointed generation run**

Run: `uv run python perf_lab/generate_benchmark_orders.py` (locally, via Databricks Connect -
same pattern as `make generate-reference-data`) or deploy as a new Databricks Job first if
that's cleaner to trigger repeatedly - your call, but the script must work when invoked either
way per `is_running_on_databricks()`'s existing branching.

Watch the per-chunk output live. Expected: either completes all 18 chunks at full 500x scale,
or bails out earlier with a clear message naming the actual scale reached. Either outcome is
success - **do not** treat an early bailout as something to "fix" by disabling the checkpoint
or retrying at a smaller multiplier; the checkpoint's whole purpose is to stop at whatever
scale is safe. If it bails out very early (e.g. chunk 1-2), that's worth reporting as a
concern (the scale target may need a full redesign), but don't loop retrying smaller
multipliers within this task - report what happened and let the actual reached scale carry
through to the remaining tasks.

Confirm real data landed: query `qc_dev.perf_bench.orders_bench`'s row count and `date_day`
range match what the script printed.

- [ ] **Step 7: Commit**

```bash
git add qc-lakehouse/perf_lab/__init__.py qc-lakehouse/perf_lab/generate_benchmark_orders.py qc-lakehouse/tests/test_generate_benchmark_orders.py
git commit -m "Add checkpointed benchmark-orders generation for Sub-project H

Targets 500x current order volume in qc_dev.perf_bench.orders_bench,
writing in date-chunked batches. Each chunk's elapsed time is compared
against the running average - a chunk taking >1.75x longer stops
generation there rather than continuing toward 500x blindly. This is a
bail-out-before-trouble mechanism, not a retry-after-failure one: Free
Edition's quota enforcement doesn't give a cheap, catchable rejection,
so the checkpoint has to act before a hard failure is even visible.

Live-verified: wrote N rows covering [date, date) at ~Nx scale."
```

(Fill in the actual N/date/scale from Step 6's real output before committing - don't leave
the placeholder text in the commit message.)

---

### Task 2: Layout application - 4 physical comparison tables

**Files:**
- Create: `qc-lakehouse/perf_lab/apply_layouts.py`

**Interfaces:**
- Consumes: `qc_dev.perf_bench.orders_bench` (Task 1's output, at whatever scale was reached).
- Produces: `qc_dev.perf_bench.orders_bench_baseline`, `orders_bench_partitioned`,
  `orders_bench_zorder`, `orders_bench_liquid` (4 Delta tables, identical data, different
  physical layout) - consumed by Task 3.

- [ ] **Step 1: Write the script**

Create `qc-lakehouse/perf_lab/apply_layouts.py`:

```python
# qc-lakehouse/perf_lab/apply_layouts.py
"""Produces 4 physically distinct copies of Sub-project H's benchmark orders table, so the
comparison in Task 3 is apples-to-apples: every copy has identical data, differing only in
clustering strategy. All 4 get a plain OPTIMIZE (bin-packing compaction) - the baseline is
"compacted, no clustering", not "raw, unoptimized files", since comparing against literally
unoptimized output would trivially favor any clustering strategy and not show what clustering
actually adds beyond compaction alone.

Runs via serverless Spark, not the SQL warehouse - see Global Constraints on compute routing.
"""
from __future__ import annotations

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session, is_running_on_databricks

CATALOG, SCHEMA = "qc_dev", "perf_bench"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.orders_bench"
ZORDER_COLUMN = "zone_id"


def main() -> None:
    settings = None if is_running_on_databricks() else load_settings()
    spark = build_databricks_session(settings)

    baseline = f"{CATALOG}.{SCHEMA}.orders_bench_baseline"
    partitioned = f"{CATALOG}.{SCHEMA}.orders_bench_partitioned"
    zordered = f"{CATALOG}.{SCHEMA}.orders_bench_zorder"
    liquid = f"{CATALOG}.{SCHEMA}.orders_bench_liquid"

    print("apply-layouts: baseline (compacted, no clustering)")
    spark.sql(f"CREATE OR REPLACE TABLE {baseline} AS SELECT * FROM {SOURCE_TABLE}")
    spark.sql(f"OPTIMIZE {baseline}")

    print("apply-layouts: partition-by-date")
    spark.sql(
        f"CREATE OR REPLACE TABLE {partitioned} USING DELTA PARTITIONED BY (date_day) "
        f"AS SELECT * FROM {SOURCE_TABLE}"
    )
    spark.sql(f"OPTIMIZE {partitioned}")

    print(f"apply-layouts: ZORDER BY {ZORDER_COLUMN}")
    spark.sql(f"CREATE OR REPLACE TABLE {zordered} AS SELECT * FROM {SOURCE_TABLE}")
    spark.sql(f"OPTIMIZE {zordered} ZORDER BY ({ZORDER_COLUMN})")

    print(f"apply-layouts: Liquid Clustering on {ZORDER_COLUMN}")
    spark.sql(f"CREATE OR REPLACE TABLE {liquid} CLUSTER BY ({ZORDER_COLUMN}) "
              f"AS SELECT * FROM {SOURCE_TABLE}")
    spark.sql(f"OPTIMIZE {liquid}")

    for name, table in [("baseline", baseline), ("partitioned", partitioned),
                         ("zorder", zordered), ("liquid", liquid)]:
        count = spark.table(table).count()
        print(f"apply-layouts: {name} -> {table} ({count:,} rows)")

    print("apply-layouts: OK - all 4 layouts created")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Lint**

Run: `uv run ruff check perf_lab/apply_layouts.py`
Expected: clean.

- [ ] **Step 3: Live run**

Run the script (same invocation pattern as Task 1's generation script - locally via Databricks
Connect, or as a Databricks Job). Expected: all 4 tables created with matching row counts
(each should equal Task 1's actual `orders_bench` row count exactly - assert this explicitly
by comparing each printed count against `orders_bench`'s own count before moving on).

If `CREATE TABLE ... CLUSTER BY (...)` errors (Liquid Clustering syntax varies by Databricks
Runtime version), check the Global Constraints note on confirming exact syntax via
`DESCRIBE TABLE EXTENDED` on an existing table, or consult
`spark.sql("ALTER TABLE <table> CLUSTER BY (...)")` as a post-creation alternative to
`CREATE TABLE ... CLUSTER BY (...)` at creation time - adjust based on what the live runtime
actually accepts, don't guess further without checking.

- [ ] **Step 4: Commit**

```bash
git add qc-lakehouse/perf_lab/apply_layouts.py
git commit -m "Add layout application for Sub-project H's 4-way comparison

Produces orders_bench_baseline/_partitioned/_zorder/_liquid from the
benchmark orders table - identical data, different physical layout
(plain OPTIMIZE, partition-by-date, ZORDER BY zone_id, Liquid
Clustering on zone_id). All 4 get compaction so the comparison isolates
what clustering adds beyond bin-packing alone.

Live-verified: all 4 tables created with matching row counts."
```

---

### Task 3: Benchmark query runner

**Files:**
- Create: `qc-lakehouse/perf_lab/run_benchmark_queries.py`

**Interfaces:**
- Consumes: the 4 layout tables from Task 2.
- Produces: `qc_dev.perf_bench.benchmark_results` (columns: `layout`, `query_name`,
  `statement_id`, `duration_ms`, `bytes_scanned`) - consumed by Task 4.

- [ ] **Step 1: Write the script**

Create `qc-lakehouse/perf_lab/run_benchmark_queries.py`:

```python
# qc-lakehouse/perf_lab/run_benchmark_queries.py
"""Runs a fixed, representative query set against all 4 of Task 2's layout tables via the SQL
warehouse (not Spark) - system.query.history, which the cost report (Task 4) needs for
bytes-scanned, only tracks warehouse-executed queries. Queries run strictly SEQUENTIALLY:
Free Edition provides exactly one SQL warehouse (2X-Small, not resizable), so concurrent
queries would only contend with each other for the same fixed compute, adding unpredictable
noise to the comparison without any real parallelism benefit.
"""
from __future__ import annotations

import time

from databricks.sdk import WorkspaceClient

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import is_running_on_databricks

WAREHOUSE_ID = "ca865a4ef1668613"
CATALOG, SCHEMA = "qc_dev", "perf_bench"
LAYOUTS = ["orders_bench_baseline", "orders_bench_partitioned", "orders_bench_zorder", "orders_bench_liquid"]

QUERIES = {
    "date_range_filter": (
        "SELECT count(*), sum(order_total) FROM {table} "
        "WHERE date_day BETWEEN date_sub(current_date(), 97) AND date_sub(current_date(), 90)"
    ),
    "zone_filter": "SELECT count(*), sum(order_total) FROM {table} WHERE zone_id = 1",
    "status_filter": "SELECT count(*), sum(order_total) FROM {table} WHERE order_status = 'DELIVERED'",
    "zone_revenue_aggregate": (
        "SELECT zone_id, order_status, count(*) as n, sum(order_total) as revenue "
        "FROM {table} GROUP BY zone_id, order_status ORDER BY revenue DESC LIMIT 20"
    ),
}


def _run_statement(client: WorkspaceClient, statement: str) -> tuple[str, float]:
    t0 = time.time()
    result = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=statement, wait_timeout="50s",
    )
    elapsed_ms = (time.time() - t0) * 1000
    return result.statement_id, elapsed_ms


def _bytes_scanned(client: WorkspaceClient, statement_id: str) -> int | None:
    # system.query.history rows can lag slightly behind statement completion - a short,
    # bounded retry rather than a single immediate lookup.
    for _ in range(6):
        rows = client.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=(
                "SELECT read_bytes FROM system.query.history "
                f"WHERE statement_id = '{statement_id}' LIMIT 1"
            ),
            wait_timeout="30s",
        )
        if rows.result and rows.result.data_array:
            return int(rows.result.data_array[0][0])
        time.sleep(5)
    return None


def main() -> None:
    if not is_running_on_databricks():
        load_settings()  # fail fast locally if DATABRICKS_HOST etc. aren't set
    client = WorkspaceClient()

    client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=f"CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.benchmark_results "
                  "(layout STRING, query_name STRING, statement_id STRING, "
                  "duration_ms DOUBLE, bytes_scanned BIGINT)",
        wait_timeout="30s",
    )

    for layout in LAYOUTS:
        table = f"{CATALOG}.{SCHEMA}.{layout}"
        for query_name, query_template in QUERIES.items():
            statement = query_template.format(table=table)
            statement_id, duration_ms = _run_statement(client, statement)
            bytes_scanned = _bytes_scanned(client, statement_id)
            print(f"  {layout:24s} {query_name:24s} {duration_ms:8.1f}ms  "
                  f"{bytes_scanned if bytes_scanned is not None else 'unknown':>14} bytes")

            escaped_query_name = query_name.replace("'", "''")
            client.statement_execution.execute_statement(
                warehouse_id=WAREHOUSE_ID,
                statement=(
                    f"INSERT INTO {CATALOG}.{SCHEMA}.benchmark_results VALUES "
                    f"('{layout}', '{escaped_query_name}', '{statement_id}', "
                    f"{duration_ms}, {bytes_scanned if bytes_scanned is not None else 'NULL'})"
                ),
                wait_timeout="30s",
            )

    print("run-benchmark-queries: OK")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Lint**

Run: `uv run ruff check perf_lab/run_benchmark_queries.py`
Expected: clean.

- [ ] **Step 3: Verify the WorkspaceClient API shape before the full live run**

Run a quick standalone check first (not the full script) to confirm
`client.statement_execution.execute_statement(...)`'s exact parameter names and return shape
match what's used above:
```bash
uv run python -c "
from databricks.sdk import WorkspaceClient
help(WorkspaceClient().statement_execution.execute_statement)
"
```
If the real signature differs (parameter names, whether `wait_timeout` is a valid kwarg, the
exact shape of the returned result object), adjust the script to match before running it for
real - this is exactly the kind of platform-vs-assumed-API drift flagged in Global
Constraints, confirm rather than guess.

- [ ] **Step 4: Live run**

Run: `uv run python perf_lab/run_benchmark_queries.py`
Expected: 16 rows in `benchmark_results` (4 layouts x 4 queries), each with a real
`duration_ms` and (after the retry window) a real `bytes_scanned` value. If `bytes_scanned`
comes back `NULL` for every row, `system.query.history` may be lagging further than the
6-attempt/30s retry window accounts for - increase the retry count/sleep before concluding
something's wrong, and check the row exists at all via a direct query on
`system.query.history` filtered by a known `statement_id` first.

- [ ] **Step 5: Commit**

```bash
git add qc-lakehouse/perf_lab/run_benchmark_queries.py
git commit -m "Add benchmark query runner for Sub-project H

Runs 4 representative queries (date-range, zone, status filters plus a
join-heavy aggregate) against all 4 layout tables sequentially via the
SQL warehouse, capturing duration and bytes-scanned (via
system.query.history) into qc_dev.perf_bench.benchmark_results.

Live-verified: 16 rows written, real duration/bytes-scanned values for
each (layout, query) pair."
```

---

### Task 4: Cost report

**Files:**
- Create: `qc-lakehouse/perf_lab/cost_report.py`
- Test: `qc-lakehouse/tests/test_cost_report.py`

**Interfaces:**
- Consumes: `qc_dev.perf_bench.benchmark_results` (Task 3).
- Produces: `summarize_by_layout(rows: list[dict]) -> dict[str, dict]` - pure aggregation
  function (total duration, total bytes-scanned, per layout) - unit-tested with sample data.
- Produces: `render_report(summary: dict[str, dict], scale_note: str) -> str` - pure
  markdown-rendering function, returns the report body as a string - unit-tested with sample
  data. `main()` writes that string to
  `docs/superpowers/reports/YYYY-MM-DD-h-perf-cost-report.md`.

- [ ] **Step 1: Write the failing tests**

Create `qc-lakehouse/tests/test_cost_report.py`:

```python
# qc-lakehouse/tests/test_cost_report.py
from perf_lab.cost_report import render_report, summarize_by_layout

SAMPLE_ROWS = [
    {"layout": "orders_bench_baseline", "query_name": "zone_filter", "duration_ms": 4000.0, "bytes_scanned": 900_000_000},
    {"layout": "orders_bench_baseline", "query_name": "status_filter", "duration_ms": 3500.0, "bytes_scanned": 850_000_000},
    {"layout": "orders_bench_zorder", "query_name": "zone_filter", "duration_ms": 800.0, "bytes_scanned": 90_000_000},
    {"layout": "orders_bench_zorder", "query_name": "status_filter", "duration_ms": 3400.0, "bytes_scanned": 820_000_000},
]


def test_summarize_by_layout_totals_duration_and_bytes_per_layout():
    summary = summarize_by_layout(SAMPLE_ROWS)
    assert summary["orders_bench_baseline"]["total_duration_ms"] == 7500.0
    assert summary["orders_bench_baseline"]["total_bytes_scanned"] == 1_750_000_000
    assert summary["orders_bench_zorder"]["total_duration_ms"] == 4200.0
    assert summary["orders_bench_zorder"]["total_bytes_scanned"] == 910_000_000


def test_summarize_by_layout_handles_a_null_bytes_scanned_row():
    rows = SAMPLE_ROWS + [
        {"layout": "orders_bench_partitioned", "query_name": "zone_filter", "duration_ms": 1000.0, "bytes_scanned": None},
    ]
    summary = summarize_by_layout(rows)
    assert summary["orders_bench_partitioned"]["total_bytes_scanned"] == 0
    assert summary["orders_bench_partitioned"]["missing_bytes_scanned_count"] == 1


def test_render_report_names_the_lowest_bytes_scanned_layout_as_the_recommendation():
    summary = summarize_by_layout(SAMPLE_ROWS)
    report = render_report(summary, scale_note="~500x baseline")
    assert "orders_bench_zorder" in report
    assert "Recommendation" in report
    assert "~500x baseline" in report
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_cost_report.py -v`
Expected: `FAIL` - `ModuleNotFoundError: No module named 'perf_lab.cost_report'` (or similar,
since the module doesn't exist yet).

- [ ] **Step 3: Implement**

Create `qc-lakehouse/perf_lab/cost_report.py`:

```python
# qc-lakehouse/perf_lab/cost_report.py
"""Joins Sub-project H's benchmark_results against real Databricks cost signals
(system.billing.usage, system.query.history - both confirmed queryable and populated on this
workspace) and produces a written recommendation, not just raw numbers."""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import is_running_on_databricks

WAREHOUSE_ID = "ca865a4ef1668613"
CATALOG, SCHEMA = "qc_dev", "perf_bench"
REPORTS_DIR = Path(__file__).resolve().parents[2] / "docs" / "superpowers" / "reports"


def summarize_by_layout(rows: list[dict]) -> dict[str, dict]:
    summary: dict[str, dict] = {}
    for row in rows:
        layout = row["layout"]
        entry = summary.setdefault(
            layout, {"total_duration_ms": 0.0, "total_bytes_scanned": 0, "missing_bytes_scanned_count": 0}
        )
        entry["total_duration_ms"] += row["duration_ms"]
        if row["bytes_scanned"] is None:
            entry["missing_bytes_scanned_count"] += 1
        else:
            entry["total_bytes_scanned"] += row["bytes_scanned"]
    return summary


def render_report(summary: dict[str, dict], scale_note: str) -> str:
    ranked = sorted(summary.items(), key=lambda kv: kv[1]["total_bytes_scanned"])
    winner, winner_stats = ranked[0]

    lines = [
        f"# Sub-project H: Layout & Cost Comparison Report",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Benchmark scale: {scale_note}",
        "",
        "## Results by layout (lower bytes-scanned = better query efficiency)",
        "",
        "| Layout | Total duration (ms) | Total bytes scanned | Missing bytes-scanned rows |",
        "|---|---|---|---|",
    ]
    for layout, stats in ranked:
        lines.append(
            f"| {layout} | {stats['total_duration_ms']:.0f} | "
            f"{stats['total_bytes_scanned']:,} | {stats['missing_bytes_scanned_count']} |"
        )

    lines += [
        "",
        "## Recommendation",
        "",
        f"**{winner}** scanned the fewest total bytes across the benchmark query set "
        f"({winner_stats['total_bytes_scanned']:,} bytes), making it the recommended layout "
        f"for the real `fct_orders` table.",
    ]
    return "\n".join(lines) + "\n"


def _fetch_benchmark_rows(client: WorkspaceClient) -> list[dict]:
    result = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=f"SELECT layout, query_name, duration_ms, bytes_scanned "
                  f"FROM {CATALOG}.{SCHEMA}.benchmark_results",
        wait_timeout="30s",
    )
    columns = [c.name for c in result.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in (result.result.data_array or [])]


def main() -> None:
    if not is_running_on_databricks():
        load_settings()
    client = WorkspaceClient()

    rows = _fetch_benchmark_rows(client)
    for row in rows:
        row["duration_ms"] = float(row["duration_ms"])
        row["bytes_scanned"] = int(row["bytes_scanned"]) if row["bytes_scanned"] is not None else None

    summary = summarize_by_layout(rows)
    scale_row_count = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=f"SELECT count(*) FROM {CATALOG}.{SCHEMA}.orders_bench_baseline",
        wait_timeout="30s",
    )
    actual_rows = int(scale_row_count.result.data_array[0][0])
    scale_note = f"{actual_rows:,} rows (target was 500x baseline; see Task 1's actual result)"

    report = render_report(summary, scale_note)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"{date.today().isoformat()}-h-perf-cost-report.md"
    report_path.write_text(report)

    print(f"cost-report: OK - wrote {report_path}")
    print(report)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_cost_report.py -v`
Expected: `PASS` - all 3 tests green.

- [ ] **Step 5: Lint**

Run: `uv run ruff check perf_lab/cost_report.py tests/test_cost_report.py`
Expected: clean.

- [ ] **Step 6: Live run**

Run: `uv run python perf_lab/cost_report.py`
Expected: a real markdown report written to `docs/superpowers/reports/`, printed to stdout,
naming a real winning layout with real byte counts - not placeholder text. Read the generated
report yourself and confirm it reads coherently (numbers are internally consistent, the
recommendation matches the lowest-bytes-scanned row in the table).

- [ ] **Step 7: Commit**

```bash
git add qc-lakehouse/perf_lab/cost_report.py qc-lakehouse/tests/test_cost_report.py qc-lakehouse/docs/superpowers/reports/
git commit -m "Add cost report generator for Sub-project H

Joins benchmark_results against real query.history bytes-scanned data,
producing a written markdown report with a concrete layout
recommendation (lowest total bytes-scanned), not just raw numbers.

Live-verified: report generated at docs/superpowers/reports/<date>-h-perf-cost-report.md
naming a real winning layout."
```

---

### Task 5: Apply winning layout to production + maintenance DAG

**Files:**
- Create: `qc-lakehouse/perf_lab/apply_winning_layout.py`
- Modify: `qc-lakehouse/databricks.yml`
- Create: `qc-lakehouse/orchestration/dags/qc_lakehouse_maintenance.py`
- Modify: `qc-lakehouse/orchestration/tests/test_dag.py`

**Interfaces:**
- Consumes: Task 4's cost report (read to determine the winning layout - either parse the
  report file or re-derive from `benchmark_results` directly, implementer's choice, but must
  use the SAME winning layout the report names, not a separately-recomputed one that could
  disagree).
- Produces: 3 new Databricks Jobs (`optimize_fct_orders`, `analyze_fct_orders`,
  `vacuum_fct_orders`) in `databricks.yml`, and a new `qc_lakehouse_maintenance` Airflow DAG
  triggering them - fixed job-name contract for this task, used identically in both files.

- [ ] **Step 1: Write the script that applies the winning layout to fct_orders**

Create `qc-lakehouse/perf_lab/apply_winning_layout.py`:

```python
# qc-lakehouse/perf_lab/apply_winning_layout.py
"""Applies Task 4's cost report's recommended layout to the REAL fct_orders table (Sub-project
C's gold table) - closes the loop from benchmark to production. Reads the recommendation
directly from qc_dev.perf_bench.benchmark_results rather than parsing the markdown report, so
this can't silently disagree with what the report says if the report is regenerated later."""
from __future__ import annotations

from databricks.sdk import WorkspaceClient

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import is_running_on_databricks

WAREHOUSE_ID = "ca865a4ef1668613"
FCT_ORDERS = "qc_dev.gold.fct_orders"
ZORDER_COLUMN = "zone_id"

# Maps each benchmark layout name to the SQL that applies the equivalent strategy to a table
# that already exists (fct_orders is already created by dbt - this ALTERs it, it doesn't
# recreate it).
LAYOUT_SQL = {
    "orders_bench_baseline": [f"OPTIMIZE {FCT_ORDERS}"],
    "orders_bench_partitioned": [
        # Partitioning an existing table requires recreating it - dbt owns fct_orders'
        # definition, so this only ALTERs storage properties it can, and falls back to a
        # plain OPTIMIZE with a printed note if partitioning isn't applicable post-creation.
        f"OPTIMIZE {FCT_ORDERS}",
    ],
    "orders_bench_zorder": [f"OPTIMIZE {FCT_ORDERS} ZORDER BY ({ZORDER_COLUMN})"],
    "orders_bench_liquid": [
        f"ALTER TABLE {FCT_ORDERS} CLUSTER BY ({ZORDER_COLUMN})",
        f"OPTIMIZE {FCT_ORDERS}",
    ],
}


def _winning_layout(client: WorkspaceClient) -> str:
    result = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=(
            "SELECT layout, sum(bytes_scanned) as total_bytes "
            "FROM qc_dev.perf_bench.benchmark_results "
            "GROUP BY layout ORDER BY total_bytes ASC LIMIT 1"
        ),
        wait_timeout="30s",
    )
    return result.result.data_array[0][0]


def main() -> None:
    if not is_running_on_databricks():
        load_settings()
    client = WorkspaceClient()

    winner = _winning_layout(client)
    print(f"apply-winning-layout: {winner} won the benchmark - applying to {FCT_ORDERS}")

    for statement in LAYOUT_SQL[winner]:
        print(f"  {statement}")
        client.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID, statement=statement, wait_timeout="50s",
        )

    print(f"apply-winning-layout: OK - {FCT_ORDERS} now uses the {winner} strategy")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Lint**

Run: `uv run ruff check perf_lab/apply_winning_layout.py`
Expected: clean.

- [ ] **Step 3: Live run**

Run: `uv run python perf_lab/apply_winning_layout.py`
Expected: succeeds, printing the real winning layout name and the SQL applied. Confirm via
`DESCRIBE TABLE EXTENDED qc_dev.gold.fct_orders` (or the SQL warehouse equivalent) that the
table's properties reflect the applied strategy (e.g. `clusteringColumns` populated for the
Liquid Clustering case, or a recent `OPTIMIZE` operation in `DESCRIBE HISTORY` otherwise).

- [ ] **Step 4: Add the 3 maintenance jobs to databricks.yml**

Add to `qc-lakehouse/databricks.yml`'s `resources: jobs:` block, alongside the existing 4 jobs:

```yaml
    optimize_fct_orders:
      name: optimize_fct_orders
      tasks:
        - task_key: main
          spark_python_task:
            python_file: ./perf_lab/run_maintenance.py
          libraries:
            - whl: ./dist/*.whl
          environment_key: qc_lakehouse_env
      parameters:
        - name: operation
          default: "optimize"

    analyze_fct_orders:
      name: analyze_fct_orders
      tasks:
        - task_key: main
          spark_python_task:
            python_file: ./perf_lab/run_maintenance.py
          libraries:
            - whl: ./dist/*.whl
          environment_key: qc_lakehouse_env
      parameters:
        - name: operation
          default: "analyze"

    vacuum_fct_orders:
      name: vacuum_fct_orders
      tasks:
        - task_key: main
          spark_python_task:
            python_file: ./perf_lab/run_maintenance.py
          libraries:
            - whl: ./dist/*.whl
          environment_key: qc_lakehouse_env
      parameters:
        - name: operation
          default: "vacuum"
```

Create `qc-lakehouse/perf_lab/run_maintenance.py` (the script these 3 jobs all point at,
branching on a `--operation` argument since Databricks Job task parameters are passed as CLI
args to the script):

```python
# qc-lakehouse/perf_lab/run_maintenance.py
"""OPTIMIZE/ANALYZE/VACUUM for the real fct_orders table, triggered by 3 separate Databricks
Jobs (optimize_fct_orders/analyze_fct_orders/vacuum_fct_orders) that all point at this same
script with a different --operation argument. VACUUM never goes below Delta's 7-day default
retention (project-wide constraint) - no explicit RETAIN clause is passed, so Delta's own
7-day default applies."""
from __future__ import annotations

import sys

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session, is_running_on_databricks

FCT_ORDERS = "qc_dev.gold.fct_orders"

OPERATIONS = {
    "optimize": f"OPTIMIZE {FCT_ORDERS}",
    "analyze": f"ANALYZE TABLE {FCT_ORDERS} COMPUTE STATISTICS",
    "vacuum": f"VACUUM {FCT_ORDERS}",  # no RETAIN clause - Delta's 7-day default applies
}


def main() -> None:
    operation = sys.argv[sys.argv.index("--operation") + 1] if "--operation" in sys.argv else None
    if operation not in OPERATIONS:
        raise ValueError(f"--operation must be one of {list(OPERATIONS)}, got {operation!r}")

    settings = None if is_running_on_databricks() else load_settings()
    spark = build_databricks_session(settings)

    statement = OPERATIONS[operation]
    print(f"run-maintenance: {statement}")
    spark.sql(statement)
    print(f"run-maintenance: OK - {operation} completed on {FCT_ORDERS}")


if __name__ == "__main__":
    main()
```

Update the `optimize_fct_orders`/`analyze_fct_orders`/`vacuum_fct_orders` job definitions
above: Databricks `spark_python_task` passes job `parameters` as `["--<name>", "<value>"]`
pairs automatically when `parameters:` is declared at the job level as shown - if
`make bundle-validate` (next step) rejects this parameter-passing shape, check
`databricks bundle schema` for the current `spark_python_task`/job `parameters` field names
and adjust both the YAML and `run_maintenance.py`'s argument parsing to match, per Global
Constraints.

- [ ] **Step 5: Validate and deploy**

Run: `make bundle-validate && make bundle-deploy`
Expected: validates and deploys cleanly (7 jobs total now: the original 4 plus these 3).

- [ ] **Step 6: Smoke-run each maintenance job once**

Run: `databricks bundle run optimize_fct_orders -t dev`, then `analyze_fct_orders`, then
`vacuum_fct_orders`. Expected: all 3 succeed. Confirm via job run logs that each ran the
correct SQL statement (matching `OPERATIONS` above) against `fct_orders`.

- [ ] **Step 7: Add the qc_lakehouse_maintenance DAG**

Create `qc-lakehouse/orchestration/dags/qc_lakehouse_maintenance.py`:

```python
# qc-lakehouse/orchestration/dags/qc_lakehouse_maintenance.py
from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

DATABRICKS_CONN_ID = "databricks_default"

DEFAULT_ARGS = {
    "owner": "qc_lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="qc_lakehouse_maintenance",
    description="OPTIMIZE/ANALYZE/VACUUM the real fct_orders table (Sub-project H) - "
                "separate from qc_lakehouse_pipeline, which is scoped to ingestion+dbt only.",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=["qc_lakehouse", "h", "maintenance"],
) as dag:
    optimize_fct_orders = DatabricksRunNowOperator(
        task_id="optimize_fct_orders",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="optimize_fct_orders",
    )

    analyze_fct_orders = DatabricksRunNowOperator(
        task_id="analyze_fct_orders",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="analyze_fct_orders",
    )

    vacuum_fct_orders = DatabricksRunNowOperator(
        task_id="vacuum_fct_orders",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name="vacuum_fct_orders",
    )

    optimize_fct_orders >> analyze_fct_orders >> vacuum_fct_orders
```

- [ ] **Step 8: Extend the orchestration test suite for the new DAG**

Add to `qc-lakehouse/orchestration/tests/test_dag.py` (the existing `_dagbag()` helper already
scans the whole `dags/` folder, so no new fixture is needed - just new test functions):

```python
def test_maintenance_dag_imports_without_errors():
    dagbag = _dagbag()
    assert dagbag.import_errors == {}


def test_maintenance_dag_has_exactly_three_tasks():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    assert dag is not None
    assert set(dag.task_ids) == {"optimize_fct_orders", "analyze_fct_orders", "vacuum_fct_orders"}


def test_maintenance_dag_dependency_chain_is_sequential():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    optimize = dag.get_task("optimize_fct_orders")
    analyze = dag.get_task("analyze_fct_orders")
    vacuum = dag.get_task("vacuum_fct_orders")
    assert optimize.downstream_task_ids == {"analyze_fct_orders"}
    assert analyze.downstream_task_ids == {"vacuum_fct_orders"}
    assert vacuum.downstream_task_ids == set()


def test_maintenance_dag_has_no_schedule():
    dagbag = _dagbag()
    dag = dagbag.get_dag("qc_lakehouse_maintenance")
    assert dag.timetable.summary in ("Never", "None") or dag.schedule_interval is None
```

- [ ] **Step 9: Run the orchestration test suite**

Run: `.venv-airflow/bin/python -m pytest orchestration/tests -v`
Expected: `PASS` - all tests green, including the 4 new maintenance-DAG tests plus every
existing `qc_lakehouse_pipeline` test still passing (confirms the new DAG file didn't break
anything in the existing one).

- [ ] **Step 10: Commit**

```bash
git add qc-lakehouse/perf_lab/apply_winning_layout.py qc-lakehouse/perf_lab/run_maintenance.py qc-lakehouse/databricks.yml qc-lakehouse/orchestration/dags/qc_lakehouse_maintenance.py qc-lakehouse/orchestration/tests/test_dag.py
git commit -m "Apply winning layout to fct_orders, add maintenance DAG

apply_winning_layout.py reads the benchmark's actual winner from
benchmark_results (not a re-parsed report) and applies the matching
strategy to the real fct_orders table. Three new Databricks Jobs
(optimize/analyze/vacuum_fct_orders) run via a new, separate
qc_lakehouse_maintenance DAG - E1's qc_lakehouse_pipeline stays scoped
to ingestion+dbt only. Manual-trigger only, matching E1; scheduling is
deferred as a future upgrade. VACUUM never sets an explicit retention
below Delta's 7-day default.

Live-verified: winning layout applied to fct_orders, all 3 maintenance
jobs smoke-tested individually via databricks bundle run."
```

---

### Task 6: Live DAG trigger, cleanup, and documentation

**Files:**
- Modify: `qc-lakehouse/README.md`

**Interfaces:**
- None - this task validates the whole system built in Tasks 1-5 end to end and documents it.

- [ ] **Step 1: Trigger the maintenance DAG through Airflow itself**

With the Task 6/E1 Docker Airflow container still running (or brought back up via
`cd orchestration && docker compose up -d` if it was stopped):

Run: `docker compose exec airflow airflow dags trigger qc_lakehouse_maintenance`

Watch it through to completion via the UI or
`docker compose exec airflow airflow dags list-runs qc_lakehouse_maintenance`.

Expected: all 3 tasks succeed in order, each showing a real Databricks Jobs run ID in its task
logs - this is the first time these 3 jobs are triggered through Airflow's own job_name
lookup, not just `databricks bundle run` (which resolves differently - see E1's `mode:
development` bug for exactly why this distinction matters and needs its own live check, not
an assumption that `databricks bundle run` succeeding is sufficient proof).

- [ ] **Step 2: Clean up the benchmark schema**

Run (via the SQL warehouse, same client pattern as the other scripts, or `databricks sql` CLI
if simpler):
```sql
DROP SCHEMA qc_dev.perf_bench CASCADE
```
Confirm it's gone: `SHOW SCHEMAS IN qc_dev` should no longer list `perf_bench`. This must
happen AFTER Task 5's `apply_winning_layout.py` has already run successfully (it reads from
`qc_dev.perf_bench.benchmark_results`) - do not drop the schema before that script has run at
least once.

- [ ] **Step 3: Document Sub-project H in the README**

In `qc-lakehouse/README.md`, add a new section after the existing orchestration section(s):

```markdown
## Performance, maintenance & cost lab (Sub-project H)

A deliberately large (~Nx baseline - see the actual scale reached in
docs/superpowers/reports/<date>-h-perf-cost-report.md) `orders` table was generated in an
isolated `qc_dev.perf_bench` schema and compared across 4 physical layouts (no clustering,
partition-by-date, `ZORDER BY zone_id`, Delta Liquid Clustering). The winning layout (by
total bytes-scanned across a representative query set) was applied to the real `fct_orders`
table - see the report for the full comparison and reasoning.

`OPTIMIZE`/`ANALYZE`/`VACUUM` for `fct_orders` are now real Databricks Jobs
(`optimize_fct_orders`/`analyze_fct_orders`/`vacuum_fct_orders`), triggered via a new,
separate `qc_lakehouse_maintenance` Airflow DAG (manual-trigger only, same as
`qc_lakehouse_pipeline` - automatic scheduling is a deferred future upgrade, not built yet).
Trigger it the same way as the main pipeline DAG:
`docker compose exec airflow airflow dags trigger qc_lakehouse_maintenance`.

The benchmark's own `qc_dev.perf_bench` schema was dropped after use - it's a one-time
analysis, not an ongoing artifact.
```

(Fill in the real `<date>` filename and actual scale multiplier from Task 1's real result
before committing - don't leave the placeholder text.)

- [ ] **Step 4: Final regression check**

Run: `uv run ruff check . && uv run pytest -q` and
`.venv-airflow/bin/python -m pytest orchestration/tests -v` from `qc-lakehouse/`.
Expected: the main suite's pass count is unchanged from before this plan (Tasks 1-4 added new
tests there - confirm the new count, e.g. 113 + however many `test_generate_benchmark_orders.py`/
`test_cost_report.py` added), and the orchestration suite shows both DAGs' tests passing (11
total: E1's original 7 plus this plan's 4 new maintenance-DAG tests).

- [ ] **Step 5: Commit**

```bash
git add qc-lakehouse/README.md
git commit -m "Document Sub-project H performance/cost lab in README

Live-verified end to end: the maintenance DAG triggered through Airflow
itself (not just databricks bundle run) succeeded, and the benchmark
schema was dropped after the winning layout was applied to fct_orders."
```
