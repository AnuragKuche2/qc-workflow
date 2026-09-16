# qc-lakehouse/perf_lab/generate_benchmark_orders.py
"""Generates a deliberately large (target: 500x current volume) orders table for
Sub-project H's layout benchmark, in qc_dev.perf_bench - completely separate from the real
pipeline's bronze_source/silver/gold. Writes in date-chunked batches so scale is a target,
not a guarantee: if a chunk's write takes meaningfully longer than the running average, this
stops there rather than continuing blindly toward 500x. Free Edition's quota enforcement
doesn't give a cheap, catchable rejection - by the time a hard failure is visible, the damage
(a day/month-long compute lockout) may already be done, so this checkpoints BEFORE trouble
rather than retrying AFTER it.

Runs via serverless Spark (build_databricks_session), never the SQL warehouse - the
benchmark queries (Task 3) are what actually need system.query.history, which only tracks
warehouse-executed queries; this script's plain writes don't need that signal.

ACCEPTED REALIZED SCALE (live run of 2026-09-15): the checkpoint fired early - not via the
should_bail_out slow-chunk threshold, but via a hard Databricks Connect session error mid-run
(see _run_chunks's docstring). qc_dev.perf_bench.orders_bench currently holds 73,723,047
rows (~51.7x baseline, NOT the 500x/~675M target below) covering date_day 2026-06-01 through
2026-06-10 inclusive (2 of the planned 18 chunks). This has been accepted as this benchmark's
final scale - no further live generation of this table is authorized. Task 2+ must build
against these real numbers, not against SCALE_MULTIPLIER/config.days below.
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
from qc_lakehouse.generator.fact_entities import (
    build_order_items,
    build_orders_shell,
    finalize_orders,
)
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


def _run_chunks(
    chunks: list[tuple[str, str]],
    write_chunk,
    start_date: str,
) -> tuple[int, str, list[float], bool]:
    """Runs `write_chunk(chunk_start, chunk_end, write_mode) -> row_count` once per chunk,
    in order, and stops - cleanly, never by crashing - the first time either checkpoint
    fires:

    - a chunk whose write+count together took >BAILOUT_THRESHOLD x the running average
      (`should_bail_out`, the original slow-chunk checkpoint), or
    - a chunk whose `write_chunk` call raises at all (a hard failure, e.g. a dropped
      Databricks Connect session mid-write/count - Task 1's live run hit exactly this: a
      SESSION_NOT_FOUND gRPC error on the post-write `.count()` call, which crashed the
      whole script with an unhandled traceback instead of stopping gracefully).

    Kept Spark-free and injectable so both checkpoints - including the exception path - are
    directly unit-testable without a live Spark session: tests pass a stub `write_chunk`
    that raises to simulate a dropped session.

    A chunk whose `write_chunk` call raises contributes nothing to the returned totals, even
    though (as the live run showed) the write itself may have actually landed real data -
    there's no reliable way here to know how much of a failed chunk's data is real, so this
    conservatively counts none of it. `main()`'s final summary print always runs, using
    whatever this function returns, whichever checkpoint (if either) fired.

    The 4th return value, `stopped_early`, is True whenever either checkpoint fired (the loop
    `break`s before exhausting `chunks`) and False only when every chunk in `chunks` was
    processed - callers need this explicitly rather than inferring it from `reached_chunk_end`
    or `total_rows`, so a crashed/checkpointed run can never print the same "OK" outcome a
    full run does.
    """
    elapsed_per_chunk: list[float] = []
    total_rows = 0
    reached_chunk_end = start_date
    stopped_early = False

    for i, (chunk_start, chunk_end) in enumerate(chunks):
        write_mode = "overwrite" if i == 0 else "append"

        t0 = time.time()
        try:
            chunk_rows = write_chunk(chunk_start, chunk_end, write_mode)
        except Exception as exc:  # noqa: BLE001 - any failure here means "stop", not "diagnose"
            print(f"generate-benchmark-orders: BAILOUT at chunk {i + 1}/{len(chunks)} - "
                  f"unhandled error writing/counting this chunk: {exc!r}. Stopping here "
                  f"rather than risking further quota exhaustion - this is the checkpoint "
                  f"mechanism working as designed, not a failure.")
            stopped_early = True
            break
        elapsed = time.time() - t0
        elapsed_per_chunk.append(elapsed)

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
            stopped_early = True
            break

    return total_rows, reached_chunk_end, elapsed_per_chunk, stopped_early


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

    def write_chunk(chunk_start: str, chunk_end: str, write_mode: str) -> int:
        chunk_df = orders_df.filter(
            (F.col("date_day") >= F.lit(chunk_start)) & (F.col("date_day") < F.lit(chunk_end))
        )
        (chunk_df.write.mode(write_mode).option("overwriteSchema", "true").saveAsTable(table)
         if write_mode == "overwrite" else chunk_df.write.mode(write_mode).saveAsTable(table))
        return chunk_df.count()

    chunks = chunk_date_ranges(config.start_date, config.days, CHUNK_DAYS)
    total_rows, reached_chunk_end, _, stopped_early = _run_chunks(
        chunks, write_chunk, config.start_date
    )

    actual_scale = total_rows / 1_424_757  # baseline order count measured 2026-09-15
    # "STOPPED EARLY" whenever either checkpoint fired (slow chunk or a hard write/count
    # error) - never "OK" for that case, so an unattended run (e.g. a Databricks Job where
    # nobody's watching the live chunk-by-chunk output) can't look identical in its own final
    # line to a run that actually reached the end of `chunks`.
    status = "STOPPED EARLY" if stopped_early else "OK"
    print(f"\ngenerate-benchmark-orders: {status} - wrote {total_rows:,} rows to {table} "
          f"(covers [{config.start_date}, {reached_chunk_end}), "
          f"~{actual_scale:.0f}x baseline scale)")


if __name__ == "__main__":
    main()
