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

HISTORY (live run of 2026-09-15 through 2026-09-17): the initial 2026-09-15 attempt hit a
hard Databricks Connect session error after 2 of 18 chunks (73,723,047 rows, ~51.7x baseline).
That run used a single continuous session for all 18 chunks; session-duration limits on
Databricks Connect prevented it from completing. The resumable-generation capability added in
Task 3 was then used to extend this via 8 further independent Databricks Job invocations
(day_offset 10, 20, 30, 40, 50, 60, 70, 80 via `databricks bundle run generate_benchmark_orders
-- --day-offset N --window-days 10`), each as a separate short-lived session and thus never
hitting the duration cap. Each invocation covered its own non-overlapping 10-day slice of the
90-day window and its own reserved id block (offset * 10,000,000). All 9 invocations (including
the initial offset-0 baseline) succeeded with TERMINATED SUCCESS; the full 90-day window
(2026-06-01 through 2026-08-30, exclusive end) was fully reached and generated. Final totals:
799,389,745 rows cumulative (~561x baseline; per-invocation counts: 73.7M, 96.7M, 85.3M,
90.4M, 92.0M, 84.6M, 97.8M, 82.3M, 96.7M for offsets 0, 10, 20, 30, 40, 50, 60, 70, 80
respectively). Zero ID collisions verified two ways: (1) after each invocation, cumulative
total_rows == distinct order_id count; (2) final per-block GROUP BY (order_id DIV 100_000_000)
check: exactly 9 blocks, each with row count matching its invocation, each block's [min_id,
max_id] strictly non-overlapping with all others.

CAVEAT - windowed generation is NOT distributionally equivalent to one contiguous run: "the
full 90-day window was reached" above means every calendar day got written, not that the
result matches what a single 90-day `build_demand_curve` call would have produced. Each of the
9 invocations calls `build_demand_curve` with only its own 10-day (or shorter) `config.days`
slice and the SAME fixed `config.seed` (unchanged across invocations), so each invocation
independently calls `resolve_events(EVENTS, config.days)` - deterministic given `days`, so a
10-day invocation always resolves the same event at the same relative day-index - and seeds its
noise RNG stream (`stream_seed(config.seed, "noise")`) fresh each time and draws exactly
`config.days` values from it. Event-day placement and noise patterns therefore REPEAT
per-window (the same relative event day, the same relative noise sequence, in every same-length
window) rather than spanning the full 90-day window the way a single contiguous generation pass
would. This does not invalidate Sub-project H's layout recommendation (that rests entirely on
the original, unrelated 51.7x-scale contiguous run - see the report's addendum), but it is a
real, non-cosmetic limitation of this windowed-resumable pattern worth knowing before reusing it
for another generator.

Each invocation covers a `BENCH_WINDOW_DAYS`-day slice starting `BENCH_DAY_OFFSET` days into
the overall SCALE_MULTIPLIER/config.days target window, and adds to whatever earlier
invocations already wrote by replacing only its own chunk date ranges, so re-running an
invocation is idempotent (day_offset 0 is the only invocation allowed to overwrite the whole
table, i.e. a genuine from-scratch run). order_id would otherwise collide across invocations - it's a
contiguous integer computed fresh, starting near 1, by every independent build_orders_shell
call, regardless of calendar date - so every invocation's order_id is shifted by
id_offset_for_day_offset(BENCH_DAY_OFFSET), reserving a non-overlapping ID_BLOCK_SIZE-sized
block per day-offset.
"""
from __future__ import annotations

import sys
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

# order_id is a contiguous integer computed fresh (starting near 1) by every independent
# invocation's own build_orders_shell call - two invocations covering different calendar
# windows would otherwise still collide on order_id. ID_BLOCK_SIZE reserves a per-day-offset
# block of id space (id_offset_for_day_offset): a `window_days`-day invocation gets
# `window_days * ID_BLOCK_SIZE` contiguous ids before the next invocation's block starts.
#
# The naive per-day estimate (500 * 15,000 baseline * ~1.06 event bump =~ 7.95M orders/day)
# is NOT the right number to size this against: this project's resumable design windows
# generation into 10-day chunks, not single days, so a single invocation's real order count is
# roughly 10x that per-day estimate, not 1x it. Live-verified worst case: the day_offset=60
# invocation (a 10-day window, reserved 100,000,000 ids) generated 97,752,042 orders - a 2.2%
# margin, not "comfortable". main() asserts the projected order count for THIS invocation
# against its reserved block (see check_projected_orders_within_id_block) before any Spark
# write happens, specifically so a larger window_days or a demand-curve change can never
# silently blow through this and corrupt the next invocation's id range.
ID_BLOCK_SIZE = 10_000_000


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


def id_offset_for_day_offset(day_offset: int, id_block_size: int = ID_BLOCK_SIZE) -> int:
    """The order_id offset a resumed invocation starting `day_offset` days into the overall
    window must add to every order_id it generates, so its ids can never collide with an
    earlier invocation's - see ID_BLOCK_SIZE."""
    return day_offset * id_block_size


def resolve_window(base_start_date: str, day_offset: int, window_days: int, total_days: int) -> tuple[str, int]:
    """The (start_date, days) a single invocation should generate, `day_offset` days into
    the overall `total_days`-day target window - shifted by `day_offset` and clamped so it
    never runs past `total_days`, however large `window_days` is."""
    if day_offset >= total_days:
        raise ValueError(
            f"day_offset ({day_offset}) already covers or exceeds total_days ({total_days}) - "
            "nothing left for this invocation to generate."
        )
    start_date = (date.fromisoformat(base_start_date) + timedelta(days=day_offset)).isoformat()
    days = min(window_days, total_days - day_offset)
    return start_date, days


def parse_args(argv: list[str]) -> tuple[int, int]:
    """(day_offset, window_days) from `--day-offset N --window-days N` CLI flags (either
    order), matching the CLI-args convention `databricks.yml`'s spark_python_task.parameters
    already uses for run_maintenance.py - Databricks Jobs pass per-run overrides as argv, not
    environment variables.

    `--day-offset` is REQUIRED and any unrecognized token raises: day_offset=0 triggers
    write_mode="overwrite" (wipes the whole table), so a missing or misspelled --day-offset
    (e.g. --day_offset, --dayoffset) must fail loudly rather than silently defaulting to that
    single most-destructive path - matching run_maintenance.py, which itself validates and
    raises on a missing/unrecognized --operation rather than guessing one. `--window-days`
    still defaults to 10 - it controls generation granularity, not overwrite-vs-append, so it
    is not the dangerous parameter here."""
    day_offset, window_days = None, 10
    i = 0
    while i < len(argv):
        if argv[i] == "--day-offset":
            day_offset = int(argv[i + 1])
            i += 2
        elif argv[i] == "--window-days":
            window_days = int(argv[i + 1])
            i += 2
        else:
            raise ValueError(
                f"generate-benchmark-orders: unrecognized argument {argv[i]!r} - expected "
                "--day-offset (required) and/or --window-days."
            )
    if day_offset is None:
        raise ValueError(
            "generate-benchmark-orders: --day-offset is required (day_offset=0 triggers "
            "write_mode='overwrite', wiping the whole table - refusing to silently default to "
            "that)."
        )
    return day_offset, window_days


def check_projected_orders_within_id_block(
    projected_orders: int, window_days: int, id_block_size: int = ID_BLOCK_SIZE
) -> None:
    """Raises ValueError if `projected_orders` - summed from the driver-side hourly demand
    curve (the `hourly` list `build_demand_curve` returns), BEFORE any Spark write happens -
    would meet or exceed the id space this invocation has reserved (`window_days *
    id_block_size`; see ID_BLOCK_SIZE and id_offset_for_day_offset). Catching this here, cheaply
    and pre-write, is the whole point: once this invocation's order_ids overrun their own
    block, they silently collide with the NEXT invocation's block rather than failing this
    invocation's own write - by the time that's visible (a downstream id-collision check), the
    corrupting data is already durably written."""
    reserved = window_days * id_block_size
    if projected_orders >= reserved:
        raise ValueError(
            f"generate-benchmark-orders: projected {projected_orders:,} orders for this "
            f"invocation would meet or exceed its reserved id block of {reserved:,} "
            f"(window_days={window_days} * ID_BLOCK_SIZE={id_block_size:,}) - proceeding would "
            "risk colliding with the next invocation's id block. Increase ID_BLOCK_SIZE or "
            "reduce window_days before retrying."
        )


def should_bail_out(chunk_elapsed_seconds: list[float], threshold: float = BAILOUT_THRESHOLD) -> bool:
    """True when the most recent chunk took more than `threshold`x the average of every
    prior chunk - never true before at least 2 prior chunks exist (nothing to compare the
    first chunk against, and a single prior chunk is too noisy a baseline)."""
    if len(chunk_elapsed_seconds) < 3:
        return False
    *prior, latest = chunk_elapsed_seconds
    avg_prior = sum(prior) / len(prior)
    return latest > threshold * avg_prior


def chunk_writer_options(chunk_start: str, chunk_end: str, write_mode: str) -> dict[str, str]:
    """Delta writer options for one chunk, always used with `.mode("overwrite")`.

    "overwrite" replaces the whole table (a from-scratch day_offset 0 run). "append" replaces
    only this chunk's [chunk_start, chunk_end) date_day slice via replaceWhere, so re-running
    a resumed invocation after a partial failure rewrites its own chunks instead of
    duplicating their rows."""
    if write_mode == "overwrite":
        return {"overwriteSchema": "true"}
    return {"replaceWhere": f"date_day >= '{chunk_start}' AND date_day < '{chunk_end}'"}


def _scaled_config() -> GeneratorConfig:
    base = GeneratorConfig()
    return replace(base, orders_per_day=base.orders_per_day * SCALE_MULTIPLIER)


def _run_chunks(
    chunks: list[tuple[str, str]],
    write_chunk,
    start_date: str,
    first_chunk_write_mode: str = "overwrite",
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
        write_mode = first_chunk_write_mode if i == 0 else "append"

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

    full_config = _scaled_config()

    # BENCH_DAY_OFFSET/BENCH_WINDOW_DAYS split the full SCALE_MULTIPLIER/90-day target across
    # several short, independent invocations - each a fresh Databricks Connect session with
    # its own session-duration budget - instead of one long session attempting all 18 chunks
    # at once (Task 1's live run: a hard session error killed that after 2 of 18 chunks).
    # day_offset 0 is a from-scratch invocation (overwrite); any later offset is a resume
    # into the table an earlier invocation already started, replacing only its own date
    # slices (see chunk_writer_options) so a re-run never duplicates rows.
    day_offset, window_days = parse_args(sys.argv[1:])
    start_date, days = resolve_window(
        base_start_date=full_config.start_date,
        day_offset=day_offset,
        window_days=window_days,
        total_days=full_config.days,
    )
    config = replace(full_config, start_date=start_date, days=days)
    id_offset = id_offset_for_day_offset(day_offset)
    first_chunk_write_mode = "overwrite" if day_offset == 0 else "append"

    print(f"generate-benchmark-orders: target scale {SCALE_MULTIPLIER}x overall "
          f"(orders_per_day={config.orders_per_day:,}) - this invocation covers "
          f"[{config.start_date}, +{config.days}d) (day_offset={day_offset}, "
          f"id_offset={id_offset:,}, first_chunk_write_mode={first_chunk_write_mode})")

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
    # Cheap, pre-write, Spark-free fail-fast: sums the driver-side hourly order counts (index 3
    # of each (day_index, order_date, hour, orders) tuple) BEFORE any Spark write happens - see
    # check_projected_orders_within_id_block and ID_BLOCK_SIZE's comment for why this matters.
    check_projected_orders_within_id_block(sum(row[3] for row in hourly), window_days)
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
    if id_offset:
        # Applied before order_items_df is built, so the offset order_id is what
        # order_items' FK join (and everything downstream) sees consistently.
        orders_shell_df = orders_shell_df.withColumn(
            "order_id", F.col("order_id") + F.lit(id_offset)
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
        chunk_df.write.mode("overwrite").options(
            **chunk_writer_options(chunk_start, chunk_end, write_mode)
        ).saveAsTable(table)
        return chunk_df.count()

    chunks = chunk_date_ranges(config.start_date, config.days, CHUNK_DAYS)
    total_rows, reached_chunk_end, _, stopped_early = _run_chunks(
        chunks, write_chunk, config.start_date, first_chunk_write_mode=first_chunk_write_mode
    )

    actual_scale = total_rows / 1_424_757  # baseline order count measured 2026-09-15
    # "STOPPED EARLY" whenever either checkpoint fired (slow chunk or a hard write/count
    # error) - never "OK" for that case, so an unattended run (e.g. a Databricks Job where
    # nobody's watching the live chunk-by-chunk output) can't look identical in its own final
    # line to a run that actually reached the end of `chunks`.
    status = "STOPPED EARLY" if stopped_early else "OK"
    print(f"\ngenerate-benchmark-orders: {status} - this invocation wrote {total_rows:,} rows "
          f"to {table} (covers [{config.start_date}, {reached_chunk_end}), "
          f"~{actual_scale:.0f}x baseline scale for THIS invocation alone - query the table "
          f"for the true cumulative total across all invocations)")


if __name__ == "__main__":
    main()
