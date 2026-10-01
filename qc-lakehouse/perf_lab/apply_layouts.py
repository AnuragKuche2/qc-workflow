# qc-lakehouse/perf_lab/apply_layouts.py
"""Produces 4 physically distinct copies of Sub-project H's benchmark orders table, so the
comparison in Task 3 is apples-to-apples: every copy has identical data, differing only in
clustering strategy. All 4 get a plain OPTIMIZE (bin-packing compaction) - the baseline is
"compacted, no clustering", not "raw, unoptimized files", since comparing against literally
unoptimized output would trivially favor any clustering strategy and not show what clustering
actually adds beyond compaction alone.

Runs via serverless Spark, not the SQL warehouse - see
docs/superpowers/plans/2026-09-16-qc-lakehouse-h-perf-cost-lab.md's Global Constraints on
compute routing (this script's CREATE/OPTIMIZE calls against the small benchmark tables don't
need system.query.history's bytes-scanned signal; only Task 3's comparison queries do).

One layout per invocation, selected via `--layout` (required - see parse_args). Originally
this ran all 4 layouts in a single Databricks Connect session; at the 799M-row (561x)
benchmark scale, that session died mid-run with INVALID_HANDLE.OPERATION_ABANDONED (the same
duration/idle-limit failure generate_benchmark_orders.py hit at scale - see that module's
docstring). Splitting into one short session per layout, run as a real Databricks Job
(properly cancellable, per that same lesson), avoids it.
"""
from __future__ import annotations

import sys

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session, is_running_on_databricks

CATALOG, SCHEMA = "qc_dev", "perf_bench"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.orders_bench"
ZORDER_COLUMN = "zone_id"
LAYOUTS = ("baseline", "partitioned", "zorder", "liquid")


def parse_args(argv: list[str]) -> str:
    """The single `--layout` this invocation creates - one of LAYOUTS. Required (never
    defaults) and validated against LAYOUTS: silently running the wrong layout, or none at
    all, would waste a full pass over the 799M-row source table for nothing."""
    layout = None
    i = 0
    while i < len(argv):
        if argv[i] == "--layout":
            layout = argv[i + 1]
            i += 2
        else:
            raise ValueError(f"apply-layouts: unrecognized argument {argv[i]!r} - expected --layout.")
    if layout is None:
        raise ValueError(f"--layout is required, one of {LAYOUTS}")
    if layout not in LAYOUTS:
        raise ValueError(f"unknown layout {layout!r}, must be one of {LAYOUTS}")
    return layout


def _create_baseline(spark) -> str:
    table = f"{CATALOG}.{SCHEMA}.orders_bench_baseline"
    print("apply-layouts: baseline (compacted, no clustering)")
    spark.sql(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {SOURCE_TABLE}")
    spark.sql(f"OPTIMIZE {table}")
    return table


def _create_partitioned(spark) -> str:
    table = f"{CATALOG}.{SCHEMA}.orders_bench_partitioned"
    print("apply-layouts: partition-by-date")
    spark.sql(
        f"CREATE OR REPLACE TABLE {table} USING DELTA PARTITIONED BY (date_day) "
        f"AS SELECT * FROM {SOURCE_TABLE}"
    )
    spark.sql(f"OPTIMIZE {table}")
    return table


def _create_zordered(spark) -> str:
    table = f"{CATALOG}.{SCHEMA}.orders_bench_zorder"
    print(f"apply-layouts: ZORDER BY {ZORDER_COLUMN}")
    spark.sql(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {SOURCE_TABLE}")
    spark.sql(f"OPTIMIZE {table} ZORDER BY ({ZORDER_COLUMN})")
    return table


def _create_liquid(spark) -> str:
    table = f"{CATALOG}.{SCHEMA}.orders_bench_liquid"
    print(f"apply-layouts: Liquid Clustering on {ZORDER_COLUMN}")
    spark.sql(f"CREATE OR REPLACE TABLE {table} CLUSTER BY ({ZORDER_COLUMN}) "
              f"AS SELECT * FROM {SOURCE_TABLE}")
    spark.sql(f"OPTIMIZE {table}")
    return table


_LAYOUT_BUILDERS = {
    "baseline": _create_baseline,
    "partitioned": _create_partitioned,
    "zorder": _create_zordered,
    "liquid": _create_liquid,
}


def main() -> None:
    layout = parse_args(sys.argv[1:])

    settings = None if is_running_on_databricks() else load_settings()
    spark = build_databricks_session(settings)

    table = _LAYOUT_BUILDERS[layout](spark)

    source_count = spark.table(SOURCE_TABLE).count()
    count = spark.table(table).count()
    print(f"apply-layouts: {layout} -> {table} ({count:,} rows, source {SOURCE_TABLE} has {source_count:,})")
    if count != source_count:
        raise ValueError(
            f"apply-layouts: row count mismatch for {table}: {count:,} rows, "
            f"expected {source_count:,} (from {SOURCE_TABLE})"
        )

    print(f"apply-layouts: OK - {layout} created, row count matches source")


if __name__ == "__main__":
    main()
