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

    source_count = spark.table(SOURCE_TABLE).count()
    print(f"apply-layouts: source {SOURCE_TABLE} -> {source_count:,} rows")

    for name, table in [("baseline", baseline), ("partitioned", partitioned),
                         ("zorder", zordered), ("liquid", liquid)]:
        count = spark.table(table).count()
        print(f"apply-layouts: {name} -> {table} ({count:,} rows)")
        if count != source_count:
            raise ValueError(
                f"apply-layouts: row count mismatch for {table}: {count:,} rows, "
                f"expected {source_count:,} (from {SOURCE_TABLE})"
            )

    print("apply-layouts: OK - all 4 layouts created, all row counts match source")


if __name__ == "__main__":
    main()
