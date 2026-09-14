# qc-lakehouse/scripts/generate_reference_data.py
"""Builds and writes the quick-commerce reference-data layer (cities, zones,
restaurants, riders, menu items, customers, payout tiers, demand curve) to
{GeneratorConfig.catalog}.{GeneratorConfig.schema} on live Databricks serverless compute.

This is both the production entrypoint and Sub-project B's smoke test - unlike
smoke_local.py/smoke_databricks.py from Sub-project A, this writes real pipeline data,
not throwaway rows, so there's nothing to clean up afterward.
"""
from __future__ import annotations

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session
from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.writer import write_reference_tables


def main() -> None:
    settings = load_settings()
    spark = build_databricks_session(settings)
    config = GeneratorConfig()

    counts = write_reference_tables(spark, config)

    print(f"catalog: {config.catalog}.{config.schema}\n")
    for name, n in counts.items():
        print(f"  {name:<28} {n:>10,}")

    total_rows = sum(counts.values())
    print(f"\ngenerate-reference-data: OK - wrote {len(counts)} tables, {total_rows:,} total rows")


if __name__ == "__main__":
    main()
