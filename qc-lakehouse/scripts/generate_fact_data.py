# qc-lakehouse/scripts/generate_fact_data.py
"""Builds and writes the quick-commerce money-chain fact tables (orders, order_items,
match_attempts, payments, refunds) to {GeneratorConfig.catalog}.{GeneratorConfig.schema}
on live Databricks serverless compute.

Requires the reference-data layer (scripts/generate_reference_data.py) to have already
been run - this script reads zones/restaurants/riders/menu_items/customers/demand_hourly
and their generator-only profile tables back from Delta.
"""
from __future__ import annotations

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session
from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.fact_writer import write_fact_tables


def main() -> None:
    settings = load_settings()
    spark = build_databricks_session(settings)
    # See scripts/generate_reference_data.py's comment: build_databricks_session's own
    # settings.databricks_catalog/settings.databricks_schema (typically workspace.dev) is
    # unrelated to this script's actual write target, GeneratorConfig's catalog/schema
    # (qc_dev.bronze_source by default).
    config = GeneratorConfig()

    counts = write_fact_tables(spark, config)

    print(f"catalog: {config.catalog}.{config.schema}\n")
    for name, n in counts.items():
        print(f"  {name:<28} {n:>10,}")

    total_rows = sum(counts.values())
    print(f"\ngenerate-fact-data: OK - wrote {len(counts)} tables, {total_rows:,} total rows")


if __name__ == "__main__":
    main()
