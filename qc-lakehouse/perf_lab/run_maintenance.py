# qc-lakehouse/perf_lab/run_maintenance.py
"""OPTIMIZE/ANALYZE/VACUUM for any gold-layer table, triggered by 3 table-agnostic Databricks
Jobs (optimize_gold_table/analyze_gold_table/vacuum_gold_table) that all point at this same
script with different --operation/--table arguments. VACUUM never goes below Delta's 7-day
default retention (project-wide constraint) - no explicit RETAIN clause is passed, so Delta's
own 7-day default applies.

Originally hardcoded to fct_orders only, triggered by 3 identically-shaped but
fct_orders-specific jobs (optimize_fct_orders/analyze_fct_orders/vacuum_fct_orders). Extended
(2026-09-18, orchestration hardening) to take --table so the maintenance DAG can run one
parallel chain per gold table instead of only maintaining the single biggest one - the jobs
themselves were renamed to be table-agnostic since a job named "optimize_fct_orders" running
against dim_customer would be a misleading name.

Runs on serverless Spark (not the SQL warehouse) because it executes as a Databricks Job
triggered by Airflow, unlike apply_winning_layout.py's one-time interactive warehouse run."""
from __future__ import annotations

import sys

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session, is_running_on_databricks

CATALOG_SCHEMA = "qc_dev.gold"
GOLD_TABLES = (
    "fct_orders", "fct_deliveries", "dim_customer", "dim_restaurant",
    "dim_rider", "dim_zone", "dim_date",
)

OPERATION_TEMPLATES = {
    "optimize": "OPTIMIZE {table}",
    "analyze": "ANALYZE TABLE {table} COMPUTE STATISTICS",
    "vacuum": "VACUUM {table}",  # no RETAIN clause - Delta's 7-day default applies
}


def parse_args(argv: list[str]) -> tuple[str, str]:
    """(operation, table_short_name) from `--operation X --table Y` CLI flags (either order).
    Both flags are required and any unrecognized token raises: a missing or misspelled
    --table (e.g. --tabel) must fail loudly rather than silently maintaining the wrong table.
    Every caller passes both - the DAG's mapped python_params and the 3 gold-table Jobs'
    default parameters in databricks.yml."""
    operation = None
    table = None
    i = 0
    while i < len(argv):
        if argv[i] == "--operation":
            operation = argv[i + 1]
            i += 2
        elif argv[i] == "--table":
            table = argv[i + 1]
            i += 2
        else:
            raise ValueError(
                f"run-maintenance: unrecognized argument {argv[i]!r} - expected --operation "
                "and --table."
            )
    if operation not in OPERATION_TEMPLATES:
        raise ValueError(f"--operation must be one of {list(OPERATION_TEMPLATES)}, got {operation!r}")
    if table not in GOLD_TABLES:
        raise ValueError(f"--table must be one of {GOLD_TABLES}, got {table!r}")
    return operation, table


def main() -> None:
    operation, table = parse_args(sys.argv[1:])
    full_table = f"{CATALOG_SCHEMA}.{table}"

    settings = None if is_running_on_databricks() else load_settings()
    spark = build_databricks_session(settings)

    statement = OPERATION_TEMPLATES[operation].format(table=full_table)
    print(f"run-maintenance: {statement}")
    spark.sql(statement).collect()
    print(f"run-maintenance: OK - {operation} completed on {full_table}")


if __name__ == "__main__":
    main()
