# qc-lakehouse/perf_lab/run_maintenance.py
"""OPTIMIZE/ANALYZE/VACUUM for the real fct_orders table, triggered by 3 separate Databricks
Jobs (optimize_fct_orders/analyze_fct_orders/vacuum_fct_orders) that all point at this same
script with a different --operation argument. VACUUM never goes below Delta's 7-day default
retention (project-wide constraint) - no explicit RETAIN clause is passed, so Delta's own
7-day default applies.

Runs on serverless Spark (not the SQL warehouse) because it executes as a Databricks Job
triggered by Airflow, unlike apply_winning_layout.py's one-time interactive warehouse run."""
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
    spark.sql(statement).collect()
    print(f"run-maintenance: OK - {operation} completed on {FCT_ORDERS}")


if __name__ == "__main__":
    main()
