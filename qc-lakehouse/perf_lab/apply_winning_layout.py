# qc-lakehouse/perf_lab/apply_winning_layout.py
"""Applies Task 4's cost report's recommended layout to the REAL fct_orders table (Sub-project
C's gold table) - closes the loop from benchmark to production. Reads the recommendation
directly from qc_dev.perf_bench.benchmark_results rather than parsing the markdown report, so
this can't silently disagree with what the report says if the report is regenerated later.

DURABILITY: fct_orders is a dbt `table`-materialized model (dbt_project.yml's
`marts: +materialized: table`), which dbt-databricks rebuilds via
`CREATE OR REPLACE TABLE ... AS SELECT` on every `dbt run` (e.g. E1's `dbt_run` Databricks
Job, triggered on every `qc_lakehouse_pipeline` DAG run). A rebuild like that does NOT
preserve clustering unless it's declared in the model's own dbt config - so durability lives
in dbt/qc_lakehouse/models/marts/fct_orders.sql's `{{ config(liquid_clustered_by=[...]) }}`
block, NOT in this script. This script is only the one-time migration that gets the table
into that state *immediately*, without waiting for the next dbt run - it must apply the same
strategy the dbt model now declares, or the two would disagree until the next rebuild.

This script issues OPTIMIZE/ALTER TABLE via the SQL warehouse because it is a one-time,
human-triggered migration run interactively; run_maintenance.py issues the same kind of
statement via serverless Spark instead because it is the recurring path triggered by Airflow
as a Databricks Job (spark_python_task), which runs on serverless Spark like the rest of this
sub-project's scheduled jobs.

qc_dev.perf_bench is intentionally dropped after Sub-project H's Task 6 cleanup - only the
perf_bench schema read in _winning_layout() is gone, NOT qc_dev.gold.fct_orders (the
production table this script writes to, which is untouched by that cleanup). Running this
script again requires first re-running Tasks 1-2 (generate_benchmark_orders.py then
apply_layouts.py) to recreate qc_dev.perf_bench."""
from __future__ import annotations

from databricks.sdk import WorkspaceClient

from perf_lab._shared import WAREHOUSE_ID
from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import is_running_on_databricks

FCT_ORDERS = "qc_dev.gold.fct_orders"
ZORDER_COLUMN = "zone_id"

# Sentinel for a layout this script cannot honestly apply to an existing table - see main()
# below, which raises loudly instead of silently falling back to a no-op OPTIMIZE.
UNSUPPORTED_ON_EXISTING_TABLE = "UNSUPPORTED_ON_EXISTING_TABLE"

# Maps each benchmark layout name to the SQL that applies the equivalent strategy to a table
# that already exists (fct_orders is already created by dbt - this ALTERs it, it doesn't
# recreate it), except orders_bench_partitioned - see UNSUPPORTED_ON_EXISTING_TABLE above.
LAYOUT_SQL = {
    "orders_bench_baseline": [f"OPTIMIZE {FCT_ORDERS}"],
    "orders_bench_partitioned": UNSUPPORTED_ON_EXISTING_TABLE,
    "orders_bench_zorder": [f"OPTIMIZE {FCT_ORDERS} ZORDER BY ({ZORDER_COLUMN})"],
    "orders_bench_liquid": [
        f"ALTER TABLE {FCT_ORDERS} CLUSTER BY ({ZORDER_COLUMN})",
        f"OPTIMIZE {FCT_ORDERS}",
    ],
}


def _winning_layout(client: WorkspaceClient) -> str:
    """Picks the layout with the lowest total bytes_scanned, considering only layouts where
    every benchmark_results row has a non-null bytes_scanned. Databricks SQL sorts NULL first
    under ASC, and sum() over an all-NULL group returns NULL too, so without the HAVING clause
    below an unresolved layout could be selected and then have its strategy applied to the
    real fct_orders table - the HAVING clause (count(*) = count(bytes_scanned), which counts
    only non-null values) excludes any layout with a still-NULL row. This function only runs
    against live Databricks and has no unit test coverage - see the module's other functions
    for the same caveat."""
    result = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=(
            "SELECT layout, sum(bytes_scanned) as total_bytes "
            "FROM qc_dev.perf_bench.benchmark_results "
            "GROUP BY layout "
            "HAVING count(*) = count(bytes_scanned) "
            "ORDER BY total_bytes ASC LIMIT 1"
        ),
        wait_timeout="30s",
    )
    rows = result.result.data_array or []
    if not rows:
        raise RuntimeError(
            "apply-winning-layout: every layout has at least one benchmark_results row with "
            "bytes_scanned still NULL - refusing to pick a winner from incomplete data. "
            "Re-run run_benchmark_queries.py's backfill before retrying this script."
        )
    return rows[0][0]


def main() -> None:
    if not is_running_on_databricks():
        load_settings()
    client = WorkspaceClient()

    winner = _winning_layout(client)
    print(f"apply-winning-layout: {winner} won the benchmark - applying to {FCT_ORDERS}")

    statements = LAYOUT_SQL[winner]
    if statements == UNSUPPORTED_ON_EXISTING_TABLE:
        # Partitioning can only be set at CREATE TABLE time - Delta/Databricks has no ALTER
        # TABLE ... PARTITIONED BY for an existing table. fct_orders is dbt-owned (created via
        # `CREATE OR REPLACE TABLE ... AS SELECT`), so applying this would require either
        # dropping dbt's ownership of the table or declaring `partition_by` in
        # fct_orders.sql's own dbt config and rerunning `dbt run` - both are deliberate,
        # reviewed changes, not something this one-shot script should do silently. Raising
        # here (instead of a silent no-op OPTIMIZE that looks identical to the baseline
        # layout while claiming success) forces that decision to be made explicitly.
        raise NotImplementedError(
            f"apply-winning-layout: {winner} won the benchmark, but partitioning an "
            f"EXISTING dbt-owned table ({FCT_ORDERS}) requires recreating it - this script "
            "cannot safely apply it via ALTER. Manual intervention required: declare "
            "`partition_by` in dbt/qc_lakehouse/models/marts/fct_orders.sql's dbt config "
            "and run `dbt run --select fct_orders`, then rerun this script (it will then "
            "correctly report the baseline OPTIMIZE path as a no-op change, since "
            "partitioning would already be in place)."
        )

    for statement in statements:
        print(f"  {statement}")
        client.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID, statement=statement, wait_timeout="50s",
        )

    print(f"apply-winning-layout: OK - {FCT_ORDERS} now uses the {winner} strategy")


if __name__ == "__main__":
    main()
