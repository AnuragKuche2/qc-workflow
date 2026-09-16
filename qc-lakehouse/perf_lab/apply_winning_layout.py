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
strategy the dbt model now declares, or the two would disagree until the next rebuild."""
from __future__ import annotations

from databricks.sdk import WorkspaceClient

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import is_running_on_databricks

WAREHOUSE_ID = "ca865a4ef1668613"
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
