# qc-lakehouse/perf_lab/run_benchmark_queries.py
"""Runs a fixed, representative query set against all 4 of Task 2's layout tables via the SQL
warehouse (not Spark) - system.query.history, which the cost report (Task 4) needs for
bytes-scanned, only tracks warehouse-executed queries. Queries run strictly SEQUENTIALLY:
Free Edition provides exactly one SQL warehouse (2X-Small, not resizable), so concurrent
queries would only contend with each other for the same fixed compute, adding unpredictable
noise to the comparison without any real parallelism benefit.

bytes_scanned collection is a separate, BULK phase after all 16 queries have run (not a
per-query retry loop): a live run against this workspace showed system.query.history lag
(~5-10 minutes between a statement finishing and its row appearing) that exceeds any
reasonable per-query wait budget. Retrying per query would pay that lag 16 times over;
instead, all 16 statements are fired first (each inserted with bytes_scanned initially NULL),
and a single bounded loop of batched `WHERE statement_id IN (...)` lookups (a handful of
rounds, sleeping between them) resolves whichever statement_ids have landed so far, until all
are resolved or the round budget is exhausted - so the lag is paid once, in parallel across
all 16, not once per query.

qc_dev.perf_bench is intentionally dropped after Sub-project H's Task 6 cleanup - running this
script again requires first re-running Tasks 1-2 (generate_benchmark_orders.py then
apply_layouts.py) to recreate it.

Each query gets a unique trailing SQL comment (`-- benchmark_run=<uuid>`) appended per
invocation of this script. A live re-run against unchanged tables with byte-for-byte
identical query text showed Databricks' SQL result cache serving cached results
(read_bytes=0, system.query.history's cache_origin_statement_id pointing back to the
statement_id of the original, real execution) instead of re-scanning - which would silently
corrupt every re-run's bytes_scanned/duration_ms for any query whose text and target table
happened not to change since a prior run. The comment is semantically a no-op but makes the
statement text unique per run, guaranteeing a cache miss (a real scan) every time, without
depending on session-scoped cache settings persisting across the Statement Execution API's
otherwise-stateless per-call requests.
"""
from __future__ import annotations

import time
import uuid

from databricks.sdk import WorkspaceClient

from perf_lab._shared import WAREHOUSE_ID
from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import is_running_on_databricks

CATALOG, SCHEMA = "qc_dev", "perf_bench"
LAYOUTS = ["orders_bench_baseline", "orders_bench_partitioned", "orders_bench_zorder", "orders_bench_liquid"]

# bytes_scanned bulk-backfill round budget: 10 rounds x 75s sleep = ~12.5min total headroom,
# comfortably above the ~5-10min system.query.history lag observed live in this workspace.
BYTES_SCANNED_ROUNDS = 10
BYTES_SCANNED_ROUND_SLEEP_S = 75

QUERIES = {
    "date_range_filter": (
        # Literal range inside Task 1's accepted, realized data window (date_day 2026-06-01
        # through 2026-06-10 - see generate_benchmark_orders.py's module docstring), not a
        # current_date()-relative range: current_date()-relative math drifts out of that fixed
        # window over time (confirmed live: it already scanned 0 bytes on all 4 layouts, since
        # Delta file-level stats pruned every file for a window with no matching data).
        "SELECT count(*), sum(order_total) FROM {table} "
        "WHERE date_day BETWEEN '2026-06-03' AND '2026-06-06'"
    ),
    "zone_filter": "SELECT count(*), sum(order_total) FROM {table} WHERE zone_id = 1",
    "status_filter": "SELECT count(*), sum(order_total) FROM {table} WHERE order_status = 'DELIVERED'",
    "zone_revenue_aggregate": (
        "SELECT zone_id, order_status, count(*) as n, sum(order_total) as revenue "
        "FROM {table} GROUP BY zone_id, order_status ORDER BY revenue DESC LIMIT 20"
    ),
}


def _run_statement(client: WorkspaceClient, statement: str) -> tuple[str, float]:
    t0 = time.time()
    result = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=statement, wait_timeout="50s",
    )
    elapsed_ms = (time.time() - t0) * 1000
    return result.statement_id, elapsed_ms


def _lookup_bytes_scanned_batch(client: WorkspaceClient, statement_ids: list[str]) -> dict[str, int]:
    """One batched system.query.history lookup for however many statement_ids are still
    pending. Tolerant of a single transient connection drop (observed live: the SDK's own
    internal retry can exhaust after several minutes and raise) - treated as "nothing new
    resolved this round" rather than aborting the whole backfill."""
    id_list = ", ".join(f"'{sid}'" for sid in statement_ids)
    try:
        rows = client.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=(
                "SELECT statement_id, read_bytes FROM system.query.history "
                f"WHERE statement_id IN ({id_list})"
            ),
            wait_timeout="30s",
        )
    except Exception as exc:  # noqa: BLE001 - transient network blip, this round yields nothing
        print(f"    (bulk bytes_scanned lookup transient error, will retry next round: {exc})")
        return {}
    if not (rows.result and rows.result.data_array):
        return {}
    return {row[0]: int(row[1]) for row in rows.result.data_array}


def _backfill_bytes_scanned(client: WorkspaceClient, pending: dict[str, tuple[str, str]]) -> None:
    """pending maps statement_id -> (layout, query_name) for rows still holding
    bytes_scanned = NULL. Mutates pending, removing each statement_id as it resolves."""
    for round_num in range(1, BYTES_SCANNED_ROUNDS + 1):
        if not pending:
            return
        found = _lookup_bytes_scanned_batch(client, list(pending.keys()))
        if found:
            case_clauses = " ".join(f"WHEN '{sid}' THEN {val}" for sid, val in found.items())
            found_id_list = ", ".join(f"'{sid}'" for sid in found)
            client.statement_execution.execute_statement(
                warehouse_id=WAREHOUSE_ID,
                statement=(
                    f"UPDATE {CATALOG}.{SCHEMA}.benchmark_results "
                    f"SET bytes_scanned = CASE statement_id {case_clauses} END "
                    f"WHERE statement_id IN ({found_id_list})"
                ),
                wait_timeout="30s",
            )
            for sid in found:
                pending.pop(sid, None)
        print(f"  bytes_scanned backfill round {round_num}/{BYTES_SCANNED_ROUNDS}: "
              f"{len(found)} resolved this round, {len(pending)} still pending")
        if pending and round_num < BYTES_SCANNED_ROUNDS:
            time.sleep(BYTES_SCANNED_ROUND_SLEEP_S)


def main() -> None:
    if not is_running_on_databricks():
        load_settings()  # fail fast locally if DATABRICKS_HOST etc. aren't set
    client = WorkspaceClient()

    client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=f"CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.benchmark_results "
                  "(layout STRING, query_name STRING, statement_id STRING, "
                  "duration_ms DOUBLE, bytes_scanned BIGINT)",
        wait_timeout="30s",
    )

    pending: dict[str, tuple[str, str]] = {}
    run_id = uuid.uuid4().hex

    for layout in LAYOUTS:
        table = f"{CATALOG}.{SCHEMA}.{layout}"
        for query_name, query_template in QUERIES.items():
            statement = query_template.format(table=table) + f" -- benchmark_run={run_id}"
            statement_id, duration_ms = _run_statement(client, statement)
            print(f"  {layout:24s} {query_name:24s} {duration_ms:8.1f}ms  statement_id={statement_id}")

            escaped_query_name = query_name.replace("'", "''")
            client.statement_execution.execute_statement(
                warehouse_id=WAREHOUSE_ID,
                statement=(
                    f"INSERT INTO {CATALOG}.{SCHEMA}.benchmark_results VALUES "
                    f"('{layout}', '{escaped_query_name}', '{statement_id}', {duration_ms}, NULL)"
                ),
                wait_timeout="30s",
            )
            pending[statement_id] = (layout, query_name)

    print(f"run-benchmark-queries: all {len(pending)} queries run, starting bytes_scanned backfill")
    _backfill_bytes_scanned(client, pending)

    if pending:
        print(f"run-benchmark-queries: WARNING - {len(pending)} rows still NULL after "
              f"{BYTES_SCANNED_ROUNDS} backfill rounds: {list(pending.values())}")
    else:
        print("run-benchmark-queries: all rows resolved with real bytes_scanned")

    print("run-benchmark-queries: OK")


if __name__ == "__main__":
    main()
