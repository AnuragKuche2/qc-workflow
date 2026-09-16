# qc-lakehouse/perf_lab/cost_report.py
"""Joins Sub-project H's benchmark_results (bytes-scanned and duration per query per layout,
already captured from system.query.history by run_benchmark_queries.py) against
orders_bench_baseline's row count, and produces a written recommendation for which physical
layout to apply to production - not just raw numbers.

Also queries system.billing.usage (joined against system.billing.list_prices for the current
USD rate) for the warehouse's aggregate DBU/USD cost over the benchmark's run window. This is
a warehouse-hour aggregate, not a per-layout or per-query cost: system.billing.usage buckets
consumption by warehouse and hour, with no per-statement or per-query cost column, so it can't
be split across the 4 layouts compared here - see render_cost_section's own output for the
full caveat. Bytes-scanned (from system.query.history, captured per query by
run_benchmark_queries.py) remains the per-layout decision signal for exactly that reason.

qc_dev.perf_bench is intentionally dropped after Sub-project H's Task 6 cleanup - running this
script again requires first re-running Tasks 1-2 (generate_benchmark_orders.py then
apply_layouts.py) to recreate it."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from databricks.sdk import WorkspaceClient

from perf_lab._shared import WAREHOUSE_ID
from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import is_running_on_databricks

CATALOG, SCHEMA = "qc_dev", "perf_bench"
REPORTS_DIR = Path(__file__).resolve().parents[2] / "docs" / "superpowers" / "reports"

# benchmark_results has no timestamp column (see run_benchmark_queries.py), so the cost window
# can't be read back from the benchmark data itself. Instead this looks back from "now" (report
# generation time) far enough to cover a normal H run: Task 3's 16 benchmark statements plus its
# ~12.5min system.query.history backfill polling, immediately followed by Task 4 (this script).
# Live observation of the original run showed the whole thing fit inside a 2-hour window: widen
# this constant if a future re-run's benchmark+report cycle runs longer than that.
COST_LOOKBACK_HOURS = 2


def summarize_by_layout(rows: list[dict]) -> dict[str, dict]:
    summary: dict[str, dict] = {}
    for row in rows:
        layout = row["layout"]
        entry = summary.setdefault(
            layout, {"total_duration_ms": 0.0, "total_bytes_scanned": 0, "missing_bytes_scanned_count": 0}
        )
        entry["total_duration_ms"] += row["duration_ms"]
        if row["bytes_scanned"] is None:
            entry["missing_bytes_scanned_count"] += 1
        else:
            entry["total_bytes_scanned"] += row["bytes_scanned"]
    return summary


def _fastest_duration_layout(summary: dict[str, dict]) -> str:
    return min(summary.items(), key=lambda kv: kv[1]["total_duration_ms"])[0]


def render_duration_note(summary: dict[str, dict], byte_winner: str) -> list[str]:
    """Returns markdown lines calling out a duration/bytes-scanned inversion when the layout
    with the lowest total wall-clock duration isn't the byte-scanned winner - empty list when
    they agree (no note needed). Kept general/data-driven (no hardcoded layout names) since
    this may run again on a different benchmark."""
    fastest = _fastest_duration_layout(summary)
    if fastest == byte_winner:
        return []
    fastest_ms = summary[fastest]["total_duration_ms"]
    winner_ms = summary[byte_winner]["total_duration_ms"]
    return [
        "",
        (
            f"**Note on duration:** `{fastest}` had the lowest total wall-clock duration "
            f"({fastest_ms:.0f}ms) among the layouts compared, faster than the recommended "
            f"`{byte_winner}` ({winner_ms:.0f}ms). Duration is not used as the decision signal "
            "here: each query ran once (no repeated sampling) on a shared, contended 2X-Small "
            "warehouse, so wall-clock time is subject to queuing/contention noise. "
            "Bytes-scanned is deterministic given the query and physical layout, which is why "
            "it - not duration - drives the recommendation above."
        ),
    ]


def render_cost_section(cost: dict) -> list[str]:
    """Pure rendering of the ## Cost section from a cost dict shaped like
    _fetch_warehouse_cost's return value. Takes the dict (not a live client) so it's
    unit-testable with fixture data."""
    start = cost["window_start"]
    end = cost["window_end"]
    total_dbu = cost["total_dbu"]
    total_usd = cost["total_usd"]
    skus = cost.get("skus", [])

    if len(skus) == 1 and skus[0].get("unit_price_usd") is not None:
        price_note = (
            f" (~${total_usd:,.2f} at ${skus[0]['unit_price_usd']:.2f}/DBU, "
            f"`{skus[0]['sku_name']}`)"
        )
    elif total_usd:
        price_note = f" (~${total_usd:,.2f})"
    else:
        price_note = ""

    return [
        "",
        "## Cost",
        "",
        (
            f"Aggregate SQL warehouse usage for the benchmark's run window "
            f"({start.isoformat()} to {end.isoformat()}, UTC): **{total_dbu:.4f} DBU**"
            f"{price_note}."
        ),
        "",
        (
            "This is a warehouse-hour aggregate, not a per-layout or per-query cost: "
            "`system.billing.usage` buckets consumption by warehouse and hour, with no "
            "per-statement or per-query cost column, so this total cannot be split across the "
            "4 layouts compared above - it covers everything the warehouse did in that window "
            "(the benchmark queries, the `system.query.history` backfill polling, and this "
            "report's own generation queries), not any single layout's cost alone."
        ),
        "",
        (
            "Bytes-scanned, not this billing total, is the per-layout decision signal used "
            "above: it is captured per query via `system.query.history` (see "
            "`run_benchmark_queries.py`), and DBU consumption for serverless SQL scales with "
            "compute-time/bytes-processed, so a lower-bytes-scanned layout is the one that "
            "would cost less at scale, even though this billing table's granularity can't "
            "prove that arithmetically at the level available here."
        ),
    ]


def render_report(summary: dict[str, dict], scale_note: str, cost: dict) -> str:
    ranked = sorted(summary.items(), key=lambda kv: kv[1]["total_bytes_scanned"])
    eligible = [(name, stats) for name, stats in ranked if stats["missing_bytes_scanned_count"] == 0]
    if not eligible:
        raise ValueError(
            "cost-report: every layout has at least one row with missing bytes_scanned - "
            "cannot recommend a winner. Re-run run_benchmark_queries.py's backfill."
        )
    winner, winner_stats = eligible[0]

    lines = [
        "# Sub-project H: Layout & Cost Comparison Report",
        "",
        f"Generated: {datetime.now(UTC).isoformat()}",
        f"Benchmark scale: {scale_note}",
        "",
        "## Results by layout (lower bytes-scanned = better query efficiency)",
        "",
        "| Layout | Total duration (ms) | Total bytes scanned | Missing bytes-scanned rows |",
        "|---|---|---|---|",
    ]
    for layout, stats in ranked:
        lines.append(
            f"| {layout} | {stats['total_duration_ms']:.0f} | "
            f"{stats['total_bytes_scanned']:,} | {stats['missing_bytes_scanned_count']} |"
        )

    lines += [
        "",
        "## Recommendation",
        "",
        (
            f"**{winner}** scanned the fewest total bytes across the benchmark query set "
            f"({winner_stats['total_bytes_scanned']:,} bytes), making it the recommended layout "
            f"for the real `fct_orders` table."
        ),
    ]
    lines += render_duration_note(summary, winner)
    lines += render_cost_section(cost)
    return "\n".join(lines) + "\n"


def _fetch_benchmark_rows(client: WorkspaceClient) -> list[dict]:
    result = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=f"SELECT layout, query_name, duration_ms, bytes_scanned "
                  f"FROM {CATALOG}.{SCHEMA}.benchmark_results",
        wait_timeout="30s",
    )
    columns = [c.name for c in result.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in (result.result.data_array or [])]


def _fetch_warehouse_cost(client: WorkspaceClient, window_start: datetime, window_end: datetime) -> dict:
    """Queries system.billing.usage for WAREHOUSE_ID's aggregate DBU consumption in
    [window_start, window_end), joined against system.billing.list_prices for each SKU's
    current USD rate. Returns a dict shaped for render_cost_section. This is a coarse,
    warehouse-hour-bucketed aggregate (system.billing.usage has no per-statement/per-query
    cost column) - see render_cost_section for the full caveat included in the report."""
    usage_result = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=(
            "SELECT sku_name, usage_unit, sum(usage_quantity) AS total_quantity "
            "FROM system.billing.usage "
            f"WHERE usage_metadata.warehouse_id = '{WAREHOUSE_ID}' "
            f"AND usage_start_time >= '{window_start.isoformat()}' "
            f"AND usage_start_time < '{window_end.isoformat()}' "
            "GROUP BY sku_name, usage_unit"
        ),
        wait_timeout="30s",
    )
    usage_columns = [c.name for c in usage_result.manifest.schema.columns]
    usage_rows = [dict(zip(usage_columns, row)) for row in (usage_result.result.data_array or [])]

    total_dbu = 0.0
    total_usd = 0.0
    skus: list[dict] = []
    for row in usage_rows:
        sku_name = row["sku_name"]
        quantity = float(row["total_quantity"])
        total_dbu += quantity

        price_result = client.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=(
                "SELECT pricing.default FROM system.billing.list_prices "
                f"WHERE sku_name = '{sku_name}' AND currency_code = 'USD' "
                "ORDER BY price_start_time DESC LIMIT 1"
            ),
            wait_timeout="30s",
        )
        price_rows = price_result.result.data_array or []
        unit_price = float(price_rows[0][0]) if price_rows else None
        if unit_price is not None:
            total_usd += quantity * unit_price
        skus.append({"sku_name": sku_name, "dbu": quantity, "unit_price_usd": unit_price})

    return {
        "window_start": window_start,
        "window_end": window_end,
        "total_dbu": total_dbu,
        "total_usd": total_usd,
        "skus": skus,
    }


def main() -> None:
    if not is_running_on_databricks():
        load_settings()
    client = WorkspaceClient()

    rows = _fetch_benchmark_rows(client)
    for row in rows:
        row["duration_ms"] = float(row["duration_ms"])
        row["bytes_scanned"] = int(row["bytes_scanned"]) if row["bytes_scanned"] is not None else None

    summary = summarize_by_layout(rows)
    scale_row_count = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=f"SELECT count(*) FROM {CATALOG}.{SCHEMA}.orders_bench_baseline",
        wait_timeout="30s",
    )
    actual_rows = int(scale_row_count.result.data_array[0][0])
    scale_note = f"{actual_rows:,} rows (~51.7x baseline; the 500x target was not reached - see README)"

    window_end = datetime.now(UTC)
    window_start = window_end - timedelta(hours=COST_LOOKBACK_HOURS)
    cost = _fetch_warehouse_cost(client, window_start, window_end)

    report = render_report(summary, scale_note, cost)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"{datetime.now(UTC).date().isoformat()}-h-perf-cost-report.md"
    report_path.write_text(report)

    print(f"cost-report: OK - wrote {report_path}")
    print(report)


if __name__ == "__main__":
    main()
