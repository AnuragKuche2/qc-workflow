# qc-lakehouse/perf_lab/cost_report.py
"""Joins Sub-project H's benchmark_results (bytes-scanned and duration per query per layout,
already captured from system.query.history by run_benchmark_queries.py) against
orders_bench_baseline's row count, and produces a written recommendation for which physical
layout to apply to production - not just raw numbers. Does not query system.billing.usage or
system.query.history directly.

qc_dev.perf_bench is intentionally dropped after Sub-project H's Task 6 cleanup - running this
script again requires first re-running Tasks 1-2 (generate_benchmark_orders.py then
apply_layouts.py) to recreate it."""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from databricks.sdk import WorkspaceClient

from perf_lab._shared import WAREHOUSE_ID
from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import is_running_on_databricks

CATALOG, SCHEMA = "qc_dev", "perf_bench"
REPORTS_DIR = Path(__file__).resolve().parents[2] / "docs" / "superpowers" / "reports"


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


def render_report(summary: dict[str, dict], scale_note: str) -> str:
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

    report = render_report(summary, scale_note)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"{datetime.now(UTC).date().isoformat()}-h-perf-cost-report.md"
    report_path.write_text(report)

    print(f"cost-report: OK - wrote {report_path}")
    print(report)


if __name__ == "__main__":
    main()
