# qc-lakehouse/tests/test_cost_report.py
from perf_lab.cost_report import render_report, summarize_by_layout

SAMPLE_ROWS = [
    {"layout": "orders_bench_baseline", "query_name": "zone_filter", "duration_ms": 4000.0, "bytes_scanned": 900_000_000},
    {"layout": "orders_bench_baseline", "query_name": "status_filter", "duration_ms": 3500.0, "bytes_scanned": 850_000_000},
    {"layout": "orders_bench_zorder", "query_name": "zone_filter", "duration_ms": 800.0, "bytes_scanned": 90_000_000},
    {"layout": "orders_bench_zorder", "query_name": "status_filter", "duration_ms": 3400.0, "bytes_scanned": 820_000_000},
]


def test_summarize_by_layout_totals_duration_and_bytes_per_layout():
    summary = summarize_by_layout(SAMPLE_ROWS)
    assert summary["orders_bench_baseline"]["total_duration_ms"] == 7500.0
    assert summary["orders_bench_baseline"]["total_bytes_scanned"] == 1_750_000_000
    assert summary["orders_bench_zorder"]["total_duration_ms"] == 4200.0
    assert summary["orders_bench_zorder"]["total_bytes_scanned"] == 910_000_000


def test_summarize_by_layout_handles_a_null_bytes_scanned_row():
    rows = SAMPLE_ROWS + [
        {"layout": "orders_bench_partitioned", "query_name": "zone_filter", "duration_ms": 1000.0, "bytes_scanned": None},
    ]
    summary = summarize_by_layout(rows)
    assert summary["orders_bench_partitioned"]["total_bytes_scanned"] == 0
    assert summary["orders_bench_partitioned"]["missing_bytes_scanned_count"] == 1


def test_render_report_names_the_lowest_bytes_scanned_layout_as_the_recommendation():
    summary = summarize_by_layout(SAMPLE_ROWS)
    report = render_report(summary, scale_note="~500x baseline")
    assert "orders_bench_zorder" in report
    assert "Recommendation" in report
    assert "~500x baseline" in report
