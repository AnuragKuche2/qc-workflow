# qc-lakehouse/tests/test_cost_report.py
from datetime import UTC, datetime

import pytest

from perf_lab.cost_report import (
    build_scale_note,
    render_cost_section,
    render_duration_note,
    render_report,
    summarize_by_layout,
)

SAMPLE_ROWS = [
    {"layout": "orders_bench_baseline", "query_name": "zone_filter", "duration_ms": 4000.0, "bytes_scanned": 900_000_000},
    {"layout": "orders_bench_baseline", "query_name": "status_filter", "duration_ms": 3500.0, "bytes_scanned": 850_000_000},
    {"layout": "orders_bench_zorder", "query_name": "zone_filter", "duration_ms": 800.0, "bytes_scanned": 90_000_000},
    {"layout": "orders_bench_zorder", "query_name": "status_filter", "duration_ms": 3400.0, "bytes_scanned": 820_000_000},
]

SAMPLE_COST = {
    "window_start": datetime(2026, 9, 16, 13, 0, tzinfo=UTC),
    "window_end": datetime(2026, 9, 16, 15, 0, tzinfo=UTC),
    "total_dbu": 7.767645555555556,
    "total_usd": 5.44,
    "skus": [
        {
            "sku_name": "PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO",
            "dbu": 7.767645555555556,
            "unit_price_usd": 0.70,
        }
    ],
}


def test_build_scale_note_reports_the_measured_scale_without_a_stale_target_claim():
    # Regression for a bug where regenerating the report would silently overwrite a corrected
    # scale-note with a hardcoded, stale "500x target was not reached" claim - the note must be
    # derived fresh from the live row count every time, and must never assert a target was "not
    # reached" as a hardcoded fact.
    note = build_scale_note(73_723_047, baseline_order_count=1_424_757)
    assert "73,723,047 rows in orders_bench_baseline" in note
    assert "~51.7x baseline" in note
    assert "500x target" not in note
    assert "not reached" not in note
    # Must flag that the raw orders_bench source table can have grown separately since.
    assert "orders_bench" in note.split("orders_bench_baseline", 1)[1]


def test_build_scale_note_recomputes_the_multiplier_from_whatever_row_count_it_is_given():
    note = build_scale_note(799_389_745, baseline_order_count=1_424_757)
    assert "799,389,745 rows in orders_bench_baseline" in note
    assert "~561.1x baseline" in note


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
    report = render_report(
        summary, scale_note="~51.7x baseline (500x target not reached)", cost=SAMPLE_COST
    )
    assert "orders_bench_zorder" in report
    assert "Recommendation" in report
    assert "~51.7x baseline (500x target not reached)" in report


def test_render_report_never_lets_an_unresolved_layout_win_on_a_zero_byte_total():
    summary = {
        "orders_bench_partitioned": {
            "total_duration_ms": 500.0,
            "total_bytes_scanned": 0,
            "missing_bytes_scanned_count": 1,
        },
        "orders_bench_zorder": {
            "total_duration_ms": 4200.0,
            "total_bytes_scanned": 910_000_000,
            "missing_bytes_scanned_count": 0,
        },
    }
    report = render_report(
        summary, scale_note="~51.7x baseline (500x target not reached)", cost=SAMPLE_COST
    )
    assert "**orders_bench_zorder**" in report
    assert "**orders_bench_partitioned**" not in report
    # The unresolved layout still appears in the full results table, just not as the winner.
    assert "orders_bench_partitioned" in report


def test_render_report_raises_when_every_layout_has_a_missing_row():
    summary = {
        "orders_bench_baseline": {
            "total_duration_ms": 500.0,
            "total_bytes_scanned": 0,
            "missing_bytes_scanned_count": 1,
        },
        "orders_bench_zorder": {
            "total_duration_ms": 400.0,
            "total_bytes_scanned": 100,
            "missing_bytes_scanned_count": 2,
        },
    }
    with pytest.raises(ValueError, match="missing bytes_scanned"):
        render_report(
            summary, scale_note="~51.7x baseline (500x target not reached)", cost=SAMPLE_COST
        )


def test_render_cost_section_reports_aggregate_dbu_and_usd_with_the_warehouse_hour_caveat():
    lines = render_cost_section(SAMPLE_COST)
    text = "\n".join(lines)
    assert "## Cost" in text
    assert "7.7676 DBU" in text
    assert "$5.44" in text
    assert "0.70/DBU" in text
    assert "PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO" in text
    # Must disclose that this is a coarse aggregate, not attributable per layout/query.
    assert "warehouse-hour aggregate" in text
    assert "cannot be split across the 4 layouts" in text
    # Must explain why bytes-scanned, not this billing total, is the per-layout signal.
    assert "Bytes-scanned" in text
    assert "system.query.history" in text


def test_render_cost_section_flags_billing_lag_instead_of_a_bare_zero_dbu_when_no_rows_found():
    cost = {
        "window_start": datetime(2026, 9, 16, 13, 0, tzinfo=UTC),
        "window_end": datetime(2026, 9, 16, 15, 0, tzinfo=UTC),
        "total_dbu": 0.0,
        "total_usd": 0.0,
        "skus": [],
    }
    lines = render_cost_section(cost)
    text = "\n".join(lines)
    assert "## Cost" in text
    assert "0.0000 DBU" not in text
    assert "No `system.billing.usage` rows were found" in text
    assert "ingestion lag" in text
    assert "not that zero cost was incurred" in text


def test_render_cost_section_omits_price_note_when_no_price_is_available():
    cost = dict(SAMPLE_COST)
    cost["total_usd"] = 0.0
    cost["skus"] = [
        {"sku_name": "SOME_SKU", "dbu": 7.767645555555556, "unit_price_usd": None}
    ]
    lines = render_cost_section(cost)
    text = "\n".join(lines)
    assert "7.7676 DBU" in text
    assert "$" not in text.split("## Cost")[1].split("This is a warehouse-hour")[0]


def test_render_duration_note_is_empty_when_the_byte_winner_is_also_fastest():
    summary = {
        "orders_bench_liquid": {"total_duration_ms": 500.0, "total_bytes_scanned": 100},
        "orders_bench_zorder": {"total_duration_ms": 900.0, "total_bytes_scanned": 200},
    }
    assert render_duration_note(summary, byte_winner="orders_bench_liquid") == []


def test_render_duration_note_flags_the_inversion_when_a_slower_bytes_winner_is_recommended():
    # Mirrors the real H benchmark: orders_bench_partitioned (6899ms) was faster in wall-clock
    # than the byte-scanned winner orders_bench_liquid (7523ms).
    summary = {
        "orders_bench_liquid": {"total_duration_ms": 7523.0, "total_bytes_scanned": 420_340_485},
        "orders_bench_partitioned": {"total_duration_ms": 6899.0, "total_bytes_scanned": 552_189_357},
    }
    lines = render_duration_note(summary, byte_winner="orders_bench_liquid")
    text = "\n".join(lines)
    assert "orders_bench_partitioned" in text
    assert "6899ms" in text
    assert "orders_bench_liquid" in text
    assert "7523ms" in text
    assert "not used as the decision signal" in text


def test_render_report_includes_cost_section_and_duration_note_end_to_end():
    summary = {
        "orders_bench_liquid": {
            "total_duration_ms": 7523.0,
            "total_bytes_scanned": 420_340_485,
            "missing_bytes_scanned_count": 0,
        },
        "orders_bench_partitioned": {
            "total_duration_ms": 6899.0,
            "total_bytes_scanned": 552_189_357,
            "missing_bytes_scanned_count": 0,
        },
    }
    report = render_report(summary, scale_note="~51.7x baseline (500x target not reached)", cost=SAMPLE_COST)
    assert "## Cost" in report
    assert "**Note on duration:**" in report
    assert "orders_bench_partitioned" in report
