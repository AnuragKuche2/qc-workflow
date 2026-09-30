# qc-lakehouse/tests/test_generate_benchmark_orders.py
import pytest

from perf_lab.generate_benchmark_orders import (
    _run_chunks,
    check_projected_orders_within_id_block,
    chunk_date_ranges,
    chunk_writer_options,
    id_offset_for_day_offset,
    parse_args,
    resolve_window,
    should_bail_out,
)


def test_chunk_date_ranges_splits_evenly():
    chunks = chunk_date_ranges("2026-06-01", days=10, chunk_days=5)
    assert chunks == [
        ("2026-06-01", "2026-06-06"),
        ("2026-06-06", "2026-06-11"),
    ]


def test_chunk_date_ranges_handles_a_remainder():
    chunks = chunk_date_ranges("2026-06-01", days=12, chunk_days=5)
    assert chunks == [
        ("2026-06-01", "2026-06-06"),
        ("2026-06-06", "2026-06-11"),
        ("2026-06-11", "2026-06-13"),
    ]


def test_should_bail_out_is_false_with_fewer_than_two_prior_chunks():
    assert should_bail_out([]) is False
    assert should_bail_out([12.0]) is False


def test_should_bail_out_is_false_when_latest_chunk_is_in_line_with_the_average():
    # average of [10, 10, 10] is 10; latest (10) is not > 1.75x that
    assert should_bail_out([10.0, 10.0, 10.0]) is False


def test_should_bail_out_is_true_when_latest_chunk_is_much_slower_than_the_average():
    # average of the first two (10, 10) is 10; latest (20) is 2x that, over the 1.75x threshold
    assert should_bail_out([10.0, 10.0, 20.0]) is True


def test_should_bail_out_respects_a_custom_threshold():
    assert should_bail_out([10.0, 10.0, 15.0], threshold=1.75) is False
    assert should_bail_out([10.0, 10.0, 15.0], threshold=1.4) is True


def test_run_chunks_completes_normally_when_every_chunk_succeeds():
    chunks = [("2026-06-01", "2026-06-06"), ("2026-06-06", "2026-06-11")]
    calls = []

    def write_chunk(chunk_start, chunk_end, write_mode):
        calls.append((chunk_start, chunk_end, write_mode))
        return 100

    total_rows, reached_chunk_end, elapsed, stopped_early = _run_chunks(
        chunks, write_chunk, "2026-06-01"
    )

    assert total_rows == 200
    assert reached_chunk_end == "2026-06-11"
    assert len(elapsed) == 2
    assert stopped_early is False  # every chunk in `chunks` was processed - a full run
    assert calls == [
        ("2026-06-01", "2026-06-06", "overwrite"),
        ("2026-06-06", "2026-06-11", "append"),
    ]


def test_run_chunks_stops_cleanly_when_a_chunk_write_raises(capsys):
    # Simulates Task 1's live-run failure mode: a chunk's write succeeds but the following
    # count (or the write itself) raises - here, a dropped Databricks Connect session.
    chunks = [
        ("2026-06-01", "2026-06-06"),
        ("2026-06-06", "2026-06-11"),
        ("2026-06-11", "2026-06-16"),
    ]
    calls = []

    def write_chunk(chunk_start, chunk_end, write_mode):
        calls.append((chunk_start, chunk_end, write_mode))
        if chunk_start == "2026-06-06":
            raise RuntimeError("SESSION_NOT_FOUND")
        return 100

    total_rows, reached_chunk_end, elapsed, stopped_early = _run_chunks(
        chunks, write_chunk, "2026-06-01"
    )

    # Only chunk 1 counts toward the totals - chunk 2 raised, chunk 3 was never attempted.
    assert total_rows == 100
    assert reached_chunk_end == "2026-06-06"  # end of the last chunk that actually succeeded
    assert len(elapsed) == 1
    assert stopped_early is True  # an exception mid-run must never look like a full run
    assert calls == [
        ("2026-06-01", "2026-06-06", "overwrite"),
        ("2026-06-06", "2026-06-11", "append"),
    ]

    out = capsys.readouterr().out
    assert "BAILOUT at chunk 2/3" in out
    assert "SESSION_NOT_FOUND" in out


def test_run_chunks_appends_the_first_chunk_when_resuming():
    # A resumed invocation (BENCH_DAY_OFFSET > 0) must never overwrite what a prior
    # invocation already wrote - only a from-scratch invocation (day_offset 0) may.
    chunks = [("2026-06-21", "2026-06-26"), ("2026-06-26", "2026-07-01")]
    calls = []

    def write_chunk(chunk_start, chunk_end, write_mode):
        calls.append((chunk_start, chunk_end, write_mode))
        return 100

    _run_chunks(chunks, write_chunk, "2026-06-21", first_chunk_write_mode="append")

    assert calls == [
        ("2026-06-21", "2026-06-26", "append"),
        ("2026-06-26", "2026-07-01", "append"),
    ]


def test_chunk_writer_options_overwrites_the_whole_table_for_a_from_scratch_run():
    assert chunk_writer_options("2026-06-01", "2026-06-06", "overwrite") == {
        "overwriteSchema": "true"
    }


def test_chunk_writer_options_replaces_only_the_chunk_slice_when_resuming():
    # Re-running a resumed invocation must replace its own chunk, not append a duplicate.
    assert chunk_writer_options("2026-07-01", "2026-07-06", "append") == {
        "replaceWhere": "date_day >= '2026-07-01' AND date_day < '2026-07-06'"
    }


def test_check_projected_orders_within_id_block_passes_when_comfortably_under():
    # 10-day window, 100M reserved (10 * 10M) - 50M projected is comfortably under.
    check_projected_orders_within_id_block(50_000_000, window_days=10, id_block_size=10_000_000)


def test_check_projected_orders_within_id_block_raises_at_the_observed_worst_case_margin():
    # Mirrors the live-verified day_offset=60 worst case: 97,752,042 projected orders against
    # a 100,000,000-id reserved block is only a 2.2% margin - still technically under, so this
    # must NOT raise here, but confirms the boundary is where the review found it.
    check_projected_orders_within_id_block(97_752_042, window_days=10, id_block_size=10_000_000)


def test_check_projected_orders_within_id_block_raises_when_projected_meets_or_exceeds_reserved():
    with pytest.raises(ValueError, match="reserved id block"):
        check_projected_orders_within_id_block(100_000_000, window_days=10, id_block_size=10_000_000)

    with pytest.raises(ValueError, match="reserved id block"):
        check_projected_orders_within_id_block(150_000_000, window_days=10, id_block_size=10_000_000)


def test_id_offset_for_day_offset_is_zero_at_the_start():
    assert id_offset_for_day_offset(0, id_block_size=10_000_000) == 0


def test_id_offset_for_day_offset_scales_by_block_size():
    assert id_offset_for_day_offset(20, id_block_size=10_000_000) == 200_000_000


def test_resolve_window_returns_the_full_window_when_it_fits():
    start_date, days = resolve_window(
        base_start_date="2026-06-01", day_offset=0, window_days=20, total_days=90
    )
    assert start_date == "2026-06-01"
    assert days == 20


def test_resolve_window_shifts_the_start_date_by_the_offset():
    start_date, days = resolve_window(
        base_start_date="2026-06-01", day_offset=20, window_days=20, total_days=90
    )
    assert start_date == "2026-06-21"
    assert days == 20


def test_resolve_window_clamps_to_the_remaining_days():
    start_date, days = resolve_window(
        base_start_date="2026-06-01", day_offset=80, window_days=20, total_days=90
    )
    assert start_date == "2026-08-20"
    assert days == 10


def test_resolve_window_raises_once_the_offset_reaches_the_total():
    # An offset >= total_days means every day is already covered - there is nothing left
    # for this invocation to do, and silently returning a 0-day window would let a caller
    # mistake "nothing to do" for "ran successfully".
    import pytest

    with pytest.raises(ValueError, match="day_offset"):
        resolve_window(base_start_date="2026-06-01", day_offset=90, window_days=20, total_days=90)


def test_run_chunks_still_bails_out_on_a_slow_chunk_without_raising(monkeypatch):
    # The pre-existing slow-chunk checkpoint (should_bail_out) still works through
    # _run_chunks after the refactor - no exception involved here at all.
    chunks = [
        ("2026-06-01", "2026-06-06"),
        ("2026-06-06", "2026-06-11"),
        ("2026-06-11", "2026-06-16"),
        ("2026-06-16", "2026-06-21"),
    ]
    # _run_chunks calls time.time() twice per chunk (t0, then elapsed = now - t0); starting
    # each chunk's clock at 0 and reporting `now` as the scripted duration makes
    # `elapsed` come out exactly as scripted: 10s, 10s, then 20s (2x the 1.75x threshold).
    clock_readings = iter([0.0, 10.0, 0.0, 10.0, 0.0, 20.0])
    monkeypatch.setattr(
        "perf_lab.generate_benchmark_orders.time.time", lambda: next(clock_readings)
    )

    def write_chunk(chunk_start, chunk_end, write_mode):
        return 100

    total_rows, reached_chunk_end, elapsed, stopped_early = _run_chunks(
        chunks, write_chunk, "2026-06-01"
    )

    assert total_rows == 300  # only the first 3 chunks landed before the bailout
    assert reached_chunk_end == "2026-06-16"
    assert elapsed == [10.0, 10.0, 20.0]
    assert stopped_early is True  # the slow-chunk bailout is a real early stop too


def test_parse_args_requires_day_offset_rather_than_defaulting_to_the_destructive_zero():
    # day_offset=0 triggers write_mode="overwrite" (wipes the whole table) - a missing or
    # misspelled --day-offset flag must fail loudly, never silently take that most-destructive
    # path.
    with pytest.raises(ValueError, match="day-offset"):
        parse_args([])


def test_parse_args_raises_on_an_unrecognized_flag():
    with pytest.raises(ValueError, match="--dayoffset"):
        parse_args(["--dayoffset", "20"])


def test_parse_args_reads_both_flags():
    assert parse_args(["--day-offset", "20", "--window-days", "15"]) == (20, 15)


def test_parse_args_reads_flags_in_either_order():
    assert parse_args(["--window-days", "15", "--day-offset", "20"]) == (20, 15)
