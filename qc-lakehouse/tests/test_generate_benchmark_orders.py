# qc-lakehouse/tests/test_generate_benchmark_orders.py
from perf_lab.generate_benchmark_orders import _run_chunks, chunk_date_ranges, should_bail_out


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
