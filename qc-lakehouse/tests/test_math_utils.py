import random
from datetime import date
from decimal import Decimal

from qc_lakehouse.generator.math_utils import (
    allocate,
    day_factors,
    destination,
    events_on,
    haversine_km,
    km_per_deg_lon,
    offset_km,
    orders_by_hour,
    pick,
    resolve_events,
    sample_around,
    spiral_point,
    split_by_share,
    stream_seed,
    zipf,
    zone_density,
)


def test_stream_seed_is_deterministic_and_seed_dependent():
    a1 = stream_seed(42, "riders")
    a2 = stream_seed(42, "riders")
    b = stream_seed(43, "riders")
    c = stream_seed(42, "customers")
    assert a1 == a2
    assert a1 != b
    assert a1 != c


def test_allocate_sums_exactly_to_total_with_no_drift():
    total = Decimal("100.00")
    weights = [1, 1, 1]
    parts = allocate(total, weights, places=2)
    assert sum(parts) == total
    assert len(parts) == 3


def test_allocate_largest_remainder_gets_the_extra_unit():
    # 10 split 3 ways by equal weight: 3.33, 3.33, 3.33 -> floors 3.33 x3 = 9.99, one part
    # must get the leftover 0.01. All three have an identical remainder, so the tiebreak
    # (lowest index) decides - index 0 gets it.
    parts = allocate(Decimal("10.00"), [1, 1, 1], places=2)
    assert sum(parts) == Decimal("10.00")
    assert parts[0] == Decimal("3.34")
    assert parts[1] == Decimal("3.33")
    assert parts[2] == Decimal("3.33")


def test_allocate_rejects_zero_weight_sum():
    import pytest

    with pytest.raises(ValueError):
        allocate(Decimal(10), [0, 0], places=2)


def test_split_by_share_sums_exactly_to_total():
    parts = split_by_share(100, [0.42, 0.35, 0.23])
    assert sum(parts) == 100
    assert len(parts) == 3
    assert all(isinstance(p, int) for p in parts)


def test_zone_density_decays_and_sums_to_one():
    weights = zone_density(5, decay=6.0)
    assert len(weights) == 5
    assert abs(sum(weights) - 1.0) < 1e-9
    assert weights[0] > weights[1] > weights[4]


def test_zipf_decays_and_sums_to_one():
    weights = zipf(10)
    assert len(weights) == 10
    assert abs(sum(weights) - 1.0) < 1e-9
    assert weights[0] > weights[9]


def test_pick_respects_weights_at_extremes():
    rng = random.Random(1)
    # A near-certain option must be picked when the roll lands inside its share.
    assert pick(rng, [("only", 1.0)]) == "only"


def test_pick_never_falls_off_the_end_on_float_rounding():
    rng = random.Random(1)
    # Weights that don't sum to exactly 1.0 due to float error must still return the last
    # option rather than None if the roll exceeds the cumulative sum.
    result = pick(rng, [("a", 0.3), ("b", 0.3), ("c", 0.3999999999999)])
    assert result in ("a", "b", "c")


def test_km_per_deg_lon_shrinks_toward_the_poles():
    equator = km_per_deg_lon(0.0)
    mid_lat = km_per_deg_lon(45.0)
    assert equator > mid_lat > 0


def test_offset_km_moves_north_and_east_correctly():
    lat, lon = offset_km(0.0, 0.0, north_km=111.19, east_km=0.0)
    assert abs(lat - 1.0) < 0.01     # ~111.19 km north is ~1 degree of latitude
    assert abs(lon - 0.0) < 1e-9      # pure north movement doesn't change longitude


def test_destination_and_haversine_are_consistent():
    lat0, lon0 = 12.9716, 77.5946   # Bengaluru
    lat1, lon1 = destination(lat0, lon0, bearing=90, km=5.0)
    dist = haversine_km(lat0, lon0, lat1, lon1)
    assert abs(dist - 5.0) < 0.05    # round-trip should recover ~5 km


def test_haversine_km_zero_distance_for_same_point():
    assert haversine_km(12.97, 77.59, 12.97, 77.59) == 0.0


def test_spiral_point_places_successive_zones_farther_out():
    lat0, lon0 = 12.9716, 77.5946
    p0 = spiral_point(lat0, lon0, 0)
    p5 = spiral_point(lat0, lon0, 5)
    d0 = haversine_km(lat0, lon0, *p0)
    d5 = haversine_km(lat0, lon0, *p5)
    assert d5 > d0


def test_sample_around_stays_within_max_km():
    rng = random.Random(7)
    lat0, lon0 = 12.9716, 77.5946
    for _ in range(50):
        lat, lon = sample_around(rng, lat0, lon0, sigma_km=0.7, max_km=2.0)
        assert haversine_km(lat0, lon0, lat, lon) <= 2.0 + 1e-6


TEST_EVENTS = [
    ("sports_final", 0.13, 1.80, (18, 22), 2.2, 1.0, 1.0, 1.0),
    ("storm", 0.38, 1.45, None, 1.0, 1.40, 1.60, 2.5),
    ("festival", 0.66, 2.10, None, 1.0, 1.0, 1.0, 1.0),
    ("long_weekend", 0.87, 1.30, None, 1.0, 1.0, 1.0, 1.0),
]
TEST_DOW = [0.85, 0.82, 0.90, 1.10, 1.35, 1.45, 1.20]
TEST_DOW = [m / (sum(TEST_DOW) / 7) for m in TEST_DOW]


def test_resolve_events_scales_count_with_window_length():
    short = resolve_events(TEST_EVENTS, days=7)
    long = resolve_events(TEST_EVENTS, days=90)
    assert len(short) <= len(long)
    assert len(long) == 4    # max(1, round(90/90*4)) == 4, all events fit


def test_resolve_events_deduplicates_by_day_index():
    resolved = resolve_events(TEST_EVENTS, days=90)
    day_indices = [d for d, _ in resolved]
    assert len(day_indices) == len(set(day_indices))


def test_events_on_gives_shoulder_days_partial_share():
    resolved = [(10, TEST_EVENTS[0])]
    assert events_on(10, resolved, shoulder_before=0.30, shoulder_after=0.50) == [(TEST_EVENTS[0], 1.0)]
    assert events_on(9, resolved, shoulder_before=0.30, shoulder_after=0.50) == [(TEST_EVENTS[0], 0.30)]
    assert events_on(11, resolved, shoulder_before=0.30, shoulder_after=0.50) == [(TEST_EVENTS[0], 0.50)]
    assert events_on(12, resolved, shoulder_before=0.30, shoulder_after=0.50) == []


def test_day_factors_event_day_has_higher_total_than_non_event_day():
    resolved = [(10, TEST_EVENTS[2])]  # festival, peak=2.10
    noise = [1.0] * 20
    non_event = day_factors(0, date(2026, 6, 1), TEST_DOW, resolved, noise, 15_000, 0.30, 0.50)
    event_day = day_factors(10, date(2026, 6, 1), TEST_DOW, resolved, noise, 15_000, 0.30, 0.50)
    assert event_day[6] > non_event[6]    # index 6 is the resolved order count
    assert "festival" in event_day[7]


def test_orders_by_hour_sums_exactly_to_the_day_total():
    resolved = resolve_events(TEST_EVENTS, days=90)
    noise = [1.0] * 90
    hourly_weekday = [1 / 24] * 24
    hourly_weekend = [1 / 24] * 24
    for day_index in range(90):
        _, _weekday, *_, orders, _ = day_factors(
            day_index, date(2026, 6, 1), TEST_DOW, resolved, noise, 15_000, 0.30, 0.50
        )
        hours = orders_by_hour(
            day_index, date(2026, 6, 1), TEST_DOW, resolved, noise, 15_000,
            hourly_weekday, hourly_weekend, 0.30, 0.50,
        )
        assert sum(hours) == orders, f"day {day_index}: hours sum to {sum(hours)}, expected {orders}"
