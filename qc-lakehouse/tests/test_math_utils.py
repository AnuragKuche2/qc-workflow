import random
from decimal import Decimal

from qc_lakehouse.generator.math_utils import (
    allocate,
    pick,
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
