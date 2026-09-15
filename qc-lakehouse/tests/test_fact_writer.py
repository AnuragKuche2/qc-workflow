from datetime import UTC, datetime
from decimal import Decimal

import pytest

from qc_lakehouse.generator.fact_writer import (
    check_exactly_one_accepted_match_per_matched_order,
    check_fact_referential_integrity,
    check_order_volume_matches_demand,
    check_payment_amount_matches_order_total,
    check_subtotal_matches_line_items,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _order(order_id, status="DELIVERED", subtotal=Decimal("100.00"), order_total=Decimal("120.00")):
    return {"order_id": order_id, "order_status": status, "subtotal": subtotal, "order_total": order_total}


def test_check_order_volume_matches_demand_passes_when_bucket_counts_agree():
    order_buckets = [(0, 0), (0, 0), (0, 1)]        # 2 orders in (day 0, hour 0), 1 in (day 0, hour 1)
    hourly = [(0, None, 0, 2), (0, None, 1, 1)]      # day_index, order_date, hour, orders
    check_order_volume_matches_demand(order_buckets, hourly)  # must not raise


def test_check_order_volume_matches_demand_catches_a_per_bucket_mismatch():
    order_buckets = [(0, 0), (0, 0)]     # only 2 orders generated
    hourly = [(0, None, 0, 5)]            # but demand_hourly says 5 were expected
    with pytest.raises(AssertionError, match="volume"):
        check_order_volume_matches_demand(order_buckets, hourly)


def test_check_order_volume_matches_demand_catches_a_mismatch_hidden_by_a_correct_total():
    # 3 orders total in both cases, but distributed across the wrong buckets - a
    # total-only check would miss this; the per-(day,hour) check must not.
    order_buckets = [(0, 0), (0, 0), (0, 0)]
    hourly = [(0, None, 0, 2), (0, None, 1, 1)]
    with pytest.raises(AssertionError, match="volume"):
        check_order_volume_matches_demand(order_buckets, hourly)


def test_check_fact_referential_integrity_passes_on_a_consistent_world():
    orders = [_order(1)]
    order_items = [{"order_item_id": 1, "order_id": 1}]
    matches = [{"match_id": 1, "order_id": 1}]
    payments = [{"payment_id": 1, "order_id": 1}]
    refunds = [{"refund_id": 1, "order_id": 1, "payment_id": 1}]
    check_fact_referential_integrity(orders, order_items, matches, payments, refunds)  # must not raise


def test_check_fact_referential_integrity_catches_an_orphaned_order_item():
    orders = [_order(1)]
    order_items = [{"order_item_id": 1, "order_id": 999}]
    with pytest.raises(AssertionError, match="order_item"):
        check_fact_referential_integrity(orders, order_items, [], [], [])


def test_check_exactly_one_accepted_match_per_matched_order_passes():
    orders = [_order(1, status="DELIVERED"), _order(2, status="UNFULFILLED")]
    matches = [
        {"order_id": 1, "response": "DECLINED"},
        {"order_id": 1, "response": "ACCEPTED"},
        {"order_id": 2, "response": "DECLINED"},
        {"order_id": 2, "response": "TIMEOUT"},
    ]
    check_exactly_one_accepted_match_per_matched_order(orders, matches)  # must not raise


def test_check_exactly_one_accepted_match_per_matched_order_catches_a_missing_accept():
    orders = [_order(1, status="DELIVERED")]
    matches = [{"order_id": 1, "response": "DECLINED"}]
    with pytest.raises(AssertionError, match="ACCEPTED"):
        check_exactly_one_accepted_match_per_matched_order(orders, matches)


def test_check_subtotal_matches_line_items_passes():
    orders = [_order(1, subtotal=Decimal("30.00"))]
    order_items = [
        {"order_id": 1, "line_total": Decimal("10.00")},
        {"order_id": 1, "line_total": Decimal("20.00")},
    ]
    check_subtotal_matches_line_items(orders, order_items)  # must not raise


def test_check_subtotal_matches_line_items_catches_a_mismatch():
    orders = [_order(1, subtotal=Decimal("99.00"))]
    order_items = [{"order_id": 1, "line_total": Decimal("10.00")}]
    with pytest.raises(AssertionError, match="subtotal"):
        check_subtotal_matches_line_items(orders, order_items)


def test_check_payment_amount_matches_order_total_passes():
    orders = [_order(1, order_total=Decimal("120.00"))]
    payments = [{"order_id": 1, "amount": Decimal("120.00"), "status": "SUCCESS"}]
    check_payment_amount_matches_order_total(orders, payments)  # must not raise


def test_check_payment_amount_matches_order_total_catches_a_mismatch():
    orders = [_order(1, order_total=Decimal("120.00"))]
    payments = [{"order_id": 1, "amount": Decimal("50.00"), "status": "SUCCESS"}]
    with pytest.raises(AssertionError, match="payment"):
        check_payment_amount_matches_order_total(orders, payments)
