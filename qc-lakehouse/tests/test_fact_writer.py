# qc-lakehouse/tests/test_fact_writer.py
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from qc_lakehouse.generator.fact_writer import (
    check_exactly_one_accepted_match_per_matched_order,
    check_fact_referential_integrity,
    check_order_volume_matches_demand,
    check_payment_amount_matches_order_total,
    check_subtotal_matches_line_items,
)
from qc_lakehouse.spark_local import build_local_spark_session

D2 = DecimalType(18, 2)

ORDERS_TEST_SCHEMA = StructType([
    StructField("order_id", LongType()), StructField("order_status", StringType()),
    StructField("subtotal", D2), StructField("order_total", D2),
])
SHELL_TEST_SCHEMA = StructType([StructField("day_index", IntegerType()), StructField("hour", IntegerType())])
DEMAND_HOURLY_TEST_SCHEMA = StructType([
    StructField("day_index", IntegerType()), StructField("hour", IntegerType()),
    StructField("orders", IntegerType()),
])
ORDER_ITEMS_TEST_SCHEMA = StructType([
    StructField("order_item_id", LongType()), StructField("order_id", LongType()),
    StructField("line_total", D2),
])
MATCH_ATTEMPTS_TEST_SCHEMA = StructType([
    StructField("match_id", LongType()), StructField("order_id", LongType()),
    StructField("response", StringType()),
])
PAYMENTS_TEST_SCHEMA = StructType([
    StructField("payment_id", LongType()), StructField("order_id", LongType()),
    StructField("amount", D2), StructField("status", StringType()),
])
REFUNDS_TEST_SCHEMA = StructType([
    StructField("refund_id", LongType()), StructField("order_id", LongType()),
    StructField("payment_id", LongType()),
])


@pytest.fixture(scope="module")
def spark():
    s = build_local_spark_session()
    yield s
    s.stop()


def _order(order_id, status="DELIVERED", subtotal=Decimal("100.00"), order_total=Decimal("120.00")):
    return (order_id, status, subtotal, order_total)


def test_check_order_volume_matches_demand_passes_when_bucket_counts_agree(spark):
    shell = spark.createDataFrame([(0, 0), (0, 0), (0, 1)], SHELL_TEST_SCHEMA)  # 2 in (0,0), 1 in (0,1)
    hourly = spark.createDataFrame([(0, 0, 2), (0, 1, 1)], DEMAND_HOURLY_TEST_SCHEMA)
    check_order_volume_matches_demand(shell, hourly)  # must not raise


def test_check_order_volume_matches_demand_catches_a_per_bucket_mismatch(spark):
    shell = spark.createDataFrame([(0, 0), (0, 0)], SHELL_TEST_SCHEMA)  # only 2 orders generated
    hourly = spark.createDataFrame([(0, 0, 5)], DEMAND_HOURLY_TEST_SCHEMA)  # but 5 expected
    with pytest.raises(AssertionError, match="volume"):
        check_order_volume_matches_demand(shell, hourly)


def test_check_order_volume_matches_demand_catches_a_mismatch_hidden_by_a_correct_total(spark):
    # 3 orders total in both cases, but distributed across the wrong buckets - a
    # total-only check would miss this; the per-(day,hour) check must not.
    shell = spark.createDataFrame([(0, 0), (0, 0), (0, 0)], SHELL_TEST_SCHEMA)
    hourly = spark.createDataFrame([(0, 0, 2), (0, 1, 1)], DEMAND_HOURLY_TEST_SCHEMA)
    with pytest.raises(AssertionError, match="volume"):
        check_order_volume_matches_demand(shell, hourly)


def test_check_fact_referential_integrity_passes_on_a_consistent_world(spark):
    orders = spark.createDataFrame([_order(1)], ORDERS_TEST_SCHEMA)
    order_items = spark.createDataFrame([(1, 1, Decimal("10.00"))], ORDER_ITEMS_TEST_SCHEMA)
    matches = spark.createDataFrame([(1, 1, "ACCEPTED")], MATCH_ATTEMPTS_TEST_SCHEMA)
    payments = spark.createDataFrame([(1, 1, Decimal("120.00"), "SUCCESS")], PAYMENTS_TEST_SCHEMA)
    refunds = spark.createDataFrame([(1, 1, 1)], REFUNDS_TEST_SCHEMA)
    check_fact_referential_integrity(orders, order_items, matches, payments, refunds)  # must not raise


def test_check_fact_referential_integrity_catches_an_orphaned_order_item(spark):
    orders = spark.createDataFrame([_order(1)], ORDERS_TEST_SCHEMA)
    order_items = spark.createDataFrame([(1, 999, Decimal("10.00"))], ORDER_ITEMS_TEST_SCHEMA)
    empty_matches = spark.createDataFrame([], MATCH_ATTEMPTS_TEST_SCHEMA)
    empty_payments = spark.createDataFrame([], PAYMENTS_TEST_SCHEMA)
    empty_refunds = spark.createDataFrame([], REFUNDS_TEST_SCHEMA)
    with pytest.raises(AssertionError, match="order_item"):
        check_fact_referential_integrity(orders, order_items, empty_matches, empty_payments, empty_refunds)


def test_check_fact_referential_integrity_catches_an_orphaned_refund_payment(spark):
    orders = spark.createDataFrame([_order(1)], ORDERS_TEST_SCHEMA)
    empty_items = spark.createDataFrame([], ORDER_ITEMS_TEST_SCHEMA)
    empty_matches = spark.createDataFrame([], MATCH_ATTEMPTS_TEST_SCHEMA)
    payments = spark.createDataFrame([(1, 1, Decimal("120.00"), "SUCCESS")], PAYMENTS_TEST_SCHEMA)
    refunds = spark.createDataFrame([(1, 1, 999)], REFUNDS_TEST_SCHEMA)
    with pytest.raises(AssertionError, match="payment"):
        check_fact_referential_integrity(orders, empty_items, empty_matches, payments, refunds)


def test_check_exactly_one_accepted_match_per_matched_order_passes(spark):
    orders = spark.createDataFrame(
        [_order(1, status="DELIVERED"), _order(2, status="UNFULFILLED")], ORDERS_TEST_SCHEMA
    )
    matches = spark.createDataFrame(
        [(1, 1, "DECLINED"), (2, 1, "ACCEPTED"), (3, 2, "DECLINED"), (4, 2, "TIMEOUT")],
        MATCH_ATTEMPTS_TEST_SCHEMA,
    )
    check_exactly_one_accepted_match_per_matched_order(orders, matches)  # must not raise


def test_check_exactly_one_accepted_match_per_matched_order_catches_a_missing_accept(spark):
    orders = spark.createDataFrame([_order(1, status="DELIVERED")], ORDERS_TEST_SCHEMA)
    matches = spark.createDataFrame([(1, 1, "DECLINED")], MATCH_ATTEMPTS_TEST_SCHEMA)
    with pytest.raises(AssertionError, match="ACCEPTED"):
        check_exactly_one_accepted_match_per_matched_order(orders, matches)


def test_check_subtotal_matches_line_items_passes(spark):
    orders = spark.createDataFrame([_order(1, subtotal=Decimal("30.00"))], ORDERS_TEST_SCHEMA)
    order_items = spark.createDataFrame(
        [(1, 1, Decimal("10.00")), (2, 1, Decimal("20.00"))], ORDER_ITEMS_TEST_SCHEMA
    )
    check_subtotal_matches_line_items(orders, order_items)  # must not raise


def test_check_subtotal_matches_line_items_catches_a_mismatch(spark):
    orders = spark.createDataFrame([_order(1, subtotal=Decimal("99.00"))], ORDERS_TEST_SCHEMA)
    order_items = spark.createDataFrame([(1, 1, Decimal("10.00"))], ORDER_ITEMS_TEST_SCHEMA)
    with pytest.raises(AssertionError, match="subtotal"):
        check_subtotal_matches_line_items(orders, order_items)


def test_check_payment_amount_matches_order_total_passes(spark):
    orders = spark.createDataFrame([_order(1, order_total=Decimal("120.00"))], ORDERS_TEST_SCHEMA)
    payments = spark.createDataFrame([(1, 1, Decimal("120.00"), "SUCCESS")], PAYMENTS_TEST_SCHEMA)
    check_payment_amount_matches_order_total(orders, payments)  # must not raise


def test_check_payment_amount_matches_order_total_catches_a_mismatch(spark):
    orders = spark.createDataFrame([_order(1, order_total=Decimal("120.00"))], ORDERS_TEST_SCHEMA)
    payments = spark.createDataFrame([(1, 1, Decimal("50.00"), "SUCCESS")], PAYMENTS_TEST_SCHEMA)
    with pytest.raises(AssertionError, match="payment"):
        check_payment_amount_matches_order_total(orders, payments)
