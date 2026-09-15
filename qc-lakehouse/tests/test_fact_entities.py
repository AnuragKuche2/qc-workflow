# qc-lakehouse/tests/test_fact_entities.py
import os
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.customers import build_customers
from qc_lakehouse.generator.entities import (
    build_cities,
    build_demand_curve,
    build_menu_items,
    build_restaurants,
    build_riders,
    build_zones,
)
from qc_lakehouse.generator.fact_entities import (
    build_match_attempts,
    build_order_items,
    build_orders_shell,
    build_payments,
    build_refunds,
    finalize_orders,
)
from qc_lakehouse.generator.schemas import (
    DEMAND_HOURLY_SCHEMA,
    GEN_RESTAURANT_PROFILE_SCHEMA,
    MENU_ITEMS_SCHEMA,
    RESTAURANTS_SCHEMA,
    RIDERS_SCHEMA,
    ZONES_SCHEMA,
)
from qc_lakehouse.spark_local import build_local_spark_session

# _weighted_pick_table (see fact_entities.py) replaced the old driver-side bisect UDF with a
# 1,000,000-row broadcast lookup table per weighted-pick call - correct and necessary to
# avoid Python UDFs on Databricks serverless, but building and broadcasting two such tables
# per build_orders_shell call, repeated across this module's ~20 tests, exhausted the local
# driver JVM's default heap (java.lang.OutOfMemoryError: Java heap space partway through the
# suite). conf/spark-local.conf itself documents the fix for this class of problem: the JVM
# reads its heap size at launch, before any SparkConf is applied in application code, so
# SPARK_DRIVER_MEMORY (an env var, read before JVM launch) is the correct lever - not a
# smaller bucket count, which would silently diverge from _h's actual discretization in the
# real pipeline. Set as early as possible (module import, i.e. pytest's collection phase,
# which completes for every test file before any test actually runs) so it's in place before
# the first SparkSession of the whole pytest process launches the JVM, regardless of which
# test file happens to run first.
os.environ.setdefault("SPARK_DRIVER_MEMORY", "4g")


@pytest.fixture(scope="module")
def spark():
    """A single local Spark session shared across every test in this module, rather than a
    fresh one per test. Sharing one long-lived session lets the JVM garbage-collect old
    broadcasts normally between tests instead of accumulating overhead across repeated
    context teardown/recreation - this is the fixture-level optimization the fix's own task
    brief anticipated as the correct response to a genuine (not just slow) local-suite
    problem, used here together with the larger SPARK_DRIVER_MEMORY set above."""
    s = build_local_spark_session()
    yield s
    s.stop()


def _tiny_config(**overrides):
    defaults = {
        "n_cities": 1, "zones_per_city": 3, "n_restaurants": 5, "n_riders": 5, "n_customers": 20,
        "menu_items_per_restaurant": 4, "days": 2, "orders_per_day": 50,
    }
    defaults.update(overrides)
    return GeneratorConfig(**defaults)


def _build_reference_fixtures(spark, config):
    """Builds a tiny, real reference layer in-memory (via the spine's own builders) and
    converts it to Spark DataFrames matching the real schemas - the same shape
    fact_writer.py will read back from Delta in production, without needing live
    Databricks for tests."""
    cities = build_cities(config)
    zones = build_zones(config, cities)
    restaurants = build_restaurants(config, cities, zones)
    riders = build_riders(config, cities, zones)
    menu_items = build_menu_items(config, restaurants)
    _daily, hourly = build_demand_curve(config)
    customers_df, customer_profile_df = build_customers(spark, config, cities, zones)

    zones_df = spark.createDataFrame(zones, ZONES_SCHEMA)
    restaurants_df = spark.createDataFrame([tuple(r[:14]) for r in restaurants], RESTAURANTS_SCHEMA)
    restaurant_profile_df = spark.createDataFrame(
        [(r[0], float(r[14]), float(r[15])) for r in restaurants], GEN_RESTAURANT_PROFILE_SCHEMA
    )
    riders_df = spark.createDataFrame(riders, RIDERS_SCHEMA)
    menu_items_df = spark.createDataFrame(menu_items, MENU_ITEMS_SCHEMA)
    demand_hourly_df = spark.createDataFrame(hourly, DEMAND_HOURLY_SCHEMA)

    return {
        "zones_df": zones_df, "restaurants_df": restaurants_df,
        "restaurant_profile_df": restaurant_profile_df, "riders_df": riders_df,
        "menu_items_df": menu_items_df, "demand_hourly_df": demand_hourly_df,
        "customers_df": customers_df, "customer_profile_df": customer_profile_df,
    }


def test_build_orders_shell_produces_exactly_the_demand_hourly_total(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    expected_total = sum(r["orders"] for r in fx["demand_hourly_df"].collect())

    orders = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )

    assert orders.count() == expected_total


def test_build_orders_shell_order_ids_are_unique_and_contiguous(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    orders = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    ids = sorted(r["order_id"] for r in orders.select("order_id").collect())
    assert ids == list(range(1, len(ids) + 1))


def test_build_orders_shell_references_are_all_real(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    orders = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    restaurant_ids = {r["restaurant_id"] for r in fx["restaurants_df"].select("restaurant_id").collect()}
    customer_ids = {r["customer_id"] for r in fx["customers_df"].select("customer_id").collect()}
    zone_ids = {r["zone_id"] for r in fx["zones_df"].select("zone_id").collect()}

    rows = orders.select("restaurant_id", "customer_id", "zone_id", "order_status").collect()
    assert all(r["restaurant_id"] in restaurant_ids for r in rows)
    assert all(r["customer_id"] in customer_ids for r in rows)
    assert all(r["zone_id"] in zone_ids for r in rows)
    assert all(r["order_status"] in ("DELIVERED", "CANCELLED", "UNFULFILLED") for r in rows)


def test_build_orders_shell_is_deterministic_for_the_same_seed(spark):
    config = _tiny_config(seed=555)
    fx = _build_reference_fixtures(spark, config)
    a = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    ).collect()
    b = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    ).collect()
    a_sorted = sorted((r["order_id"], r["restaurant_id"], r["customer_id"]) for r in a)
    b_sorted = sorted((r["order_id"], r["restaurant_id"], r["customer_id"]) for r in b)
    assert a_sorted == b_sorted


def test_build_order_items_every_item_belongs_to_a_real_order_and_menu_item(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    orders = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, orders, fx["menu_items_df"])

    order_ids = {r["order_id"] for r in orders.select("order_id").collect()}
    menu_item_ids = {r["menu_item_id"] for r in fx["menu_items_df"].select("menu_item_id").collect()}
    rows = items.select("order_id", "menu_item_id", "quantity", "line_total", "unit_price").collect()

    assert len(rows) > 0
    assert all(r["order_id"] in order_ids for r in rows)
    assert all(r["menu_item_id"] in menu_item_ids for r in rows)
    assert all(r["quantity"] >= 1 for r in rows)
    assert all(r["line_total"] == r["unit_price"] * r["quantity"] for r in rows)


def test_build_order_items_every_order_gets_at_least_one_item(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    orders = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, orders, fx["menu_items_df"])

    order_ids = {r["order_id"] for r in orders.select("order_id").collect()}
    item_order_ids = {r["order_id"] for r in items.select("order_id").collect()}
    assert item_order_ids == order_ids


def test_build_order_items_picks_from_the_orders_own_restaurant(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    orders = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, orders, fx["menu_items_df"])

    order_restaurant = {r["order_id"]: r["restaurant_id"] for r in orders.select("order_id", "restaurant_id").collect()}
    menu_restaurant = {r["menu_item_id"]: r["restaurant_id"] for r in fx["menu_items_df"].select("menu_item_id", "restaurant_id").collect()}

    for row in items.select("order_id", "menu_item_id").collect():
        assert menu_restaurant[row["menu_item_id"]] == order_restaurant[row["order_id"]]


def test_finalize_orders_subtotal_equals_sum_of_line_totals(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)

    item_subtotals = {
        r["order_id"]: r["subtotal"]
        for r in items.groupBy("order_id").agg(F.sum("line_total").alias("subtotal")).collect()
    }
    for row in orders.select("order_id", "subtotal").collect():
        assert row["subtotal"] == item_subtotals[row["order_id"]]


def test_finalize_orders_order_total_equals_subtotal_plus_delivery_fee(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)

    for row in orders.select("subtotal", "delivery_fee", "order_total").collect():
        assert row["order_total"] == row["subtotal"] + row["delivery_fee"]


def test_finalize_orders_has_the_full_orders_schema_columns(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)

    expected = {
        "order_id", "order_ref", "customer_id", "restaurant_id", "zone_id", "placed_at",
        "order_status", "subtotal", "delivery_fee", "commission_pct", "commission_amount",
        "order_total", "delivery_notes",
    }
    assert set(orders.columns) == expected


def test_finalize_orders_applies_text_noise_defect_at_the_configured_rate(spark):
    config = _tiny_config(text_noise_defect_rate=1.0)   # force every note to be mangled
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)

    notes = [r["delivery_notes"] for r in orders.select("delivery_notes").collect() if r["delivery_notes"]]
    assert notes    # at least one order got a note
    # every non-null note must show a mangling signature: fully upper, fully lower,
    # or surrounded by extra whitespace - never the clean original casing/spacing
    for n in notes:
        mangled = n.isupper() or n.islower() or n != n.strip()
        assert mangled, f"note not mangled: {n!r}"


def test_build_match_attempts_matched_orders_have_exactly_one_accepted(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)
    matches = build_match_attempts(config, orders, fx["riders_df"])

    accepted_counts = {
        r["order_id"]: r["n"]
        for r in matches.filter("response = 'ACCEPTED'").groupBy("order_id").count().withColumnRenamed("count", "n").collect()
    }
    matched_order_ids = {
        r["order_id"] for r in orders.filter("order_status != 'UNFULFILLED'").select("order_id").collect()
    }
    for oid in matched_order_ids:
        assert accepted_counts.get(oid) == 1, f"order {oid} does not have exactly one ACCEPTED attempt"


def test_build_match_attempts_unfulfilled_orders_have_zero_accepted(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)
    matches = build_match_attempts(config, orders, fx["riders_df"])

    unfulfilled_ids = {r["order_id"] for r in orders.filter("order_status = 'UNFULFILLED'").select("order_id").collect()}
    accepted_ids = {r["order_id"] for r in matches.filter("response = 'ACCEPTED'").select("order_id").collect()}
    assert unfulfilled_ids.isdisjoint(accepted_ids)


def test_build_match_attempts_riders_belong_to_the_orders_delivery_zone(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)
    matches = build_match_attempts(config, orders, fx["riders_df"])

    order_zone = {r["order_id"]: r["zone_id"] for r in orders.select("order_id", "zone_id").collect()}
    rider_zone = {r["rider_id"]: r["home_zone_id"] for r in fx["riders_df"].select("rider_id", "home_zone_id").collect()}
    for row in matches.select("order_id", "rider_id").collect():
        assert rider_zone[row["rider_id"]] == order_zone[row["order_id"]]


def test_build_payments_covers_every_non_unfulfilled_order_exactly_once(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)
    payments = build_payments(config, orders)

    payable_ids = {r["order_id"] for r in orders.filter("order_status != 'UNFULFILLED'").select("order_id").collect()}
    payment_order_ids = [r["order_id"] for r in payments.select("order_id").collect()]
    assert set(payment_order_ids) == payable_ids
    assert len(payment_order_ids) == len(set(payment_order_ids))   # exactly once each


def test_build_payments_amount_matches_order_total(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)
    payments = build_payments(config, orders)

    order_total = {r["order_id"]: r["order_total"] for r in orders.select("order_id", "order_total").collect()}
    for row in payments.select("order_id", "amount").collect():
        assert row["amount"] == order_total[row["order_id"]]


def test_build_payments_method_and_status_are_valid_values(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)
    payments = build_payments(config, orders)

    rows = payments.select("method", "status").collect()
    assert all(r["method"] in ("upi", "card", "wallet", "cod") for r in rows)
    assert all(r["status"] in ("SUCCESS", "FAILED") for r in rows)


def test_build_refunds_covers_every_cancelled_order(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)
    payments = build_payments(config, orders)
    refunds = build_refunds(config, orders, payments)

    cancelled_ids = {r["order_id"] for r in orders.filter("order_status = 'CANCELLED'").select("order_id").collect()}
    refund_ids = {r["order_id"] for r in refunds.select("order_id").collect()}
    assert cancelled_ids.issubset(refund_ids)


def test_build_refunds_reasons_are_valid_and_payment_id_is_real(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)
    payments = build_payments(config, orders)
    refunds = build_refunds(config, orders, payments)

    valid_reasons = {"CANCELLED", "QUALITY_ISSUE", "LATE_DELIVERY", "MISSING_ITEMS"}
    payment_ids = {r["payment_id"] for r in payments.select("payment_id").collect()}
    rows = refunds.select("reason", "payment_id").collect()
    assert all(r["reason"] in valid_reasons for r in rows)
    assert all(r["payment_id"] in payment_ids for r in rows)


def test_build_refunds_applies_money_text_defect_at_the_configured_rate(spark):
    config = _tiny_config(money_text_defect_rate=1.0, cancel_rate=0.5, unfulfilled_rate=0.0)
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)
    payments = build_payments(config, orders)
    refunds = build_refunds(config, orders, payments)

    raw_amounts = [r["refund_amount_raw"] for r in refunds.select("refund_amount_raw").collect()]
    assert raw_amounts    # at least one refund exists at cancel_rate=0.5
    assert all(a.startswith("(") and a.endswith(")") for a in raw_amounts)


def test_build_refunds_amount_never_exceeds_the_order_total(spark):
    config = _tiny_config()
    fx = _build_reference_fixtures(spark, config)
    shell = build_orders_shell(
        spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
        fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
    )
    items = build_order_items(config, shell, fx["menu_items_df"])
    orders = finalize_orders(config, shell, items)
    payments = build_payments(config, orders)
    refunds = build_refunds(config, orders, payments)

    order_total = {r["order_id"]: r["order_total"] for r in orders.select("order_id", "order_total").collect()}
    for row in refunds.select("order_id", "refund_amount_raw").collect():
        raw = row["refund_amount_raw"].strip("()")
        from decimal import Decimal
        assert Decimal(raw) <= order_total[row["order_id"]]


def test_finalize_orders_and_build_refunds_defect_formatting_matches_defects_module():
    """fact_entities.py duplicates (rather than calls) defects.py's pure-Python defect
    functions, to avoid Python UDFs (broken on this workspace's serverless compute). This
    test is the guard against the two implementations drifting apart."""
    from qc_lakehouse.generator.defects import (
        format_refund_amount,
        mangle_text_casing_and_whitespace,
    )

    # Text-mangle parity: same 4 modes, same order, as the inline F.when chain's mode_idx 0-3.
    text = "Ring the bell twice"
    assert mangle_text_casing_and_whitespace(text, "UPPER") == text.upper()
    assert mangle_text_casing_and_whitespace(text, "LOWER") == text.lower()
    assert mangle_text_casing_and_whitespace(text, "LEADING_SPACE") == f"   {text}"
    assert mangle_text_casing_and_whitespace(text, "TRAILING_SPACE") == f"{text}   "

    # Money-format parity: Python's f"{amount:.2f}" against what a decimal(18,2)-to-string
    # cast produces in Spark - spot-check a representative set of values including a
    # single-digit whole part, a value needing zero-padding, and a larger amount.
    for amount in [Decimal("5.00"), Decimal("0.47"), Decimal("1234.50"), Decimal("99999999.99")]:
        plain = format_refund_amount(amount, use_accounting_format=False)
        accounting = format_refund_amount(amount, use_accounting_format=True)
        assert plain == f"{amount:.2f}"
        assert accounting == f"({amount:.2f})"
