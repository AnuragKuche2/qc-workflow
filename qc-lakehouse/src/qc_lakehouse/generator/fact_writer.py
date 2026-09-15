# qc-lakehouse/src/qc_lakehouse/generator/fact_writer.py
from __future__ import annotations

from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.fact_entities import (
    build_match_attempts,
    build_order_items,
    build_orders_shell,
    build_payments,
    build_refunds,
    finalize_orders,
)
from qc_lakehouse.generator.schemas import (
    MATCH_ATTEMPTS_SCHEMA,
    ORDER_ITEMS_SCHEMA,
    ORDERS_SCHEMA,
    PAYMENTS_SCHEMA,
    REFUNDS_SCHEMA,
)


def check_order_volume_matches_demand(order_buckets, demand_hourly) -> None:
    """Every (day_index, hour) bucket's order count must exactly match demand_hourly - the
    source of truth for volume, not a target to approximate. A total-only comparison would
    miss orders landing in the wrong bucket while the grand total still happens to agree,
    so this checks every bucket individually. `order_buckets` is one (day_index, hour)
    tuple per generated order (from orders_shell_df, collected before finalize_orders
    drops those columns); `demand_hourly` rows are (day_index, order_date, hour, orders)."""
    actual: dict[tuple[int, int], int] = {}
    for day_index, hour in order_buckets:
        key = (day_index, hour)
        actual[key] = actual.get(key, 0) + 1

    bad = []
    for row in demand_hourly:
        key = (row[0], row[2])
        expected = row[3]
        if actual.get(key, 0) != expected:
            bad.append((key, expected, actual.get(key, 0)))
    assert not bad, f"order volume mismatch by (day_index, hour) - (bucket, expected, actual): {bad[:5]}"


def check_fact_referential_integrity(orders, order_items, match_attempts, payments, refunds) -> None:
    """Every fact row must reference a real order. Runs against collected rows before
    anything is written."""
    order_ids = {r["order_id"] for r in orders}

    orphan_items = [r["order_item_id"] for r in order_items if r["order_id"] not in order_ids]
    assert not orphan_items, f"order_item -> order orphans: {orphan_items[:5]}"

    orphan_matches = [r["match_id"] for r in match_attempts if r["order_id"] not in order_ids]
    assert not orphan_matches, f"match_attempt -> order orphans: {orphan_matches[:5]}"

    orphan_payments = [r["payment_id"] for r in payments if r["order_id"] not in order_ids]
    assert not orphan_payments, f"payment -> order orphans: {orphan_payments[:5]}"

    orphan_refunds = [r["refund_id"] for r in refunds if r["order_id"] not in order_ids]
    assert not orphan_refunds, f"refund -> order orphans: {orphan_refunds[:5]}"


def check_exactly_one_accepted_match_per_matched_order(orders, match_attempts) -> None:
    """Every DELIVERED/CANCELLED order needs exactly one ACCEPTED attempt; every
    UNFULFILLED order needs zero."""
    accepted_by_order: dict[int, int] = {}
    for r in match_attempts:
        if r["response"] == "ACCEPTED":
            accepted_by_order[r["order_id"]] = accepted_by_order.get(r["order_id"], 0) + 1

    bad = []
    for order in orders:
        count = accepted_by_order.get(order["order_id"], 0)
        expected = 0 if order["order_status"] == "UNFULFILLED" else 1
        if count != expected:
            bad.append((order["order_id"], order["order_status"], count))
    assert not bad, f"orders with the wrong ACCEPTED count: {bad[:5]}"


def check_subtotal_matches_line_items(orders, order_items) -> None:
    """orders.subtotal must equal the sum of that order's order_items.line_total."""
    totals: dict[int, object] = {}
    for r in order_items:
        totals[r["order_id"]] = totals.get(r["order_id"], 0) + r["line_total"]

    bad = [o["order_id"] for o in orders if totals.get(o["order_id"]) != o["subtotal"]]
    assert not bad, f"orders whose subtotal disagrees with their line items: {bad[:5]}"


def check_payment_amount_matches_order_total(orders, payments) -> None:
    """Every SUCCESS payment's amount must equal its order's order_total."""
    order_total = {o["order_id"]: o["order_total"] for o in orders}
    bad = [
        p["order_id"] for p in payments
        if p["status"] == "SUCCESS" and p["amount"] != order_total.get(p["order_id"])
    ]
    assert not bad, f"SUCCESS payments whose amount disagrees with order_total: {bad[:5]}"


def _write(spark, config: GeneratorConfig, name: str, df, schema) -> int:
    """Overwrite one Delta table from a DataFrame already matching `schema`. Idempotent:
    full-table regeneration from a fixed seed, not an incremental append."""
    table = f"{config.catalog}.{config.schema}.{name}"
    (spark.createDataFrame(df.rdd, schema)
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table))
    return spark.table(table).count()


def write_fact_tables(spark, config: GeneratorConfig) -> dict[str, int]:
    catalog, schema = config.catalog, config.schema

    demand_hourly_df = spark.table(f"{catalog}.{schema}.demand_hourly")
    restaurant_profile_df = spark.table(f"{catalog}.{schema}._gen_restaurant_profile")
    customer_profile_df = spark.table(f"{catalog}.{schema}._gen_customer_profile")
    customers_df = spark.table(f"{catalog}.{schema}.customers")
    zones_df = spark.table(f"{catalog}.{schema}.zones")
    restaurants_df = spark.table(f"{catalog}.{schema}.restaurants")
    riders_df = spark.table(f"{catalog}.{schema}.riders")
    menu_items_df = spark.table(f"{catalog}.{schema}.menu_items")

    demand_hourly_rows = demand_hourly_df.collect()

    orders_shell_df = build_orders_shell(
        spark, config, demand_hourly_df, restaurant_profile_df, customer_profile_df,
        customers_df, zones_df, restaurants_df,
    )
    # Checked against the SHELL (which still carries day_index/hour) rather than the
    # final orders_df, and before finalize_orders/order_items even run - a volume defect
    # is a defect in build_orders_shell specifically, and this fails fast on it.
    shell_buckets = [
        (r["day_index"], r["hour"]) for r in orders_shell_df.select("day_index", "hour").collect()
    ]
    check_order_volume_matches_demand(shell_buckets, demand_hourly_rows)

    order_items_df = build_order_items(config, orders_shell_df, menu_items_df)
    orders_df = finalize_orders(config, orders_shell_df, order_items_df)
    match_attempts_df = build_match_attempts(config, orders_df, riders_df)
    payments_df = build_payments(config, orders_df)
    refunds_df = build_refunds(config, orders_df, payments_df)

    # Collect once each, before any write - these are the same collected lists every
    # remaining check function below consumes.
    orders_rows = orders_df.collect()
    order_items_rows = order_items_df.collect()
    match_attempts_rows = match_attempts_df.collect()
    payments_rows = payments_df.collect()
    refunds_rows = refunds_df.collect()

    check_fact_referential_integrity(orders_rows, order_items_rows, match_attempts_rows, payments_rows, refunds_rows)
    check_exactly_one_accepted_match_per_matched_order(orders_rows, match_attempts_rows)
    check_subtotal_matches_line_items(orders_rows, order_items_rows)
    check_payment_amount_matches_order_total(orders_rows, payments_rows)

    counts: dict[str, int] = {}
    counts["orders"] = _write(spark, config, "orders", orders_df, ORDERS_SCHEMA)
    counts["order_items"] = _write(spark, config, "order_items", order_items_df, ORDER_ITEMS_SCHEMA)
    counts["match_attempts"] = _write(spark, config, "match_attempts", match_attempts_df, MATCH_ATTEMPTS_SCHEMA)
    counts["payments"] = _write(spark, config, "payments", payments_df, PAYMENTS_SCHEMA)
    counts["refunds"] = _write(spark, config, "refunds", refunds_df, REFUNDS_SCHEMA)

    return counts
