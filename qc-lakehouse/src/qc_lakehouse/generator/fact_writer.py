# qc-lakehouse/src/qc_lakehouse/generator/fact_writer.py
from __future__ import annotations

from pyspark.sql import functions as F

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

# Every check below runs as a Spark aggregation/join and collects only the (small, ideally
# empty) violating rows to the driver - never the full fact tables. At the default config's
# volume (~1.35M orders), collecting orders/order_items/etc. in full would pull multi-million
# row DataFrames onto the driver on every generation run; these checks stay Spark-native so
# the cost is a distributed job instead.
_SAMPLE_LIMIT = 6


def check_order_volume_matches_demand(orders_shell_df, demand_hourly_df) -> None:
    """Every (day_index, hour) bucket's order count must exactly match demand_hourly - the
    source of truth for volume, not a target to approximate. A total-only comparison would
    miss orders landing in the wrong bucket while the grand total still happens to agree,
    so this checks every bucket individually."""
    actual = orders_shell_df.groupBy("day_index", "hour").count()
    expected = demand_hourly_df.select("day_index", "hour", F.col("orders").alias("expected"))
    mismatches = (
        expected.join(actual, ["day_index", "hour"], "full_outer")
        .select(
            "day_index", "hour",
            F.coalesce(F.col("expected"), F.lit(0)).alias("expected"),
            F.coalesce(F.col("count"), F.lit(0)).alias("actual"),
        )
        .filter(F.col("expected") != F.col("actual"))
        .limit(_SAMPLE_LIMIT)
        .collect()
    )
    bad = [((r["day_index"], r["hour"]), r["expected"], r["actual"]) for r in mismatches]
    assert not bad, f"order volume mismatch by (day_index, hour) - (bucket, expected, actual): {bad[:5]}"


def check_fact_referential_integrity(orders_df, order_items_df, match_attempts_df, payments_df, refunds_df) -> None:
    """Every fact row must reference a real order, and every refund must also reference a
    real payment. Each check is a left-anti join, so only orphaned rows (none, in a
    healthy run) ever reach the driver."""
    order_ids = orders_df.select("order_id")
    payment_ids = payments_df.select("payment_id")

    def _orphans(df, id_col, ref_col="order_id", ref_ids=order_ids):
        return [
            r[id_col] for r in
            df.select(id_col, ref_col).join(ref_ids, ref_col, "left_anti").limit(_SAMPLE_LIMIT).collect()
        ]

    orphan_items = _orphans(order_items_df, "order_item_id")
    assert not orphan_items, f"order_item -> order orphans: {orphan_items[:5]}"

    orphan_matches = _orphans(match_attempts_df, "match_id")
    assert not orphan_matches, f"match_attempt -> order orphans: {orphan_matches[:5]}"

    orphan_payments = _orphans(payments_df, "payment_id")
    assert not orphan_payments, f"payment -> order orphans: {orphan_payments[:5]}"

    orphan_refunds = _orphans(refunds_df, "refund_id")
    assert not orphan_refunds, f"refund -> order orphans: {orphan_refunds[:5]}"

    orphan_refund_payments = _orphans(refunds_df, "refund_id", ref_col="payment_id", ref_ids=payment_ids)
    assert not orphan_refund_payments, f"refund -> payment orphans: {orphan_refund_payments[:5]}"


def check_exactly_one_accepted_match_per_matched_order(orders_df, match_attempts_df) -> None:
    """Every DELIVERED/CANCELLED order needs exactly one ACCEPTED attempt; every
    UNFULFILLED order needs zero."""
    accepted_counts = (
        match_attempts_df.filter(F.col("response") == "ACCEPTED")
        .groupBy("order_id").count()
    )
    mismatches = (
        orders_df.select("order_id", "order_status")
        .join(accepted_counts, "order_id", "left")
        .select(
            "order_id", "order_status",
            F.coalesce(F.col("count"), F.lit(0)).alias("accepted_count"),
            F.when(F.col("order_status") == "UNFULFILLED", 0).otherwise(1).alias("expected"),
        )
        .filter(F.col("accepted_count") != F.col("expected"))
        .limit(_SAMPLE_LIMIT)
        .collect()
    )
    bad = [(r["order_id"], r["order_status"], r["accepted_count"]) for r in mismatches]
    assert not bad, f"orders with the wrong ACCEPTED count: {bad[:5]}"


def check_subtotal_matches_line_items(orders_df, order_items_df) -> None:
    """orders.subtotal must equal the sum of that order's order_items.line_total."""
    computed = order_items_df.groupBy("order_id").agg(F.sum("line_total").alias("computed_subtotal"))
    mismatches = (
        orders_df.select("order_id", "subtotal")
        .join(computed, "order_id", "left")
        .filter(~F.col("subtotal").eqNullSafe(F.col("computed_subtotal")))
        .select("order_id")
        .limit(_SAMPLE_LIMIT)
        .collect()
    )
    bad = [r["order_id"] for r in mismatches]
    assert not bad, f"orders whose subtotal disagrees with their line items: {bad[:5]}"


def check_payment_amount_matches_order_total(orders_df, payments_df) -> None:
    """Every SUCCESS payment's amount must equal its order's order_total."""
    order_totals = orders_df.select("order_id", "order_total")
    mismatches = (
        payments_df.filter(F.col("status") == "SUCCESS")
        .join(order_totals, "order_id", "left")
        .filter(~F.col("amount").eqNullSafe(F.col("order_total")))
        .select("order_id")
        .limit(_SAMPLE_LIMIT)
        .collect()
    )
    bad = [r["order_id"] for r in mismatches]
    assert not bad, f"SUCCESS payments whose amount disagrees with order_total: {bad[:5]}"


def _write(spark, config: GeneratorConfig, name: str, df, schema) -> int:
    """Overwrite one Delta table from a DataFrame already matching `schema`. Idempotent:
    full-table regeneration from a fixed seed, not an incremental append.

    Writes `df` directly - it already matches `schema` by construction (every builder's own
    .select() call produces exactly the target columns). The old `spark.createDataFrame(df.rdd,
    schema)` round-trip was a no-op re-materialization that doesn't even work on Databricks
    serverless/Standard-access-mode compute (`DataFrame.rdd` raises RDD_NOT_SUPPORTED there -
    this is what broke the live run). The assertion below is a cheap safety net for the same
    guarantee that round-trip was reaching for, without needing the RDD API to get it - it
    checks both column names and types, so a type regression (e.g. a decimal precision widen)
    fails loudly here instead of landing silently in the Delta table."""
    actual = [(f.name, f.dataType) for f in df.schema.fields]
    expected = [(f.name, f.dataType) for f in schema.fields]
    assert actual == expected, (
        f"{name}: DataFrame columns {actual} do not match expected schema columns {expected}"
    )
    table = f"{config.catalog}.{config.schema}.{name}"
    (df.write.mode("overwrite")
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

    orders_shell_df = build_orders_shell(
        spark, config, demand_hourly_df, restaurant_profile_df, customer_profile_df,
        customers_df, zones_df, restaurants_df,
    )
    # Checked against the SHELL (which still carries day_index/hour) rather than the
    # final orders_df, and before finalize_orders/order_items even run - a volume defect
    # is a defect in build_orders_shell specifically, and this fails fast on it.
    check_order_volume_matches_demand(orders_shell_df, demand_hourly_df)

    order_items_df = build_order_items(config, orders_shell_df, menu_items_df)
    orders_df = finalize_orders(config, orders_shell_df, order_items_df)
    match_attempts_df = build_match_attempts(config, orders_df, riders_df)
    payments_df = build_payments(config, orders_df)
    refunds_df = build_refunds(config, orders_df, payments_df)

    # Every check below is a Spark-native join/aggregation - no full-table collect of the
    # (potentially multi-million-row) fact DataFrames.
    check_fact_referential_integrity(orders_df, order_items_df, match_attempts_df, payments_df, refunds_df)
    check_exactly_one_accepted_match_per_matched_order(orders_df, match_attempts_df)
    check_subtotal_matches_line_items(orders_df, order_items_df)
    check_payment_amount_matches_order_total(orders_df, payments_df)

    counts: dict[str, int] = {}
    counts["orders"] = _write(spark, config, "orders", orders_df, ORDERS_SCHEMA)
    counts["order_items"] = _write(spark, config, "order_items", order_items_df, ORDER_ITEMS_SCHEMA)
    counts["match_attempts"] = _write(spark, config, "match_attempts", match_attempts_df, MATCH_ATTEMPTS_SCHEMA)
    counts["payments"] = _write(spark, config, "payments", payments_df, PAYMENTS_SCHEMA)
    counts["refunds"] = _write(spark, config, "refunds", refunds_df, REFUNDS_SCHEMA)

    return counts
