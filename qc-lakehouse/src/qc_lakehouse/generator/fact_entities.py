# qc-lakehouse/src/qc_lakehouse/generator/fact_entities.py
from __future__ import annotations

import bisect

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType

from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.defects import format_refund_amount, mangle_text_casing_and_whitespace


def _h(col, salt: int):
    """Reproducible uniform [0,1) draw from a column plus a salt - same technique
    customers.py uses. Duplicated rather than imported to keep this module independently
    readable; both files are small."""
    return F.pmod(F.hash(col, F.lit(salt)), F.lit(1_000_000)) / 1_000_000.0


def _weighted_pick_udf(cum_pairs: list[tuple[int, float]]):
    """cum_pairs: (id, cumulative_threshold) tuples, already sorted ascending by threshold,
    last threshold ~1.0. Returns a Spark UDF mapping a uniform [0,1) draw to the id of the
    smallest threshold that clears it.

    This replaces an earlier broadcast-inequality-join approach (`draw_col <= cum`, picking
    the top-1 by a window function). That join has no equality predicate, so Spark plans it
    as a BroadcastNestedLoopJoin: O(rows_left * rows_right) pairwise evaluations. At
    production GeneratorConfig defaults that's ~1.35M orders x 300,000 customers for the
    customer-weighting step alone - on the order of 4x10^11 pairwise evaluations before the
    window-based top-1 filter even runs, a serious risk of timing out or failing outright on
    a live Databricks run. A driver-side binary search via bisect is O(log n) per row and
    needs no join at all - the cumulative table only has to exist once, broadcast as a
    Python closure captured in the UDF, not as a Spark DataFrame."""
    ids = [p[0] for p in cum_pairs]
    thresholds = [p[1] for p in cum_pairs]

    def pick(draw: float) -> int:
        idx = bisect.bisect_left(thresholds, draw)
        if idx >= len(ids):
            idx = len(ids) - 1
        return ids[idx]

    return F.udf(pick, LongType())


def build_orders_shell(spark, config: GeneratorConfig, demand_hourly_df, restaurant_profile_df,
                        customer_profile_df, customers_df, zones_df, restaurants_df):
    """Builds every orders.* column EXCEPT subtotal/commission_amount/order_total/order_ref,
    which depend on order_items and are filled in by finalize_orders. One row per order,
    with a deterministic, contiguous order_id matching demand_hourly's exact per-(day,hour)
    counts."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    seed = config.seed

    # Tiny (days * 24 hours, a few thousand rows at most) - safe to collect and compute
    # prefix-sum offsets driver-side, giving every order a contiguous order_id without a
    # global Spark window function over the (much larger) order-level data.
    hourly_rows = sorted(
        demand_hourly_df.select("day_index", "order_date", "hour", "orders").collect(),
        key=lambda r: (r["day_index"], r["hour"]),
    )
    offset, offset_rows = 0, []
    for row in hourly_rows:
        offset_rows.append((row["day_index"], row["hour"], offset))
        offset += row["orders"]
    offsets_df = spark.createDataFrame(offset_rows, "day_index int, hour int, order_offset long")

    # Only explode hours that actually have orders - F.sequence(0, orders - 1) with the
    # default step is -1 when orders == 0 (start 0 > stop -1), which would yield the
    # 2-element array [0, -1] instead of an empty one and silently manufacture two
    # bogus orders per empty hour.
    exploded = (
        demand_hourly_df
        .filter(F.col("orders") > 0)
        .join(F.broadcast(offsets_df), ["day_index", "hour"])
        .withColumn("seq", F.explode(F.sequence(F.lit(0), F.col("orders") - F.lit(1))))
        .withColumn("order_id", (F.col("order_offset") + F.col("seq") + F.lit(1)).cast("long"))
        .select("order_id", "day_index", "order_date", "hour")
    )

    # Restaurant popularity and customer order-propensity: cumulative-threshold inverse-CDF
    # bucketing, same idea customers.py uses for zone assignment, but resolved via a
    # driver-side binary search UDF (see _weighted_pick_udf) rather than a broadcast join -
    # a broadcast INEQUALITY join here would plan as a BroadcastNestedLoopJoin
    # (O(rows_left * rows_right), infeasible at production order-count x customer-count
    # scale). Both reference tables are small enough (thousands / low hundred-thousands of
    # rows) to collect and prefix-sum driver-side in well under a second.
    #
    # Weights are normalized by their own sum before accumulating, so the cumulative
    # thresholds always land in [0, 1] - the same range as the uniform draw in `_h`.
    # restaurant_profile_df's popularity_weight (zipf) already sums to ~1 across all
    # restaurants, so normalizing is a no-op there, but customer_profile_df's
    # order_propensity is an UNBOUNDED lognormal draw per customer (mean ~0.9, summing to
    # roughly n_customers across the table) - without normalizing, the cumulative sum blows
    # past 1 after just the first one or two customer_ids, and since every draw is < 1,
    # bucketing would always resolve to whichever of those first few ids has the smallest
    # cum that still clears the draw. Concretely: every order collapses onto customer_id 1.
    def _cumulative(rows, id_field, weight_field) -> list[tuple[int, float]]:
        rows = sorted(rows, key=lambda r: r[id_field])
        total = sum(row[weight_field] for row in rows)
        cum, out = 0.0, []
        for row in rows:
            cum += row[weight_field] / total
            out.append((row[id_field], cum))
        return out

    restaurant_cum = _cumulative(
        restaurant_profile_df.select("restaurant_id", "popularity_weight").collect(),
        "restaurant_id", "popularity_weight",
    )
    customer_cum = _cumulative(
        customer_profile_df.select("customer_id", "order_propensity").collect(),
        "customer_id", "order_propensity",
    )
    restaurant_pick_udf = _weighted_pick_udf(restaurant_cum)
    customer_pick_udf = _weighted_pick_udf(customer_cum)

    orders = (
        exploded
        .withColumn("u_restaurant", _h(F.col("order_id"), seed + 101))
        .withColumn("restaurant_id", restaurant_pick_udf(F.col("u_restaurant")))
        .withColumn("u_customer", _h(F.col("order_id"), seed + 102))
        .withColumn("customer_id", customer_pick_udf(F.col("u_customer")))
        .drop("u_restaurant", "u_customer")
    )

    orders = (
        orders
        .join(customers_df.select("customer_id", F.col("home_zone_id").alias("zone_id")), "customer_id")
        .join(zones_df.select("zone_id", F.col("base_delivery_fee").alias("delivery_fee")), "zone_id")
        .join(restaurants_df.select("restaurant_id", "commission_pct"), "restaurant_id")
    )

    u_status = _h(F.col("order_id"), seed + 103)
    orders = orders.withColumn(
        "order_status",
        F.when(u_status < F.lit(config.unfulfilled_rate), F.lit("UNFULFILLED"))
         .when(u_status < F.lit(config.unfulfilled_rate + config.cancel_rate), F.lit("CANCELLED"))
         .otherwise(F.lit("DELIVERED")),
    )

    orders = orders.withColumn(
        "placed_at",
        F.expr(
            f"timestampadd(SECOND, hour * 3600 + pmod(abs(hash(order_id, {seed + 104})), 3600), "
            "to_timestamp(order_date))"
        ),
    )

    notes_pool = [
        "Ring the bell twice", "Leave at the door", "Call on arrival",
        "No onions please", "Extra spicy", "Gate code 1234",
    ]
    orders = (
        orders
        .withColumn("has_note", _h(F.col("order_id"), seed + 105) < F.lit(0.25))
        .withColumn(
            "delivery_notes",
            F.when(
                F.col("has_note"),
                F.element_at(
                    F.array(*[F.lit(x) for x in notes_pool]),
                    (F.pmod(F.hash("order_id", F.lit(seed + 106)), F.lit(len(notes_pool))) + F.lit(1)).cast("int"),
                ),
            ),
        )
        .drop("has_note")
    )

    # day_index/hour are carried through (not part of the final `orders` table's schema -
    # finalize_orders' own .select drops them) so fact_writer.py's volume check can verify
    # per-(day,hour) counts against demand_hourly directly, independent of whether
    # placed_at was computed correctly - checking placed_at-derived buckets would make the
    # check partly circular with the code it's meant to verify.
    return orders.select(
        "order_id", "customer_id", "restaurant_id", "zone_id", "placed_at",
        "order_status", "delivery_fee", "commission_pct", "delivery_notes",
        "day_index", "hour",
    )


def build_order_items(config: GeneratorConfig, orders_shell_df, menu_items_df):
    """One or more line items per order, sampled from that order's own restaurant's menu.
    Item count is weighted toward 1-2 items (45%/30%/15%/10% for 1/2/3/4)."""
    seed = config.seed

    restaurant_menus = (
        menu_items_df
        .groupBy("restaurant_id")
        .agg(F.collect_list(F.struct("menu_item_id", "price")).alias("items"))
    )

    u_count = _h(F.col("order_id"), seed + 201)
    items_base = (
        orders_shell_df.select("order_id", "restaurant_id")
        .withColumn(
            "item_count",
            F.when(u_count < F.lit(0.45), F.lit(1))
             .when(u_count < F.lit(0.75), F.lit(2))
             .when(u_count < F.lit(0.90), F.lit(3))
             .otherwise(F.lit(4)),
        )
        .join(F.broadcast(restaurant_menus), "restaurant_id")
    )

    exploded = (
        items_base
        .withColumn("item_slot", F.explode(F.sequence(F.lit(0), F.col("item_count") - F.lit(1))))
        .withColumn(
            "item_idx",
            F.pmod(F.hash("order_id", "item_slot", F.lit(seed + 202)), F.size("items")),
        )
        .withColumn("picked", F.element_at(F.col("items"), F.col("item_idx") + F.lit(1)))
        .withColumn("menu_item_id", F.col("picked.menu_item_id"))
        .withColumn("unit_price", F.col("picked.price").cast("decimal(18,2)"))
        .withColumn(
            "quantity",
            (F.pmod(F.hash("order_id", "item_slot", F.lit(seed + 203)), F.lit(3)) + F.lit(1)).cast("int"),
        )
        .withColumn("line_total", (F.col("unit_price") * F.col("quantity")).cast("decimal(18,2)"))
    )

    w = Window.orderBy("order_id", "item_slot")
    exploded = exploded.withColumn("order_item_id", F.row_number().over(w).cast("long"))

    return exploded.select(
        "order_item_id", "order_id", "menu_item_id", "quantity", "unit_price", "line_total",
    )


def finalize_orders(config: GeneratorConfig, orders_shell_df, order_items_df):
    """Joins order_items' aggregated subtotal back onto the shell, computes
    commission_amount and order_total, and applies the text-noise defect to
    delivery_notes. This is the FINAL orders DataFrame - the shell alone is never
    written to Delta."""
    seed = config.seed

    # F.sum() on a decimal(18,2) column widens the inferred result type to decimal(28,2)
    # (extra headroom for the aggregate) - cast back down to match ORDERS_SCHEMA's
    # subtotal decimal(18,2), same as commission_amount/order_total below.
    subtotals = order_items_df.groupBy("order_id").agg(
        F.sum("line_total").cast("decimal(18,2)").alias("subtotal")
    )

    orders = (
        orders_shell_df
        .join(subtotals, "order_id")
        .withColumn("commission_amount", (F.col("subtotal") * F.col("commission_pct")).cast("decimal(18,2)"))
        .withColumn("order_total", (F.col("subtotal") + F.col("delivery_fee")).cast("decimal(18,2)"))
        .withColumn("order_ref", F.format_string("O-%07d", F.col("order_id")))
    )

    mangle_udf = F.udf(
        lambda text, mode: mangle_text_casing_and_whitespace(text, mode) if text is not None else None,
        StringType(),
    )
    u_defect = _h(F.col("order_id"), seed + 301)
    mode_idx = F.pmod(F.hash("order_id", F.lit(seed + 302)), F.lit(4))
    modes = F.array(*[F.lit(m) for m in ["UPPER", "LOWER", "LEADING_SPACE", "TRAILING_SPACE"]])
    orders = orders.withColumn(
        "delivery_notes",
        F.when(
            F.col("delivery_notes").isNotNull() & (u_defect < F.lit(config.text_noise_defect_rate)),
            mangle_udf(F.col("delivery_notes"), F.element_at(modes, mode_idx + F.lit(1))),
        ).otherwise(F.col("delivery_notes")),
    )

    return orders.select(
        "order_id", "order_ref", "customer_id", "restaurant_id", "zone_id", "placed_at",
        "order_status", "subtotal", "delivery_fee", "commission_pct", "commission_amount",
        "order_total", "delivery_notes",
    )


def build_match_attempts(config: GeneratorConfig, orders_df, riders_df):
    """1+ offer/response rows per order. Matched orders (DELIVERED/CANCELLED) get 1-3
    attempts with the LAST marked ACCEPTED; UNFULFILLED orders get 2-3 attempts, all
    DECLINED/TIMEOUT - the rider pool is restricted to riders whose home_zone_id matches
    the order's delivery zone."""
    seed = config.seed

    zone_riders = (
        riders_df.filter("is_active")
        .groupBy("home_zone_id")
        .agg(F.collect_list("rider_id").alias("rider_ids"))
    )

    u_count = _h(F.col("order_id"), seed + 401)
    base = (
        orders_df.select("order_id", "zone_id", "order_status", "placed_at")
        .withColumn(
            "attempt_count",
            F.when(
                F.col("order_status") == "UNFULFILLED",
                F.when(u_count < F.lit(0.5), F.lit(2)).otherwise(F.lit(3)),
            ).otherwise(
                F.when(u_count < F.lit(0.7), F.lit(1))
                 .when(u_count < F.lit(0.9), F.lit(2))
                 .otherwise(F.lit(3))
            ),
        )
        .join(F.broadcast(zone_riders), F.col("zone_id") == F.col("home_zone_id"))
    )

    exploded = (
        base
        .withColumn("attempt_number", F.explode(F.sequence(F.lit(1), F.col("attempt_count"))))
        .withColumn(
            "rider_idx",
            F.pmod(F.hash("order_id", "attempt_number", F.lit(seed + 402)), F.size("rider_ids")),
        )
        .withColumn("rider_id", F.element_at(F.col("rider_ids"), F.col("rider_idx") + F.lit(1)))
        .withColumn(
            "response",
            F.when(
                (F.col("order_status") != "UNFULFILLED") & (F.col("attempt_number") == F.col("attempt_count")),
                F.lit("ACCEPTED"),
            ).otherwise(
                F.when(
                    F.pmod(F.hash("order_id", "attempt_number", F.lit(seed + 403)), F.lit(2)) == 0,
                    F.lit("DECLINED"),
                ).otherwise(F.lit("TIMEOUT"))
            ),
        )
        .withColumn("offered_at", F.expr("timestampadd(MINUTE, (attempt_number - 1) * 2, placed_at)"))
        .withColumn(
            "responded_at",
            F.expr(
                f"timestampadd(SECOND, 15 + pmod(abs(hash(order_id, attempt_number, {seed + 404})), 60), offered_at)"
            ),
        )
    )

    w = Window.orderBy("order_id", "attempt_number")
    exploded = exploded.withColumn("match_id", F.row_number().over(w).cast("long"))

    return exploded.select(
        "match_id", "order_id", "rider_id", "attempt_number", "offered_at", "response", "responded_at",
    )


def build_payments(config: GeneratorConfig, orders_df):
    """One row per order that reached a payable state (DELIVERED or CANCELLED - both
    were charged; UNFULFILLED orders never matched, so were never charged)."""
    seed = config.seed

    payable = orders_df.filter("order_status != 'UNFULFILLED'")

    u_status = _h(F.col("order_id"), seed + 501)
    u_method = F.pmod(F.hash("order_id", F.lit(seed + 502)), F.lit(1000)) / 1000.0

    payments = (
        payable
        .withColumn(
            "status",
            F.when(u_status < F.lit(config.payment_failure_rate), F.lit("FAILED")).otherwise(F.lit("SUCCESS")),
        )
        .withColumn(
            "method",
            F.when(u_method < F.lit(0.45), F.lit("upi"))
             .when(u_method < F.lit(0.75), F.lit("card"))
             .when(u_method < F.lit(0.90), F.lit("wallet"))
             .otherwise(F.lit("cod")),
        )
        .withColumn(
            "paid_at",
            F.expr(f"timestampadd(SECOND, 5 + pmod(abs(hash(order_id, {seed + 503})), 30), placed_at)"),
        )
        .withColumnRenamed("order_total", "amount")
    )

    w = Window.orderBy("order_id")
    payments = payments.withColumn("payment_id", F.row_number().over(w).cast("long"))

    return payments.select("payment_id", "order_id", "amount", "method", "status", "paid_at")


def build_refunds(config: GeneratorConfig, orders_df, payments_df):
    """One row for every CANCELLED order (full refund) plus a refund_rate fraction of
    DELIVERED orders (a partial quality-issue credit, 30-100% of order_total)."""
    seed = config.seed

    cancelled = (
        orders_df.filter("order_status = 'CANCELLED'")
        .withColumn("refund_fraction", F.lit(1.0))
        .withColumn("reason", F.lit("CANCELLED"))
    )

    u_refund = _h(F.col("order_id"), seed + 601)
    delivered_refunded = (
        orders_df.filter("order_status = 'DELIVERED'")
        .withColumn("u_refund", u_refund)
        .filter(F.col("u_refund") < F.lit(config.refund_rate))
        .drop("u_refund")
        .withColumn("refund_fraction", F.lit(0.3) + _h(F.col("order_id"), seed + 602) * F.lit(0.7))
        .withColumn(
            "reason_bucket",
            F.pmod(F.hash("order_id", F.lit(seed + 603)), F.lit(3)),
        )
        .withColumn(
            "reason",
            F.when(F.col("reason_bucket") == 0, F.lit("QUALITY_ISSUE"))
             .when(F.col("reason_bucket") == 1, F.lit("LATE_DELIVERY"))
             .otherwise(F.lit("MISSING_ITEMS")),
        )
        .drop("reason_bucket")
    )

    candidates = cancelled.unionByName(delivered_refunded)

    joined = (
        candidates
        .join(payments_df.select("order_id", "payment_id"), "order_id")
        .withColumn("refund_amount", (F.col("order_total") * F.col("refund_fraction")).cast("decimal(18,2)"))
        .withColumn("use_accounting_format", _h(F.col("order_id"), seed + 604) < F.lit(config.money_text_defect_rate))
    )

    format_udf = F.udf(
        lambda amount, use_accounting: format_refund_amount(amount, bool(use_accounting)),
        StringType(),
    )
    joined = joined.withColumn(
        "refund_amount_raw", format_udf(F.col("refund_amount"), F.col("use_accounting_format")),
    )

    joined = joined.withColumn(
        "refunded_at",
        F.when(
            F.col("order_status") == "CANCELLED",
            F.expr(f"timestampadd(MINUTE, 5 + pmod(abs(hash(order_id, {seed + 605})), 30), placed_at)"),
        ).otherwise(
            F.expr(f"timestampadd(HOUR, 24 + pmod(abs(hash(order_id, {seed + 606})), 48), placed_at)")
        ),
    )

    w = Window.orderBy("order_id")
    joined = joined.withColumn("refund_id", F.row_number().over(w).cast("long"))

    return joined.select(
        "refund_id", "order_id", "payment_id", "refund_amount_raw", "reason", "refunded_at",
    )
