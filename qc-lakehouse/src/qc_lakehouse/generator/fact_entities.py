# qc-lakehouse/src/qc_lakehouse/generator/fact_entities.py
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from qc_lakehouse.generator.config import GeneratorConfig


def _h(col, salt: int):
    """Reproducible uniform [0,1) draw from a column plus a salt - same technique
    customers.py uses. Duplicated rather than imported to keep this module independently
    readable; both files are small."""
    return F.pmod(F.hash(col, F.lit(salt)), F.lit(1_000_000)) / 1_000_000.0


def _bucket(df, cum_df, draw_col: str):
    """Assigns each row of `df` to the smallest bucket in `cum_df` (a `cum` column of
    ascending cumulative thresholds) whose threshold clears `draw_col` - the same
    inverse-CDF pattern customers.py uses for zone assignment, generalized to any
    cumulative-weight table."""
    joined = df.join(F.broadcast(cum_df), df[draw_col] <= cum_df["cum"], "left")
    w = Window.partitionBy("order_id").orderBy("cum")
    return joined.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn", "cum", draw_col)


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

    # Restaurant popularity and customer order-propensity: same cumulative-threshold
    # broadcast-join bucketing customers.py uses for zone assignment. Both reference
    # tables are small enough (thousands / low hundred-thousands of rows) to collect and
    # prefix-sum driver-side in well under a second.
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
    def _cumulative(rows, id_field, weight_field, out_schema):
        rows = sorted(rows, key=lambda r: r[id_field])
        total = sum(row[weight_field] for row in rows)
        cum, out = 0.0, []
        for row in rows:
            cum += row[weight_field] / total
            out.append((row[id_field], cum))
        return spark.createDataFrame(out, out_schema)

    restaurant_cum_df = _cumulative(
        restaurant_profile_df.select("restaurant_id", "popularity_weight").collect(),
        "restaurant_id", "popularity_weight", "restaurant_id long, cum double",
    )
    customer_cum_df = _cumulative(
        customer_profile_df.select("customer_id", "order_propensity").collect(),
        "customer_id", "order_propensity", "customer_id long, cum double",
    )

    orders = exploded.withColumn("u_restaurant", _h(F.col("order_id"), seed + 101))
    orders = _bucket(orders, restaurant_cum_df.withColumnRenamed("restaurant_id", "restaurant_id"), "u_restaurant")
    orders = orders.withColumn("u_customer", _h(F.col("order_id"), seed + 102))
    orders = _bucket(orders, customer_cum_df.withColumnRenamed("customer_id", "customer_id"), "u_customer")

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
