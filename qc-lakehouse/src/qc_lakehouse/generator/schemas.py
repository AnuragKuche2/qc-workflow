# qc-lakehouse/src/qc_lakehouse/generator/schemas.py
from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Explicit types, because inferring a schema from Python would turn every Decimal into a
# double and silently lose money precision.
TS, D2, D4, R5 = TimestampType(), DecimalType(18, 2), DecimalType(18, 4), DecimalType(5, 4)

CITIES_SCHEMA = StructType([
    StructField("city_id", LongType()), StructField("name", StringType()),
    StructField("country_code", StringType()), StructField("timezone", StringType()),
    StructField("lat", DoubleType()), StructField("lon", DoubleType()),
    StructField("created_at", TS), StructField("updated_at", TS),
])

ZONES_SCHEMA = StructType([
    StructField("zone_id", LongType()), StructField("city_id", LongType()),
    StructField("name", StringType()), StructField("center_lat", DoubleType()),
    StructField("center_lon", DoubleType()), StructField("ring", IntegerType()),
    StructField("base_delivery_fee", D2), StructField("sla_target_minutes", IntegerType()),
    StructField("is_active", BooleanType()),
    StructField("created_at", TS), StructField("updated_at", TS),
])

# The OLTP source does not know how popular a restaurant is - that has to be discovered
# from order volume. This 14-field schema (no popularity_weight, no price_index) is what
# the source-visible `restaurants` table gets; those two columns go to
# _gen_restaurant_profile instead, in a separate table so it's obvious the pipeline must
# never join to it.
RESTAURANTS_SCHEMA = StructType([
    StructField("restaurant_id", LongType()), StructField("restaurant_ref", StringType()),
    StructField("name", StringType()), StructField("cuisine_type", StringType()),
    StructField("city_id", LongType()), StructField("zone_id", LongType()),
    StructField("lat", DoubleType()), StructField("lon", DoubleType()),
    StructField("commission_pct", R5), StructField("payout_account_id", StringType()),
    StructField("prep_time_p50_minutes", IntegerType()), StructField("is_active", BooleanType()),
    StructField("created_at", TS), StructField("updated_at", TS),
])

GEN_RESTAURANT_PROFILE_SCHEMA = StructType([
    StructField("restaurant_id", LongType()),
    StructField("popularity_weight", DoubleType()),
    StructField("price_index", DoubleType()),
])

RIDERS_SCHEMA = StructType([
    StructField("rider_id", LongType()), StructField("rider_ref", StringType()),
    StructField("vehicle_type", StringType()), StructField("payout_tier", StringType()),
    StructField("home_zone_id", LongType()), StructField("payout_account_id", StringType()),
    StructField("is_active", BooleanType()),
    StructField("created_at", TS), StructField("updated_at", TS),
])

MENU_ITEMS_SCHEMA = StructType([
    StructField("menu_item_id", LongType()), StructField("restaurant_id", LongType()),
    StructField("name", StringType()), StructField("category", StringType()),
    StructField("price", D2), StructField("is_available", BooleanType()),
    StructField("created_at", TS), StructField("updated_at", TS),
])

# DECIMAL(18,4) on the rates, (18,2) on the fare: rates get multiplied by distance and
# time, so they need extra places before the result is quantized.
RIDER_PAYOUT_TIERS_SCHEMA = StructType([
    StructField("tier", StringType()), StructField("valid_from", TS),
    StructField("valid_to", TS), StructField("base_fare", D2),
    StructField("per_km_rate", D4), StructField("per_minute_rate", D4),
])

DEMAND_DAILY_SCHEMA = StructType([
    StructField("day_index", IntegerType()), StructField("order_date", DateType()),
    StructField("weekday", IntegerType()), StructField("dow_multiplier", DoubleType()),
    StructField("event_multiplier", DoubleType()), StructField("noise", DoubleType()),
    StructField("total_multiplier", DoubleType()), StructField("orders", IntegerType()),
    StructField("event_names", StringType()),
])

DEMAND_HOURLY_SCHEMA = StructType([
    StructField("day_index", IntegerType()), StructField("order_date", DateType()),
    StructField("hour", IntegerType()), StructField("orders", IntegerType()),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", LongType()), StructField("order_ref", StringType()),
    StructField("customer_id", LongType()), StructField("restaurant_id", LongType()),
    StructField("zone_id", LongType()), StructField("placed_at", TS),
    StructField("order_status", StringType()), StructField("subtotal", D2),
    StructField("delivery_fee", D2), StructField("commission_pct", R5),
    StructField("commission_amount", D2), StructField("order_total", D2),
    StructField("delivery_notes", StringType()),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id", LongType()), StructField("order_id", LongType()),
    StructField("menu_item_id", LongType()), StructField("quantity", IntegerType()),
    StructField("unit_price", D2), StructField("line_total", D2),
])

MATCH_ATTEMPTS_SCHEMA = StructType([
    StructField("match_id", LongType()), StructField("order_id", LongType()),
    StructField("rider_id", LongType()), StructField("attempt_number", IntegerType()),
    StructField("offered_at", TS), StructField("response", StringType()),
    StructField("responded_at", TS),
])

PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", LongType()), StructField("order_id", LongType()),
    StructField("amount", D2), StructField("method", StringType()),
    StructField("status", StringType()), StructField("paid_at", TS),
])

REFUNDS_SCHEMA = StructType([
    StructField("refund_id", LongType()), StructField("order_id", LongType()),
    StructField("payment_id", LongType()), StructField("refund_amount_raw", StringType()),
    StructField("reason", StringType()), StructField("refunded_at", TS),
])
