# qc-lakehouse/tests/test_fact_entities.py
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
from qc_lakehouse.generator.fact_entities import build_orders_shell
from qc_lakehouse.generator.schemas import (
    DEMAND_HOURLY_SCHEMA,
    GEN_RESTAURANT_PROFILE_SCHEMA,
    MENU_ITEMS_SCHEMA,
    RESTAURANTS_SCHEMA,
    RIDERS_SCHEMA,
    ZONES_SCHEMA,
)
from qc_lakehouse.spark_local import build_local_spark_session


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


def test_build_orders_shell_produces_exactly_the_demand_hourly_total():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        expected_total = sum(r["orders"] for r in fx["demand_hourly_df"].collect())

        orders = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )

        assert orders.count() == expected_total
    finally:
        spark.stop()


def test_build_orders_shell_order_ids_are_unique_and_contiguous():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        orders = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        ids = sorted(r["order_id"] for r in orders.select("order_id").collect())
        assert ids == list(range(1, len(ids) + 1))
    finally:
        spark.stop()


def test_build_orders_shell_references_are_all_real():
    spark = build_local_spark_session()
    try:
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
    finally:
        spark.stop()


def test_build_orders_shell_is_deterministic_for_the_same_seed():
    spark = build_local_spark_session()
    try:
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
    finally:
        spark.stop()
