# qc-lakehouse/src/qc_lakehouse/generator/writer.py
from __future__ import annotations

from qc_lakehouse.databricks_session import ensure_schema_exists
from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.customers import build_customers
from qc_lakehouse.generator.entities import (
    build_cities,
    build_demand_curve,
    build_menu_items,
    build_payout_tiers,
    build_restaurants,
    build_riders,
    build_zones,
)
from qc_lakehouse.generator.schemas import (
    CITIES_SCHEMA,
    DEMAND_DAILY_SCHEMA,
    DEMAND_HOURLY_SCHEMA,
    GEN_RESTAURANT_PROFILE_SCHEMA,
    MENU_ITEMS_SCHEMA,
    RESTAURANTS_SCHEMA,
    RIDER_PAYOUT_TIERS_SCHEMA,
    RIDERS_SCHEMA,
    ZONES_SCHEMA,
)


def check_referential_integrity(zones, restaurants, riders, menu_items) -> None:
    """Every row must reference a real parent. Runs against in-memory Python objects
    before anything is written, so a broken world never reaches Delta."""
    zone_ids = {z[0] for z in zones}
    restaurant_ids = {r[0] for r in restaurants}

    orphan_restaurants = [r[0] for r in restaurants if r[5] not in zone_ids]
    assert not orphan_restaurants, f"restaurant -> zone orphans: {orphan_restaurants[:5]}"

    orphan_riders = [r[0] for r in riders if r[4] not in zone_ids]
    assert not orphan_riders, f"rider -> zone orphans: {orphan_riders[:5]}"

    orphan_menu_items = [m[0] for m in menu_items if m[1] not in restaurant_ids]
    assert not orphan_menu_items, f"menu_item -> restaurant orphans: {orphan_menu_items[:5]}"


def check_demand_conservation(daily, hourly) -> None:
    """The 24 hourly counts must reconstruct the day exactly. If this fails, the
    allocator is wrong and the same bug would be losing cents in the payouts."""
    by_day: dict[int, int] = {}
    for day_index, _date, _hour, orders in hourly:
        by_day[day_index] = by_day.get(day_index, 0) + orders
    bad = [d[0] for d in daily if by_day.get(d[0], 0) != d[7]]
    assert not bad, f"hour/day conservation broken on days: {bad}"


def check_rider_capacity(riders, daily, max_deliveries_per_rider_per_day: int = 12) -> None:
    """At ~35 min per delivery plus a return leg, one rider sustains roughly 10-12
    deliveries in a shift. Anything past that means the fleet is physically incapable of
    the day and delivery times downstream would be fiction."""
    active = sum(1 for r in riders if r[6])
    peak = max(d[7] for d in daily)
    peak_per_rider = peak / active
    assert peak_per_rider <= max_deliveries_per_rider_per_day, (
        f"rider capacity exceeded: peak day needs {peak_per_rider:.1f} deliveries per "
        f"active rider, ceiling is {max_deliveries_per_rider_per_day}"
    )


def _write(spark, config: GeneratorConfig, name: str, rows, schema) -> int:
    """Overwrite one Delta table. Idempotent: this is a full-table regeneration from a
    fixed seed, not an incremental append, so rerunning is safe."""
    table = f"{config.catalog}.{config.schema}.{name}"
    (spark.createDataFrame(rows, schema)
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table))
    return spark.table(table).count()


def write_reference_tables(spark, config: GeneratorConfig) -> dict[str, int]:
    ensure_schema_exists(spark, config.catalog, config.schema)

    cities = build_cities(config)
    zones = build_zones(config, cities)
    restaurants = build_restaurants(config, cities, zones)
    riders = build_riders(config, cities, zones)
    menu_items = build_menu_items(config, restaurants)
    payout_tiers = build_payout_tiers(config)
    daily, hourly = build_demand_curve(config)

    # Fail before writing anything, not after.
    check_referential_integrity(zones, restaurants, riders, menu_items)
    check_demand_conservation(daily, hourly)
    check_rider_capacity(riders, daily)

    counts: dict[str, int] = {}
    counts["cities"] = _write(spark, config, "cities", cities, CITIES_SCHEMA)
    counts["zones"] = _write(spark, config, "zones", zones, ZONES_SCHEMA)
    counts["restaurants"] = _write(spark, config, "restaurants",
                                    [tuple(r[:14]) for r in restaurants], RESTAURANTS_SCHEMA)
    counts["riders"] = _write(spark, config, "riders", riders, RIDERS_SCHEMA)
    counts["menu_items"] = _write(spark, config, "menu_items", menu_items, MENU_ITEMS_SCHEMA)
    counts["rider_payout_tiers"] = _write(spark, config, "rider_payout_tiers",
                                           payout_tiers, RIDER_PAYOUT_TIERS_SCHEMA)
    counts["_gen_restaurant_profile"] = _write(
        spark, config, "_gen_restaurant_profile",
        [(r[0], float(r[14]), float(r[15])) for r in restaurants],
        GEN_RESTAURANT_PROFILE_SCHEMA,
    )
    counts["demand_daily"] = _write(spark, config, "demand_daily", daily, DEMAND_DAILY_SCHEMA)
    counts["demand_hourly"] = _write(spark, config, "demand_hourly", hourly, DEMAND_HOURLY_SCHEMA)

    customers_df, gen_customer_profile_df = build_customers(spark, config, cities, zones)
    (customers_df.write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{config.catalog}.{config.schema}.customers"))
    (gen_customer_profile_df.write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{config.catalog}.{config.schema}._gen_customer_profile"))
    counts["customers"] = spark.table(f"{config.catalog}.{config.schema}.customers").count()
    counts["_gen_customer_profile"] = spark.table(
        f"{config.catalog}.{config.schema}._gen_customer_profile"
    ).count()

    return counts
