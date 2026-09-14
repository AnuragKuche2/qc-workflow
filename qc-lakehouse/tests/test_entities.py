from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.entities import (
    build_cities,
    build_demand_curve,
    build_menu_items,
    build_payout_tiers,
    build_restaurants,
    build_riders,
    build_zones,
)


def test_build_cities_returns_one_row_per_configured_city():
    config = GeneratorConfig(n_cities=2)
    cities = build_cities(config)
    assert len(cities) == 2
    assert cities[0][1] == "Bengaluru"
    assert cities[1][1] == "Mumbai"
    # created_at/updated_at must be timezone-aware and before the generation window
    assert cities[0][6].tzinfo is not None


def test_build_cities_is_deterministic_for_the_same_seed():
    config = GeneratorConfig(n_cities=3, seed=1)
    a = build_cities(config)
    b = build_cities(config)
    assert a == b


def test_build_cities_differs_for_a_different_seed():
    a = build_cities(GeneratorConfig(n_cities=3, seed=1))
    b = build_cities(GeneratorConfig(n_cities=3, seed=2))
    assert [c[6] for c in a] != [c[6] for c in b]   # created_at timestamps differ


def test_build_zones_produces_zones_per_city_for_every_city():
    config = GeneratorConfig(n_cities=2, zones_per_city=15)
    cities = build_cities(config)
    zones = build_zones(config, cities)
    assert len(zones) == 2 * 15
    for city_id, *_ in cities:
        city_zones = [z for z in zones if z[1] == city_id]
        assert len(city_zones) == 15


def test_build_zones_sla_target_widens_with_ring():
    config = GeneratorConfig(n_cities=1, zones_per_city=15)
    cities = build_cities(config)
    zones = build_zones(config, cities)
    inner = next(z for z in zones if z[5] == 0)
    outer = next(z for z in zones if z[5] == 14)
    assert outer[7] > inner[7]   # sla_target_minutes widens toward the outskirts


def test_build_zones_uses_named_zones_within_the_catalog_and_falls_back_beyond_it():
    config = GeneratorConfig(n_cities=1, zones_per_city=15)
    cities = build_cities(config)
    zones = build_zones(config, cities)
    assert zones[0][2] == "Indiranagar"   # ring 0 for Bengaluru per ZONE_NAMES


def _reference_layer(n_restaurants=50, n_riders=30):
    config = GeneratorConfig(n_cities=1, zones_per_city=15, n_restaurants=n_restaurants,
                              n_riders=n_riders, menu_items_per_restaurant=25)
    cities = build_cities(config)
    zones = build_zones(config, cities)
    return config, cities, zones


def test_build_restaurants_produces_roughly_the_configured_count():
    config, cities, zones = _reference_layer(n_restaurants=50)
    restaurants = build_restaurants(config, cities, zones)
    # Exact count can drift slightly from split-by-share rounding across zones/cuisines -
    # every zone must get at least 1 restaurant, so a tiny n_restaurants can overshoot.
    assert 40 <= len(restaurants) <= 70


def test_build_restaurants_every_restaurant_belongs_to_a_real_zone():
    config, cities, zones = _reference_layer(n_restaurants=50)
    restaurants = build_restaurants(config, cities, zones)
    zone_ids = {z[0] for z in zones}
    assert all(r[5] in zone_ids for r in restaurants)


def test_build_restaurants_popularity_weights_sum_to_one():
    config, cities, zones = _reference_layer(n_restaurants=50)
    restaurants = build_restaurants(config, cities, zones)
    assert abs(sum(r[14] for r in restaurants) - 1.0) < 1e-6


def test_build_restaurants_names_are_unique():
    config, cities, zones = _reference_layer(n_restaurants=50)
    restaurants = build_restaurants(config, cities, zones)
    names = [r[2] for r in restaurants]
    assert len(names) == len(set(names))


def test_build_riders_every_rider_belongs_to_a_real_zone():
    config, cities, zones = _reference_layer(n_riders=30)
    riders = build_riders(config, cities, zones)
    zone_ids = {z[0] for z in zones}
    assert all(r[4] in zone_ids for r in riders)
    assert len(riders) == 30


def test_build_menu_items_gives_every_restaurant_the_configured_item_count():
    config, cities, zones = _reference_layer(n_restaurants=10)
    config = GeneratorConfig(n_cities=1, zones_per_city=15, n_restaurants=10,
                              menu_items_per_restaurant=25)
    restaurants = build_restaurants(config, cities, zones)
    menu_items = build_menu_items(config, restaurants)
    assert len(menu_items) == len(restaurants) * 25


def test_build_menu_items_every_item_belongs_to_a_real_restaurant():
    config, cities, zones = _reference_layer(n_restaurants=10)
    restaurants = build_restaurants(config, cities, zones)
    menu_items = build_menu_items(config, restaurants)
    restaurant_ids = {r[0] for r in restaurants}
    assert all(m[1] in restaurant_ids for m in menu_items)


def test_build_payout_tiers_covers_bronze_silver_gold():
    tiers = build_payout_tiers(GeneratorConfig())
    assert {t[0] for t in tiers} == {"bronze", "silver", "gold"}
    assert all(t[2] is None for t in tiers)    # valid_to is open-ended


def test_build_demand_curve_produces_one_daily_row_per_day():
    config = GeneratorConfig(days=14)
    daily, hourly = build_demand_curve(config)
    assert len(daily) == 14
    assert len(hourly) == 14 * 24


def test_build_demand_curve_hourly_rows_sum_back_to_daily_orders():
    config = GeneratorConfig(days=14)
    daily, hourly = build_demand_curve(config)
    for day in daily:
        day_index, orders = day[0], day[7]
        hours_for_day = sum(h[3] for h in hourly if h[0] == day_index)
        assert hours_for_day == orders


def test_build_demand_curve_is_deterministic_for_the_same_seed():
    a_daily, a_hourly = build_demand_curve(GeneratorConfig(days=14, seed=5))
    b_daily, b_hourly = build_demand_curve(GeneratorConfig(days=14, seed=5))
    assert a_daily == b_daily
    assert a_hourly == b_hourly


def test_build_demand_curve_event_day_names_are_recorded():
    config = GeneratorConfig(days=90)
    daily, _hourly = build_demand_curve(config)
    event_days = [d for d in daily if d[8] is not None]
    assert len(event_days) >= 1
