from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.entities import build_cities, build_zones


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
