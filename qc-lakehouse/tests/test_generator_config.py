from qc_lakehouse.generator.config import (
    BRAND_CORE,
    BRAND_PREFIX,
    CITIES,
    CUISINES,
    EVENTS,
    MENUS,
    TIER_MIX,
    TIER_RATES,
    VEHICLE_MIX,
    ZONE_NAMES,
    GeneratorConfig,
)


def test_generator_config_defaults_match_the_source_notebook():
    config = GeneratorConfig()
    assert config.catalog == "qc_dev"
    assert config.schema == "bronze_source"
    assert config.days == 90
    assert config.orders_per_day == 15_000
    assert config.seed == 20260908
    assert config.start_date == "2026-06-01"
    assert config.n_cities == 3
    assert config.zones_per_city == 15
    assert config.n_restaurants == 1_000
    assert config.n_riders == 3_000
    assert config.n_customers == 300_000
    assert config.menu_items_per_restaurant == 25


def test_every_city_has_a_full_set_of_zone_names():
    for name, *_ in CITIES:
        assert name in ZONE_NAMES
        assert len(ZONE_NAMES[name]) == GeneratorConfig().zones_per_city


def test_cuisine_shares_sum_to_one():
    assert abs(sum(c[1] for c in CUISINES) - 1.0) < 1e-9


def test_every_cuisine_has_brand_names_and_a_menu():
    cuisine_names = {c[0] for c in CUISINES}
    assert cuisine_names == set(BRAND_PREFIX.keys())
    assert cuisine_names == set(BRAND_CORE.keys())
    assert cuisine_names == set(MENUS.keys())
    for cuisine, templates in MENUS.items():
        assert len(templates) == 12, f"{cuisine} has {len(templates)} menu templates, expected 12"


def test_vehicle_and_tier_mixes_sum_to_one():
    assert abs(sum(share for _, share in VEHICLE_MIX) - 1.0) < 1e-9
    assert abs(sum(share for _, share in TIER_MIX) - 1.0) < 1e-9


def test_tier_rates_cover_every_tier_in_the_mix():
    tier_names = {t for t, _ in TIER_MIX}
    rate_names = {t[0] for t in TIER_RATES}
    assert tier_names == rate_names


def test_events_have_the_documented_eight_field_shape():
    for ev in EVENTS:
        name, position, peak, _hour_window, _hour_focus, _transit, _surge, _cancel = ev
        assert isinstance(name, str)
        assert 0.0 <= position <= 1.0
        assert peak > 0


def test_generator_config_fact_table_defaults():
    config = GeneratorConfig()
    assert config.cancel_rate == 0.06
    assert config.unfulfilled_rate == 0.025
    assert config.refund_rate == 0.028
    assert config.payment_failure_rate == 0.01
    assert config.money_text_defect_rate == 0.15
    assert config.text_noise_defect_rate == 0.08
