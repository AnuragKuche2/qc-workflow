# qc-lakehouse/tests/test_writer.py
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.writer import (
    check_demand_conservation,
    check_every_zone_has_an_active_rider,
    check_referential_integrity,
    check_rider_capacity,
    write_reference_tables,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _zone(zone_id, city_id=1):
    return (zone_id, city_id, f"Zone{zone_id}", 12.9, 77.5, 0, Decimal("19.00"), 38, True, NOW, NOW)


def _restaurant(rid, zone_id):
    return [rid, f"R-{rid:05d}", f"Restaurant{rid}", "North Indian", 1, zone_id, 12.9, 77.5,
            Decimal("0.18"), f"ACCT-R-{rid:06d}", 20, True, NOW, NOW, 0.1, 1.0]


def _rider(did, zone_id, active=True):
    return (did, f"D-{did:05d}", "bike", "bronze", zone_id, f"ACCT-D-{did:06d}", active, NOW, NOW)


def _menu_item(mid, restaurant_id):
    return (mid, restaurant_id, "Item", "Mains", Decimal("100.00"), True, NOW, NOW)


def test_check_referential_integrity_passes_on_a_consistent_world():
    zones = [_zone(1)]
    restaurants = [_restaurant(1, zone_id=1)]
    riders = [_rider(1, zone_id=1)]
    menu_items = [_menu_item(1, restaurant_id=1)]
    check_referential_integrity(zones, restaurants, riders, menu_items)  # must not raise


def test_check_referential_integrity_catches_an_orphaned_restaurant():
    zones = [_zone(1)]
    restaurants = [_restaurant(1, zone_id=99)]   # zone 99 doesn't exist
    riders = [_rider(1, zone_id=1)]
    menu_items = [_menu_item(1, restaurant_id=1)]
    with pytest.raises(AssertionError, match="restaurant"):
        check_referential_integrity(zones, restaurants, riders, menu_items)


def test_check_referential_integrity_catches_an_orphaned_menu_item():
    zones = [_zone(1)]
    restaurants = [_restaurant(1, zone_id=1)]
    riders = [_rider(1, zone_id=1)]
    menu_items = [_menu_item(1, restaurant_id=999)]   # restaurant 999 doesn't exist
    with pytest.raises(AssertionError, match="menu_item"):
        check_referential_integrity(zones, restaurants, riders, menu_items)


def test_check_demand_conservation_passes_when_hours_sum_to_days():
    daily = [(0, None, 0, 1.0, 1.0, 1.0, 1.0, 100, None)]
    hourly = [(0, None, h, 100 // 24 + (1 if h < 100 % 24 else 0)) for h in range(24)]
    check_demand_conservation(daily, hourly)  # must not raise


def test_check_demand_conservation_catches_a_broken_day():
    daily = [(0, None, 0, 1.0, 1.0, 1.0, 1.0, 100, None)]
    hourly = [(0, None, h, 1) for h in range(24)]   # sums to 24, not 100
    with pytest.raises(AssertionError, match="conservation"):
        check_demand_conservation(daily, hourly)


def test_check_rider_capacity_passes_within_the_threshold():
    riders = [_rider(i, zone_id=1) for i in range(1, 11)]     # 10 active riders
    daily = [(0, None, 0, 1.0, 1.0, 1.0, 1.0, 100, None)]      # peak 100 orders / 10 riders = 10
    check_rider_capacity(riders, daily, max_deliveries_per_rider_per_day=12)  # must not raise


def test_check_rider_capacity_catches_an_overloaded_fleet():
    riders = [_rider(1, zone_id=1)]                            # 1 active rider
    daily = [(0, None, 0, 1.0, 1.0, 1.0, 1.0, 100, None)]       # 100 orders / 1 rider = 100
    with pytest.raises(AssertionError, match="capacity"):
        check_rider_capacity(riders, daily, max_deliveries_per_rider_per_day=12)


def test_check_rider_capacity_ignores_churned_riders():
    riders = [_rider(1, zone_id=1, active=False)]   # the only rider is churned
    daily = [(0, None, 0, 1.0, 1.0, 1.0, 1.0, 0, None)]
    with pytest.raises(ZeroDivisionError):
        # No active riders at all is a real config error the caller must fix, not
        # something this check should mask - the exact exception isn't the interesting
        # part, only that it fails loudly rather than dividing silently by zero and
        # passing.
        check_rider_capacity(riders, daily, max_deliveries_per_rider_per_day=12)


def test_check_every_zone_has_an_active_rider_passes_when_all_zones_are_covered():
    zones = [_zone(1), _zone(2)]
    riders = [_rider(1, zone_id=1), _rider(2, zone_id=2)]
    check_every_zone_has_an_active_rider(zones, riders)  # must not raise


def test_check_every_zone_has_an_active_rider_catches_a_zone_with_no_riders_at_all():
    zones = [_zone(1), _zone(2)]
    riders = [_rider(1, zone_id=1)]   # zone 2 has no rider of any kind
    with pytest.raises(AssertionError, match="active rider"):
        check_every_zone_has_an_active_rider(zones, riders)


def test_check_every_zone_has_an_active_rider_catches_a_zone_with_only_churned_riders():
    zones = [_zone(1), _zone(2)]
    riders = [_rider(1, zone_id=1), _rider(2, zone_id=2, active=False)]   # zone 2's only rider is churned
    with pytest.raises(AssertionError, match="active rider"):
        check_every_zone_has_an_active_rider(zones, riders)


class _NoWriteSpark:
    """A fake Spark session that allows the DDL calls `write_reference_tables` makes
    before its checks run (`ensure_schema_exists`, `CREATE CATALOG IF NOT EXISTS`), but
    fails loudly if anything tries to actually write data. Used to prove the checks run
    before any write - not just before the code that intentionally calls saveAsTable."""

    def __init__(self):
        self.sql_calls: list[str] = []

    def sql(self, query: str):
        self.sql_calls.append(query)
        return self

    def createDataFrame(self, *args, **kwargs):
        raise RuntimeError("createDataFrame called - a write was attempted before checks passed")

    def table(self, *args, **kwargs):
        raise RuntimeError("table() called - a write was attempted before checks passed")


def test_write_reference_tables_runs_checks_before_any_write():
    # zones_per_city=15 with only 3 riders spreads them so thin (density is inner-heavy)
    # that most zones get zero riders, guaranteeing check_every_zone_has_an_active_rider
    # fails - deterministically, from a structural config choice, not a monkeypatch.
    config = GeneratorConfig(
        n_cities=1, zones_per_city=15, n_restaurants=10, n_riders=3,
        n_customers=10, days=7, menu_items_per_restaurant=2,
    )
    spark = _NoWriteSpark()

    with pytest.raises(AssertionError, match="active rider"):
        write_reference_tables(spark, config)

    # The only calls that reached the fake were schema/catalog DDL (spark.sql) - no
    # createDataFrame/table (and therefore no saveAsTable) was ever invoked.
    assert spark.sql_calls   # ensure_schema_exists + CREATE CATALOG did run
    assert any("CREATE CATALOG" in q for q in spark.sql_calls)
