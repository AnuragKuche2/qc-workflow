# qc-lakehouse/tests/test_customers.py
from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.customers import build_customers
from qc_lakehouse.generator.entities import build_cities, build_zones
from qc_lakehouse.spark_local import build_local_spark_session


def test_build_customers_produces_the_configured_row_count_with_valid_zones():
    spark = build_local_spark_session()
    try:
        config = GeneratorConfig(n_cities=1, zones_per_city=15, n_customers=500)
        cities = build_cities(config)
        zones = build_zones(config, cities)
        customers_df, profile_df = build_customers(spark, config, cities, zones)

        assert customers_df.count() == 500
        assert profile_df.count() == 500

        zone_ids = {z[0] for z in zones}
        home_zones = {row["home_zone_id"] for row in customers_df.select("home_zone_id").collect()}
        assert home_zones.issubset(zone_ids)

        emails = [row["email"] for row in customers_df.select("email").collect()]
        assert len(emails) == len(set(emails))    # every email must be unique

        expected_columns = {"customer_id", "customer_ref", "email", "phone", "full_name",
                             "city_id", "home_zone_id", "lat", "lon", "signup_ts",
                             "created_at", "updated_at"}
        assert set(customers_df.columns) == expected_columns
        assert set(profile_df.columns) == {"customer_id", "order_propensity"}
    finally:
        spark.stop()


def test_build_customers_is_deterministic_for_the_same_seed():
    spark = build_local_spark_session()
    try:
        config = GeneratorConfig(n_cities=1, zones_per_city=15, n_customers=200, seed=99)
        cities = build_cities(config)
        zones = build_zones(config, cities)
        a_df, _ = build_customers(spark, config, cities, zones)
        b_df, _ = build_customers(spark, config, cities, zones)
        a_rows = sorted(r["email"] for r in a_df.collect())
        b_rows = sorted(r["email"] for r in b_df.collect())
        assert a_rows == b_rows
    finally:
        spark.stop()
