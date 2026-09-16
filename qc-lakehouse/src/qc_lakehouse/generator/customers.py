# qc-lakehouse/src/qc_lakehouse/generator/customers.py
from __future__ import annotations

import math

from pyspark.sql import Window
from pyspark.sql import functions as F

from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.math_utils import split_by_share

FIRST = ["Aarav", "Aditi", "Advait", "Ananya", "Arjun", "Bhavna", "Chirag", "Deepa",
         "Devansh", "Divya", "Farhan", "Gauri", "Harsh", "Ishaan", "Isha", "Jatin",
         "Kavya", "Kabir", "Lakshmi", "Manav", "Meera", "Neha", "Nikhil", "Ojas",
         "Pallavi", "Pranav", "Priya", "Rahul", "Rhea", "Rohit", "Sanjana", "Sameer",
         "Shreya", "Siddharth", "Tanvi", "Tarun", "Uma", "Varun", "Vidya", "Yash"]
LAST = ["Agarwal", "Bansal", "Bhat", "Chandra", "Chopra", "Desai", "Dutta", "Gupta",
        "Iyer", "Jain", "Joshi", "Kapoor", "Khanna", "Kulkarni", "Kumar", "Malhotra",
        "Mehta", "Menon", "Mishra", "Nair", "Pandey", "Patel", "Pillai", "Rao",
        "Reddy", "Saxena", "Shah", "Sharma", "Shetty", "Singh", "Sinha", "Verma"]
DOMAINS = ["gmail.com", "outlook.com", "yahoo.in", "hotmail.com", "proton.me"]

HISTORY_DAYS = 730


def _h(col, salt: int):
    """Reproducible uniform [0,1) from a column value plus a salt. F.rand() is
    non-deterministic under task retries and shuffles, so a rerun would produce
    different data. hash(id, salt) is a pure function of the row."""
    return F.pmod(F.hash(col, F.lit(salt)), F.lit(1_000_000)) / 1_000_000.0


def build_customers(spark, config: GeneratorConfig, cities: list[tuple], zones: list[tuple]):
    from datetime import UTC, date, datetime

    from qc_lakehouse.generator.config import CITIES
    from qc_lakehouse.generator.math_utils import zone_density

    # signup_ts below is built from a bare SQL timestamp literal (F.expr), which Spark
    # parses in the SESSION's configured timezone - unlike every other table's
    # timestamps, which come from tz-aware Python datetime objects and are correctly UTC
    # regardless of session config. Without pinning this, the same seed/config on a
    # differently-configured session produces a different signup_ts, which defeats the
    # seeded generator. Must be set before the F.expr below is evaluated (i.e. before any
    # action like .collect()/.count()), so it goes here, before the DataFrame chain.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    start = date.fromisoformat(config.start_date)
    epoch = datetime.combine(start, datetime.min.time(), tzinfo=UTC)
    seed = config.seed

    # Same inner-heavy density as restaurants, as CUMULATIVE thresholds so Spark can
    # bucket a uniform draw with a join instead of a per-row loop. Built `cities` rows
    # don't carry the "share of business" column (build_cities deliberately drops it,
    # matching the source notebook) - the static CITIES catalog does. Both are in the
    # same order (build_cities iterates CITIES[:config.n_cities] in order), so zipping
    # the built `cities` param against a share list derived from the static catalog
    # pairs them correctly without re-deriving city order from anything.
    zones_of_city = {c[0]: [z for z in zones if z[1] == c[0]] for c in cities}
    per_city_cust = split_by_share(config.n_customers, [c[5] for c in CITIES[: config.n_cities]])

    zone_probs, cum = [], 0.0
    for city, ccount in zip(cities, per_city_cust):
        czones = zones_of_city[city[0]]
        for zone, zcount in zip(czones, split_by_share(ccount, zone_density(len(czones)))):
            cum += zcount / config.n_customers
            zone_probs.append((zone[0], city[0], zone[3], zone[4], cum))

    zone_map = spark.createDataFrame(
        zone_probs, "zone_id long, city_id long, zlat double, zlon double, cum double"
    )

    base = (
        spark.range(1, config.n_customers + 1)
        .withColumnRenamed("id", "customer_id")
        .withColumn("u_zone", _h(F.col("customer_id"), seed + 1))
    )

    customers = (
        base
        # Broadcast the small zone map to every executor - no shuffle. The join matches
        # every zone whose cumulative threshold clears the draw; the smallest one is the
        # bucket the draw actually landed in.
        .join(F.broadcast(zone_map), base.u_zone <= zone_map.cum, "left")
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("customer_id").orderBy("cum")))
        .filter("rn = 1").drop("rn", "cum", "u_zone")
        # element_at is 1-based, hence the + 1.
        .withColumn("first_name", F.element_at(
            F.array(*[F.lit(x) for x in FIRST]),
            (F.pmod(F.hash("customer_id", F.lit(seed + 2)), F.lit(len(FIRST))) + 1).cast("int")))
        .withColumn("last_name", F.element_at(
            F.array(*[F.lit(x) for x in LAST]),
            (F.pmod(F.hash("customer_id", F.lit(seed + 3)), F.lit(len(LAST))) + 1).cast("int")))
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("customer_ref", F.format_string("C-%07d", F.col("customer_id")))
        # Raw PII, stored deliberately: pseudonymization at Silver has to be real work
        # with a real column mask. customer_id in the local part keeps every email
        # unique from a 40x32 name pool.
        .withColumn("email", F.concat(
            F.lower(F.col("first_name")), F.lit("."), F.lower(F.col("last_name")),
            F.col("customer_id").cast("string"), F.lit("@"),
            F.element_at(F.array(*[F.lit(x) for x in DOMAINS]),
                (F.pmod(F.hash("customer_id", F.lit(seed + 4)), F.lit(len(DOMAINS))) + 1).cast("int"))))
        # Indian mobile range: 10 digits starting 6-9.
        .withColumn("phone", F.concat(F.lit("+91"),
            (F.lit(6_000_000_000) + F.pmod(F.abs(F.hash("customer_id", F.lit(seed + 5))),
                                            F.lit(4_000_000_000))).cast("string")))
        # +/- 0.009 deg (~1 km) around the zone centre. Longitude divided by cos(lat) so
        # the spread is circular on the ground, not an ellipse.
        .withColumn("lat", F.col("zlat") + (_h(F.col("customer_id"), seed + 6) - 0.5) * 0.018)
        .withColumn("lon", F.col("zlon") + (_h(F.col("customer_id"), seed + 7) - 0.5) * 0.018
                    / F.cos(F.radians(F.col("zlat"))))
        .withColumn("signup_ts", F.expr(
            f"timestampadd(SECOND, "
            f"-(pmod(abs(hash(customer_id, {seed + 8})), {HISTORY_DAYS * 86_400}) + 1), "
            f"timestamp'{epoch.strftime('%Y-%m-%d %H:%M:%S')}')"))
        # Lognormal via Box-Muller: most people order occasionally and a few order
        # constantly. Gaussian would give everyone identical habits.
        .withColumn("order_propensity",
            F.exp(F.lit(0.75) * F.sqrt(F.lit(-2.0) * F.log(_h(F.col("customer_id"), seed + 9)
                  + F.lit(1e-9))) * F.cos(F.lit(2 * math.pi) * _h(F.col("customer_id"), seed + 10))))
        .select("customer_id", "customer_ref", "email", "phone", "full_name", "city_id",
                F.col("zone_id").alias("home_zone_id"), "lat", "lon", "signup_ts",
                F.col("signup_ts").alias("created_at"), F.col("signup_ts").alias("updated_at"),
                "order_propensity")
    )

    customers_df = customers.drop("order_propensity")
    profile_df = customers.select("customer_id", "order_propensity")
    return customers_df, profile_df
