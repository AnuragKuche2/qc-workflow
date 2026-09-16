# QC Lakehouse - Sub-project B Widen W1a: Money-Chain Fact Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Widen Sub-project B with the commercial transaction layer - `orders`, `order_items`, `match_attempts`, `payments`, `refunds` - generated from the already-built `demand_hourly` table and reference-layer profile tables, written to `qc_dev.bronze_source` on live Databricks.

**Architecture:** A new `fact_entities.py` builds each table as a Spark DataFrame pipeline (deterministic hash-based derivation, broadcast joins against small reference tables, `element_at` array-indexing for weighted picks) - the same pattern `customers.py` already uses, not the Python-loop pattern `entities.py` uses for the much-smaller reference tables. A new `defects.py` holds the two deliberate data-quality defects as pure, independently-testable functions, wrapped as Spark UDFs where they're applied. A new `fact_writer.py` orchestrates: read reference tables back from Delta, build the five fact tables, run real `assert`-based integrity checks before any write, then write.

**Tech Stack:** Same as the rest of `qc-lakehouse` - PySpark (local dev-loop + Databricks serverless via Databricks Connect), `pytest`, `uv`.

**Spec:** `docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md` (section 6.6 is this widen step's detailed design; section 6 is the already-built spine this depends on).

## Global Constraints

- **Money-chain tables only**: `orders`, `order_items`, `match_attempts`, `payments`, `refunds`. `order_events`, `courier_shifts`, `gps_pings`, and Auto Loader/streaming are explicitly out of scope (W1b, a separate future design/plan).
- **No Auto Loader here either** - still a from-scratch generation run, direct-write to Delta via `saveAsTable`, `mode("overwrite")`.
- **Order volume comes from `demand_hourly`, not a new count.** Every `(day, hour, order_count)` row in the already-written `demand_hourly` table gets exactly that many orders.
- **Spark-native generation throughout** - `fact_entities.py` follows `customers.py`'s pattern (hash+salt deterministic draws via `F.hash`, never `F.rand()`; broadcast joins; `element_at` array-indexing), not `entities.py`'s Python-loop pattern. Tested via `qc_lakehouse.spark_local.build_local_spark_session()`, same as `test_customers.py`.
- **Reads reference tables back from Delta** (`spark.table(...)`) in `fact_writer.py`, then passes DataFrames into `fact_entities.py`'s `build_*` functions as parameters - the builders themselves never call `spark.table()` directly, matching the project's established dependency-injection discipline.
- **Idempotent by full-table `mode("overwrite")`** regeneration from a fixed seed - same justification as the spine's `writer.py`.
- **Real `assert`-based integrity checks, run before any write** - same discipline as the spine's `writer.py` (`check_referential_integrity`, `check_demand_conservation`, etc.), extended for this layer.
- **Two deliberate, isolated, rate-controlled defects**: `refunds.refund_amount_raw` (STRING, accounting-negative-in-parens for a `money_text_defect_rate` fraction) and `orders.delivery_notes` (casing/whitespace mangling for a `text_noise_defect_rate` fraction of non-null notes). Both live in `defects.py` as pure functions with no rate logic - callers decide whether to apply based on `GeneratorConfig`.
- **No "Co-Authored-By" or "Claude-Session" trailer** on any commit - this violates the project owner's own standing instruction. Every task's commit must be checked (`git log -1 --format=fuller`) before considering the task done.
- **`uv` end to end.**

---

### Task 1: Defect functions (`generator/defects.py`)

**Files:**
- Create: `qc-lakehouse/src/qc_lakehouse/generator/defects.py`
- Create: `qc-lakehouse/tests/test_defects.py`

**Interfaces:**
- Produces: `format_refund_amount(amount: Decimal, use_accounting_format: bool) -> str`, `mangle_text_casing_and_whitespace(text: str, mode: str) -> str` (`mode` one of `"UPPER"`/`"LOWER"`/`"LEADING_SPACE"`/`"TRAILING_SPACE"`, raises `ValueError` on any other value). Task 5 (`finalize_orders`) and Task 8 (`build_refunds`) wrap these as Spark UDFs - the rate/probability decision (whether to apply a defect to a given row) happens in those tasks via `GeneratorConfig`'s rate fields, never inside `defects.py`.

- [ ] **Step 1: Write the failing tests**

```python
# qc-lakehouse/tests/test_defects.py
from decimal import Decimal

import pytest

from qc_lakehouse.generator.defects import (
    format_refund_amount,
    mangle_text_casing_and_whitespace,
)


def test_format_refund_amount_plain():
    assert format_refund_amount(Decimal("20.47"), use_accounting_format=False) == "20.47"


def test_format_refund_amount_accounting_negative():
    assert format_refund_amount(Decimal("20.47"), use_accounting_format=True) == "(20.47)"


def test_format_refund_amount_pads_to_two_decimal_places():
    assert format_refund_amount(Decimal("5"), use_accounting_format=False) == "5.00"


def test_mangle_text_casing_and_whitespace_upper():
    assert mangle_text_casing_and_whitespace("Late again", "UPPER") == "LATE AGAIN"


def test_mangle_text_casing_and_whitespace_lower():
    assert mangle_text_casing_and_whitespace("Late again", "LOWER") == "late again"


def test_mangle_text_casing_and_whitespace_leading_space():
    assert mangle_text_casing_and_whitespace("Late again", "LEADING_SPACE") == "   Late again"


def test_mangle_text_casing_and_whitespace_trailing_space():
    assert mangle_text_casing_and_whitespace("Late again", "TRAILING_SPACE") == "Late again   "


def test_mangle_text_casing_and_whitespace_rejects_unknown_mode():
    with pytest.raises(ValueError):
        mangle_text_casing_and_whitespace("x", "NOT_A_MODE")
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_defects.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'qc_lakehouse.generator.defects'`

- [ ] **Step 3: Write the implementation**

```python
# qc-lakehouse/src/qc_lakehouse/generator/defects.py
from __future__ import annotations

from decimal import Decimal


def format_refund_amount(amount: Decimal, use_accounting_format: bool) -> str:
    """Defect #18-flavored (from the superseded old plan's defect catalog): money stored
    as text, with a fraction using accounting-negative notation for a credit/reduction -
    parens instead of a minus sign, e.g. "(20.47)" - which breaks a naive CAST under ANSI
    mode. `amount` is always the positive refund magnitude; `use_accounting_format`
    controls only the STRING representation, never the underlying value."""
    plain = f"{amount:.2f}"
    return f"({plain})" if use_accounting_format else plain


def mangle_text_casing_and_whitespace(text: str, mode: str) -> str:
    """Defect #17-flavored noise: casing/whitespace damage on free-text fields, which
    inflates naive GROUP BY cardinality. The caller decides which mode via a per-row hash
    draw; this function only applies one deterministically."""
    if mode == "UPPER":
        return text.upper()
    if mode == "LOWER":
        return text.lower()
    if mode == "LEADING_SPACE":
        return f"   {text}"
    if mode == "TRAILING_SPACE":
        return f"{text}   "
    raise ValueError(f"unknown casing mode: {mode}")
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_defects.py -v`
Expected: `8 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `75 passed` (67 existing + 8 new)

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/defects.py tests/test_defects.py
git commit -m "Add deliberate defect functions for the money-chain fact tables"
```

Then run `git log -1 --format=fuller` and confirm no `Co-Authored-By`/`Claude-Session` trailer - amend immediately if one appears.

---

### Task 2: `GeneratorConfig` fields + fact-table schemas

**Files:**
- Modify: `qc-lakehouse/src/qc_lakehouse/generator/config.py`
- Modify: `qc-lakehouse/src/qc_lakehouse/generator/schemas.py`
- Modify: `qc-lakehouse/tests/test_generator_config.py`

**Interfaces:**
- Produces: six new `GeneratorConfig` fields (`cancel_rate`, `unfulfilled_rate`, `refund_rate`, `payment_failure_rate`, `money_text_defect_rate`, `text_noise_defect_rate`), and five new schemas (`ORDERS_SCHEMA`, `ORDER_ITEMS_SCHEMA`, `MATCH_ATTEMPTS_SCHEMA`, `PAYMENTS_SCHEMA`, `REFUNDS_SCHEMA`). Tasks 3-9 depend on all of these by exact name.

- [ ] **Step 1: Write the failing test**

Append to `qc-lakehouse/tests/test_generator_config.py`:

```python
def test_generator_config_fact_table_defaults():
    config = GeneratorConfig()
    assert config.cancel_rate == 0.06
    assert config.unfulfilled_rate == 0.025
    assert config.refund_rate == 0.028
    assert config.payment_failure_rate == 0.01
    assert config.money_text_defect_rate == 0.15
    assert config.text_noise_defect_rate == 0.08
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `cd qc-lakehouse && uv run pytest tests/test_generator_config.py -v -k fact_table_defaults`
Expected: FAIL with `AttributeError`

- [ ] **Step 3: Add the fields to `GeneratorConfig`**

Append inside the `GeneratorConfig` dataclass in `qc-lakehouse/src/qc_lakehouse/generator/config.py`, after `menu_items_per_restaurant: int = 25`:

```python
    cancel_rate: float = 0.06
    unfulfilled_rate: float = 0.025
    refund_rate: float = 0.028           # fraction of DELIVERED orders that also get a
                                          # quality-issue refund
    payment_failure_rate: float = 0.01
    money_text_defect_rate: float = 0.15   # fraction of refunds using accounting-negative
                                             # text formatting
    text_noise_defect_rate: float = 0.08   # fraction of non-null delivery_notes with
                                             # casing/whitespace mangling
```

- [ ] **Step 4: Run the test and confirm it passes**

Run: `cd qc-lakehouse && uv run pytest tests/test_generator_config.py -v`
Expected: `8 passed` (7 existing + 1 new)

- [ ] **Step 5: Add the five new schemas**

Append to `qc-lakehouse/src/qc_lakehouse/generator/schemas.py`:

```python
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
```

These five schemas have no dedicated unit test (matching the spine's own precedent - `schemas.py` is exercised implicitly through the DataFrames built against it in later tasks' tests, the same way `test_customers.py` checks `set(df.columns) == expected_columns` rather than testing `schemas.py` directly).

- [ ] **Step 6: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `76 passed`

- [ ] **Step 7: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/config.py src/qc_lakehouse/generator/schemas.py tests/test_generator_config.py
git commit -m "Add GeneratorConfig fact-table fields and fact-table schemas"
```

Check for a trailer as in Task 1's Step 6.

---

### Task 3: `build_orders_shell` (`generator/fact_entities.py`)

**Files:**
- Create: `qc-lakehouse/src/qc_lakehouse/generator/fact_entities.py`
- Create: `qc-lakehouse/tests/test_fact_entities.py`

**Interfaces:**
- Consumes: `GeneratorConfig` (Task 2), `qc_lakehouse.spark_local.build_local_spark_session` (Sub-project A), the existing `entities.py`/`customers.py` builders (spine) for constructing test fixtures.
- Produces: `build_orders_shell(spark, config, demand_hourly_df, restaurant_profile_df, customer_profile_df, customers_df, zones_df, restaurants_df) -> DataFrame` with columns `order_id, customer_id, restaurant_id, zone_id, placed_at, order_status, delivery_fee, commission_pct, delivery_notes, day_index, hour` - everything `orders` needs EXCEPT `subtotal`/`commission_amount`/`order_total`/`order_ref`, which depend on `order_items` and are filled in by `finalize_orders` (Task 5). `day_index`/`hour` are NOT part of the final `orders` table (`finalize_orders`'s own `.select` drops them) - they exist only so Task 9's `fact_writer.py` can verify order volume per-(day,hour) against `demand_hourly` directly from the shell, before `finalize_orders` runs. Task 4 (`build_order_items`) and Task 5 (`finalize_orders`) consume this shell's `order_id`/`restaurant_id` columns.

- [ ] **Step 1: Write the failing test**

```python
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
    CITIES_SCHEMA,
    DEMAND_HOURLY_SCHEMA,
    GEN_RESTAURANT_PROFILE_SCHEMA,
    MENU_ITEMS_SCHEMA,
    RESTAURANTS_SCHEMA,
    RIDERS_SCHEMA,
    ZONES_SCHEMA,
)
from qc_lakehouse.spark_local import build_local_spark_session


def _tiny_config(**overrides):
    defaults = dict(
        n_cities=1, zones_per_city=3, n_restaurants=5, n_riders=5, n_customers=20,
        menu_items_per_restaurant=4, days=2, orders_per_day=50,
    )
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
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_entities.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'qc_lakehouse.generator.fact_entities'`

- [ ] **Step 3: Write the implementation**

```python
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

    exploded = (
        demand_hourly_df
        .join(F.broadcast(offsets_df), ["day_index", "hour"])
        .withColumn("seq", F.explode(F.sequence(F.lit(0), F.col("orders") - F.lit(1))))
        .withColumn("order_id", (F.col("order_offset") + F.col("seq") + F.lit(1)).cast("long"))
        .select("order_id", "day_index", "order_date", "hour")
    )

    # Restaurant popularity and customer order-propensity: same cumulative-threshold
    # broadcast-join bucketing customers.py uses for zone assignment. Both reference
    # tables are small enough (thousands / low hundred-thousands of rows) to collect and
    # prefix-sum driver-side in well under a second.
    def _cumulative(rows, id_field, weight_field, out_schema):
        rows = sorted(rows, key=lambda r: r[id_field])
        cum, out = 0.0, []
        for row in rows:
            cum += row[weight_field]
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
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_entities.py -v`
Expected: `4 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `80 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/fact_entities.py tests/test_fact_entities.py
git commit -m "Add build_orders_shell: Spark-native order generation from demand_hourly"
```

Check for a trailer.

---

### Task 4: `build_order_items` (`generator/fact_entities.py`)

**Files:**
- Modify: `qc-lakehouse/src/qc_lakehouse/generator/fact_entities.py`
- Modify: `qc-lakehouse/tests/test_fact_entities.py`

**Interfaces:**
- Consumes: `build_orders_shell`'s output shape (Task 3), `menu_items_df` (schema: `MENU_ITEMS_SCHEMA`).
- Produces: `build_order_items(config, orders_shell_df, menu_items_df) -> DataFrame` with columns `order_item_id, order_id, menu_item_id, quantity, unit_price, line_total`. Task 5 (`finalize_orders`) consumes this to compute `orders.subtotal`.

- [ ] **Step 1: Write the failing tests**

Append to `qc-lakehouse/tests/test_fact_entities.py`:

```python
from qc_lakehouse.generator.fact_entities import build_order_items


def test_build_order_items_every_item_belongs_to_a_real_order_and_menu_item():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        orders = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, orders, fx["menu_items_df"])

        order_ids = {r["order_id"] for r in orders.select("order_id").collect()}
        menu_item_ids = {r["menu_item_id"] for r in fx["menu_items_df"].select("menu_item_id").collect()}
        rows = items.select("order_id", "menu_item_id", "quantity", "line_total", "unit_price").collect()

        assert len(rows) > 0
        assert all(r["order_id"] in order_ids for r in rows)
        assert all(r["menu_item_id"] in menu_item_ids for r in rows)
        assert all(r["quantity"] >= 1 for r in rows)
        assert all(r["line_total"] == r["unit_price"] * r["quantity"] for r in rows)
    finally:
        spark.stop()


def test_build_order_items_every_order_gets_at_least_one_item():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        orders = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, orders, fx["menu_items_df"])

        order_ids = {r["order_id"] for r in orders.select("order_id").collect()}
        item_order_ids = {r["order_id"] for r in items.select("order_id").collect()}
        assert item_order_ids == order_ids
    finally:
        spark.stop()


def test_build_order_items_picks_from_the_orders_own_restaurant():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        orders = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, orders, fx["menu_items_df"])

        order_restaurant = {r["order_id"]: r["restaurant_id"] for r in orders.select("order_id", "restaurant_id").collect()}
        menu_restaurant = {r["menu_item_id"]: r["restaurant_id"] for r in fx["menu_items_df"].select("menu_item_id", "restaurant_id").collect()}

        for row in items.select("order_id", "menu_item_id").collect():
            assert menu_restaurant[row["menu_item_id"]] == order_restaurant[row["order_id"]]
    finally:
        spark.stop()
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_entities.py -v -k order_items`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Add the implementation**

Append to `qc-lakehouse/src/qc_lakehouse/generator/fact_entities.py`:

```python
def build_order_items(config: GeneratorConfig, orders_shell_df, menu_items_df):
    """One or more line items per order, sampled from that order's own restaurant's menu.
    Item count is weighted toward 1-2 items (45%/30%/15%/10% for 1/2/3/4)."""
    seed = config.seed

    restaurant_menus = (
        menu_items_df
        .groupBy("restaurant_id")
        .agg(F.collect_list(F.struct("menu_item_id", "price")).alias("items"))
    )

    u_count = _h(F.col("order_id"), seed + 201)
    items_base = (
        orders_shell_df.select("order_id", "restaurant_id")
        .withColumn(
            "item_count",
            F.when(u_count < F.lit(0.45), F.lit(1))
             .when(u_count < F.lit(0.75), F.lit(2))
             .when(u_count < F.lit(0.90), F.lit(3))
             .otherwise(F.lit(4)),
        )
        .join(F.broadcast(restaurant_menus), "restaurant_id")
    )

    exploded = (
        items_base
        .withColumn("item_slot", F.explode(F.sequence(F.lit(0), F.col("item_count") - F.lit(1))))
        .withColumn(
            "item_idx",
            F.pmod(F.hash("order_id", "item_slot", F.lit(seed + 202)), F.size("items")),
        )
        .withColumn("picked", F.element_at(F.col("items"), F.col("item_idx") + F.lit(1)))
        .withColumn("menu_item_id", F.col("picked.menu_item_id"))
        .withColumn("unit_price", F.col("picked.price").cast("decimal(18,2)"))
        .withColumn(
            "quantity",
            (F.pmod(F.hash("order_id", "item_slot", F.lit(seed + 203)), F.lit(3)) + F.lit(1)).cast("int"),
        )
        .withColumn("line_total", (F.col("unit_price") * F.col("quantity")).cast("decimal(18,2)"))
    )

    w = Window.orderBy("order_id", "item_slot")
    exploded = exploded.withColumn("order_item_id", F.row_number().over(w).cast("long"))

    return exploded.select(
        "order_item_id", "order_id", "menu_item_id", "quantity", "unit_price", "line_total",
    )
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_entities.py -v`
Expected: `7 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `83 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/fact_entities.py tests/test_fact_entities.py
git commit -m "Add build_order_items: line items sampled from each order's own restaurant menu"
```

Check for a trailer.

---

### Task 5: `finalize_orders` (`generator/fact_entities.py`)

**Files:**
- Modify: `qc-lakehouse/src/qc_lakehouse/generator/fact_entities.py`
- Modify: `qc-lakehouse/tests/test_fact_entities.py`

**Interfaces:**
- Consumes: `build_orders_shell`'s output (Task 3), `build_order_items`'s output (Task 4), `mangle_text_casing_and_whitespace` (Task 1).
- Produces: `finalize_orders(config, orders_shell_df, order_items_df) -> DataFrame` with the FULL `orders` schema (`order_id, order_ref, customer_id, restaurant_id, zone_id, placed_at, order_status, subtotal, delivery_fee, commission_pct, commission_amount, order_total, delivery_notes`) - this is the table `fact_writer.py` (Task 9) actually writes as `orders`; the shell alone is never written.

- [ ] **Step 1: Write the failing tests**

Append to `qc-lakehouse/tests/test_fact_entities.py`:

```python
from qc_lakehouse.generator.fact_entities import finalize_orders


def test_finalize_orders_subtotal_equals_sum_of_line_totals():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)

        item_subtotals = {
            r["order_id"]: r["subtotal"]
            for r in items.groupBy("order_id").agg(F.sum("line_total").alias("subtotal")).collect()
        }
        for row in orders.select("order_id", "subtotal").collect():
            assert row["subtotal"] == item_subtotals[row["order_id"]]
    finally:
        spark.stop()


def test_finalize_orders_order_total_equals_subtotal_plus_delivery_fee():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)

        for row in orders.select("subtotal", "delivery_fee", "order_total").collect():
            assert row["order_total"] == row["subtotal"] + row["delivery_fee"]
    finally:
        spark.stop()


def test_finalize_orders_has_the_full_orders_schema_columns():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)

        expected = {
            "order_id", "order_ref", "customer_id", "restaurant_id", "zone_id", "placed_at",
            "order_status", "subtotal", "delivery_fee", "commission_pct", "commission_amount",
            "order_total", "delivery_notes",
        }
        assert set(orders.columns) == expected


def test_finalize_orders_applies_text_noise_defect_at_the_configured_rate():
    spark = build_local_spark_session()
    try:
        config = _tiny_config(text_noise_defect_rate=1.0)   # force every note to be mangled
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)

        notes = [r["delivery_notes"] for r in orders.select("delivery_notes").collect() if r["delivery_notes"]]
        assert notes    # at least one order got a note
        # every non-null note must show a mangling signature: fully upper, fully lower,
        # or surrounded by extra whitespace - never the clean original casing/spacing
        for n in notes:
            mangled = n.isupper() or n.islower() or n != n.strip()
            assert mangled, f"note not mangled: {n!r}"
    finally:
        spark.stop()
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_entities.py -v -k finalize_orders`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Add the implementation**

Add this import near the top of `qc-lakehouse/src/qc_lakehouse/generator/fact_entities.py` (alongside the existing `pyspark` imports):

```python
from pyspark.sql.types import StringType

from qc_lakehouse.generator.defects import mangle_text_casing_and_whitespace
```

Append the function:

```python
def finalize_orders(config: GeneratorConfig, orders_shell_df, order_items_df):
    """Joins order_items' aggregated subtotal back onto the shell, computes
    commission_amount and order_total, and applies the text-noise defect to
    delivery_notes. This is the FINAL orders DataFrame - the shell alone is never
    written to Delta."""
    seed = config.seed

    subtotals = order_items_df.groupBy("order_id").agg(F.sum("line_total").alias("subtotal"))

    orders = (
        orders_shell_df
        .join(subtotals, "order_id")
        .withColumn("commission_amount", (F.col("subtotal") * F.col("commission_pct")).cast("decimal(18,2)"))
        .withColumn("order_total", (F.col("subtotal") + F.col("delivery_fee")).cast("decimal(18,2)"))
        .withColumn("order_ref", F.format_string("O-%07d", F.col("order_id")))
    )

    mangle_udf = F.udf(
        lambda text, mode: mangle_text_casing_and_whitespace(text, mode) if text is not None else None,
        StringType(),
    )
    u_defect = _h(F.col("order_id"), seed + 301)
    mode_idx = F.pmod(F.hash("order_id", F.lit(seed + 302)), F.lit(4))
    modes = F.array(*[F.lit(m) for m in ["UPPER", "LOWER", "LEADING_SPACE", "TRAILING_SPACE"]])
    orders = orders.withColumn(
        "delivery_notes",
        F.when(
            F.col("delivery_notes").isNotNull() & (u_defect < F.lit(config.text_noise_defect_rate)),
            mangle_udf(F.col("delivery_notes"), F.element_at(modes, mode_idx + F.lit(1))),
        ).otherwise(F.col("delivery_notes")),
    )

    return orders.select(
        "order_id", "order_ref", "customer_id", "restaurant_id", "zone_id", "placed_at",
        "order_status", "subtotal", "delivery_fee", "commission_pct", "commission_amount",
        "order_total", "delivery_notes",
    )
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_entities.py -v`
Expected: `11 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `87 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/fact_entities.py tests/test_fact_entities.py
git commit -m "Add finalize_orders: subtotal/commission/total plus text-noise defect"
```

Check for a trailer.

---

### Task 6: `build_match_attempts` (`generator/fact_entities.py`)

**Files:**
- Modify: `qc-lakehouse/src/qc_lakehouse/generator/fact_entities.py`
- Modify: `qc-lakehouse/tests/test_fact_entities.py`

**Interfaces:**
- Consumes: `finalize_orders`'s output shape (Task 5), `riders_df` (schema: `RIDERS_SCHEMA`).
- Produces: `build_match_attempts(config, orders_df, riders_df) -> DataFrame` with columns `match_id, order_id, rider_id, attempt_number, offered_at, response, responded_at`. Task 9's writer and its integrity checks depend on this shape (`response` values, exactly one `ACCEPTED` per matched order).

- [ ] **Step 1: Write the failing tests**

Append to `qc-lakehouse/tests/test_fact_entities.py`:

```python
from qc_lakehouse.generator.fact_entities import build_match_attempts


def test_build_match_attempts_matched_orders_have_exactly_one_accepted():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)
        matches = build_match_attempts(config, orders, fx["riders_df"])

        accepted_counts = {
            r["order_id"]: r["n"]
            for r in matches.filter("response = 'ACCEPTED'").groupBy("order_id").count().withColumnRenamed("count", "n").collect()
        }
        matched_order_ids = {
            r["order_id"] for r in orders.filter("order_status != 'UNFULFILLED'").select("order_id").collect()
        }
        for oid in matched_order_ids:
            assert accepted_counts.get(oid) == 1, f"order {oid} does not have exactly one ACCEPTED attempt"
    finally:
        spark.stop()


def test_build_match_attempts_unfulfilled_orders_have_zero_accepted():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)
        matches = build_match_attempts(config, orders, fx["riders_df"])

        unfulfilled_ids = {r["order_id"] for r in orders.filter("order_status = 'UNFULFILLED'").select("order_id").collect()}
        accepted_ids = {r["order_id"] for r in matches.filter("response = 'ACCEPTED'").select("order_id").collect()}
        assert unfulfilled_ids.isdisjoint(accepted_ids)
    finally:
        spark.stop()


def test_build_match_attempts_riders_belong_to_the_orders_delivery_zone():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)
        matches = build_match_attempts(config, orders, fx["riders_df"])

        order_zone = {r["order_id"]: r["zone_id"] for r in orders.select("order_id", "zone_id").collect()}
        rider_zone = {r["rider_id"]: r["home_zone_id"] for r in fx["riders_df"].select("rider_id", "home_zone_id").collect()}
        for row in matches.select("order_id", "rider_id").collect():
            assert rider_zone[row["rider_id"]] == order_zone[row["order_id"]]
    finally:
        spark.stop()
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_entities.py -v -k match_attempts`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Add the implementation**

Append to `qc-lakehouse/src/qc_lakehouse/generator/fact_entities.py`:

```python
def build_match_attempts(config: GeneratorConfig, orders_df, riders_df):
    """1+ offer/response rows per order. Matched orders (DELIVERED/CANCELLED) get 1-3
    attempts with the LAST marked ACCEPTED; UNFULFILLED orders get 2-3 attempts, all
    DECLINED/TIMEOUT - the rider pool is restricted to riders whose home_zone_id matches
    the order's delivery zone."""
    seed = config.seed

    zone_riders = (
        riders_df.filter("is_active")
        .groupBy("home_zone_id")
        .agg(F.collect_list("rider_id").alias("rider_ids"))
    )

    u_count = _h(F.col("order_id"), seed + 401)
    base = (
        orders_df.select("order_id", "zone_id", "order_status", "placed_at")
        .withColumn(
            "attempt_count",
            F.when(
                F.col("order_status") == "UNFULFILLED",
                F.when(u_count < F.lit(0.5), F.lit(2)).otherwise(F.lit(3)),
            ).otherwise(
                F.when(u_count < F.lit(0.7), F.lit(1))
                 .when(u_count < F.lit(0.9), F.lit(2))
                 .otherwise(F.lit(3))
            ),
        )
        .join(F.broadcast(zone_riders), F.col("zone_id") == F.col("home_zone_id"))
    )

    exploded = (
        base
        .withColumn("attempt_number", F.explode(F.sequence(F.lit(1), F.col("attempt_count"))))
        .withColumn(
            "rider_idx",
            F.pmod(F.hash("order_id", "attempt_number", F.lit(seed + 402)), F.size("rider_ids")),
        )
        .withColumn("rider_id", F.element_at(F.col("rider_ids"), F.col("rider_idx") + F.lit(1)))
        .withColumn(
            "response",
            F.when(
                (F.col("order_status") != "UNFULFILLED") & (F.col("attempt_number") == F.col("attempt_count")),
                F.lit("ACCEPTED"),
            ).otherwise(
                F.when(
                    F.pmod(F.hash("order_id", "attempt_number", F.lit(seed + 403)), F.lit(2)) == 0,
                    F.lit("DECLINED"),
                ).otherwise(F.lit("TIMEOUT"))
            ),
        )
        .withColumn("offered_at", F.expr("timestampadd(MINUTE, (attempt_number - 1) * 2, placed_at)"))
        .withColumn(
            "responded_at",
            F.expr(
                f"timestampadd(SECOND, 15 + pmod(abs(hash(order_id, attempt_number, {seed + 404})), 60), offered_at)"
            ),
        )
    )

    w = Window.orderBy("order_id", "attempt_number")
    exploded = exploded.withColumn("match_id", F.row_number().over(w).cast("long"))

    return exploded.select(
        "match_id", "order_id", "rider_id", "attempt_number", "offered_at", "response", "responded_at",
    )
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_entities.py -v`
Expected: `14 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `90 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/fact_entities.py tests/test_fact_entities.py
git commit -m "Add build_match_attempts: courier-offer process with exactly one ACCEPTED per matched order"
```

Check for a trailer.

---

### Task 7: `build_payments` (`generator/fact_entities.py`)

**Files:**
- Modify: `qc-lakehouse/src/qc_lakehouse/generator/fact_entities.py`
- Modify: `qc-lakehouse/tests/test_fact_entities.py`

**Interfaces:**
- Consumes: `finalize_orders`'s output shape (Task 5).
- Produces: `build_payments(config, orders_df) -> DataFrame` with columns `payment_id, order_id, amount, method, status, paid_at`. Task 8 (`build_refunds`) consumes this table's `payment_id`/`order_id`.

- [ ] **Step 1: Write the failing tests**

Append to `qc-lakehouse/tests/test_fact_entities.py`:

```python
from qc_lakehouse.generator.fact_entities import build_payments


def test_build_payments_covers_every_non_unfulfilled_order_exactly_once():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)
        payments = build_payments(config, orders)

        payable_ids = {r["order_id"] for r in orders.filter("order_status != 'UNFULFILLED'").select("order_id").collect()}
        payment_order_ids = [r["order_id"] for r in payments.select("order_id").collect()]
        assert set(payment_order_ids) == payable_ids
        assert len(payment_order_ids) == len(set(payment_order_ids))   # exactly once each
    finally:
        spark.stop()


def test_build_payments_amount_matches_order_total():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)
        payments = build_payments(config, orders)

        order_total = {r["order_id"]: r["order_total"] for r in orders.select("order_id", "order_total").collect()}
        for row in payments.select("order_id", "amount").collect():
            assert row["amount"] == order_total[row["order_id"]]
    finally:
        spark.stop()


def test_build_payments_method_and_status_are_valid_values():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)
        payments = build_payments(config, orders)

        rows = payments.select("method", "status").collect()
        assert all(r["method"] in ("upi", "card", "wallet", "cod") for r in rows)
        assert all(r["status"] in ("SUCCESS", "FAILED") for r in rows)
    finally:
        spark.stop()
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_entities.py -v -k build_payments`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Add the implementation**

Append to `qc-lakehouse/src/qc_lakehouse/generator/fact_entities.py`:

```python
def build_payments(config: GeneratorConfig, orders_df):
    """One row per order that reached a payable state (DELIVERED or CANCELLED - both
    were charged; UNFULFILLED orders never matched, so were never charged)."""
    seed = config.seed

    payable = orders_df.filter("order_status != 'UNFULFILLED'")

    u_status = _h(F.col("order_id"), seed + 501)
    u_method = F.pmod(F.hash("order_id", F.lit(seed + 502)), F.lit(1000)) / 1000.0

    payments = (
        payable
        .withColumn(
            "status",
            F.when(u_status < F.lit(config.payment_failure_rate), F.lit("FAILED")).otherwise(F.lit("SUCCESS")),
        )
        .withColumn(
            "method",
            F.when(u_method < F.lit(0.45), F.lit("upi"))
             .when(u_method < F.lit(0.75), F.lit("card"))
             .when(u_method < F.lit(0.90), F.lit("wallet"))
             .otherwise(F.lit("cod")),
        )
        .withColumn(
            "paid_at",
            F.expr(f"timestampadd(SECOND, 5 + pmod(abs(hash(order_id, {seed + 503})), 30), placed_at)"),
        )
        .withColumnRenamed("order_total", "amount")
    )

    w = Window.orderBy("order_id")
    payments = payments.withColumn("payment_id", F.row_number().over(w).cast("long"))

    return payments.select("payment_id", "order_id", "amount", "method", "status", "paid_at")
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_entities.py -v`
Expected: `17 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `93 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/fact_entities.py tests/test_fact_entities.py
git commit -m "Add build_payments: one row per payable order, amount matching order_total"
```

Check for a trailer.

---

### Task 8: `build_refunds` (`generator/fact_entities.py`)

**Files:**
- Modify: `qc-lakehouse/src/qc_lakehouse/generator/fact_entities.py`
- Modify: `qc-lakehouse/tests/test_fact_entities.py`

**Interfaces:**
- Consumes: `finalize_orders`'s output shape (Task 5), `build_payments`'s output (Task 7), `format_refund_amount` (Task 1).
- Produces: `build_refunds(config, orders_df, payments_df) -> DataFrame` with columns `refund_id, order_id, payment_id, refund_amount_raw, reason, refunded_at`.

- [ ] **Step 1: Write the failing tests**

Append to `qc-lakehouse/tests/test_fact_entities.py`:

```python
from qc_lakehouse.generator.fact_entities import build_refunds


def test_build_refunds_covers_every_cancelled_order():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)
        payments = build_payments(config, orders)
        refunds = build_refunds(config, orders, payments)

        cancelled_ids = {r["order_id"] for r in orders.filter("order_status = 'CANCELLED'").select("order_id").collect()}
        refund_ids = {r["order_id"] for r in refunds.select("order_id").collect()}
        assert cancelled_ids.issubset(refund_ids)


def test_build_refunds_reasons_are_valid_and_payment_id_is_real():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)
        payments = build_payments(config, orders)
        refunds = build_refunds(config, orders, payments)

        valid_reasons = {"CANCELLED", "QUALITY_ISSUE", "LATE_DELIVERY", "MISSING_ITEMS"}
        payment_ids = {r["payment_id"] for r in payments.select("payment_id").collect()}
        rows = refunds.select("reason", "payment_id").collect()
        assert all(r["reason"] in valid_reasons for r in rows)
        assert all(r["payment_id"] in payment_ids for r in rows)
    finally:
        spark.stop()


def test_build_refunds_applies_money_text_defect_at_the_configured_rate():
    spark = build_local_spark_session()
    try:
        config = _tiny_config(money_text_defect_rate=1.0, cancel_rate=0.5, unfulfilled_rate=0.0)
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)
        payments = build_payments(config, orders)
        refunds = build_refunds(config, orders, payments)

        raw_amounts = [r["refund_amount_raw"] for r in refunds.select("refund_amount_raw").collect()]
        assert raw_amounts    # at least one refund exists at cancel_rate=0.5
        assert all(a.startswith("(") and a.endswith(")") for a in raw_amounts)
    finally:
        spark.stop()


def test_build_refunds_amount_never_exceeds_the_order_total():
    spark = build_local_spark_session()
    try:
        config = _tiny_config()
        fx = _build_reference_fixtures(spark, config)
        shell = build_orders_shell(
            spark, config, fx["demand_hourly_df"], fx["restaurant_profile_df"],
            fx["customer_profile_df"], fx["customers_df"], fx["zones_df"], fx["restaurants_df"],
        )
        items = build_order_items(config, shell, fx["menu_items_df"])
        orders = finalize_orders(config, shell, items)
        payments = build_payments(config, orders)
        refunds = build_refunds(config, orders, payments)

        order_total = {r["order_id"]: r["order_total"] for r in orders.select("order_id", "order_total").collect()}
        for row in refunds.select("order_id", "refund_amount_raw").collect():
            raw = row["refund_amount_raw"].strip("()")
            from decimal import Decimal
            assert Decimal(raw) <= order_total[row["order_id"]]
    finally:
        spark.stop()
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_entities.py -v -k build_refunds`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Add the implementation**

Add this import near the top of `qc-lakehouse/src/qc_lakehouse/generator/fact_entities.py`, alongside the `mangle_text_casing_and_whitespace` import:

```python
from qc_lakehouse.generator.defects import format_refund_amount, mangle_text_casing_and_whitespace
```

Append the function:

```python
def build_refunds(config: GeneratorConfig, orders_df, payments_df):
    """One row for every CANCELLED order (full refund) plus a refund_rate fraction of
    DELIVERED orders (a partial quality-issue credit, 30-100% of order_total)."""
    seed = config.seed

    cancelled = (
        orders_df.filter("order_status = 'CANCELLED'")
        .withColumn("refund_fraction", F.lit(1.0))
        .withColumn("reason", F.lit("CANCELLED"))
    )

    u_refund = _h(F.col("order_id"), seed + 601)
    delivered_refunded = (
        orders_df.filter("order_status = 'DELIVERED'")
        .withColumn("u_refund", u_refund)
        .filter(F.col("u_refund") < F.lit(config.refund_rate))
        .drop("u_refund")
        .withColumn("refund_fraction", F.lit(0.3) + _h(F.col("order_id"), seed + 602) * F.lit(0.7))
        .withColumn(
            "reason_bucket",
            F.pmod(F.hash("order_id", F.lit(seed + 603)), F.lit(3)),
        )
        .withColumn(
            "reason",
            F.when(F.col("reason_bucket") == 0, F.lit("QUALITY_ISSUE"))
             .when(F.col("reason_bucket") == 1, F.lit("LATE_DELIVERY"))
             .otherwise(F.lit("MISSING_ITEMS")),
        )
        .drop("reason_bucket")
    )

    candidates = cancelled.unionByName(delivered_refunded)

    joined = (
        candidates
        .join(payments_df.select("order_id", "payment_id"), "order_id")
        .withColumn("refund_amount", (F.col("order_total") * F.col("refund_fraction")).cast("decimal(18,2)"))
        .withColumn("use_accounting_format", _h(F.col("order_id"), seed + 604) < F.lit(config.money_text_defect_rate))
    )

    format_udf = F.udf(
        lambda amount, use_accounting: format_refund_amount(amount, bool(use_accounting)),
        StringType(),
    )
    joined = joined.withColumn(
        "refund_amount_raw", format_udf(F.col("refund_amount"), F.col("use_accounting_format")),
    )

    joined = joined.withColumn(
        "refunded_at",
        F.when(
            F.col("order_status") == "CANCELLED",
            F.expr(f"timestampadd(MINUTE, 5 + pmod(abs(hash(order_id, {seed + 605})), 30), placed_at)"),
        ).otherwise(
            F.expr(f"timestampadd(HOUR, 24 + pmod(abs(hash(order_id, {seed + 606})), 48), placed_at)")
        ),
    )

    w = Window.orderBy("order_id")
    joined = joined.withColumn("refund_id", F.row_number().over(w).cast("long"))

    return joined.select(
        "refund_id", "order_id", "payment_id", "refund_amount_raw", "reason", "refunded_at",
    )
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_entities.py -v`
Expected: `21 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `97 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/fact_entities.py tests/test_fact_entities.py
git commit -m "Add build_refunds: full refunds for cancellations, partial credits for quality issues"
```

Check for a trailer.

---

### Task 9: Integrity checks and write orchestration (`generator/fact_writer.py`)

**Files:**
- Create: `qc-lakehouse/src/qc_lakehouse/generator/fact_writer.py`
- Create: `qc-lakehouse/tests/test_fact_writer.py`

**Interfaces:**
- Consumes: every `build_*`/`finalize_orders` function from Tasks 3-8, `GeneratorConfig` (Task 2), the five schemas (Task 2).
- Produces: `check_order_volume_matches_demand(order_buckets: list[tuple[int, int]], demand_hourly: list) -> None` (`order_buckets` is one `(day_index, hour)` tuple per generated order, from the *shell* before `finalize_orders` drops those columns - see Task 3), `check_fact_referential_integrity(orders_df, order_items_df, match_attempts_df, payments_df, refunds_df) -> None`, `check_exactly_one_accepted_match_per_matched_order(orders_df, match_attempts_df) -> None`, `check_subtotal_matches_line_items(orders_df, order_items_df) -> None`, `check_payment_amount_matches_order_total(orders_df, payments_df) -> None`, and `write_fact_tables(spark, config) -> dict[str, int]` - reads reference tables from `{config.catalog}.{config.schema}` via `spark.table(...)`, calls every builder in order, runs all five checks before any write, then writes all five fact tables. Task 10's entrypoint calls `write_fact_tables`.

- [ ] **Step 1: Write the failing tests**

```python
# qc-lakehouse/tests/test_fact_writer.py
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from qc_lakehouse.generator.fact_writer import (
    check_exactly_one_accepted_match_per_matched_order,
    check_fact_referential_integrity,
    check_order_volume_matches_demand,
    check_payment_amount_matches_order_total,
    check_subtotal_matches_line_items,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _order(order_id, status="DELIVERED", subtotal=Decimal("100.00"), order_total=Decimal("120.00")):
    return {"order_id": order_id, "order_status": status, "subtotal": subtotal, "order_total": order_total}


def test_check_order_volume_matches_demand_passes_when_bucket_counts_agree():
    order_buckets = [(0, 0), (0, 0), (0, 1)]        # 2 orders in (day 0, hour 0), 1 in (day 0, hour 1)
    hourly = [(0, None, 0, 2), (0, None, 1, 1)]      # day_index, order_date, hour, orders
    check_order_volume_matches_demand(order_buckets, hourly)  # must not raise


def test_check_order_volume_matches_demand_catches_a_per_bucket_mismatch():
    order_buckets = [(0, 0), (0, 0)]     # only 2 orders generated
    hourly = [(0, None, 0, 5)]            # but demand_hourly says 5 were expected
    with pytest.raises(AssertionError, match="volume"):
        check_order_volume_matches_demand(order_buckets, hourly)


def test_check_order_volume_matches_demand_catches_a_mismatch_hidden_by_a_correct_total():
    # 3 orders total in both cases, but distributed across the wrong buckets - a
    # total-only check would miss this; the per-(day,hour) check must not.
    order_buckets = [(0, 0), (0, 0), (0, 0)]
    hourly = [(0, None, 0, 2), (0, None, 1, 1)]
    with pytest.raises(AssertionError, match="volume"):
        check_order_volume_matches_demand(order_buckets, hourly)


def test_check_fact_referential_integrity_passes_on_a_consistent_world():
    orders = [_order(1)]
    order_items = [{"order_item_id": 1, "order_id": 1}]
    matches = [{"match_id": 1, "order_id": 1}]
    payments = [{"payment_id": 1, "order_id": 1}]
    refunds = [{"refund_id": 1, "order_id": 1, "payment_id": 1}]
    check_fact_referential_integrity(orders, order_items, matches, payments, refunds)  # must not raise


def test_check_fact_referential_integrity_catches_an_orphaned_order_item():
    orders = [_order(1)]
    order_items = [{"order_item_id": 1, "order_id": 999}]
    with pytest.raises(AssertionError, match="order_item"):
        check_fact_referential_integrity(orders, order_items, [], [], [])


def test_check_exactly_one_accepted_match_per_matched_order_passes():
    orders = [_order(1, status="DELIVERED"), _order(2, status="UNFULFILLED")]
    matches = [
        {"order_id": 1, "response": "DECLINED"},
        {"order_id": 1, "response": "ACCEPTED"},
        {"order_id": 2, "response": "DECLINED"},
        {"order_id": 2, "response": "TIMEOUT"},
    ]
    check_exactly_one_accepted_match_per_matched_order(orders, matches)  # must not raise


def test_check_exactly_one_accepted_match_per_matched_order_catches_a_missing_accept():
    orders = [_order(1, status="DELIVERED")]
    matches = [{"order_id": 1, "response": "DECLINED"}]
    with pytest.raises(AssertionError, match="ACCEPTED"):
        check_exactly_one_accepted_match_per_matched_order(orders, matches)


def test_check_subtotal_matches_line_items_passes():
    orders = [_order(1, subtotal=Decimal("30.00"))]
    order_items = [
        {"order_id": 1, "line_total": Decimal("10.00")},
        {"order_id": 1, "line_total": Decimal("20.00")},
    ]
    check_subtotal_matches_line_items(orders, order_items)  # must not raise


def test_check_subtotal_matches_line_items_catches_a_mismatch():
    orders = [_order(1, subtotal=Decimal("99.00"))]
    order_items = [{"order_id": 1, "line_total": Decimal("10.00")}]
    with pytest.raises(AssertionError, match="subtotal"):
        check_subtotal_matches_line_items(orders, order_items)


def test_check_payment_amount_matches_order_total_passes():
    orders = [_order(1, order_total=Decimal("120.00"))]
    payments = [{"order_id": 1, "amount": Decimal("120.00"), "status": "SUCCESS"}]
    check_payment_amount_matches_order_total(orders, payments)  # must not raise


def test_check_payment_amount_matches_order_total_catches_a_mismatch():
    orders = [_order(1, order_total=Decimal("120.00"))]
    payments = [{"order_id": 1, "amount": Decimal("50.00"), "status": "SUCCESS"}]
    with pytest.raises(AssertionError, match="payment"):
        check_payment_amount_matches_order_total(orders, payments)
```

**Implementer note:** these check functions take plain lists of dict-like rows (not Spark
Rows) in the tests above for simplicity - when called from `write_fact_tables` against real
Spark DataFrames, pass `df.collect()` (a list of `pyspark.sql.Row`, which supports the same
`row["field"]` access dict-like rows in these tests use) rather than the DataFrame itself, so
the check functions stay Spark-independent and directly unit-testable.

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_writer.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'qc_lakehouse.generator.fact_writer'`

- [ ] **Step 3: Write the check functions and orchestration**

```python
# qc-lakehouse/src/qc_lakehouse/generator/fact_writer.py
from __future__ import annotations

from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.fact_entities import (
    build_match_attempts,
    build_order_items,
    build_orders_shell,
    build_payments,
    build_refunds,
    finalize_orders,
)
from qc_lakehouse.generator.schemas import (
    MATCH_ATTEMPTS_SCHEMA,
    ORDER_ITEMS_SCHEMA,
    ORDERS_SCHEMA,
    PAYMENTS_SCHEMA,
    REFUNDS_SCHEMA,
)


def check_order_volume_matches_demand(order_buckets, demand_hourly) -> None:
    """Every (day_index, hour) bucket's order count must exactly match demand_hourly - the
    source of truth for volume, not a target to approximate. A total-only comparison would
    miss orders landing in the wrong bucket while the grand total still happens to agree,
    so this checks every bucket individually. `order_buckets` is one (day_index, hour)
    tuple per generated order (from orders_shell_df, collected before finalize_orders
    drops those columns); `demand_hourly` rows are (day_index, order_date, hour, orders)."""
    actual: dict[tuple[int, int], int] = {}
    for day_index, hour in order_buckets:
        key = (day_index, hour)
        actual[key] = actual.get(key, 0) + 1

    bad = []
    for row in demand_hourly:
        key = (row[0], row[2])
        expected = row[3]
        if actual.get(key, 0) != expected:
            bad.append((key, expected, actual.get(key, 0)))
    assert not bad, f"order volume mismatch by (day_index, hour) - (bucket, expected, actual): {bad[:5]}"


def check_fact_referential_integrity(orders, order_items, match_attempts, payments, refunds) -> None:
    """Every fact row must reference a real order. Runs against collected rows before
    anything is written."""
    order_ids = {r["order_id"] for r in orders}

    orphan_items = [r["order_item_id"] for r in order_items if r["order_id"] not in order_ids]
    assert not orphan_items, f"order_item -> order orphans: {orphan_items[:5]}"

    orphan_matches = [r["match_id"] for r in match_attempts if r["order_id"] not in order_ids]
    assert not orphan_matches, f"match_attempt -> order orphans: {orphan_matches[:5]}"

    orphan_payments = [r["payment_id"] for r in payments if r["order_id"] not in order_ids]
    assert not orphan_payments, f"payment -> order orphans: {orphan_payments[:5]}"

    orphan_refunds = [r["refund_id"] for r in refunds if r["order_id"] not in order_ids]
    assert not orphan_refunds, f"refund -> order orphans: {orphan_refunds[:5]}"


def check_exactly_one_accepted_match_per_matched_order(orders, match_attempts) -> None:
    """Every DELIVERED/CANCELLED order needs exactly one ACCEPTED attempt; every
    UNFULFILLED order needs zero."""
    accepted_by_order: dict[int, int] = {}
    for r in match_attempts:
        if r["response"] == "ACCEPTED":
            accepted_by_order[r["order_id"]] = accepted_by_order.get(r["order_id"], 0) + 1

    bad = []
    for order in orders:
        count = accepted_by_order.get(order["order_id"], 0)
        expected = 0 if order["order_status"] == "UNFULFILLED" else 1
        if count != expected:
            bad.append((order["order_id"], order["order_status"], count))
    assert not bad, f"orders with the wrong ACCEPTED count: {bad[:5]}"


def check_subtotal_matches_line_items(orders, order_items) -> None:
    """orders.subtotal must equal the sum of that order's order_items.line_total."""
    totals: dict[int, object] = {}
    for r in order_items:
        totals[r["order_id"]] = totals.get(r["order_id"], 0) + r["line_total"]

    bad = [o["order_id"] for o in orders if totals.get(o["order_id"]) != o["subtotal"]]
    assert not bad, f"orders whose subtotal disagrees with their line items: {bad[:5]}"


def check_payment_amount_matches_order_total(orders, payments) -> None:
    """Every SUCCESS payment's amount must equal its order's order_total."""
    order_total = {o["order_id"]: o["order_total"] for o in orders}
    bad = [
        p["order_id"] for p in payments
        if p["status"] == "SUCCESS" and p["amount"] != order_total.get(p["order_id"])
    ]
    assert not bad, f"SUCCESS payments whose amount disagrees with order_total: {bad[:5]}"


def _write(spark, config: GeneratorConfig, name: str, df, schema) -> int:
    """Overwrite one Delta table from a DataFrame already matching `schema`. Idempotent:
    full-table regeneration from a fixed seed, not an incremental append."""
    table = f"{config.catalog}.{config.schema}.{name}"
    (spark.createDataFrame(df.rdd, schema)
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table))
    return spark.table(table).count()


def write_fact_tables(spark, config: GeneratorConfig) -> dict[str, int]:
    catalog, schema = config.catalog, config.schema

    demand_hourly_df = spark.table(f"{catalog}.{schema}.demand_hourly")
    restaurant_profile_df = spark.table(f"{catalog}.{schema}._gen_restaurant_profile")
    customer_profile_df = spark.table(f"{catalog}.{schema}._gen_customer_profile")
    customers_df = spark.table(f"{catalog}.{schema}.customers")
    zones_df = spark.table(f"{catalog}.{schema}.zones")
    restaurants_df = spark.table(f"{catalog}.{schema}.restaurants")
    riders_df = spark.table(f"{catalog}.{schema}.riders")
    menu_items_df = spark.table(f"{catalog}.{schema}.menu_items")

    demand_hourly_rows = demand_hourly_df.collect()

    orders_shell_df = build_orders_shell(
        spark, config, demand_hourly_df, restaurant_profile_df, customer_profile_df,
        customers_df, zones_df, restaurants_df,
    )
    # Checked against the SHELL (which still carries day_index/hour) rather than the
    # final orders_df, and before finalize_orders/order_items even run - a volume defect
    # is a defect in build_orders_shell specifically, and this fails fast on it.
    shell_buckets = [
        (r["day_index"], r["hour"]) for r in orders_shell_df.select("day_index", "hour").collect()
    ]
    check_order_volume_matches_demand(shell_buckets, demand_hourly_rows)

    order_items_df = build_order_items(config, orders_shell_df, menu_items_df)
    orders_df = finalize_orders(config, orders_shell_df, order_items_df)
    match_attempts_df = build_match_attempts(config, orders_df, riders_df)
    payments_df = build_payments(config, orders_df)
    refunds_df = build_refunds(config, orders_df, payments_df)

    # Collect once each, before any write - these are the same collected lists every
    # remaining check function below consumes.
    orders_rows = orders_df.collect()
    order_items_rows = order_items_df.collect()
    match_attempts_rows = match_attempts_df.collect()
    payments_rows = payments_df.collect()
    refunds_rows = refunds_df.collect()

    check_fact_referential_integrity(orders_rows, order_items_rows, match_attempts_rows, payments_rows, refunds_rows)
    check_exactly_one_accepted_match_per_matched_order(orders_rows, match_attempts_rows)
    check_subtotal_matches_line_items(orders_rows, order_items_rows)
    check_payment_amount_matches_order_total(orders_rows, payments_rows)

    counts: dict[str, int] = {}
    counts["orders"] = _write(spark, config, "orders", orders_df, ORDERS_SCHEMA)
    counts["order_items"] = _write(spark, config, "order_items", order_items_df, ORDER_ITEMS_SCHEMA)
    counts["match_attempts"] = _write(spark, config, "match_attempts", match_attempts_df, MATCH_ATTEMPTS_SCHEMA)
    counts["payments"] = _write(spark, config, "payments", payments_df, PAYMENTS_SCHEMA)
    counts["refunds"] = _write(spark, config, "refunds", refunds_df, REFUNDS_SCHEMA)

    return counts
```

**Implementer note on `check_order_volume_matches_demand`'s `demand_hourly` row access
(`row[0]`, `row[2]`, `row[3]`)**: these match `demand_hourly`'s schema field order
(`day_index, order_date, hour, orders` - `day_index` is index 0, `hour` is index 2, `orders`
is index 3), consistent with how the spine's own `writer.py` indexes
`demand_daily`/`demand_hourly` rows positionally rather than by name.

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_fact_writer.py -v`
Expected: `11 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `108 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/fact_writer.py tests/test_fact_writer.py
git commit -m "Add fact-table integrity checks and write orchestration"
```

Check for a trailer.

---

### Task 10: Entrypoint, Makefile, README, and live validation

**Files:**
- Create: `qc-lakehouse/scripts/generate_fact_data.py`
- Modify: `qc-lakehouse/Makefile`
- Modify: `qc-lakehouse/README.md`

**Interfaces:**
- Consumes: `load_settings`/`build_databricks_session` (Sub-project A), `write_fact_tables` (Task 9).
- Produces: `make generate-fact-data`, and a documented real run against live Databricks - this is the acceptance test for the whole widen step.

- [ ] **Step 1: Write the entrypoint script**

```python
# qc-lakehouse/scripts/generate_fact_data.py
"""Builds and writes the quick-commerce money-chain fact tables (orders, order_items,
match_attempts, payments, refunds) to {GeneratorConfig.catalog}.{GeneratorConfig.schema}
on live Databricks serverless compute.

Requires the reference-data layer (scripts/generate_reference_data.py) to have already
been run - this script reads zones/restaurants/riders/menu_items/customers/demand_hourly
and their generator-only profile tables back from Delta.
"""
from __future__ import annotations

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session
from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.fact_writer import write_fact_tables


def main() -> None:
    settings = load_settings()
    spark = build_databricks_session(settings)
    # See scripts/generate_reference_data.py's comment: build_databricks_session's own
    # settings.databricks_catalog/schema (typically workspace.dev) is unrelated to this
    # script's actual write target, GeneratorConfig's catalog/schema (qc_dev.bronze_source
    # by default).
    config = GeneratorConfig()

    counts = write_fact_tables(spark, config)

    print(f"catalog: {config.catalog}.{config.schema}\n")
    for name, n in counts.items():
        print(f"  {name:<28} {n:>10,}")

    total_rows = sum(counts.values())
    print(f"\ngenerate-fact-data: OK - wrote {len(counts)} tables, {total_rows:,} total rows")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Add the Makefile target**

Append to `qc-lakehouse/Makefile` (keep every existing target):

```makefile
.PHONY: generate-fact-data

# Writes real fact data to qc_dev.bronze_source on live Databricks. Requires
# generate-reference-data to have been run first (this reads the reference tables it
# wrote). Uses .venv-databricks, same as generate-reference-data.
generate-fact-data:
	.venv-databricks/bin/python scripts/generate_fact_data.py
```

Add `generate-fact-data` to the top `.PHONY:` line alongside the existing targets.

- [ ] **Step 3: Run it for real against live Databricks**

First confirm the reference layer is present (it should already be, from the spine's own
live-verified run):

Run: `cd qc-lakehouse && .venv-databricks/bin/python -c "
from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session
s = build_databricks_session(load_settings())
print(s.table('qc_dev.bronze_source.demand_hourly').count())
"`
Expected: prints a positive row count (2,160 at the spine's default 90-day config) - if this
fails because the table doesn't exist, run `make generate-reference-data` first.

Then run the fact-table generation:

Run: `cd qc-lakehouse && make generate-fact-data`
Expected: prints one row-count line per table (`orders`, `order_items`, `match_attempts`,
`payments`, `refunds`), then `generate-fact-data: OK - wrote 5 tables, N total rows`. If any
of the five integrity checks from Task 9 fail, the run raises `AssertionError` before writing
anything - that would mean a real bug in the generation logic (the hand-written Spark code in
this plan is intricate; treat any check failure as a genuine bug to fix, not something to
work around) - stop and investigate.

- [ ] **Step 4: Verify idempotency**

Run `make generate-fact-data` a second time and confirm the printed row counts are identical
to the first run - the same seed regenerating the same deterministic data through
`mode("overwrite")` must produce identical counts.

- [ ] **Step 5: Add a "Money-chain fact tables (W1a)" section to the README**

Append to `qc-lakehouse/README.md`, after the existing "Reference data generator" section:

```markdown
## Money-chain fact tables (Sub-project B widen step W1a)

Ported design from `docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md`,
section 6.6. Generates `orders`, `order_items`, `match_attempts`, `payments`, `refunds` from
the already-written `demand_hourly` table and reference-layer profile tables, and writes them
to `qc_dev.bronze_source`. Requires `make generate-reference-data` to have been run first.

```bash
make generate-fact-data
```

Two deliberate, isolated defects are present in the generated data (see
`src/qc_lakehouse/generator/defects.py`), matching the superseded old plan's defect catalog:
`refunds.refund_amount_raw` is a STRING, with a `money_text_defect_rate` fraction using
accounting-negative parens formatting (e.g. `"(20.47)"`); `orders.delivery_notes` gets
casing/whitespace mangling on a `text_noise_defect_rate` fraction of non-null notes. Both are
there deliberately, for Sub-project C's dbt staging models to clean.

No Auto Loader here either - still a from-scratch generation run, direct-write to Delta.
Idempotent the same way the reference layer is: `mode("overwrite")` from a fixed seed.
```

- [ ] **Step 6: Run `make check` one more time to confirm nothing regressed**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `108 passed`

- [ ] **Step 7: Commit**

```bash
cd qc-lakehouse
git add scripts/generate_fact_data.py Makefile README.md
git commit -m "Add money-chain fact-table generator entrypoint, wire into Makefile, document in README"
```

Check for a trailer.
