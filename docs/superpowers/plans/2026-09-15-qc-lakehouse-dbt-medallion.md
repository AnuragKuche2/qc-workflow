# QC Lakehouse dbt Medallion (Sub-project C) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a full bronze->silver->gold dbt project (14 staging models, 2 intermediate
models, 5 dimensions, 2 facts) against the `qc_dev.bronze_source` data Sub-project B already
wrote, cleaning the two deliberate defects and masking customer PII, with generic dbt tests
plus custom singular tests re-implementing the Python-side money-chain invariants, and
generated docs/lineage.

**Architecture:** dbt-databricks against a Databricks serverless SQL warehouse. Bronze is
declared as dbt `sources:` (read-only, never written by dbt). A custom `generate_schema_name`
macro routes staging/intermediate models to `qc_dev.silver` and marts to `qc_dev.gold` without
dbt's default schema-prefixing. Staging and intermediate models materialize as views
(cheap, cleaning/composition lenses only); gold marts materialize as tables (queried
repeatedly downstream). No incremental models - bronze is a full-refresh regeneration, not an
incremental append, so there is no natural watermark to key incrementality off.

**Tech Stack:** dbt-databricks (already installed by Sub-project A's `make install-dbt`),
Databricks serverless SQL warehouse, `uv`.

**Spec:** `docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md`, section 8.

## Global Constraints

- dbt uses its OWN env vars (`DBT_DATABRICKS_CATALOG`, `DBT_DATABRICKS_SCHEMA`) and its own dbt
  target (`qc_dev`) for real medallion work. The EXISTING `dev` target and
  `DATABRICKS_CATALOG`/`DATABRICKS_SCHEMA` env vars must stay exactly as they are (pointed at
  `workspace.dev`) - they back Sub-project A's already-working `make smoke-dbt` test and must
  not be repointed or removed.
- Staging (`stg_*`) and intermediate (`int_*`) models materialize as `view`, land in
  `qc_dev.silver`. Marts (`dim_*`/`fct_*`) materialize as `table`, land in `qc_dev.gold`. No
  incremental materialization anywhere in this plan.
- `dim_restaurant` sources ONLY the `restaurants` bronze table - never `_gen_restaurant_profile`
  (generator-only internals; the OLTP source would never know a restaurant's true popularity).
- `stg_customers` must mask `email` and `phone` - the raw values must never appear in any
  downstream (silver or gold) model.
- `fct_deliveries` must not fabricate a delivery-completion timestamp. Scope strictly to what
  `match_attempts` actually supports: match acceptance, attempts-per-order, time-to-accept.
- Every model gets a `description:` in its schema `.yml` (column-level too), plus appropriate
  generic tests (`not_null`/`unique` on keys, `relationships` on every FK, `accepted_values` on
  enum-like columns).
- Never use an em dash character anywhere (SQL, YAML, comments, commit messages) - plain
  hyphen only.
- Never add `Co-Authored-By`/`Claude-Session` trailers to any commit.
- All dbt commands run via `set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse
  uv run dbt <command> --project-dir dbt/qc_lakehouse --target qc_dev` - the same pattern the
  existing `smoke-dbt` Makefile target already uses, with `--target qc_dev` added (the default
  `dev` target must never be used for this plan's work).
- **On dbt's model-then-test order**: unlike Python TDD (write a failing test, then make it
  pass), dbt tests run against already-materialized model output - there is no "run a test
  before the model exists" step in dbt's own tooling. Every task below instead follows: write
  the model SQL, run it live, verify its output directly with a spot-check query, THEN write
  its schema tests and run those. This is the dbt-idiomatic equivalent of the same discipline,
  not a shortcut around it.
- dbt auto-creates a target schema (`CREATE SCHEMA IF NOT EXISTS`) the first time any model
  materializes into it - no manual schema-creation step is needed for `qc_dev.silver`/
  `qc_dev.gold` (unlike the Python-side generator, which needed `ensure_schema_exists` because
  it wrote directly via Spark, not through dbt). The `qc_dev` catalog itself already exists
  (created by Sub-project B).

---

### Task 1: dbt foundation - env vars, schema routing, sources, first staging model

**Files:**
- Modify: `qc-lakehouse/.env.example`
- Modify: `qc-lakehouse/.env` (not committed - gitignored; update your local copy directly)
- Modify: `qc-lakehouse/Makefile`
- Modify: `qc-lakehouse/dbt/qc_lakehouse/profiles.yml`
- Modify: `qc-lakehouse/dbt/qc_lakehouse/dbt_project.yml`
- Create: `qc-lakehouse/dbt/qc_lakehouse/macros/generate_schema_name.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/_staging__sources.yml`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_cities.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/_staging__models.yml`

**Interfaces:**
- Produces: `{{ source('bronze', '<table>') }}` refs for all 14 bronze tables, usable by every
  later task. `{{ ref('stg_cities') }}`, columns `city_id, name, country_code, timezone, lat,
  lon` - consumed by Task 10's `dim_zone`.

- [ ] **Step 1: Add dbt's own env vars**

Add to `qc-lakehouse/.env.example`, after the existing `DATABRICKS_SCHEMA=` line:

```
# dbt's own target catalog/schema for real medallion work (silver/gold). Kept separate from
# DATABRICKS_CATALOG/DATABRICKS_SCHEMA above, which stay pointed at the Sub-project A smoke
# test location - dbt uses a separate "qc_dev" target (see profiles.yml) so the two never
# collide. DBT_DATABRICKS_SCHEMA is a fallback only (every real model overrides its own
# schema via dbt_project.yml's +schema config) - "silver" is a safe default for anything
# unconfigured, never gold.
DBT_DATABRICKS_CATALOG=qc_dev
DBT_DATABRICKS_SCHEMA=silver
```

Add the identical two lines with real values to your local `qc-lakehouse/.env` (not committed):
```
DBT_DATABRICKS_CATALOG=qc_dev
DBT_DATABRICKS_SCHEMA=silver
```

- [ ] **Step 2: Add a second dbt target to profiles.yml**

Replace the full contents of `qc-lakehouse/dbt/qc_lakehouse/profiles.yml`:

```yaml
qc_lakehouse:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: "{{ env_var('DATABRICKS_CATALOG') }}"
      schema: "{{ env_var('DATABRICKS_SCHEMA') }}"
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      auth_type: oauth
      threads: 4
    qc_dev:
      type: databricks
      catalog: "{{ env_var('DBT_DATABRICKS_CATALOG') }}"
      schema: "{{ env_var('DBT_DATABRICKS_SCHEMA') }}"
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      auth_type: oauth
      threads: 4
```

The `dev` target block is byte-for-byte unchanged from before - only the new `qc_dev` block
and the `outputs:` nesting to hold both are new. `host`/`http_path` are shared (same
Databricks SQL warehouse for both targets) - only `catalog`/`schema` differ.

- [ ] **Step 3: Add the schema-routing macro**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

This is dbt's own documented pattern for opting out of the default behavior (which prefixes a
custom schema with the target's default schema, e.g. `dev_silver` instead of `silver`). With
this macro, a model configured `+schema: silver` lands in exactly `qc_dev.silver`, not
`qc_dev.<target-schema>_silver`.

- [ ] **Step 4: Configure per-directory schema routing in dbt_project.yml**

Replace the `models:` block in `qc-lakehouse/dbt/qc_lakehouse/dbt_project.yml`:

```yaml
models:
  qc_lakehouse:
    example:
      +materialized: view
    staging:
      +materialized: view
      +schema: silver
    intermediate:
      +materialized: view
      +schema: silver
    marts:
      +materialized: table
      +schema: gold
```

The `example:` block (backing the existing `hello_dbt` smoke-test model) is unchanged.

- [ ] **Step 5: Declare all 14 bronze sources**

```yaml
# qc-lakehouse/dbt/qc_lakehouse/models/staging/_staging__sources.yml
version: 2

sources:
  - name: bronze
    description: >
      Raw generated quick-commerce data written by the Sub-project B Spark generator
      (reference tables from the spine, fact tables from widen step W1a). Read-only from
      dbt's perspective - never written here.
    database: qc_dev
    schema: bronze_source
    tables:
      - name: cities
      - name: zones
      - name: restaurants
        description: >
          Source-visible restaurant attributes only. Does NOT include popularity_weight or
          price_index - those are generator-only internals (_gen_restaurant_profile), which
          this project must never source, matching the constraint that the OLTP source would
          never know a restaurant's true popularity.
      - name: riders
      - name: menu_items
      - name: rider_payout_tiers
      - name: demand_daily
      - name: demand_hourly
      - name: customers
        description: >
          Contains raw PII (email, phone) - staging must mask both, they must never reach
          silver or gold unmasked.
      - name: orders
        description: >
          delivery_notes carries a deliberate text-noise defect (casing/whitespace mangling)
          on a fraction of non-null rows - staging must clean it.
      - name: order_items
      - name: match_attempts
      - name: payments
      - name: refunds
        description: >
          refund_amount_raw is a STRING, not a decimal - a deliberate defect, sometimes
          accounting-parens formatted (e.g. "(20.47)"). Staging must parse it back into a
          proper decimal(18,2), always as a positive magnitude (parens are a formatting
          quirk, never a sign - the underlying refund amount is always positive).
```

- [ ] **Step 6: Write the first staging model (proves the whole wiring end to end)**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_cities.sql
select
    city_id,
    name,
    country_code,
    timezone,
    lat,
    lon,
    created_at,
    updated_at
from {{ source('bronze', 'cities') }}
```

- [ ] **Step 7: Add stg_cities' schema (descriptions + generic tests)**

```yaml
# qc-lakehouse/dbt/qc_lakehouse/models/staging/_staging__models.yml
version: 2

models:
  - name: stg_cities
    description: One row per city, 1:1 with bronze cities, no cleaning needed.
    columns:
      - name: city_id
        description: Primary key.
        tests: [unique, not_null]
      - name: name
        tests: [not_null]
      - name: country_code
        tests: [not_null]
      - name: timezone
        tests: [not_null]
```

(Later tasks append to this same file rather than creating a new one per model - dbt allows
multiple `models:` blocks across `.yml` files in the same directory, but one growing file per
directory keeps the schema definitions easy to scan in one place, matching how
`schemas.py` does it on the Python side.)

- [ ] **Step 8: Run it live and verify the schema routing actually worked**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev --select stg_cities`

Expected: `Completed successfully`, and the model materializes as a VIEW.

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt show --project-dir dbt/qc_lakehouse --target qc_dev --select stg_cities --limit 3`

Expected: prints 3 sample rows. This confirms `stg_cities` landed in `qc_dev.silver` (not
`qc_dev.default` or some prefixed variant) - if the macro or schema config is wrong, this
step is where it surfaces, since `dbt show` reads from the actual materialized location.

- [ ] **Step 9: Run the generic tests**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev --select stg_cities`

Expected: all tests pass (`PASS=4` for the 4 generic tests above).

- [ ] **Step 10: Confirm the existing smoke-dbt target is unaffected**

Run: `cd qc-lakehouse && make smoke-dbt`

Expected: still passes exactly as before (uses the untouched `dev` target and
`DATABRICKS_CATALOG`/`DATABRICKS_SCHEMA`) - this proves the new `qc_dev` target is additive,
not a modification of the existing one.

- [ ] **Step 11: Add the new Makefile targets**

Append to `qc-lakehouse/Makefile` (and add `dbt-run dbt-test dbt-docs` to the top `.PHONY:`
line, alongside the existing targets):

```makefile
# Real medallion work against qc_dev.silver/qc_dev.gold - never the smoke-test `dev` target.
dbt-run:
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev

dbt-test:
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev

dbt-docs:
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt docs generate --project-dir dbt/qc_lakehouse --target qc_dev
```

- [ ] **Step 12: Commit**

```bash
cd qc-lakehouse
git add .env.example Makefile dbt/qc_lakehouse/profiles.yml dbt/qc_lakehouse/dbt_project.yml \
  dbt/qc_lakehouse/macros/generate_schema_name.sql \
  dbt/qc_lakehouse/models/staging/_staging__sources.yml \
  dbt/qc_lakehouse/models/staging/stg_cities.sql \
  dbt/qc_lakehouse/models/staging/_staging__models.yml
git commit -m "Add dbt medallion foundation: qc_dev target, schema routing, sources, stg_cities"
```

Check for a trailer.

---

### Task 2: Simple reference staging models (zones, restaurants, riders, menu_items, rider_payout_tiers, demand_daily, demand_hourly)

**Files:**
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_zones.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_restaurants.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_riders.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_menu_items.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_rider_payout_tiers.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_demand_daily.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_demand_hourly.sql`
- Modify: `qc-lakehouse/dbt/qc_lakehouse/models/staging/_staging__models.yml`

**Interfaces:**
- Consumes: `{{ source('bronze', '<table>') }}` refs from Task 1.
- Produces: `{{ ref('stg_zones') }}` (zone_id, city_id, name, center_lat, center_lon, ring,
  base_delivery_fee, sla_target_minutes, is_active) - consumed by Task 10's `dim_zone`.
  `{{ ref('stg_restaurants') }}` (restaurant_id, restaurant_ref, name, cuisine_type, city_id,
  zone_id, commission_pct, prep_time_p50_minutes, is_active) - consumed by Task 9's
  `dim_restaurant`. `{{ ref('stg_riders') }}` (rider_id, rider_ref, vehicle_type, payout_tier,
  home_zone_id, is_active) - consumed by Task 9's `dim_rider`. `{{ ref('stg_demand_daily') }}`
  (day_index, order_date, weekday, dow_multiplier, event_multiplier, event_names) - consumed
  by Task 10's `dim_date`. `stg_menu_items`/`stg_rider_payout_tiers`/`stg_demand_hourly` are
  produced for completeness (every bronze source gets a staging model) but not consumed by any
  later task in this plan.

These 7 models are all pure 1:1 type-preserving selects with no joins and no cleaning logic -
bundled into one task since they're genuinely the same shape of work.

- [ ] **Step 1: Write all 7 models**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_zones.sql
select
    zone_id,
    city_id,
    name,
    center_lat,
    center_lon,
    ring,
    base_delivery_fee,
    sla_target_minutes,
    is_active,
    created_at,
    updated_at
from {{ source('bronze', 'zones') }}
```

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_restaurants.sql
select
    restaurant_id,
    restaurant_ref,
    name,
    cuisine_type,
    city_id,
    zone_id,
    lat,
    lon,
    commission_pct,
    payout_account_id,
    prep_time_p50_minutes,
    is_active,
    created_at,
    updated_at
from {{ source('bronze', 'restaurants') }}
```

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_riders.sql
select
    rider_id,
    rider_ref,
    vehicle_type,
    payout_tier,
    home_zone_id,
    payout_account_id,
    is_active,
    created_at,
    updated_at
from {{ source('bronze', 'riders') }}
```

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_menu_items.sql
select
    menu_item_id,
    restaurant_id,
    name,
    category,
    price,
    is_available,
    created_at,
    updated_at
from {{ source('bronze', 'menu_items') }}
```

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_rider_payout_tiers.sql
select
    tier,
    valid_from,
    valid_to,
    base_fare,
    per_km_rate,
    per_minute_rate
from {{ source('bronze', 'rider_payout_tiers') }}
```

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_demand_daily.sql
select
    day_index,
    order_date,
    weekday,
    dow_multiplier,
    event_multiplier,
    noise,
    total_multiplier,
    orders,
    event_names
from {{ source('bronze', 'demand_daily') }}
```

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_demand_hourly.sql
select
    day_index,
    order_date,
    hour,
    orders
from {{ source('bronze', 'demand_hourly') }}
```

- [ ] **Step 2: Append schema/tests for all 7 to `_staging__models.yml`**

Add these entries to the existing `models:` list in
`qc-lakehouse/dbt/qc_lakehouse/models/staging/_staging__models.yml` (keep `stg_cities`'s
existing entry, add these after it):

```yaml
  - name: stg_zones
    description: One row per zone, 1:1 with bronze zones, no cleaning needed.
    columns:
      - name: zone_id
        description: Primary key.
        tests: [unique, not_null]
      - name: city_id
        description: Foreign key to cities.
        tests:
          - not_null
          - relationships:
              to: ref('stg_cities')
              field: city_id
      - name: is_active
        tests: [not_null]

  - name: stg_restaurants
    description: >
      One row per restaurant, 1:1 with bronze restaurants. Deliberately excludes
      popularity_weight/price_index - those are generator-only internals this project must
      never source.
    columns:
      - name: restaurant_id
        description: Primary key.
        tests: [unique, not_null]
      - name: zone_id
        description: Foreign key to zones.
        tests:
          - not_null
          - relationships:
              to: ref('stg_zones')
              field: zone_id
      - name: cuisine_type
        tests: [not_null]
      - name: commission_pct
        tests: [not_null]

  - name: stg_riders
    description: One row per rider, 1:1 with bronze riders, no cleaning needed.
    columns:
      - name: rider_id
        description: Primary key.
        tests: [unique, not_null]
      - name: home_zone_id
        description: Foreign key to zones.
        tests:
          - not_null
          - relationships:
              to: ref('stg_zones')
              field: zone_id
      - name: vehicle_type
        tests: [not_null]
      - name: payout_tier
        description: Foreign key to rider_payout_tiers.tier.
        tests:
          - not_null
          - relationships:
              to: ref('stg_rider_payout_tiers')
              field: tier

  - name: stg_menu_items
    description: One row per menu item, 1:1 with bronze menu_items, no cleaning needed.
    columns:
      - name: menu_item_id
        description: Primary key.
        tests: [unique, not_null]
      - name: restaurant_id
        description: Foreign key to restaurants.
        tests:
          - not_null
          - relationships:
              to: ref('stg_restaurants')
              field: restaurant_id
      - name: price
        tests: [not_null]

  - name: stg_rider_payout_tiers
    description: Rider payout rate card, 1:1 with bronze rider_payout_tiers.
    columns:
      - name: tier
        description: Primary key.
        tests: [unique, not_null]
      - name: base_fare
        tests: [not_null]

  - name: stg_demand_daily
    description: Daily order-volume curve, 1:1 with bronze demand_daily.
    columns:
      - name: day_index
        description: Primary key.
        tests: [unique, not_null]
      - name: order_date
        tests: [unique, not_null]

  - name: stg_demand_hourly
    description: >
      Hourly order-volume curve, 1:1 with bronze demand_hourly. Grain is (day_index, hour).
    columns:
      - name: day_index
        tests: [not_null]
      - name: hour
        tests: [not_null]
      - name: orders
        tests: [not_null]
```

- [ ] **Step 3: Run all 7 live**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev --select stg_zones stg_restaurants stg_riders stg_menu_items stg_rider_payout_tiers stg_demand_daily stg_demand_hourly`

Expected: `Completed successfully` for all 7.

- [ ] **Step 4: Run their tests**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev --select stg_zones stg_restaurants stg_riders stg_menu_items stg_rider_payout_tiers stg_demand_daily stg_demand_hourly`

Expected: all pass. If `stg_riders`' `payout_tier` relationships test fails, check the actual
tier values in bronze `riders`/`rider_payout_tiers` match exactly (case-sensitive string
comparison) before assuming a model bug.

- [ ] **Step 5: Commit**

```bash
cd qc-lakehouse
git add dbt/qc_lakehouse/models/staging/stg_zones.sql dbt/qc_lakehouse/models/staging/stg_restaurants.sql \
  dbt/qc_lakehouse/models/staging/stg_riders.sql dbt/qc_lakehouse/models/staging/stg_menu_items.sql \
  dbt/qc_lakehouse/models/staging/stg_rider_payout_tiers.sql dbt/qc_lakehouse/models/staging/stg_demand_daily.sql \
  dbt/qc_lakehouse/models/staging/stg_demand_hourly.sql dbt/qc_lakehouse/models/staging/_staging__models.yml
git commit -m "Add 7 simple reference staging models"
```

Check for a trailer.

---

### Task 3: stg_customers - PII masking

**Files:**
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_customers.sql`
- Modify: `qc-lakehouse/dbt/qc_lakehouse/models/staging/_staging__models.yml`

**Interfaces:**
- Consumes: `{{ source('bronze', 'customers') }}` (customer_id, customer_ref, email, phone,
  full_name, city_id, home_zone_id, lat, lon, signup_ts, created_at, updated_at).
- Produces: `{{ ref('stg_customers') }}` (customer_id, customer_ref, email_hash, phone_masked,
  full_name, city_id, home_zone_id, lat, lon, signup_ts) - consumed by Task 9's
  `dim_customer`. `email`/`phone` do NOT appear in this model's output at all - only
  `email_hash`/`phone_masked`.

The bronze `customers.email` values look like `aarav.sharma1@gmail.com` (lowercased
first.last + customer_id + domain, from a fixed 5-domain pool - see `customers.py`).
`customers.phone` values look like `+916123456789` (always `+91` + 10 digits). Two different
masking techniques, matching what each PII type is actually used for in practice: email gets
a one-way hash (still equality-joinable/dedupable without exposing the real address); phone
gets a format-preserving partial mask (keeps the country code and last 3 digits visible, the
common real-world UX pattern for displaying a masked phone number).

- [ ] **Step 1: Write the model**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_customers.sql
select
    customer_id,
    customer_ref,
    sha2(lower(trim(email)), 256) as email_hash,
    concat(
        substring(phone, 1, 3),
        repeat('*', length(phone) - 6),
        substring(phone, length(phone) - 2, 3)
    ) as phone_masked,
    full_name,
    city_id,
    home_zone_id,
    lat,
    lon,
    signup_ts,
    created_at,
    updated_at
from {{ source('bronze', 'customers') }}
```

`sha2(..., 256)` and `repeat`/`substring`/`length` are all native Databricks SQL functions -
no UDF needed (this project has already been through a real incident, in Sub-project B,
where Python UDFs turned out to be unsupported on this workspace's serverless compute - stick
to native SQL/Spark-SQL functions throughout this whole sub-project for the same reason).

- [ ] **Step 2: Run it live and spot-check the masking actually worked**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev --select stg_customers`

Expected: `Completed successfully`.

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt show --project-dir dbt/qc_lakehouse --target qc_dev --select stg_customers --limit 5`

Expected: 5 sample rows where `email_hash` is a 64-character hex string (not a readable email
address) and `phone_masked` looks like `+91****789` (10-digit phone -> `+91` + 4 stars +
last 3 digits - verify the actual star count matches `length(phone) - 6`, i.e. exactly
enough stars to cover the middle digits with no gap or overlap against the kept prefix/suffix).

- [ ] **Step 3: Add schema/tests**

Append to `_staging__models.yml`:

```yaml
  - name: stg_customers
    description: >
      One row per customer. email/phone are masked here and never appear unmasked in this or
      any downstream model - email_hash is a one-way SHA-256 hash, phone_masked keeps the
      country code and last 3 digits visible with the middle digits starred out.
    columns:
      - name: customer_id
        description: Primary key.
        tests: [unique, not_null]
      - name: email_hash
        tests: [not_null]
      - name: phone_masked
        tests: [not_null]
      - name: home_zone_id
        description: Foreign key to zones.
        tests:
          - not_null
          - relationships:
              to: ref('stg_zones')
              field: zone_id
```

- [ ] **Step 4: Run the tests**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev --select stg_customers`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cd qc-lakehouse
git add dbt/qc_lakehouse/models/staging/stg_customers.sql dbt/qc_lakehouse/models/staging/_staging__models.yml
git commit -m "Add stg_customers with PII masking (email hash, phone partial mask)"
```

Check for a trailer.

---

### Task 4: stg_orders - text-noise defect cleaning

**Files:**
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_orders.sql`
- Modify: `qc-lakehouse/dbt/qc_lakehouse/models/staging/_staging__models.yml`

**Interfaces:**
- Consumes: `{{ source('bronze', 'orders') }}` (order_id, order_ref, customer_id,
  restaurant_id, zone_id, placed_at, order_status, subtotal, delivery_fee, commission_pct,
  commission_amount, order_total, delivery_notes).
- Produces: `{{ ref('stg_orders') }}` (order_id, order_ref, customer_id, restaurant_id,
  zone_id, placed_at, order_status, subtotal, delivery_fee, commission_pct, commission_amount,
  order_total, delivery_notes, was_text_noise_defect) - consumed by Tasks 7, 8, 11, 12, and
  the singular tests in Task 13.

The defect: `delivery_notes` is drawn from a fixed pool of 6 phrases, all in **sentence
case** (first letter capitalized, rest lowercase - e.g. `"Ring the bell twice"`,
`"No onions please"`). A fraction of non-null rows get mangled into `UPPER`, `LOWER`, 3
leading spaces, or 3 trailing spaces (see `qc-lakehouse/src/qc_lakehouse/generator/defects.py`'s
`mangle_text_casing_and_whitespace` for the exact reference logic - it is no longer called by
the Spark pipeline, but its 4 branches are exactly what needs undoing here). Since every
canonical phrase is sentence-case, cleaning is: trim whitespace, then re-apply sentence case
(uppercase the first character, lowercase the rest) - this recovers the exact original text
regardless of which of the 4 mangling modes was applied, without needing to know which mode
produced any given row.

- [ ] **Step 1: Write the model**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_orders.sql
with cleaned as (
    select
        order_id,
        order_ref,
        customer_id,
        restaurant_id,
        zone_id,
        placed_at,
        order_status,
        subtotal,
        delivery_fee,
        commission_pct,
        commission_amount,
        order_total,
        delivery_notes as delivery_notes_raw,
        case
            when delivery_notes is null then null
            else concat(
                upper(substring(trim(delivery_notes), 1, 1)),
                lower(substring(trim(delivery_notes), 2))
            )
        end as delivery_notes
    from {{ source('bronze', 'orders') }}
)
select
    order_id,
    order_ref,
    customer_id,
    restaurant_id,
    zone_id,
    placed_at,
    order_status,
    subtotal,
    delivery_fee,
    commission_pct,
    commission_amount,
    order_total,
    delivery_notes,
    delivery_notes_raw is not null and delivery_notes_raw != delivery_notes as was_text_noise_defect
from cleaned
```

- [ ] **Step 2: Run it live and spot-check the cleaning**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev --select stg_orders`

Expected: `Completed successfully`.

Run this ad hoc SQL against the live warehouse (via `dbt show` with an inline query, or any
SQL client pointed at `qc_dev.silver.stg_orders` and `qc_dev.bronze_source.orders`) to confirm
real defect rows exist and got cleaned:

```
set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt show --project-dir dbt/qc_lakehouse --target qc_dev --inline "select delivery_notes, was_text_noise_defect from {{ ref('stg_orders') }} where was_text_noise_defect limit 5"
```

Expected: 5 rows, each `delivery_notes` reading as one of the 6 canonical sentence-case
phrases (not still UPPER/lower/padded), `was_text_noise_defect` = true. If this returns 0
rows, `text_noise_defect_rate` in bronze may be genuinely low for the sampled rows - increase
`limit` or confirm bronze actually has defect rows before treating it as a model bug.

- [ ] **Step 3: Add schema/tests**

Append to `_staging__models.yml`:

```yaml
  - name: stg_orders
    description: >
      One row per order. delivery_notes has the text-noise defect (casing/whitespace
      mangling) cleaned - was_text_noise_defect flags which rows needed it, for auditability.
    columns:
      - name: order_id
        description: Primary key.
        tests: [unique, not_null]
      - name: customer_id
        description: Foreign key to customers.
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
      - name: restaurant_id
        description: Foreign key to restaurants.
        tests:
          - not_null
          - relationships:
              to: ref('stg_restaurants')
              field: restaurant_id
      - name: zone_id
        description: Foreign key to zones.
        tests:
          - not_null
          - relationships:
              to: ref('stg_zones')
              field: zone_id
      - name: order_status
        tests:
          - not_null
          - accepted_values:
              values: ['DELIVERED', 'CANCELLED', 'UNFULFILLED']
      - name: subtotal
        tests: [not_null]
      - name: order_total
        tests: [not_null]
```

- [ ] **Step 4: Run the tests**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev --select stg_orders`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cd qc-lakehouse
git add dbt/qc_lakehouse/models/staging/stg_orders.sql dbt/qc_lakehouse/models/staging/_staging__models.yml
git commit -m "Add stg_orders with text-noise defect cleaning"
```

Check for a trailer.

---

### Task 5: Simple fact staging models (order_items, match_attempts, payments)

**Files:**
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_order_items.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_match_attempts.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_payments.sql`
- Modify: `qc-lakehouse/dbt/qc_lakehouse/models/staging/_staging__models.yml`

**Interfaces:**
- Consumes: `{{ source('bronze', 'order_items') }}`, `{{ source('bronze', 'match_attempts') }}`,
  `{{ source('bronze', 'payments') }}`, plus `{{ ref('stg_orders') }}`/`{{ ref('stg_riders') }}`
  for relationship tests.
- Produces: `{{ ref('stg_order_items') }}` (order_item_id, order_id, menu_item_id, quantity,
  unit_price, line_total) - consumed by Task 8's `int_order_economics` and Task 13's singular
  tests. `{{ ref('stg_match_attempts') }}` (match_id, order_id, rider_id, attempt_number,
  offered_at, response, responded_at) - consumed by Task 7's `int_order_matching` and Task
  13's singular tests. `{{ ref('stg_payments') }}` (payment_id, order_id, amount, method,
  status, paid_at) - consumed by Task 8's `int_order_economics` and Task 13's singular tests.

All three are pure 1:1 type-preserving selects, no cleaning needed - bundled for the same
reason as Task 2.

- [ ] **Step 1: Write all 3 models**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_order_items.sql
select
    order_item_id,
    order_id,
    menu_item_id,
    quantity,
    unit_price,
    line_total
from {{ source('bronze', 'order_items') }}
```

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_match_attempts.sql
select
    match_id,
    order_id,
    rider_id,
    attempt_number,
    offered_at,
    response,
    responded_at
from {{ source('bronze', 'match_attempts') }}
```

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_payments.sql
select
    payment_id,
    order_id,
    amount,
    method,
    status,
    paid_at
from {{ source('bronze', 'payments') }}
```

- [ ] **Step 2: Append schema/tests to `_staging__models.yml`**

```yaml
  - name: stg_order_items
    description: One row per order line item, 1:1 with bronze order_items.
    columns:
      - name: order_item_id
        description: Primary key.
        tests: [unique, not_null]
      - name: order_id
        description: Foreign key to orders.
        tests:
          - not_null
          - relationships:
              to: ref('stg_orders')
              field: order_id
      - name: menu_item_id
        description: Foreign key to menu_items.
        tests:
          - not_null
          - relationships:
              to: ref('stg_menu_items')
              field: menu_item_id
      - name: line_total
        tests: [not_null]

  - name: stg_match_attempts
    description: One offer/response row per courier match attempt, 1:1 with bronze match_attempts.
    columns:
      - name: match_id
        description: Primary key.
        tests: [unique, not_null]
      - name: order_id
        description: Foreign key to orders.
        tests:
          - not_null
          - relationships:
              to: ref('stg_orders')
              field: order_id
      - name: rider_id
        description: Foreign key to riders.
        tests:
          - not_null
          - relationships:
              to: ref('stg_riders')
              field: rider_id
      - name: response
        tests:
          - not_null
          - accepted_values:
              values: ['ACCEPTED', 'DECLINED', 'TIMEOUT']

  - name: stg_payments
    description: One row per payable order (DELIVERED/CANCELLED), 1:1 with bronze payments.
    columns:
      - name: payment_id
        description: Primary key.
        tests: [unique, not_null]
      - name: order_id
        description: Foreign key to orders.
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('stg_orders')
              field: order_id
      - name: status
        tests:
          - not_null
          - accepted_values:
              values: ['SUCCESS', 'FAILED']
      - name: amount
        tests: [not_null]
```

- [ ] **Step 3: Run all 3 live**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev --select stg_order_items stg_match_attempts stg_payments`

Expected: `Completed successfully` for all 3.

- [ ] **Step 4: Run their tests**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev --select stg_order_items stg_match_attempts stg_payments`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cd qc-lakehouse
git add dbt/qc_lakehouse/models/staging/stg_order_items.sql dbt/qc_lakehouse/models/staging/stg_match_attempts.sql \
  dbt/qc_lakehouse/models/staging/stg_payments.sql dbt/qc_lakehouse/models/staging/_staging__models.yml
git commit -m "Add stg_order_items, stg_match_attempts, stg_payments"
```

Check for a trailer.

---

### Task 6: stg_refunds - money-text defect cleaning

**Files:**
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_refunds.sql`
- Modify: `qc-lakehouse/dbt/qc_lakehouse/models/staging/_staging__models.yml`

**Interfaces:**
- Consumes: `{{ source('bronze', 'refunds') }}` (refund_id, order_id, payment_id,
  refund_amount_raw, reason, refunded_at).
- Produces: `{{ ref('stg_refunds') }}` (refund_id, order_id, payment_id, refund_amount,
  was_money_text_defect, reason, refunded_at) - consumed by Task 8's `int_order_economics`.
  `refund_amount_raw` does NOT appear in this model's output - only the cleaned
  `refund_amount decimal(18,2)`.

The defect: `refund_amount_raw` is a STRING, sometimes plain (`"20.47"`) and sometimes
accounting-parens formatted (`"(20.47)"`). Per `defects.py`'s own docstring, the underlying
amount is ALWAYS a positive magnitude - the parens are purely a display convention, never a
sign flip. Cleaning: strip any `(`/`)` characters, cast to `decimal(18,2)`.

- [ ] **Step 1: Write the model**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/staging/stg_refunds.sql
select
    refund_id,
    order_id,
    payment_id,
    cast(regexp_replace(refund_amount_raw, '[()]', '') as decimal(18, 2)) as refund_amount,
    refund_amount_raw like '(%' as was_money_text_defect,
    reason,
    refunded_at
from {{ source('bronze', 'refunds') }}
```

- [ ] **Step 2: Run it live and spot-check the cleaning**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev --select stg_refunds`

Expected: `Completed successfully`.

Run: `set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt show --project-dir dbt/qc_lakehouse --target qc_dev --inline "select refund_amount, was_money_text_defect from {{ ref('stg_refunds') }} where was_money_text_defect limit 5"`

Expected: 5 rows, each `refund_amount` a plain positive decimal (no parens, no negative
sign), `was_money_text_defect` = true.

- [ ] **Step 3: Add schema/tests**

Append to `_staging__models.yml`:

```yaml
  - name: stg_refunds
    description: >
      One row per refund. refund_amount_raw's accounting-parens text defect is cleaned into
      a proper positive decimal(18,2) - was_money_text_defect flags which rows needed it.
    columns:
      - name: refund_id
        description: Primary key.
        tests: [unique, not_null]
      - name: order_id
        description: Foreign key to orders.
        tests:
          - not_null
          - relationships:
              to: ref('stg_orders')
              field: order_id
      - name: payment_id
        description: Foreign key to payments.
        tests:
          - not_null
          - relationships:
              to: ref('stg_payments')
              field: payment_id
      - name: refund_amount
        tests: [not_null]
      - name: reason
        tests:
          - not_null
          - accepted_values:
              values: ['CANCELLED', 'QUALITY_ISSUE', 'LATE_DELIVERY', 'MISSING_ITEMS']
```

- [ ] **Step 4: Run the tests**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev --select stg_refunds`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cd qc-lakehouse
git add dbt/qc_lakehouse/models/staging/stg_refunds.sql dbt/qc_lakehouse/models/staging/_staging__models.yml
git commit -m "Add stg_refunds with money-text defect cleaning"
```

Check for a trailer.

---

### Task 7: int_order_matching

**Files:**
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/intermediate/int_order_matching.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/intermediate/_intermediate__models.yml`

**Interfaces:**
- Consumes: `{{ ref('stg_orders') }}`, `{{ ref('stg_match_attempts') }}`.
- Produces: `{{ ref('int_order_matching') }}` (order_id, placed_at, accepted_match_id,
  accepted_rider_id, accepted_offered_at, accepted_at, total_attempts, was_matched) -
  consumed by Task 12's `fct_deliveries`.

Resolves each order's one ACCEPTED match attempt (or none, for UNFULFILLED orders) plus the
total attempt count. The bronze-side generator already guarantees exactly one ACCEPTED
attempt per non-UNFULFILLED order (Task 13's singular tests re-verify this invariant
independently against `stg_match_attempts` directly, not against this model, precisely
because this model's own join silently assumes it - if that assumption were ever violated,
this join would fan out rows without erroring, so the invariant must be checked upstream of
this model, not through it).

- [ ] **Step 1: Write the model**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/intermediate/int_order_matching.sql
with attempt_counts as (
    select order_id, count(*) as total_attempts
    from {{ ref('stg_match_attempts') }}
    group by order_id
),
accepted as (
    select
        order_id,
        match_id as accepted_match_id,
        rider_id as accepted_rider_id,
        offered_at as accepted_offered_at,
        responded_at as accepted_at
    from {{ ref('stg_match_attempts') }}
    where response = 'ACCEPTED'
)
select
    o.order_id,
    o.placed_at,
    a.accepted_match_id,
    a.accepted_rider_id,
    a.accepted_offered_at,
    a.accepted_at,
    coalesce(c.total_attempts, 0) as total_attempts,
    a.accepted_match_id is not null as was_matched
from {{ ref('stg_orders') }} o
left join attempt_counts c on o.order_id = c.order_id
left join accepted a on o.order_id = a.order_id
```

- [ ] **Step 2: Run it live and verify grain**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev --select int_order_matching`

Expected: `Completed successfully`.

Run: `set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt show --project-dir dbt/qc_lakehouse --target qc_dev --inline "select order_id, was_matched, count(*) from {{ ref('int_order_matching') }} group by order_id, was_matched having count(*) > 1 limit 5"`

Expected: 0 rows - confirms the join did not fan out (exactly one row per order_id).

- [ ] **Step 3: Add schema/tests**

```yaml
# qc-lakehouse/dbt/qc_lakehouse/models/intermediate/_intermediate__models.yml
version: 2

models:
  - name: int_order_matching
    description: >
      One row per order, resolving its ACCEPTED match attempt (if any) and total attempt
      count. UNFULFILLED orders have was_matched = false and all accepted_* columns null.
    columns:
      - name: order_id
        description: Primary key (one row per order).
        tests: [unique, not_null]
      - name: total_attempts
        tests: [not_null]
      - name: was_matched
        tests: [not_null]
```

- [ ] **Step 4: Run the tests**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev --select int_order_matching`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cd qc-lakehouse
git add dbt/qc_lakehouse/models/intermediate/int_order_matching.sql dbt/qc_lakehouse/models/intermediate/_intermediate__models.yml
git commit -m "Add int_order_matching"
```

Check for a trailer.

---

### Task 8: int_order_economics

**Files:**
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/intermediate/int_order_economics.sql`
- Modify: `qc-lakehouse/dbt/qc_lakehouse/models/intermediate/_intermediate__models.yml`

**Interfaces:**
- Consumes: `{{ ref('stg_orders') }}`, `{{ ref('stg_order_items') }}`,
  `{{ ref('stg_refunds') }}`.
- Produces: `{{ ref('int_order_economics') }}` (order_id, order_ref, customer_id,
  restaurant_id, zone_id, placed_at, order_status, subtotal, delivery_fee, commission_pct,
  commission_amount, order_total, refund_amount, net_revenue, item_count) - consumed by Task
  11's `fct_orders`.

One row per order with the full money picture: `order_total` carried through as-is from
`stg_orders`, `refund_amount` summed from `stg_refunds` (0 if none), `net_revenue = order_total
- refund_amount`, `item_count` from `stg_order_items`.

- [ ] **Step 1: Write the model**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/intermediate/int_order_economics.sql
with item_totals as (
    select order_id, count(*) as item_count
    from {{ ref('stg_order_items') }}
    group by order_id
),
refund_totals as (
    select order_id, sum(refund_amount) as total_refunded
    from {{ ref('stg_refunds') }}
    group by order_id
)
select
    o.order_id,
    o.order_ref,
    o.customer_id,
    o.restaurant_id,
    o.zone_id,
    o.placed_at,
    o.order_status,
    o.subtotal,
    o.delivery_fee,
    o.commission_pct,
    o.commission_amount,
    o.order_total,
    coalesce(r.total_refunded, 0) as refund_amount,
    o.order_total - coalesce(r.total_refunded, 0) as net_revenue,
    coalesce(i.item_count, 0) as item_count
from {{ ref('stg_orders') }} o
left join item_totals i on o.order_id = i.order_id
left join refund_totals r on o.order_id = r.order_id
```

- [ ] **Step 2: Run it live and spot-check**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev --select int_order_economics`

Expected: `Completed successfully`.

Run: `set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt show --project-dir dbt/qc_lakehouse --target qc_dev --inline "select count(*) as total, sum(case when net_revenue < 0 then 1 else 0 end) as negative_revenue_orders from {{ ref('int_order_economics') }}"`

Expected: `negative_revenue_orders` = 0 (a refund should never exceed the order total it's
refunding from, by construction of the generator - if this is nonzero, investigate before
proceeding, it indicates either a bronze data issue or a join fan-out in this model).

- [ ] **Step 3: Append schema/tests**

```yaml
  - name: int_order_economics
    description: >
      One row per order with the full money picture - subtotal/fees carried from stg_orders,
      refunds summed from stg_refunds, net_revenue computed as order_total minus refunds.
    columns:
      - name: order_id
        description: Primary key (one row per order).
        tests: [unique, not_null]
      - name: net_revenue
        tests: [not_null]
      - name: refund_amount
        tests: [not_null]
```

- [ ] **Step 4: Run the tests**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev --select int_order_economics`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cd qc-lakehouse
git add dbt/qc_lakehouse/models/intermediate/int_order_economics.sql dbt/qc_lakehouse/models/intermediate/_intermediate__models.yml
git commit -m "Add int_order_economics"
```

Check for a trailer.

---

### Task 9: Simple gold dimensions (dim_customer, dim_restaurant, dim_rider)

**Files:**
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/marts/dim_customer.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/marts/dim_restaurant.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/marts/dim_rider.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/marts/_marts__models.yml`

**Interfaces:**
- Consumes: `{{ ref('stg_customers') }}`, `{{ ref('stg_restaurants') }}`, `{{ ref('stg_riders') }}`.
- Produces: `{{ ref('dim_customer') }}` (customer_id, customer_ref, email_hash, phone_masked,
  full_name, city_id, home_zone_id, lat, lon, signup_ts). `{{ ref('dim_restaurant') }}`
  (restaurant_id, restaurant_ref, name, cuisine_type, city_id, zone_id, commission_pct,
  prep_time_p50_minutes, is_active). `{{ ref('dim_rider') }}` (rider_id, rider_ref,
  vehicle_type, payout_tier, home_zone_id, is_active). All three consumed by Task 11's
  `fct_orders` and/or Task 12's `fct_deliveries` for FK relationship tests (the facts
  themselves store only the FK id, not a join - standard star-schema practice).

Pure passthroughs from staging (no new logic) - materialized as `table` per the
`marts:` config in `dbt_project.yml`, landing in `qc_dev.gold`.

- [ ] **Step 1: Write all 3 models**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/marts/dim_customer.sql
select
    customer_id,
    customer_ref,
    email_hash,
    phone_masked,
    full_name,
    city_id,
    home_zone_id,
    lat,
    lon,
    signup_ts
from {{ ref('stg_customers') }}
```

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/marts/dim_restaurant.sql
select
    restaurant_id,
    restaurant_ref,
    name,
    cuisine_type,
    city_id,
    zone_id,
    commission_pct,
    prep_time_p50_minutes,
    is_active
from {{ ref('stg_restaurants') }}
```

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/marts/dim_rider.sql
select
    rider_id,
    rider_ref,
    vehicle_type,
    payout_tier,
    home_zone_id,
    is_active
from {{ ref('stg_riders') }}
```

- [ ] **Step 2: Add schema/tests**

```yaml
# qc-lakehouse/dbt/qc_lakehouse/models/marts/_marts__models.yml
version: 2

models:
  - name: dim_customer
    description: Customer dimension. email/phone are masked (see stg_customers).
    columns:
      - name: customer_id
        description: Primary key.
        tests: [unique, not_null]

  - name: dim_restaurant
    description: >
      Restaurant dimension. Deliberately excludes popularity/price-index internals - see
      stg_restaurants.
    columns:
      - name: restaurant_id
        description: Primary key.
        tests: [unique, not_null]

  - name: dim_rider
    description: Rider dimension.
    columns:
      - name: rider_id
        description: Primary key.
        tests: [unique, not_null]
```

- [ ] **Step 3: Run all 3 live**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev --select dim_customer dim_restaurant dim_rider`

Expected: `Completed successfully` for all 3, materialized as TABLEs in `qc_dev.gold`.

- [ ] **Step 4: Run their tests**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev --select dim_customer dim_restaurant dim_rider`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cd qc-lakehouse
git add dbt/qc_lakehouse/models/marts/dim_customer.sql dbt/qc_lakehouse/models/marts/dim_restaurant.sql \
  dbt/qc_lakehouse/models/marts/dim_rider.sql dbt/qc_lakehouse/models/marts/_marts__models.yml
git commit -m "Add dim_customer, dim_restaurant, dim_rider"
```

Check for a trailer.

---

### Task 10: dim_zone and dim_date

**Files:**
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/marts/dim_zone.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/marts/dim_date.sql`
- Modify: `qc-lakehouse/dbt/qc_lakehouse/models/marts/_marts__models.yml`

**Interfaces:**
- Consumes: `{{ ref('stg_zones') }}`, `{{ ref('stg_cities') }}`, `{{ ref('stg_demand_daily') }}`.
- Produces: `{{ ref('dim_zone') }}` (zone_id, zone_name, center_lat, center_lon, ring,
  base_delivery_fee, sla_target_minutes, is_active, city_id, city_name, country_code,
  timezone) - consumed by Task 11's `fct_orders` and Task 12's `fct_deliveries`.
  `{{ ref('dim_date') }}` (date_day, day_index, weekday, dow_multiplier, event_multiplier,
  is_weekend, event_names) - consumed by Task 11's `fct_orders` and Task 12's
  `fct_deliveries`.

`dim_zone` folds city attributes directly in rather than a separate `dim_city` (only 3 cities
total - not worth a standalone dimension at this data volume, per spec 8.3). `dim_date` is
derived directly from `stg_demand_daily` (which already has exactly one row per date in the
generator's window) rather than an independently generated date spine - simpler, and exactly
matches the actual data window with no risk of drifting out of sync with it.
`is_weekend` is computed from the date itself via `dayofweek()` (Spark SQL: 1=Sunday,
7=Saturday) rather than from the `weekday` column, since that avoids depending on knowing
which weekday-numbering convention the Python generator used internally.

- [ ] **Step 1: Write both models**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/marts/dim_zone.sql
select
    z.zone_id,
    z.name as zone_name,
    z.center_lat,
    z.center_lon,
    z.ring,
    z.base_delivery_fee,
    z.sla_target_minutes,
    z.is_active,
    c.city_id,
    c.name as city_name,
    c.country_code,
    c.timezone
from {{ ref('stg_zones') }} z
join {{ ref('stg_cities') }} c on z.city_id = c.city_id
```

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/marts/dim_date.sql
select
    order_date as date_day,
    day_index,
    weekday,
    dow_multiplier,
    event_multiplier,
    dayofweek(order_date) in (1, 7) as is_weekend,
    event_names
from {{ ref('stg_demand_daily') }}
```

- [ ] **Step 2: Run both live and verify grain**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev --select dim_zone dim_date`

Expected: `Completed successfully` for both.

Run: `set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt show --project-dir dbt/qc_lakehouse --target qc_dev --inline "select zone_id, count(*) from {{ ref('dim_zone') }} group by zone_id having count(*) > 1 limit 5"`

Expected: 0 rows (the city join must not fan out - each zone has exactly one city).

- [ ] **Step 3: Append schema/tests**

```yaml
  - name: dim_zone
    description: Zone dimension, with city attributes folded in directly.
    columns:
      - name: zone_id
        description: Primary key.
        tests: [unique, not_null]
      - name: city_id
        tests: [not_null]

  - name: dim_date
    description: >
      Date dimension covering the generator's demand window, one row per date, derived
      directly from stg_demand_daily.
    columns:
      - name: date_day
        description: Primary key.
        tests: [unique, not_null]
      - name: is_weekend
        tests: [not_null]
```

- [ ] **Step 4: Run the tests**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev --select dim_zone dim_date`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cd qc-lakehouse
git add dbt/qc_lakehouse/models/marts/dim_zone.sql dbt/qc_lakehouse/models/marts/dim_date.sql dbt/qc_lakehouse/models/marts/_marts__models.yml
git commit -m "Add dim_zone, dim_date"
```

Check for a trailer.

---

### Task 11: fct_orders

**Files:**
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/marts/fct_orders.sql`
- Modify: `qc-lakehouse/dbt/qc_lakehouse/models/marts/_marts__models.yml`

**Interfaces:**
- Consumes: `{{ ref('int_order_economics') }}`.
- Produces: `{{ ref('fct_orders') }}` (order_id, order_ref, customer_id, restaurant_id,
  zone_id, date_day, placed_at, order_status, subtotal, delivery_fee, commission_pct,
  commission_amount, order_total, refund_amount, net_revenue, item_count) - grain: one row
  per order. Consumed by Task 13's singular test for subtotal-vs-line-items.

The order-economics gold fact. `date_day` is `date(placed_at)`, joinable to `dim_date`.

- [ ] **Step 1: Write the model**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/marts/fct_orders.sql
select
    order_id,
    order_ref,
    customer_id,
    restaurant_id,
    zone_id,
    date(placed_at) as date_day,
    placed_at,
    order_status,
    subtotal,
    delivery_fee,
    commission_pct,
    commission_amount,
    order_total,
    refund_amount,
    net_revenue,
    item_count
from {{ ref('int_order_economics') }}
```

- [ ] **Step 2: Run it live**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev --select fct_orders`

Expected: `Completed successfully`, materialized as a TABLE in `qc_dev.gold`.

- [ ] **Step 3: Append schema/tests**

```yaml
  - name: fct_orders
    description: >
      Order economics fact - grain is one row per order. subtotal/fees/commission carried
      from bronze via stg_orders; refund_amount and net_revenue computed in
      int_order_economics.
    columns:
      - name: order_id
        description: Primary key (one row per order).
        tests: [unique, not_null]
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_id
      - name: restaurant_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_restaurant')
              field: restaurant_id
      - name: zone_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_zone')
              field: zone_id
      - name: date_day
        tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_day
      - name: net_revenue
        tests: [not_null]
```

- [ ] **Step 4: Run the tests**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev --select fct_orders`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cd qc-lakehouse
git add dbt/qc_lakehouse/models/marts/fct_orders.sql dbt/qc_lakehouse/models/marts/_marts__models.yml
git commit -m "Add fct_orders"
```

Check for a trailer.

---

### Task 12: fct_deliveries

**Files:**
- Create: `qc-lakehouse/dbt/qc_lakehouse/models/marts/fct_deliveries.sql`
- Modify: `qc-lakehouse/dbt/qc_lakehouse/models/marts/_marts__models.yml`

**Interfaces:**
- Consumes: `{{ ref('int_order_matching') }}`, `{{ ref('stg_orders') }}`.
- Produces: `{{ ref('fct_deliveries') }}` (order_id, customer_id, restaurant_id, zone_id,
  date_day, placed_at, accepted_rider_id, total_attempts, was_matched, accepted_at,
  seconds_to_accept) - grain: one row per order's delivery outcome.

The operations fact - deliberately scoped to match-acceptance metrics only, per the
constraint that no delivery-completion timestamp exists in current bronze data.
`seconds_to_accept` is null for unmatched (UNFULFILLED) orders.

- [ ] **Step 1: Write the model**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/models/marts/fct_deliveries.sql
select
    om.order_id,
    o.customer_id,
    o.restaurant_id,
    o.zone_id,
    date(om.placed_at) as date_day,
    om.placed_at,
    om.accepted_rider_id,
    om.total_attempts,
    om.was_matched,
    om.accepted_at,
    case
        when om.accepted_at is not null
        then unix_timestamp(om.accepted_at) - unix_timestamp(om.placed_at)
    end as seconds_to_accept
from {{ ref('int_order_matching') }} om
join {{ ref('stg_orders') }} o on om.order_id = o.order_id
```

- [ ] **Step 2: Run it live and spot-check**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev --select fct_deliveries`

Expected: `Completed successfully`, materialized as a TABLE in `qc_dev.gold`.

Run: `set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt show --project-dir dbt/qc_lakehouse --target qc_dev --inline "select count(*) as total, sum(case when was_matched and seconds_to_accept is null then 1 else 0 end) as broken from {{ ref('fct_deliveries') }}"`

Expected: `broken` = 0 (every matched order must have a non-null time-to-accept).

- [ ] **Step 3: Append schema/tests**

```yaml
  - name: fct_deliveries
    description: >
      Delivery-operations fact - grain is one row per order's match outcome. Deliberately
      does NOT measure delivery-completion SLA - no such timestamp exists in bronze data yet
      (see spec section 9). Measures match acceptance, attempts-per-order, time-to-accept only.
    columns:
      - name: order_id
        description: Primary key (one row per order).
        tests: [unique, not_null]
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_id
      - name: restaurant_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_restaurant')
              field: restaurant_id
      - name: was_matched
        tests: [not_null]
      - name: total_attempts
        tests: [not_null]
      - name: zone_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_zone')
              field: zone_id
```

- [ ] **Step 4: Run the tests**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev --select fct_deliveries`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cd qc-lakehouse
git add dbt/qc_lakehouse/models/marts/fct_deliveries.sql dbt/qc_lakehouse/models/marts/_marts__models.yml
git commit -m "Add fct_deliveries"
```

Check for a trailer.

---

### Task 13: Custom singular tests - the money-chain invariants, ported to SQL

**Files:**
- Create: `qc-lakehouse/dbt/qc_lakehouse/tests/assert_fct_orders_subtotal_matches_line_items.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/tests/assert_payment_amount_matches_order_total.sql`
- Create: `qc-lakehouse/dbt/qc_lakehouse/tests/assert_exactly_one_accepted_match_per_matched_order.sql`

**Interfaces:**
- Consumes: `{{ ref('fct_orders') }}`, `{{ ref('stg_order_items') }}`,
  `{{ ref('stg_payments') }}`, `{{ ref('stg_orders') }}`, `{{ ref('stg_match_attempts') }}`.
- Produces: nothing consumed elsewhere - these are dbt's own test artifacts, run via
  `dbt test`.

Re-implements 3 of the 5 invariants `qc-lakehouse/src/qc_lakehouse/generator/fact_writer.py`
already checks in Python (`check_subtotal_matches_line_items`,
`check_payment_amount_matches_order_total`, `check_exactly_one_accepted_match_per_matched_order`),
now as dbt singular tests - each verified independently by a different tool at a different
layer. A dbt singular test passes when its query returns ZERO rows (each query below selects
exactly the rows that VIOLATE the invariant).

Two of the three are tested against the layer where the check is actually meaningful:
`subtotal-vs-line-items` runs against gold (`fct_orders`, since that is what a consumer of
the dimensional model would trust); `exactly-one-accepted-match` and
`payment-amount-matches-order-total` run against silver (`stg_*`) directly, since the
`exactly-one-accepted` check specifically guards an assumption `int_order_matching`'s own
join silently relies on (checking it downstream of that join would miss exactly the failure
mode it exists to catch), and payment status/amount isn't carried into the gold dimensional
model at all (no `fct_payments` exists in this design).

- [ ] **Step 1: Write the subtotal-vs-line-items test**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/tests/assert_fct_orders_subtotal_matches_line_items.sql
-- Fails (returns rows) if any order's fct_orders.subtotal disagrees with the sum of its
-- own order_items.line_total - the same invariant fact_writer.py's
-- check_subtotal_matches_line_items already guards in Python, re-verified here over the
-- gold layer to confirm this dbt project's transformations didn't silently corrupt it.
select
    fo.order_id,
    fo.subtotal,
    sum(oi.line_total) as computed_subtotal
from {{ ref('fct_orders') }} fo
join {{ ref('stg_order_items') }} oi on fo.order_id = oi.order_id
group by fo.order_id, fo.subtotal
having fo.subtotal != sum(oi.line_total)
```

- [ ] **Step 2: Write the payment-amount-matches-order-total test**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/tests/assert_payment_amount_matches_order_total.sql
-- Fails (returns rows) if any SUCCESS payment's amount disagrees with its order's
-- order_total - the same invariant fact_writer.py's
-- check_payment_amount_matches_order_total already guards in Python.
select
    p.order_id,
    p.amount,
    o.order_total
from {{ ref('stg_payments') }} p
join {{ ref('stg_orders') }} o on p.order_id = o.order_id
where p.status = 'SUCCESS' and p.amount != o.order_total
```

- [ ] **Step 3: Write the exactly-one-accepted-match test**

```sql
-- qc-lakehouse/dbt/qc_lakehouse/tests/assert_exactly_one_accepted_match_per_matched_order.sql
-- Fails (returns rows) if any DELIVERED/CANCELLED order does not have exactly one ACCEPTED
-- match attempt, or any UNFULFILLED order has one or more - the same invariant
-- fact_writer.py's check_exactly_one_accepted_match_per_matched_order already guards in
-- Python. Runs against stg_match_attempts directly (not int_order_matching), since that
-- model's own join silently assumes this invariant already holds.
with accepted_counts as (
    select order_id, count(*) as accepted_count
    from {{ ref('stg_match_attempts') }}
    where response = 'ACCEPTED'
    group by order_id
)
select
    o.order_id,
    o.order_status,
    coalesce(ac.accepted_count, 0) as accepted_count
from {{ ref('stg_orders') }} o
left join accepted_counts ac on o.order_id = ac.order_id
where
    (o.order_status = 'UNFULFILLED' and coalesce(ac.accepted_count, 0) != 0)
    or (o.order_status != 'UNFULFILLED' and coalesce(ac.accepted_count, 0) != 1)
```

- [ ] **Step 4: Run all 3 live**

Run: `cd qc-lakehouse && set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev --select assert_fct_orders_subtotal_matches_line_items assert_payment_amount_matches_order_total assert_exactly_one_accepted_match_per_matched_order`

Expected: all 3 `PASS` (0 rows returned by each). If any fails, this is a genuine bug -
either in this dbt project's transformation logic, or (less likely, since Sub-project B's own
Python-side checks already passed on this exact live data) in the underlying bronze data -
investigate and fix the dbt model, do not weaken or delete the test to force a pass.

- [ ] **Step 5: Commit**

```bash
cd qc-lakehouse
git add dbt/qc_lakehouse/tests/assert_fct_orders_subtotal_matches_line_items.sql \
  dbt/qc_lakehouse/tests/assert_payment_amount_matches_order_total.sql \
  dbt/qc_lakehouse/tests/assert_exactly_one_accepted_match_per_matched_order.sql
git commit -m "Add custom singular tests re-implementing the money-chain invariants in SQL"
```

Check for a trailer.

---

### Task 14: Docs, lineage, and README

**Files:**
- Modify: `qc-lakehouse/README.md`

**Interfaces:**
- Consumes: every model/schema `.yml` written by Tasks 1-13 (their `description:` fields feed
  directly into the generated docs site).
- Produces: nothing consumed by later tasks - this is the last content task before Task 15's
  final live verification.

The `make dbt-docs` target already exists (Task 1). This task verifies it actually produces a
working docs site with visible lineage, and documents the whole sub-project in the README.

- [ ] **Step 1: Generate the docs site**

Run: `cd qc-lakehouse && make dbt-docs`

Expected: succeeds, writes `dbt/qc_lakehouse/target/catalog.json` and
`dbt/qc_lakehouse/target/manifest.json`.

- [ ] **Step 2: Spot-check the manifest for lineage completeness**

Run:

```bash
cd qc-lakehouse && python3 -c "
import json
m = json.load(open('dbt/qc_lakehouse/target/manifest.json'))
nodes = [n for n in m['nodes'].values() if n['resource_type'] == 'model']
print(f'{len(nodes)} models in manifest')
fct_orders = m['nodes']['model.qc_lakehouse.fct_orders']
print(f'fct_orders depends_on: {fct_orders[\"depends_on\"][\"nodes\"]}')"
```

Expected: 24 models (14 staging + 2 intermediate + 5 dims + 2 facts + 1 example/hello_dbt
- adjust the expected count if any task above added a different number than planned, but
verify the actual count rather than assume). `fct_orders`'s `depends_on` should trace back
through `int_order_economics` to `stg_orders`/`stg_order_items`/`stg_refunds` - confirming
the DAG is real lineage, not a flat list.

- [ ] **Step 3: Add the README section**

Append to `qc-lakehouse/README.md`, after the existing "Money-chain fact tables" section:

```markdown
## dbt medallion transformation (Sub-project C)

Full bronze -> silver -> gold dbt project against `qc_dev.bronze_source`, per
`docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md` section 8.

- **Silver** (`qc_dev.silver`): 14 staging models (1:1 with each bronze source, type
  casting), plus 2 intermediate models for cross-table logic. Cleans both of W1a's
  deliberate defects (`stg_orders`'s text-noise mangling, `stg_refunds`'s money-as-text
  formatting - both flagged via a `was_*_defect` boolean column for auditability) and masks
  customer PII (`stg_customers`: email one-way hashed, phone partially masked).
- **Gold** (`qc_dev.gold`): a proper dimensional model - `dim_customer`, `dim_restaurant`,
  `dim_rider`, `dim_zone`, `dim_date`, plus `fct_orders` (order economics) and
  `fct_deliveries` (match/courier-assignment operations - deliberately NOT a full
  delivery-SLA fact, since no delivery-completion timestamp exists in bronze data yet).
- **Testing**: generic dbt tests (not_null/unique/relationships/accepted_values) on every
  model, plus 3 custom singular SQL tests re-implementing the money-chain invariants
  `fact_writer.py` already checks in Python - the same invariants verified independently by
  two different tools at two different layers.

```bash
make dbt-run    # builds every silver + gold model against qc_dev
make dbt-test   # runs every generic + custom test
make dbt-docs   # generates the browsable docs site + lineage DAG
```

Requires `make generate-reference-data` and `make generate-fact-data` to have been run first
(dbt only reads `qc_dev.bronze_source`, never writes to it).
```

- [ ] **Step 4: Commit**

```bash
cd qc-lakehouse
git add README.md
git commit -m "Document Sub-project C dbt medallion in README"
```

Check for a trailer.

---

### Task 15: Full live build, full test suite, and idempotency check

**Files:** none (verification-only task, no new files).

**Interfaces:** none - this is the acceptance test for the whole sub-project.

This is the actual completion criterion for Sub-project C, mirroring how W1a's final task
was a full live run plus idempotency check.

- [ ] **Step 1: Full clean build from scratch**

Run: `cd qc-lakehouse && make dbt-run`

Expected: all 24 models (14 staging + 2 intermediate + 5 dims + 2 facts + hello_dbt) build
successfully against live Databricks. If this is slower than expected, that's fine - this
runs once per full rebuild, not per query, and none of these tables are enormous relative to
the fact-table generator's own live run (which already proved Databricks serverless compute
here handles millions of rows fine).

- [ ] **Step 2: Full test suite**

Run: `cd qc-lakehouse && make dbt-test`

Expected: every generic test plus all 3 custom singular tests pass. If anything fails, this
is a genuine bug (in this dbt project's SQL, or - less likely - in bronze data itself,
already validated by Sub-project B's own Python checks) - diagnose and fix properly, do not
weaken a test to force a pass.

- [ ] **Step 3: Idempotency check - rerun and confirm identical results**

Run: `cd qc-lakehouse && make dbt-run && make dbt-test` a second time.

Expected: identical success - `dbt run` on views/tables built from a `select` over
already-static bronze data is naturally idempotent (no incremental state to diverge), so a
second run should produce byte-identical row counts. Spot-check with:

`set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt show --project-dir dbt/qc_lakehouse --target qc_dev --inline "select count(*) from {{ ref('fct_orders') }}"`

before and after the second run - confirm the counts match exactly.

- [ ] **Step 4: Final `make check` to confirm no Python-side regression**

Run: `cd qc-lakehouse && make check`

Expected: unaffected, still passing (this sub-project added no Python code) - confirms
nothing in this dbt work accidentally touched the Python package.

No commit needed for this task - it is verification-only. If Step 1 or 2 surfaces a bug in an
earlier task's model, fix it in that model's own file and amend forward with a new commit
referencing which task's model was fixed, rather than editing history.
