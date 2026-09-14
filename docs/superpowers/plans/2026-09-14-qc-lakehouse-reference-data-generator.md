# QC Lakehouse - Sub-project B: Reference Data Generator (spine) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the reference-data half of an already-validated Databricks notebook (cities, zones, restaurants, riders, menu items, customers, payout tiers, demand curve) into tested `qc_lakehouse` code that writes real data to `qc_dev.bronze_source` on live Databricks - proving the reference layer works end to end before the fact-table/Auto-Loader widen phase.

**Architecture:** A new `qc_lakehouse.generator` subpackage holds pure, unit-tested math/domain functions (no Spark dependency except for the one Spark-scale piece, customer generation) plus an orchestrating `writer.py` that builds every table, runs real `assert`-based integrity checks *before* writing anything, and writes to Delta. A thin entrypoint script (`scripts/generate_reference_data.py`) wires this to Sub-project A's already-built `load_settings()`/`build_databricks_session()` and is both the production entrypoint and this sub-project's live smoke test.

**Tech Stack:** Python 3.12, `uv`, the existing `qc-lakehouse` project (PySpark 3.5.x for local dev-loop, `databricks-connect`/`databricks-sdk` via `.venv-databricks`), `pytest`.

**Spec:** `docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md` (section 6 is this sub-project's detailed design; sections 1-5, 7 are overall project context - section 5 is Sub-project A, already built and merged).

**Source material:** `docs/superpowers/plans/2026-09-14-qc-lakehouse-b-notebook-source.txt` - the original notebook's cell-by-cell source, extracted from a Databricks HTML export. Cell numbers referenced below (e.g. "CELL 4") are exact `grep -n "^--- CELL" ...` markers in that file - use them to locate exact source content. This plan's code differs from the notebook in specific, deliberate ways (see Global Constraints); everywhere else, port values and logic verbatim.

## Global Constraints

- **Faithful port, not a rewrite.** Domain logic, distributions, and constants (cuisines, menus, event catalogue, demand curve shape, etc.) are carried over from the source notebook as-is - they are already correct and were already empirically tuned. Do not "improve" the domain modeling.
- **Deliberate deviations from the notebook** (the only changes allowed):
  1. Every pure function gets a `seed`/config value passed as an explicit parameter instead of reading a module-level global (the notebook's `stream_seed(name)` read a global `SEED`; this plan's `stream_seed(seed, name)` takes it explicitly). This is required to make the functions genuinely unit-testable - it is not scope creep, it is the mechanism by which "real pytest coverage" becomes possible.
  2. The notebook's `print`-based sanity checks (referential integrity, hour/day conservation, rider capacity) become real `assert` statements that raise before any data is written - restructured to run against the in-memory Python/Spark objects the generator already built, not by writing first and querying the written Delta tables after (which the notebook did, but which would make the checks impossible to unit test without live Databricks).
  3. `CATALOG`/`SCHEMA`/`DAYS`/`SEED`/etc. become fields on a `GeneratorConfig` dataclass instead of hardcoded module-level constants.
  4. Positional tuple/list entity records (e.g. `restaurants` as `list[list]`, accessed by index) are **not** touched - this is explicitly deferred, not part of this port.
- **No Auto Loader, no fact tables.** Orders, `order_events`, `courier_shifts`, `gps_pings` are out of scope. Every table this plan builds is written directly to Delta via `saveAsTable` - no file-landing, no Auto Loader.
- **Real data lands in `qc_dev.bronze_source`** - not `workspace.dev` (Sub-project A's `.env`-configured catalog/schema, which stays reserved for A's own throwaway smoke-test tables and must not be reused for real pipeline data).
- **Reuse Sub-project A's session machinery exactly as-is:** `qc_lakehouse.config.load_settings() -> Settings` and `qc_lakehouse.databricks_session.build_databricks_session(settings) -> DatabricksSession` (already pass an explicit host, already serverless). Do not modify either file. `qc_lakehouse.databricks_session.ensure_schema_exists(session, catalog, schema)` is also already exported and reusable for creating `qc_dev.bronze_source` (it only takes plain strings, not a `Settings` object).
- **Runs via Databricks Connect from local** (the `.venv-databricks` environment Sub-project A already built), not as a deployed Databricks Job. `make smoke-databricks`-style execution, not Jobs API packaging.
- **Idempotent by construction:** every write is `mode("overwrite")` - full-table regeneration from a fixed seed, not an incremental append. This is a deliberate, already-approved exception to the project's general MERGE-based idempotency rule (see spec section 3): regenerating the same seed produces the same deterministic content, so overwrite is safe to retry, unlike an accumulating append would be.
- **Python tooling is `uv` end to end.**

---

### Task 1: Core allocation/random helpers (`math_utils.py`)

**Files:**
- Create: `qc-lakehouse/src/qc_lakehouse/generator/__init__.py` (empty)
- Create: `qc-lakehouse/src/qc_lakehouse/generator/math_utils.py`
- Test: `qc-lakehouse/tests/test_math_utils.py`

**Interfaces:**
- Produces: `stream_seed(seed: int, name: str) -> int`, `allocate(total: Decimal, weights: Sequence, places: int = 2) -> list[Decimal]`, `split_by_share(total: int, shares: Sequence) -> list[int]`, `zone_density(n: int, decay: float = 6.0) -> list[float]`, `zipf(n: int, exponent: float = 0.85, offset: float = 4.0) -> list[float]`, `pick(rng: random.Random, options: Sequence[tuple]) -> object`. All in `qc_lakehouse.generator.math_utils`. Every later task that needs randomness or weighted splitting imports from here.

Source: CELL 3 (stream_seed, allocate, split_by_share) and CELL 5 (zone_density, zipf, pick) of the notebook source file.

- [ ] **Step 1: Write the failing tests**

```python
# qc-lakehouse/tests/test_math_utils.py
from decimal import Decimal
import random

from qc_lakehouse.generator.math_utils import (
    allocate,
    pick,
    split_by_share,
    stream_seed,
    zipf,
    zone_density,
)


def test_stream_seed_is_deterministic_and_seed_dependent():
    a1 = stream_seed(42, "riders")
    a2 = stream_seed(42, "riders")
    b = stream_seed(43, "riders")
    c = stream_seed(42, "customers")
    assert a1 == a2
    assert a1 != b
    assert a1 != c


def test_allocate_sums_exactly_to_total_with_no_drift():
    total = Decimal("100.00")
    weights = [1, 1, 1]
    parts = allocate(total, weights, places=2)
    assert sum(parts) == total
    assert len(parts) == 3


def test_allocate_largest_remainder_gets_the_extra_unit():
    # 10 split 3 ways by equal weight: 3.33, 3.33, 3.33 -> floors 3.33 x3 = 9.99, one part
    # must get the leftover 0.01. All three have an identical remainder, so the tiebreak
    # (lowest index) decides - index 0 gets it.
    parts = allocate(Decimal("10.00"), [1, 1, 1], places=2)
    assert sum(parts) == Decimal("10.00")
    assert parts[0] == Decimal("3.34")
    assert parts[1] == Decimal("3.33")
    assert parts[2] == Decimal("3.33")


def test_allocate_rejects_zero_weight_sum():
    import pytest

    with pytest.raises(ValueError):
        allocate(Decimal("10"), [0, 0], places=2)


def test_split_by_share_sums_exactly_to_total():
    parts = split_by_share(100, [0.42, 0.35, 0.23])
    assert sum(parts) == 100
    assert len(parts) == 3
    assert all(isinstance(p, int) for p in parts)


def test_zone_density_decays_and_sums_to_one():
    weights = zone_density(5, decay=6.0)
    assert len(weights) == 5
    assert abs(sum(weights) - 1.0) < 1e-9
    assert weights[0] > weights[1] > weights[4]


def test_zipf_decays_and_sums_to_one():
    weights = zipf(10)
    assert len(weights) == 10
    assert abs(sum(weights) - 1.0) < 1e-9
    assert weights[0] > weights[9]


def test_pick_respects_weights_at_extremes():
    rng = random.Random(1)
    # A near-certain option must be picked when the roll lands inside its share.
    assert pick(rng, [("only", 1.0)]) == "only"


def test_pick_never_falls_off_the_end_on_float_rounding():
    rng = random.Random(1)
    # Weights that don't sum to exactly 1.0 due to float error must still return the last
    # option rather than None if the roll exceeds the cumulative sum.
    result = pick(rng, [("a", 0.3), ("b", 0.3), ("c", 0.3999999999999)])
    assert result in ("a", "b", "c")
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_math_utils.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'qc_lakehouse.generator'`

- [ ] **Step 3: Create the package and write the implementation**

```python
# qc-lakehouse/src/qc_lakehouse/generator/__init__.py
```//empty file

```python
# qc-lakehouse/src/qc_lakehouse/generator/math_utils.py
from __future__ import annotations

import math
import random
from decimal import ROUND_HALF_UP, Decimal
from typing import Sequence


def stream_seed(seed: int, name: str) -> int:
    """FNV-1a hash of `name`, XORed with `seed`. Python's built-in hash() is salted per
    process, so it cannot be used for anything that has to reproduce across runs. Separate
    streams per entity type mean adding a feature cannot shift another feature's draws."""
    h = 2_166_136_261
    for b in name.encode():
        h = ((h ^ b) * 16_777_619) & 0xFFFF_FFFF
    return seed ^ (h & 0x7FFF_FFFF)


def allocate(total: Decimal, weights: Sequence, places: int = 2) -> list[Decimal]:
    """Largest-remainder split: parts sum EXACTLY to total, no drift.
    Same routine used for money; also used to split entity counts and hours."""
    q = Decimal(1).scaleb(-places)
    wsum = sum(weights)
    if wsum == 0:
        raise ValueError("weights sum to zero")
    exact = [total * Decimal(w) / Decimal(wsum) for w in weights]
    floors = [e.quantize(q, rounding="ROUND_DOWN") for e in exact]
    shortfall = int(((total - sum(floors)) / q).to_integral_value(ROUND_HALF_UP))
    order = sorted(range(len(exact)), key=lambda i: (floors[i] - exact[i], i))
    for i in order[:shortfall]:
        floors[i] += q
    return floors


def split_by_share(total: int, shares: Sequence) -> list[int]:
    return [int(p) for p in allocate(Decimal(total), [Decimal(str(s)) for s in shares], 0)]


def zone_density(n: int, decay: float = 6.0) -> list[float]:
    """Weights decaying exponentially from zone 0 outward, summing to 1. A flat
    distribution would erase the hot-partition problem this project exists for."""
    raw = [math.exp(-i / decay) for i in range(n)]
    return [w / sum(raw) for w in raw]


def zipf(n: int, exponent: float = 0.85, offset: float = 4.0) -> list[float]:
    """Popularity weight per rank, summing to 1. `offset` flattens the head; a
    pure 1/rank curve hands one restaurant an implausible share of a city."""
    raw = [1.0 / (i + 1 + offset) ** exponent for i in range(n)]
    return [w / sum(raw) for w in raw]


def pick(rng: random.Random, options: Sequence):
    """Weighted choice from [(value, probability), ...]. Walks the cumulative sum."""
    roll, acc = rng.random(), 0.0
    for value, w in options:
        acc += w
        if roll < acc:
            return value
    return options[-1][0]      # float error guard: never fall off the end
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_math_utils.py -v`
Expected: `9 passed`

- [ ] **Step 5: Run `make check` to confirm the full suite is still green**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `17 passed` (8 from Sub-project A + 9 new)

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/__init__.py src/qc_lakehouse/generator/math_utils.py tests/test_math_utils.py
git commit -m "Add core allocation/random helpers for the reference-data generator"
```

---

### Task 2: Geographic helpers (`math_utils.py`)

**Files:**
- Modify: `qc-lakehouse/src/qc_lakehouse/generator/math_utils.py`
- Modify: `qc-lakehouse/tests/test_math_utils.py`

**Interfaces:**
- Consumes: nothing from Task 1's functions directly, but lives in the same file.
- Produces: `km_per_deg_lon(lat: float) -> float`, `offset_km(lat: float, lon: float, north_km: float, east_km: float) -> tuple[float, float]`, `destination(lat: float, lon: float, bearing: float, km: float) -> tuple[float, float]`, `haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float`, `spiral_point(lat: float, lon: float, i: int, inner: float = 1.6, step: float = 1.15) -> tuple[float, float]`, `sample_around(rng: random.Random, lat: float, lon: float, sigma_km: float, max_km: float) -> tuple[float, float]`. Task 5 (`build_zones`, `build_restaurants` via `sample_around`) depends on all six.

Source: CELL 5 of the notebook source file (geo functions, verbatim - no parameterization changes needed, none of these read a global).

- [ ] **Step 1: Write the failing tests**

Append to `qc-lakehouse/tests/test_math_utils.py`:

```python
import math

from qc_lakehouse.generator.math_utils import (
    destination,
    haversine_km,
    km_per_deg_lon,
    offset_km,
    sample_around,
    spiral_point,
)


def test_km_per_deg_lon_shrinks_toward_the_poles():
    equator = km_per_deg_lon(0.0)
    mid_lat = km_per_deg_lon(45.0)
    assert equator > mid_lat > 0


def test_offset_km_moves_north_and_east_correctly():
    lat, lon = offset_km(0.0, 0.0, north_km=111.19, east_km=0.0)
    assert abs(lat - 1.0) < 0.01     # ~111.19 km north is ~1 degree of latitude
    assert abs(lon - 0.0) < 1e-9      # pure north movement doesn't change longitude


def test_destination_and_haversine_are_consistent():
    lat0, lon0 = 12.9716, 77.5946   # Bengaluru
    lat1, lon1 = destination(lat0, lon0, bearing=90, km=5.0)
    dist = haversine_km(lat0, lon0, lat1, lon1)
    assert abs(dist - 5.0) < 0.05    # round-trip should recover ~5 km


def test_haversine_km_zero_distance_for_same_point():
    assert haversine_km(12.97, 77.59, 12.97, 77.59) == 0.0


def test_spiral_point_places_successive_zones_farther_out():
    lat0, lon0 = 12.9716, 77.5946
    p0 = spiral_point(lat0, lon0, 0)
    p5 = spiral_point(lat0, lon0, 5)
    d0 = haversine_km(lat0, lon0, *p0)
    d5 = haversine_km(lat0, lon0, *p5)
    assert d5 > d0


def test_sample_around_stays_within_max_km():
    import random

    rng = random.Random(7)
    lat0, lon0 = 12.9716, 77.5946
    for _ in range(50):
        lat, lon = sample_around(rng, lat0, lon0, sigma_km=0.7, max_km=2.0)
        assert haversine_km(lat0, lon0, lat, lon) <= 2.0 + 1e-6
```

- [ ] **Step 2: Run the new tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_math_utils.py -v -k "km_per_deg or offset_km or destination or haversine or spiral or sample_around"`
Expected: FAIL with `ImportError` (functions not defined yet)

- [ ] **Step 3: Add the implementation**

Append to `qc-lakehouse/src/qc_lakehouse/generator/math_utils.py`:

```python
EARTH_KM = 6371.0088
KM_PER_DEG_LAT = math.pi * EARTH_KM / 180.0     # ~111.19 km, constant everywhere
GOLDEN_ANGLE = 137.50776405003785               # spreads points so none share a bearing


def km_per_deg_lon(lat: float) -> float:
    """Kilometres per degree of longitude AT THIS LATITUDE. Shrinks toward the poles."""
    return KM_PER_DEG_LAT * math.cos(math.radians(lat))


def offset_km(lat: float, lon: float, north_km: float, east_km: float) -> tuple[float, float]:
    """Move a point by a north/east displacement in km. Returns (lat, lon)."""
    new_lat = lat + north_km / KM_PER_DEG_LAT
    # Longitude degrees shrink with cos(latitude). Ignoring that is the classic
    # bug that stretches a city east-west and inflates every rider payout.
    scale = km_per_deg_lon((lat + new_lat) / 2)
    return new_lat, lon + (east_km / scale if scale else 0.0)


def destination(lat: float, lon: float, bearing: float, km: float) -> tuple[float, float]:
    """Point reached by travelling `km` along a compass bearing (0 = north)."""
    t = math.radians(bearing)
    return offset_km(lat, lon, km * math.cos(t), km * math.sin(t))


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points, in km. ~0.5% off an ellipsoid,
    far below the error from straight-line interpolation between GPS pings."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * EARTH_KM * math.asin(math.sqrt(min(1.0, a)))


def spiral_point(lat: float, lon: float, i: int, inner: float = 1.6, step: float = 1.15) -> tuple[float, float]:
    """Place zone `i` on a phyllotactic spiral around a city centre. sqrt radius
    keeps the points roughly equal-area instead of bunching near the middle."""
    return destination(lat, lon, (i * GOLDEN_ANGLE) % 360.0, inner + step * math.sqrt(i))


def sample_around(rng: random.Random, lat: float, lon: float, sigma_km: float, max_km: float) -> tuple[float, float]:
    """Draw a point near a centre, gaussian in both axes, truncated at max_km.
    Gaussian because businesses cluster on a high street and thin out."""
    for _ in range(12):
        n, e = rng.gauss(0, sigma_km), rng.gauss(0, sigma_km)
        if math.hypot(n, e) <= max_km:
            return offset_km(lat, lon, n, e)
    # Fell through 12 draws. Clamp rather than return the exact centre, which
    # would create an implausible pile-up on one coordinate.
    return offset_km(lat, lon, max_km * 0.7, max_km * 0.7)
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_math_utils.py -v`
Expected: `15 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `23 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/math_utils.py tests/test_math_utils.py
git commit -m "Add geographic helpers for the reference-data generator"
```

---

### Task 3: Demand-curve helpers (`math_utils.py`)

**Files:**
- Modify: `qc-lakehouse/src/qc_lakehouse/generator/math_utils.py`
- Modify: `qc-lakehouse/tests/test_math_utils.py`

**Interfaces:**
- Consumes: `allocate` from Task 1 (used inside `orders_by_hour`).
- Produces: `resolve_events(events: Sequence[tuple], days: int) -> list[tuple[int, tuple]]`, `events_on(day_index: int, resolved_events: Sequence[tuple[int, tuple]], shoulder_before: float, shoulder_after: float) -> list[tuple[tuple, float]]`, `day_factors(day_index: int, start_date: date, dow_weights: Sequence[float], resolved_events, noise: Sequence[float], orders_per_day: int, shoulder_before: float, shoulder_after: float) -> tuple`, `orders_by_hour(day_index: int, start_date: date, dow_weights, resolved_events, noise, orders_per_day: int, hourly_weekday: Sequence[float], hourly_weekend: Sequence[float], shoulder_before: float, shoulder_after: float) -> list[int]`. Task 7's `build_demand_curve` calls all four with values it gets from `GeneratorConfig` (Task 4).

**Signature note:** these differ from the source notebook by taking every input explicitly instead of reading module globals (`start`, `DOW`, `RESOLVED`, `NOISE`, `ORDERS_PER_DAY`, `HOURLY_WEEKDAY`, `HOURLY_WEEKEND`, `SHOULDER_BEFORE`, `SHOULDER_AFTER`) - this is Global Constraint deviation #1, required for testability. The event tuple shape stays exactly as the notebook defines it: `(name, position, peak, hour_window_or_None, hour_focus, transit, surge, cancel)`.

Source: CELL 6 of the notebook source file.

- [ ] **Step 1: Write the failing tests**

Append to `qc-lakehouse/tests/test_math_utils.py`:

```python
from datetime import date

from qc_lakehouse.generator.math_utils import (
    day_factors,
    events_on,
    orders_by_hour,
    resolve_events,
)

TEST_EVENTS = [
    ("sports_final", 0.13, 1.80, (18, 22), 2.2, 1.0, 1.0, 1.0),
    ("storm", 0.38, 1.45, None, 1.0, 1.40, 1.60, 2.5),
    ("festival", 0.66, 2.10, None, 1.0, 1.0, 1.0, 1.0),
    ("long_weekend", 0.87, 1.30, None, 1.0, 1.0, 1.0, 1.0),
]
TEST_DOW = [0.85, 0.82, 0.90, 1.10, 1.35, 1.45, 1.20]
TEST_DOW = [m / (sum(TEST_DOW) / 7) for m in TEST_DOW]


def test_resolve_events_scales_count_with_window_length():
    short = resolve_events(TEST_EVENTS, days=7)
    long = resolve_events(TEST_EVENTS, days=90)
    assert len(short) <= len(long)
    assert len(long) == 4    # max(1, round(90/90*4)) == 4, all events fit


def test_resolve_events_deduplicates_by_day_index():
    resolved = resolve_events(TEST_EVENTS, days=90)
    day_indices = [d for d, _ in resolved]
    assert len(day_indices) == len(set(day_indices))


def test_events_on_gives_shoulder_days_partial_share():
    resolved = [(10, TEST_EVENTS[0])]
    assert events_on(10, resolved, shoulder_before=0.30, shoulder_after=0.50) == [(TEST_EVENTS[0], 1.0)]
    assert events_on(9, resolved, shoulder_before=0.30, shoulder_after=0.50) == [(TEST_EVENTS[0], 0.30)]
    assert events_on(11, resolved, shoulder_before=0.30, shoulder_after=0.50) == [(TEST_EVENTS[0], 0.50)]
    assert events_on(12, resolved, shoulder_before=0.30, shoulder_after=0.50) == []


def test_day_factors_event_day_has_higher_total_than_non_event_day():
    resolved = [(10, TEST_EVENTS[2])]  # festival, peak=2.10
    noise = [1.0] * 20
    non_event = day_factors(0, date(2026, 6, 1), TEST_DOW, resolved, noise, 15_000, 0.30, 0.50)
    event_day = day_factors(10, date(2026, 6, 1), TEST_DOW, resolved, noise, 15_000, 0.30, 0.50)
    assert event_day[6] > non_event[6]    # index 6 is the resolved order count
    assert "festival" in event_day[7]


def test_orders_by_hour_sums_exactly_to_the_day_total():
    resolved = resolve_events(TEST_EVENTS, days=90)
    noise = [1.0] * 90
    hourly_weekday = [1 / 24] * 24
    hourly_weekend = [1 / 24] * 24
    for day_index in range(90):
        _, weekday, *_, orders, _ = day_factors(
            day_index, date(2026, 6, 1), TEST_DOW, resolved, noise, 15_000, 0.30, 0.50
        )
        hours = orders_by_hour(
            day_index, date(2026, 6, 1), TEST_DOW, resolved, noise, 15_000,
            hourly_weekday, hourly_weekend, 0.30, 0.50,
        )
        assert sum(hours) == orders, f"day {day_index}: hours sum to {sum(hours)}, expected {orders}"
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_math_utils.py -v -k "resolve_events or events_on or day_factors or orders_by_hour"`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Add the implementation**

Append to `qc-lakehouse/src/qc_lakehouse/generator/math_utils.py`:

```python
from datetime import date, timedelta


def resolve_events(events: Sequence, days: int) -> list[tuple[int, tuple]]:
    """Pick which EVENTS actually land in a `days`-long window, spaced out and deduplicated
    by day index. `wanted` scales with the window length so the same catalogue works at
    7 days or 365."""
    wanted = max(1, round(days / 90 * 4))
    resolved: list[tuple[int, tuple]] = []
    for ev in events[:wanted]:
        day_index = round(ev[1] * (days - 1))
        if day_index not in [d for d, _ in resolved]:
            resolved.append((day_index, ev))
    return resolved


def events_on(day_index: int, resolved_events, shoulder_before: float, shoulder_after: float):
    """Events touching this day, each with its share of the boost."""
    out = []
    for d, ev in resolved_events:
        offset = day_index - d
        if offset == 0:
            out.append((ev, 1.0))
        elif offset == -1:
            out.append((ev, shoulder_before))
        elif offset == 1:
            out.append((ev, shoulder_after))
    return out


def day_factors(day_index: int, start_date: date, dow_weights, resolved_events, noise,
                 orders_per_day: int, shoulder_before: float, shoulder_after: float):
    """Every input to one day's order count, kept separable so it can be audited."""
    d = start_date + timedelta(days=day_index)
    dow = dow_weights[d.weekday()]
    event, names = 1.0, []
    for ev, share in events_on(day_index, resolved_events, shoulder_before, shoulder_after):
        event *= 1.0 + (ev[2] - 1.0) * share
        names.append(ev[0])
    n = noise[day_index]
    total = dow * event * n
    return d, d.weekday(), dow, event, n, total, max(1, round(orders_per_day * total)), names


def orders_by_hour(day_index: int, start_date: date, dow_weights, resolved_events, noise,
                    orders_per_day: int, hourly_weekday, hourly_weekend,
                    shoulder_before: float, shoulder_after: float) -> list[int]:
    """Split the day's orders across 24 hours, summing EXACTLY to the day total.
    Rounding 24 hours independently loses orders the way naive rounding loses cents."""
    _, weekday, *_, orders, _ = day_factors(
        day_index, start_date, dow_weights, resolved_events, noise, orders_per_day,
        shoulder_before, shoulder_after,
    )
    weights = list(hourly_weekend if weekday >= 5 else hourly_weekday)
    for ev, share in events_on(day_index, resolved_events, shoulder_before, shoulder_after):
        if ev[3] and ev[4] != 1.0:
            lo, hi = ev[3]
            for h in range(lo, hi + 1):
                weights[h % 24] *= 1.0 + (ev[4] - 1.0) * share
    parts = allocate(Decimal(orders), [Decimal(str(w)) for w in weights], 0)
    return [int(p) for p in parts]
```

Remove the now-duplicate `from datetime import date, timedelta` if one already exists at the top of the file from a prior task - keep a single import block at the top.

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_math_utils.py -v`
Expected: `20 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `28 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/math_utils.py tests/test_math_utils.py
git commit -m "Add demand-curve helpers for the reference-data generator"
```

---

### Task 4: Generator config (`generator/config.py`)

**Files:**
- Create: `qc-lakehouse/src/qc_lakehouse/generator/config.py`
- Test: `qc-lakehouse/tests/test_generator_config.py`

**Interfaces:**
- Produces: `GeneratorConfig` (frozen dataclass): `catalog: str = "qc_dev"`, `schema: str = "bronze_source"`, `days: int = 90`, `orders_per_day: int = 15_000`, `seed: int = 20260908`, `start_date: str = "2026-06-01"`, `n_cities: int = 3`, `zones_per_city: int = 15`, `n_restaurants: int = 1_000`, `n_riders: int = 3_000`, `n_customers: int = 300_000`, `menu_items_per_restaurant: int = 25`. Plus module-level static catalogs: `CITIES`, `ZONE_NAMES`, `CUISINES`, `BRAND_PREFIX`, `BRAND_CORE`, `MENUS`, `VEHICLE_MIX`, `TIER_MIX`, `TIER_RATES`, `DOW_RAW`, `HOURLY_WEEKDAY_RAW`, `HOURLY_WEEKEND_RAW`, `NOISE_SIGMA`, `SHOULDER_BEFORE`, `SHOULDER_AFTER`, `EVENTS`. Tasks 5-7 (entity builders) import these.

Source: CELL 2 (config values), CELL 4 (CITIES, ZONE_NAMES, CUISINES, BRAND_PREFIX, BRAND_CORE, MENUS, VEHICLE_MIX, TIER_MIX, TIER_RATES), CELL 6 (DOW_RAW, HOURLY_WEEKDAY/WEEKEND raw values before normalization, NOISE_SIGMA, SHOULDER_BEFORE/AFTER, EVENTS) of the notebook source file.

- [ ] **Step 1: Write the failing tests**

```python
# qc-lakehouse/tests/test_generator_config.py
from qc_lakehouse.generator.config import (
    BRAND_CORE,
    BRAND_PREFIX,
    CITIES,
    CUISINES,
    EVENTS,
    GeneratorConfig,
    MENUS,
    TIER_MIX,
    TIER_RATES,
    VEHICLE_MIX,
    ZONE_NAMES,
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
        name, position, peak, hour_window, hour_focus, transit, surge, cancel = ev
        assert isinstance(name, str)
        assert 0.0 <= position <= 1.0
        assert peak > 0
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_generator_config.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

```python
# qc-lakehouse/src/qc_lakehouse/generator/config.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GeneratorConfig:
    catalog: str = "qc_dev"
    schema: str = "bronze_source"
    days: int = 90
    orders_per_day: int = 15_000     # BASELINE for an average day. Real totals run ~6%
                                      # higher because event days are genuinely busier.
    seed: int = 20260908             # Change for a different but equally valid world.
    start_date: str = "2026-06-01"   # a Monday, so day 0 starts a clean week
    n_cities: int = 3
    zones_per_city: int = 15
    # 3,000 riders, not 1,000. At 1,000 the baseline is 15.9 deliveries per rider per day
    # (~13 hours) and the festival is 47 (physically impossible).
    n_restaurants: int = 1_000
    n_riders: int = 3_000
    n_customers: int = 300_000
    menu_items_per_restaurant: int = 25


HISTORY_DAYS = 730   # entities were created in the 2 years BEFORE day 0

# name, country, timezone, lat, lon, share of the business
CITIES = [
    ("Bengaluru", "IN", "Asia/Kolkata", 12.9716, 77.5946, 0.42),
    ("Mumbai", "IN", "Asia/Kolkata", 19.0760, 72.8777, 0.35),
    ("Delhi", "IN", "Asia/Kolkata", 28.6139, 77.2090, 0.23),
]

# Ordered inner-to-outer: index 0 is the densest zone. Density decays outward, which is
# what creates the hot-zone skew the file-layout work (Sub-project H) will address.
ZONE_NAMES = {
    "Bengaluru": ["Indiranagar", "Koramangala", "HSR Layout", "Jayanagar", "BTM Layout",
                  "Rajajinagar", "Malleshwaram", "Bellandur", "JP Nagar", "Banashankari",
                  "Marathahalli", "Hebbal", "Whitefield", "Yelahanka", "Electronic City"],
    "Mumbai": ["Lower Parel", "Bandra West", "Dadar", "Worli", "Andheri East",
               "Juhu", "Kurla", "Chembur", "Powai", "Goregaon",
               "Malad West", "Colaba", "Borivali", "Vashi", "Thane West"],
    "Delhi": ["Connaught Place", "Karol Bagh", "Hauz Khas", "Lajpat Nagar", "Greater Kailash",
              "Saket", "Nehru Place", "Model Town", "Rajouri Garden", "Pitampura",
              "Janakpuri", "Vasant Kunj", "Mayur Vihar", "Rohini", "Dwarka"],
}

# name, share of restaurants, base commission, p50 prep minutes
CUISINES = [
    ("North Indian", 0.22, 0.1800, 22), ("South Indian", 0.16, 0.1600, 14),
    ("Chinese", 0.14, 0.2000, 18), ("Biryani", 0.13, 0.2200, 26),
    ("Pizza", 0.11, 0.2400, 20), ("Burgers", 0.10, 0.2300, 12),
    ("Desserts", 0.08, 0.1900, 10), ("Healthy Bowls", 0.06, 0.1700, 16),
]

# One 6-item list per cuisine each, used to construct restaurant chain names like
# "Punjabi Dhaba". Fewer brands than outlets on purpose (see build_restaurants) -
# multi-outlet chains make "which outlet of this brand is slowest" a real question.
BRAND_PREFIX = {
    "North Indian": ["Punjabi", "Royal", "Tandoor", "Kesar", "Rajdhani", "Sagar"],
    "South Indian": ["Udupi", "Anand", "Dosa", "Madras", "Coastal", "Nandini"],
    "Chinese": ["Golden", "Wok", "Sichuan", "Chin", "Dragon", "Mandarin"],
    "Biryani": ["Paradise", "Hyderabad", "Bawarchi", "Zaiqa", "Lucknowi", "Nawab"],
    "Pizza": ["Napoli", "Crust", "Forno", "Slice", "Bella", "Roma"],
    "Burgers": ["Grill", "Patty", "Smash", "Bun", "Char", "Stack"],
    "Desserts": ["Sweet", "Cocoa", "Sugar", "Kulfi", "Frost", "Velvet"],
    "Healthy Bowls": ["Green", "Fresh", "Root", "Sprout", "Nourish", "Clean"],
}
BRAND_CORE = {
    "North Indian": ["Dhaba", "Rasoi", "Darbar", "Junction", "Tadka", "Angan"],
    "South Indian": ["Tiffin", "Bhavan", "Corner", "Sagar", "Cafe", "Mess"],
    "Chinese": ["Bowl", "Bay", "Express", "Garden", "Kitchen", "Palace"],
    "Biryani": ["House", "Handi", "Dum", "Mahal", "Point", "Kitchen"],
    "Pizza": ["Pizzeria", "Oven", "Co", "Kitchen", "Bakehouse", "Corner"],
    "Burgers": ["Shack", "Yard", "Bros", "Works", "Company", "Joint"],
    "Desserts": ["Bakery", "Patisserie", "Scoop", "Bar", "Room", "Studio"],
    "Healthy Bowls": ["Bowl", "Kitchen", "Table", "Co", "Greens", "Bar"],
}

# 12 (name, category, price) templates per cuisine. Each restaurant draws 25 items, so
# items repeat across a chain with independently drifting prices - which is what gives a
# later SCD2 price history something non-trivial to track.
MENUS = {
    "North Indian": [("Paneer Butter Masala", "Mains", 280), ("Dal Makhani", "Mains", 240),
                     ("Butter Chicken", "Mains", 360), ("Kadai Paneer", "Mains", 290),
                     ("Chole Bhature", "Mains", 180), ("Butter Naan", "Breads", 60),
                     ("Tandoori Roti", "Breads", 35), ("Laccha Paratha", "Breads", 70),
                     ("Jeera Rice", "Rice", 150), ("Paneer Tikka", "Starters", 260),
                     ("Boondi Raita", "Sides", 90), ("Gulab Jamun", "Desserts", 110)],
    "South Indian": [("Masala Dosa", "Mains", 130), ("Plain Dosa", "Mains", 90),
                     ("Ghee Roast Dosa", "Mains", 170), ("Idli Sambar", "Mains", 80),
                     ("Rava Idli", "Mains", 100), ("Medu Vada", "Starters", 70),
                     ("Upma", "Mains", 85), ("Pongal", "Mains", 110),
                     ("Curd Rice", "Rice", 95), ("Lemon Rice", "Rice", 100),
                     ("Filter Coffee", "Beverages", 45), ("Mysore Pak", "Desserts", 90)],
    "Chinese": [("Veg Hakka Noodles", "Mains", 190), ("Chicken Fried Rice", "Rice", 230),
                ("Chilli Paneer Dry", "Starters", 250), ("Chicken Manchurian", "Mains", 270),
                ("Veg Spring Rolls", "Starters", 160), ("Schezwan Noodles", "Mains", 210),
                ("Chicken Momos", "Starters", 180), ("Sweet Corn Soup", "Starters", 130),
                ("Hot and Sour Soup", "Starters", 140), ("Kung Pao Chicken", "Mains", 300),
                ("Chilli Garlic Rice", "Rice", 200), ("Honey Chilli Potato", "Sides", 170)],
    "Biryani": [("Chicken Dum Biryani", "Mains", 330), ("Mutton Biryani", "Mains", 470),
                ("Veg Biryani", "Mains", 240), ("Egg Biryani", "Mains", 260),
                ("Chicken 65", "Starters", 280), ("Kebab Platter", "Starters", 390),
                ("Mirchi Ka Salan", "Sides", 90), ("Raita", "Sides", 60),
                ("Double Ka Meetha", "Desserts", 130), ("Haleem", "Mains", 340),
                ("Chicken Fry Piece", "Starters", 220), ("Irani Chai", "Beverages", 40)],
    "Pizza": [("Margherita", "Mains", 280), ("Farmhouse", "Mains", 420),
              ("Peppy Paneer", "Mains", 400), ("Chicken Tikka Pizza", "Mains", 480),
              ("Pepperoni", "Mains", 520), ("Garlic Bread", "Sides", 150),
              ("Cheesy Dip", "Sides", 45), ("Chicken Wings", "Starters", 320),
              ("Penne Alfredo", "Mains", 350), ("Choco Lava Cake", "Desserts", 120),
              ("Cold Coffee", "Beverages", 140), ("Caesar Salad", "Starters", 260)],
    "Burgers": [("Classic Veg Burger", "Mains", 150), ("Crispy Chicken Burger", "Mains", 230),
                ("Double Cheese Burger", "Mains", 320), ("Paneer Tikka Burger", "Mains", 220),
                ("Peri Peri Fries", "Sides", 130), ("Salted Fries", "Sides", 100),
                ("Cheese Loaded Fries", "Sides", 190), ("Chicken Popcorn", "Starters", 180),
                ("Onion Rings", "Sides", 140), ("Chocolate Shake", "Beverages", 180),
                ("Iced Tea", "Beverages", 110), ("Brownie", "Desserts", 130)],
    "Desserts": [("Red Velvet Pastry", "Desserts", 160), ("Chocolate Truffle", "Desserts", 180),
                 ("Cheesecake Slice", "Desserts", 220), ("Tiramisu Jar", "Desserts", 240),
                 ("Malai Kulfi", "Desserts", 90), ("Gulab Jamun Box", "Desserts", 200),
                 ("Rasmalai", "Desserts", 150), ("Brownie Sundae", "Desserts", 210),
                 ("Assorted Macarons", "Desserts", 320), ("Banoffee Pie", "Desserts", 250),
                 ("Hot Chocolate", "Beverages", 150), ("Cold Brew", "Beverages", 170)],
    "Healthy Bowls": [("Quinoa Power Bowl", "Mains", 340), ("Grilled Chicken Bowl", "Mains", 380),
                      ("Falafel Mezze Bowl", "Mains", 320), ("Paneer Protein Bowl", "Mains", 350),
                      ("Greek Salad", "Starters", 260), ("Hummus and Pita", "Starters", 240),
                      ("Avocado Toast", "Starters", 280), ("Overnight Oats", "Mains", 190),
                      ("Cold Pressed Juice", "Beverages", 160), ("Berry Smoothie", "Beverages", 200),
                      ("Protein Ball", "Desserts", 120), ("Soup of the Day", "Starters", 180)],
}

VEHICLE_MIX = [("bike", 0.70), ("scooter", 0.22), ("car", 0.08)]
TIER_MIX = [("bronze", 0.50), ("silver", 0.35), ("gold", 0.15)]

# tier, base fare, per km, per minute - rates as STRINGS, parsed to Decimal. A float
# literal like 6.5 cannot represent a rate exactly and the error compounds across
# every payout.
TIER_RATES = [
    ("bronze", "20.00", "6.5000", "0.5000"),
    ("silver", "25.00", "7.2500", "0.6000"),
    ("gold", "30.00", "8.0000", "0.7500"),
]

# Monday-first, matching date.weekday(). Normalized in build_demand_curve so the mean is
# exactly 1.0, otherwise orders_per_day would not mean what it says.
DOW_RAW = [0.85, 0.82, 0.90, 1.10, 1.35, 1.45, 1.20]

# Share of a day's orders per hour, index 0 = midnight. Normalized in build_demand_curve.
HOURLY_WEEKDAY_RAW = [0.02, 0.01, 0.01, 0.005, 0.005, 0.01, 0.03, 0.05, 0.08, 0.06, 0.05, 0.15,
                      0.20, 0.12, 0.08, 0.06, 0.08, 0.12, 0.22, 0.25, 0.18, 0.12, 0.08, 0.05]
HOURLY_WEEKEND_RAW = [0.04, 0.03, 0.02, 0.01, 0.01, 0.01, 0.02, 0.03, 0.05, 0.07, 0.10, 0.14,
                      0.16, 0.14, 0.10, 0.08, 0.09, 0.12, 0.18, 0.22, 0.24, 0.16, 0.10, 0.07]

# Lognormal, not gaussian: demand is strictly positive and right-skewed.
NOISE_SIGMA = 0.08

# A spike has shape, not a step. The day before picks up 30% of the boost, the day after
# keeps 50% as demand settles.
SHOULDER_BEFORE, SHOULDER_AFTER = 0.30, 0.50

# name, position (fraction of window), peak multiplier, hour window or None, hour focus,
# transit multiplier, surge multiplier, cancel multiplier. One occasion roughly every 3
# weeks; `position` is a fraction of the window so the same catalogue works at 7 days or
# 365. 0.66 not 0.68 for the festival: at 0.68 it landed on a Saturday, already the
# busiest weekday; 0.66 puts it on a Thursday, giving the dataset one genuine mid-week
# demand shock.
EVENTS = [
    ("sports_final", 0.13, 1.80, (18, 22), 2.2, 1.0, 1.0, 1.0),
    ("storm", 0.38, 1.45, None, 1.0, 1.40, 1.60, 2.5),
    ("festival", 0.66, 2.10, None, 1.0, 1.0, 1.0, 1.0),
    ("long_weekend", 0.87, 1.30, None, 1.0, 1.0, 1.0, 1.0),
]
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_generator_config.py -v`
Expected: `7 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `35 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/config.py tests/test_generator_config.py
git commit -m "Add generator config and static domain catalogs"
```

---

### Task 5: City and zone builders (`generator/entities.py`)

**Files:**
- Create: `qc-lakehouse/src/qc_lakehouse/generator/entities.py`
- Test: `qc-lakehouse/tests/test_entities.py`

**Interfaces:**
- Consumes: `stream_seed` (Task 1), `spiral_point` (Task 2), `GeneratorConfig`, `CITIES`, `ZONE_NAMES` (Task 4).
- Produces: `build_cities(config: GeneratorConfig) -> list[tuple]` (rows: `city_id, name, country_code, timezone, lat, lon, created_at, updated_at`), `build_zones(config: GeneratorConfig, cities: list[tuple]) -> list[tuple]` (rows: `zone_id, city_id, name, center_lat, center_lon, ring, base_delivery_fee: Decimal, sla_target_minutes: int, is_active: bool, created_at, updated_at`). Tasks 6-7 and `customers.py` (Task 8) consume `cities`/`zones` in exactly this row shape.

Source: CELL 7 (first two blocks, cities and zones) of the notebook source file.

- [ ] **Step 1: Write the failing tests**

```python
# qc-lakehouse/tests/test_entities.py
from datetime import datetime, timezone

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
    inner = [z for z in zones if z[5] == 0][0]
    outer = [z for z in zones if z[5] == 14][0]
    assert outer[7] > inner[7]   # sla_target_minutes widens toward the outskirts


def test_build_zones_uses_named_zones_within_the_catalog_and_falls_back_beyond_it():
    config = GeneratorConfig(n_cities=1, zones_per_city=15)
    cities = build_cities(config)
    zones = build_zones(config, cities)
    assert zones[0][2] == "Indiranagar"   # ring 0 for Bengaluru per ZONE_NAMES
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_entities.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

```python
# qc-lakehouse/src/qc_lakehouse/generator/entities.py
from __future__ import annotations

import itertools
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from qc_lakehouse.generator.config import CITIES, GeneratorConfig, HISTORY_DAYS, ZONE_NAMES
from qc_lakehouse.generator.math_utils import spiral_point, stream_seed


def _epoch(config: GeneratorConfig) -> datetime:
    from datetime import date

    start = date.fromisoformat(config.start_date)
    return datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)


def _onboarded(rng: random.Random, epoch: datetime) -> datetime:
    """A creation timestamp somewhere in the 2 years before the window opens."""
    return epoch - timedelta(seconds=rng.randint(1, HISTORY_DAYS * 86_400))


def build_cities(config: GeneratorConfig) -> list[tuple]:
    epoch = _epoch(config)
    rng = random.Random(stream_seed(config.seed, "cities"))
    cities = []
    for i, (name, cc, tz, lat, lon, _share) in enumerate(CITIES[: config.n_cities], start=1):
        t = _onboarded(rng, epoch)
        cities.append((i, name, cc, tz, lat, lon, t, t))
    return cities


def build_zones(config: GeneratorConfig, cities: list[tuple]) -> list[tuple]:
    epoch = _epoch(config)
    rng = random.Random(stream_seed(config.seed, "zones"))
    zones, zid = [], itertools.count(1)
    for city_id, name, _cc, _tz, lat, lon, _c1, _c2 in cities:
        for ring in range(config.zones_per_city):
            zlat, zlon = spiral_point(lat, lon, ring)
            names = ZONE_NAMES[name]
            zname = names[ring] if ring < len(names) else f"{name} Z{ring + 1}"
            t = _onboarded(rng, epoch)
            zones.append((
                next(zid), city_id, zname, zlat, zlon, ring,
                Decimal(19 + 3 * (ring // 3)).quantize(Decimal("0.01")),
                # 38-52 min, not 28-42. Average delivery is 34.4 min, so the old ladder
                # breached on ~45% of orders. Real food delivery quotes 30-45 min; the
                # prep and transit times were fine, the target was wrong.
                38 + 2 * (ring // 2),
                True, t, t,
            ))
    return zones
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_entities.py -v`
Expected: `6 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `41 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/entities.py tests/test_entities.py
git commit -m "Add city and zone builders"
```

---

### Task 6: Restaurant, rider, menu, and payout-tier builders (`generator/entities.py`)

**Files:**
- Modify: `qc-lakehouse/src/qc_lakehouse/generator/entities.py`
- Modify: `qc-lakehouse/tests/test_entities.py`

**Interfaces:**
- Consumes: `build_cities`, `build_zones` (Task 5), `stream_seed`, `split_by_share`, `zone_density`, `zipf`, `pick`, `sample_around` (Tasks 1-2), `CUISINES`, `BRAND_PREFIX`, `BRAND_CORE`, `MENUS`, `VEHICLE_MIX`, `TIER_MIX`, `TIER_RATES`, `HISTORY_DAYS` (Task 4).
- Produces: `build_restaurants(config, cities, zones) -> list[list]` (rows, **mutable lists not tuples** because popularity is set after construction: `restaurant_id, restaurant_ref, name, cuisine_type, city_id, zone_id, lat, lon, commission_pct: Decimal, payout_account_id, prep_time_p50_minutes, is_active, created_at, updated_at, popularity_weight: float, price_index: float` - indices 0-15), `build_riders(config, cities, zones) -> list[tuple]` (rows: `rider_id, rider_ref, vehicle_type, payout_tier, home_zone_id, payout_account_id, is_active, created_at, updated_at`), `build_menu_items(config, restaurants) -> list[tuple]` (rows: `menu_item_id, restaurant_id, name, category, price: Decimal, is_available, created_at, updated_at`), `build_payout_tiers(config) -> list[tuple]` (rows: `tier, valid_from, valid_to, base_fare: Decimal, per_km_rate: Decimal, per_minute_rate: Decimal`). Task 9 (`writer.py`) writes all four; Task 6's own integrity-check tests (Task 9) reference `restaurants[i][14]` for popularity_weight and `restaurants[i][15]` for price_index, and `[:14]` to drop the two generator-only columns before writing the source-visible `restaurants` table.

Source: CELL 7 (restaurants, riders, menu_items, payout_tiers blocks) of the notebook source file.

- [ ] **Step 1: Write the failing tests**

Append to `qc-lakehouse/tests/test_entities.py`:

```python
from qc_lakehouse.generator.entities import (
    build_menu_items,
    build_payout_tiers,
    build_restaurants,
    build_riders,
)


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
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_entities.py -v -k "restaurant or rider or menu or payout"`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Add the implementation**

Append to `qc-lakehouse/src/qc_lakehouse/generator/entities.py`:

```python
from qc_lakehouse.generator.config import (
    BRAND_CORE,
    BRAND_PREFIX,
    CUISINES,
    MENUS,
    TIER_MIX,
    TIER_RATES,
    VEHICLE_MIX,
)
from qc_lakehouse.generator.math_utils import pick, sample_around, split_by_share, zipf, zone_density


def build_restaurants(config: GeneratorConfig, cities: list[tuple], zones: list[tuple]) -> list[list]:
    epoch = _epoch(config)
    rng = random.Random(stream_seed(config.seed, "restaurants"))
    zones_of_city = {c[0]: [z for z in zones if z[1] == c[0]] for c in cities}
    restaurants, rid, used_names = [], itertools.count(1), set()
    per_city = split_by_share(config.n_restaurants, [c[5] for c in CITIES[: config.n_cities]])
    for (city, count) in zip(cities, per_city):
        czones = zones_of_city[city[0]]
        for zone, zcount in zip(czones, split_by_share(count, zone_density(len(czones)))):
            zcount = max(1, zcount)   # every zone must be able to serve an order
            for cuisine, ccount in zip(CUISINES, split_by_share(zcount, [c[1] for c in CUISINES])):
                if ccount == 0:
                    continue
                combos = [f"{p} {c}" for p in BRAND_PREFIX[cuisine[0]] for c in BRAND_CORE[cuisine[0]]]
                rng.shuffle(combos)
                # Fewer brands than outlets on purpose: multi-outlet chains make "which
                # outlet of this brand is slowest" a real question.
                brands = combos[: max(1, round(ccount * 0.6))]
                for i in range(ccount):
                    brand = brands[i % len(brands)]
                    nm, n = brand, 2
                    while nm in used_names:
                        nm = f"{brand} - {zone[2]}" if n == 2 else f"{brand} - {zone[2]} {n}"
                        n += 1
                    used_names.add(nm)
                    rlat, rlon = sample_around(rng, zone[3], zone[4], 0.7, 2.0)
                    r = next(rid)
                    t = _onboarded(rng, epoch)
                    restaurants.append([
                        r, f"R-{r:05d}", nm, cuisine[0], city[0], zone[0], rlat, rlon,
                        # +/- 3 points around the cuisine midpoint, quantized to 4 places
                        # exactly ONCE. Rounding twice introduces money bugs.
                        Decimal(str(round(cuisine[2] + rng.uniform(-0.03, 0.03), 4)))
                            .quantize(Decimal("0.0001")),
                        f"ACCT-R-{r:06d}",
                        max(5, round(rng.gauss(cuisine[3], 3.5))),
                        rng.random() > 0.02,    # ~2% already closed at t0
                        t, t,
                        0.0,                                # popularity (generator-only)
                        round(rng.uniform(0.82, 1.38), 3),   # price index (generator-only)
                    ])

    # Popularity by SHUFFLED rank, so it is uncorrelated with restaurant_id. If rank
    # tracked ID, sequential-key clustering would look artificially good in a later
    # file-layout benchmark and the result would be a lie.
    ranks = list(range(len(restaurants)))
    rng.shuffle(ranks)
    zw = zipf(len(restaurants))
    for r, rank in zip(restaurants, ranks):
        r[14] = zw[rank]

    return restaurants


def build_riders(config: GeneratorConfig, cities: list[tuple], zones: list[tuple]) -> list[tuple]:
    epoch = _epoch(config)
    rng = random.Random(stream_seed(config.seed, "riders"))
    zones_of_city = {c[0]: [z for z in zones if z[1] == c[0]] for c in cities}
    riders, drid = [], itertools.count(1)
    per_city = split_by_share(config.n_riders, [c[5] for c in CITIES[: config.n_cities]])
    for city, count in zip(cities, per_city):
        czones = zones_of_city[city[0]]
        for zone, zcount in zip(czones, split_by_share(count, zone_density(len(czones)))):
            for _ in range(zcount):
                d = next(drid)
                t = _onboarded(rng, epoch)
                riders.append((
                    d, f"D-{d:05d}", pick(rng, VEHICLE_MIX), pick(rng, TIER_MIX),
                    zone[0], f"ACCT-D-{d:06d}", rng.random() > 0.04,   # ~4% churned
                    t, t,
                ))
    return riders


def build_menu_items(config: GeneratorConfig, restaurants: list[list]) -> list[tuple]:
    rng = random.Random(stream_seed(config.seed, "menu"))
    variants = ["Regular", "Large", "Family Pack", "Mini"]
    vfactor = [1.0, 1.45, 2.1, 0.7]
    menu_items, mid = [], itertools.count(1)
    for r in restaurants:
        templates = MENUS[r[3]]
        for i in range(config.menu_items_per_restaurant):
            template, variant = templates[i % len(templates)], i // len(templates)
            nm = template[0] if variant == 0 else f"{template[0]} ({variants[variant % 4]})"
            price = template[2] * r[15] * (vfactor[variant % 4] if variant else 1.0)
            price *= rng.uniform(0.96, 1.04)   # per-outlet jitter across a chain
            m = next(mid)
            # Menu prices end in 9 in reality. Quantized exactly once.
            menu_items.append((
                m, r[0], nm, template[1],
                Decimal(max(1, round(price / 10)) * 10 - 1).quantize(Decimal("0.01")),
                rng.random() > 0.05,   # ~5% out of stock
                r[12], r[12],
            ))
    return menu_items


def build_payout_tiers(config: GeneratorConfig) -> list[tuple]:
    epoch = _epoch(config)
    tier_from = epoch - timedelta(days=HISTORY_DAYS)
    # Open-ended (valid_to = NULL). A mid-window rate change is a CHANGE and belongs in a
    # future CDC stream, not in initial state.
    return [
        (t[0], tier_from, None, Decimal(t[1]), Decimal(t[2]), Decimal(t[3]))
        for t in TIER_RATES
    ]
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_entities.py -v`
Expected: `14 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `49 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/entities.py tests/test_entities.py
git commit -m "Add restaurant, rider, menu, and payout-tier builders"
```

---

### Task 7: Demand-curve builder (`generator/entities.py`)

**Files:**
- Modify: `qc-lakehouse/src/qc_lakehouse/generator/entities.py`
- Modify: `qc-lakehouse/tests/test_entities.py`

**Interfaces:**
- Consumes: `resolve_events`, `day_factors`, `orders_by_hour`, `stream_seed` (Tasks 1, 3), `DOW_RAW`, `HOURLY_WEEKDAY_RAW`, `HOURLY_WEEKEND_RAW`, `NOISE_SIGMA`, `SHOULDER_BEFORE`, `SHOULDER_AFTER`, `EVENTS` (Task 4).
- Produces: `build_demand_curve(config: GeneratorConfig) -> tuple[list[tuple], list[tuple]]` - returns `(daily, hourly)`. `daily` rows: `day_index, order_date, weekday, dow_multiplier: float, event_multiplier: float, noise: float, total_multiplier: float, orders: int, event_names: str | None`. `hourly` rows: `day_index, order_date, hour, orders`. Task 9's writer and its capacity-check test use `daily[i][7]` for that day's order count.

Source: CELL 6 (DOW/HOURLY normalization, RESOLVED, NOISE construction) and CELL 10 (the `daily`/`hourly` row-building loops) of the notebook source file.

- [ ] **Step 1: Write the failing tests**

Append to `qc-lakehouse/tests/test_entities.py`:

```python
import math

from qc_lakehouse.generator.entities import build_demand_curve


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
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_entities.py -v -k "demand_curve"`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Add the implementation**

Append to `qc-lakehouse/src/qc_lakehouse/generator/entities.py`:

```python
import math
from datetime import date

from qc_lakehouse.generator.config import (
    DOW_RAW,
    EVENTS,
    HOURLY_WEEKDAY_RAW,
    HOURLY_WEEKEND_RAW,
    NOISE_SIGMA,
    SHOULDER_AFTER,
    SHOULDER_BEFORE,
)
from qc_lakehouse.generator.math_utils import day_factors, orders_by_hour, resolve_events


def build_demand_curve(config: GeneratorConfig) -> tuple[list[tuple], list[tuple]]:
    start_date = date.fromisoformat(config.start_date)
    # Normalized so the mean is exactly 1.0, otherwise orders_per_day would not mean what
    # it says.
    dow = [m / (sum(DOW_RAW) / 7) for m in DOW_RAW]
    hourly_weekday = [w / sum(HOURLY_WEEKDAY_RAW) for w in HOURLY_WEEKDAY_RAW]
    hourly_weekend = [w / sum(HOURLY_WEEKEND_RAW) for w in HOURLY_WEEKEND_RAW]
    resolved = resolve_events(EVENTS, config.days)

    # Noise gets its own RNG stream so adding events or trends cannot shift it.
    nrng = random.Random(stream_seed(config.seed, "noise"))
    noise = [math.exp(nrng.gauss(0.0, NOISE_SIGMA)) for _ in range(config.days)]

    daily = []
    for d in range(config.days):
        dt, weekday, dowv, event, n, total, orders, names = day_factors(
            d, start_date, dow, resolved, noise, config.orders_per_day,
            SHOULDER_BEFORE, SHOULDER_AFTER,
        )
        daily.append((d, dt, weekday, float(dowv), float(event), float(n), float(total),
                      orders, ",".join(names) or None))

    hourly = [
        (d, start_date + timedelta(days=d), hour, n)
        for d in range(config.days)
        for hour, n in enumerate(orders_by_hour(
            d, start_date, dow, resolved, noise, config.orders_per_day,
            hourly_weekday, hourly_weekend, SHOULDER_BEFORE, SHOULDER_AFTER,
        ))
    ]
    return daily, hourly
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_entities.py -v`
Expected: `18 passed`

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `53 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/entities.py tests/test_entities.py
git commit -m "Add demand-curve builder"
```

---

### Task 8: Customer generation (`generator/customers.py`)

**Files:**
- Create: `qc-lakehouse/src/qc_lakehouse/generator/customers.py`
- Test: `qc-lakehouse/tests/test_customers.py`

**Interfaces:**
- Consumes: `cities` and `zones` (Task 5's `build_cities`/`build_zones` output shapes, in `build_cities`'s original order - city_id assignment order), `GeneratorConfig` (Task 4), and a live `SparkSession` (from either `qc_lakehouse.spark_local.build_local_spark_session()` for tests, or `qc_lakehouse.databricks_session.build_databricks_session()` for real runs - this module takes a session as a parameter, it does not build one itself).
- Produces: `build_customers(spark, config: GeneratorConfig, cities: list[tuple], zones: list[tuple]) -> tuple[DataFrame, DataFrame]` - returns `(customers_df, gen_customer_profile_df)`. `customers_df` columns: `customer_id, customer_ref, email, phone, full_name, city_id, home_zone_id, lat, lon, signup_ts, created_at, updated_at`. `gen_customer_profile_df` columns: `customer_id, order_propensity`. Task 9's writer calls this and writes both DataFrames.

Source: CELL 9 of the notebook source file. This is the one Spark-dependent generation function; everything else in `generator/` is plain Python.

**Testing approach:** this needs a real Spark session, but not necessarily a live Databricks one - Sub-project A already built a local Spark dev-loop (`qc_lakehouse.spark_local.build_local_spark_session()`) specifically so Spark-dependent code like this could be developed and tested without live Databricks. Use it here.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `cd qc-lakehouse && uv run pytest tests/test_customers.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'qc_lakehouse.generator.customers'`

- [ ] **Step 3: Write the implementation**

```python
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
    from datetime import date, datetime, timezone

    from qc_lakehouse.generator.config import CITIES
    from qc_lakehouse.generator.math_utils import zone_density

    start = date.fromisoformat(config.start_date)
    epoch = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
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
```

- [ ] **Step 4: Run the test and confirm it passes**

Run: `cd qc-lakehouse && uv run pytest tests/test_customers.py -v`
Expected: `2 passed` (this starts a local JVM, so it's slower than the pure-Python tests - that's expected)

- [ ] **Step 5: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `55 passed`

- [ ] **Step 6: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/customers.py tests/test_customers.py
git commit -m "Add Spark-based customer generation"
```

---

### Task 9: Table schemas and write orchestration (`generator/schemas.py`, `generator/writer.py`)

**Files:**
- Create: `qc-lakehouse/src/qc_lakehouse/generator/schemas.py`
- Create: `qc-lakehouse/src/qc_lakehouse/generator/writer.py`
- Test: `qc-lakehouse/tests/test_writer.py`

**Interfaces:**
- Consumes: every `build_*` function from Tasks 5-8, `GeneratorConfig`, `ensure_schema_exists` (from Sub-project A's `qc_lakehouse.databricks_session`, reused as-is).
- Produces: `check_referential_integrity(zones, restaurants, riders, menu_items) -> None` (raises `AssertionError` on any orphan), `check_demand_conservation(daily, hourly) -> None` (raises `AssertionError` if any day's hourly rows don't sum to its daily total), `check_rider_capacity(riders, daily, max_deliveries_per_rider_per_day: int = 12) -> None` (raises `AssertionError` if peak-day orders per active rider exceeds the threshold - the notebook's own comment states this ceiling: "one rider sustains roughly 10-12 deliveries in a shift... anything past that means the fleet is physically incapable"), `write_reference_tables(spark, config: GeneratorConfig) -> dict[str, int]` (builds everything, runs all three checks *before* writing anything, then writes every table to `{config.catalog}.{config.schema}`, returns a `{table_name: row_count}` dict). Task 10's entrypoint script calls `write_reference_tables`.

Source: CELL 8 (schemas, the `write()` helper) and CELL 14 (integrity checks, as SQL `LEFT ANTI JOIN` queries against written tables - restructured here to run against in-memory data *before* writing, per Global Constraint deviation #2) and CELL 11 (rider capacity sanity print, restructured into `check_rider_capacity`) of the notebook source file.

- [ ] **Step 1: Write the failing tests**

```python
# qc-lakehouse/tests/test_writer.py
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from qc_lakehouse.generator.writer import (
    check_demand_conservation,
    check_referential_integrity,
    check_rider_capacity,
)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


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
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cd qc-lakehouse && uv run pytest tests/test_writer.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'qc_lakehouse.generator.writer'`

- [ ] **Step 3: Write `schemas.py`**

```python
# qc-lakehouse/src/qc_lakehouse/generator/schemas.py
from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Explicit types, because inferring a schema from Python would turn every Decimal into a
# double and silently lose money precision.
TS, D2, D4, R5 = TimestampType(), DecimalType(18, 2), DecimalType(18, 4), DecimalType(5, 4)

CITIES_SCHEMA = StructType([
    StructField("city_id", LongType()), StructField("name", StringType()),
    StructField("country_code", StringType()), StructField("timezone", StringType()),
    StructField("lat", DoubleType()), StructField("lon", DoubleType()),
    StructField("created_at", TS), StructField("updated_at", TS),
])

ZONES_SCHEMA = StructType([
    StructField("zone_id", LongType()), StructField("city_id", LongType()),
    StructField("name", StringType()), StructField("center_lat", DoubleType()),
    StructField("center_lon", DoubleType()), StructField("ring", IntegerType()),
    StructField("base_delivery_fee", D2), StructField("sla_target_minutes", IntegerType()),
    StructField("is_active", BooleanType()),
    StructField("created_at", TS), StructField("updated_at", TS),
])

# The OLTP source does not know how popular a restaurant is - that has to be discovered
# from order volume. This 14-field schema (no popularity_weight, no price_index) is what
# the source-visible `restaurants` table gets; those two columns go to
# _gen_restaurant_profile instead, in a separate table so it's obvious the pipeline must
# never join to it.
RESTAURANTS_SCHEMA = StructType([
    StructField("restaurant_id", LongType()), StructField("restaurant_ref", StringType()),
    StructField("name", StringType()), StructField("cuisine_type", StringType()),
    StructField("city_id", LongType()), StructField("zone_id", LongType()),
    StructField("lat", DoubleType()), StructField("lon", DoubleType()),
    StructField("commission_pct", R5), StructField("payout_account_id", StringType()),
    StructField("prep_time_p50_minutes", IntegerType()), StructField("is_active", BooleanType()),
    StructField("created_at", TS), StructField("updated_at", TS),
])

GEN_RESTAURANT_PROFILE_SCHEMA = StructType([
    StructField("restaurant_id", LongType()),
    StructField("popularity_weight", DoubleType()),
    StructField("price_index", DoubleType()),
])

RIDERS_SCHEMA = StructType([
    StructField("rider_id", LongType()), StructField("rider_ref", StringType()),
    StructField("vehicle_type", StringType()), StructField("payout_tier", StringType()),
    StructField("home_zone_id", LongType()), StructField("payout_account_id", StringType()),
    StructField("is_active", BooleanType()),
    StructField("created_at", TS), StructField("updated_at", TS),
])

MENU_ITEMS_SCHEMA = StructType([
    StructField("menu_item_id", LongType()), StructField("restaurant_id", LongType()),
    StructField("name", StringType()), StructField("category", StringType()),
    StructField("price", D2), StructField("is_available", BooleanType()),
    StructField("created_at", TS), StructField("updated_at", TS),
])

# DECIMAL(18,4) on the rates, (18,2) on the fare: rates get multiplied by distance and
# time, so they need extra places before the result is quantized.
RIDER_PAYOUT_TIERS_SCHEMA = StructType([
    StructField("tier", StringType()), StructField("valid_from", TS),
    StructField("valid_to", TS), StructField("base_fare", D2),
    StructField("per_km_rate", D4), StructField("per_minute_rate", D4),
])

DEMAND_DAILY_SCHEMA = StructType([
    StructField("day_index", IntegerType()), StructField("order_date", DateType()),
    StructField("weekday", IntegerType()), StructField("dow_multiplier", DoubleType()),
    StructField("event_multiplier", DoubleType()), StructField("noise", DoubleType()),
    StructField("total_multiplier", DoubleType()), StructField("orders", IntegerType()),
    StructField("event_names", StringType()),
])

DEMAND_HOURLY_SCHEMA = StructType([
    StructField("day_index", IntegerType()), StructField("order_date", DateType()),
    StructField("hour", IntegerType()), StructField("orders", IntegerType()),
])
```

- [ ] **Step 4: Write `writer.py`**

```python
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
```

- [ ] **Step 5: Run the tests and confirm they pass**

Run: `cd qc-lakehouse && uv run pytest tests/test_writer.py -v`
Expected: `8 passed`

- [ ] **Step 6: Run `make check`**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `63 passed`

- [ ] **Step 7: Commit**

```bash
cd qc-lakehouse
git add src/qc_lakehouse/generator/schemas.py src/qc_lakehouse/generator/writer.py tests/test_writer.py
git commit -m "Add table schemas and write orchestration with real integrity checks"
```

---

### Task 10: Entrypoint script, Makefile wiring, README, and live validation

**Files:**
- Create: `qc-lakehouse/scripts/generate_reference_data.py`
- Modify: `qc-lakehouse/Makefile`
- Modify: `qc-lakehouse/README.md`

**Interfaces:**
- Consumes: `load_settings` (Sub-project A `qc_lakehouse.config`), `build_databricks_session` (Sub-project A `qc_lakehouse.databricks_session`), `GeneratorConfig`, `write_reference_tables` (Task 9).
- Produces: `make generate-reference-data`, a documented real run against live Databricks.

- [ ] **Step 1: Write the entrypoint script**

```python
# qc-lakehouse/scripts/generate_reference_data.py
"""Builds and writes the quick-commerce reference-data layer (cities, zones,
restaurants, riders, menu items, customers, payout tiers, demand curve) to
{GeneratorConfig.catalog}.{GeneratorConfig.schema} on live Databricks serverless compute.

This is both the production entrypoint and Sub-project B's smoke test - unlike
smoke_local.py/smoke_databricks.py from Sub-project A, this writes real pipeline data,
not throwaway rows, so there's nothing to clean up afterward.
"""
from __future__ import annotations

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session
from qc_lakehouse.generator.config import GeneratorConfig
from qc_lakehouse.generator.writer import write_reference_tables


def main() -> None:
    settings = load_settings()
    spark = build_databricks_session(settings)
    config = GeneratorConfig()

    counts = write_reference_tables(spark, config)

    print(f"catalog: {config.catalog}.{config.schema}\n")
    for name, n in counts.items():
        print(f"  {name:<28} {n:>10,}")

    total_rows = sum(counts.values())
    print(f"\ngenerate-reference-data: OK - wrote {len(counts)} tables, {total_rows:,} total rows")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Add the Makefile target**

Append to `qc-lakehouse/Makefile` (keep every existing target - this only adds one):

```makefile
.PHONY: venv install install-baseline check install-spark smoke-local install-databricks smoke-databricks install-dbt smoke-dbt smoke-all generate-reference-data

# Writes real reference data to qc_dev.bronze_source on live Databricks - not a throwaway
# smoke test like smoke-databricks. Uses .venv-databricks (same as smoke-databricks)
# because it needs databricks-connect for the serverless session.
generate-reference-data:
	.venv-databricks/bin/python scripts/generate_reference_data.py
```

- [ ] **Step 3: Run it for real against live Databricks**

Run: `cd qc-lakehouse && make generate-reference-data`
Expected: prints one row-count line per table (`cities`, `zones`, `restaurants`, `riders`,
`menu_items`, `rider_payout_tiers`, `_gen_restaurant_profile`, `demand_daily`,
`demand_hourly`, `customers`, `_gen_customer_profile`), then
`generate-reference-data: OK - wrote 11 tables, N total rows`. If any of the three
integrity checks from Task 9 fail, the run raises
`AssertionError` before writing anything - that would mean a real bug in the port, not an
environment issue; stop and investigate rather than re-running.

- [ ] **Step 4: Verify row counts independently**

Run (from the same terminal used for the Databricks CLI/dbt work in Sub-project A):
```bash
databricks catalogs list | grep qc_dev
```
Then confirm the tables exist by running the smoke-dbt-style pattern against the new
schema, or simply re-run `make generate-reference-data` a second time and confirm the
counts are identical (overwrite mode from the same seed must be idempotent - the row
counts printed on the second run should exactly match the first).

- [ ] **Step 5: Add a "Reference data generator" section to the README**

Append to `qc-lakehouse/README.md`:

```markdown
## Reference data generator (Sub-project B spine)

Ported from a Databricks notebook (see
`docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md`, section 6).
Generates the quick-commerce world's starting state - cities, zones, restaurants, riders,
menu items, customers, payout tiers, and a 90-day demand curve - and writes it directly to
Delta tables in `qc_dev.bronze_source` (not `workspace.dev`, which stays reserved for
Sub-project A's own throwaway smoke-test tables).

```bash
make generate-reference-data
```

This does not use Auto Loader - reference/dimension data is a one-time seed of the
starting world, not a stream of arriving files, so Auto Loader's incremental-ingestion
value doesn't apply here. Auto Loader is introduced in a later widen phase, for the
fact tables (orders, order_events, courier_shifts, gps_pings) this generator does not
produce.

Idempotent: every write is `mode("overwrite")` from a fixed seed
(`GeneratorConfig.seed`), so rerunning reproduces the same data rather than
accumulating duplicates.
```

- [ ] **Step 6: Run `make check` one more time to confirm nothing regressed**

Run: `cd qc-lakehouse && make check`
Expected: ruff clean, `63 passed`

- [ ] **Step 7: Commit**

```bash
cd qc-lakehouse
git add scripts/generate_reference_data.py Makefile README.md
git commit -m "Add reference-data generator entrypoint, wire into Makefile, document in README"
```
