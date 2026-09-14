from __future__ import annotations

import itertools
import random
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from qc_lakehouse.generator.config import (
    BRAND_CORE,
    BRAND_PREFIX,
    CITIES,
    CUISINES,
    HISTORY_DAYS,
    MENUS,
    TIER_MIX,
    TIER_RATES,
    VEHICLE_MIX,
    ZONE_NAMES,
    GeneratorConfig,
)
from qc_lakehouse.generator.math_utils import (
    pick,
    sample_around,
    spiral_point,
    split_by_share,
    stream_seed,
    zipf,
    zone_density,
)


def _epoch(config: GeneratorConfig) -> datetime:
    start = date.fromisoformat(config.start_date)
    return datetime.combine(start, datetime.min.time(), tzinfo=UTC)


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
