from __future__ import annotations

import itertools
import random
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from qc_lakehouse.generator.config import CITIES, HISTORY_DAYS, ZONE_NAMES, GeneratorConfig
from qc_lakehouse.generator.math_utils import spiral_point, stream_seed


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
