from __future__ import annotations

import math
import random
from collections.abc import Sequence
from decimal import ROUND_HALF_UP, Decimal


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
