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
