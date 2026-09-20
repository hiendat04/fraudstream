"""The last hour of observations, kept as bin counts in Redis.

Counts, not rows: every replica adds to the same numbers, a restart loses
nothing, and memory stays the same however much traffic arrives.
"""

import time
from typing import Callable

import numpy as np
from redis.asyncio import Redis

from fraudstream_api.drift_detection.psi import bin_counts
from fraudstream_api.drift_detection.reference import Reference

ROWS = "rows"
# Two spare minutes, so a key never vanishes while it is still inside the window.
SPARE_MINUTES = 2


class DriftWindow:
    """One Redis hash per minute, holding how many rows fell in each bin."""

    def __init__(
        self,
        redis: Redis,
        reference: Reference,
        *,
        minutes: int = 60,
        clock: Callable[[], float] = time.time,
    ):
        self._redis = redis
        self._reference = reference
        self._minutes = minutes
        self._clock = clock

    def _key(self, minute: int) -> str:
        return f"drift:v{self._reference.model_version}:{minute}"

    async def add(self, observations: list[dict[str, float | bool | None]]) -> int:
        """Bin one batch and add it to the current minute."""

        key = self._key(int(self._clock() // 60))
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hincrby(key, ROWS, len(observations))
            for name, feature in self._reference.features.items():
                values = np.array([_number(row[name]) for row in observations], dtype=float)
                for index, count in enumerate(bin_counts(values, feature.edges)):
                    if count:
                        pipe.hincrby(key, f"{name}|{index}", int(count))
            await pipe.expire(key, (self._minutes + SPARE_MINUTES) * 60)
            await pipe.execute()
        return len(observations)

    async def totals(self) -> tuple[int, dict[str, np.ndarray]]:
        """Sum the last `minutes` of counts."""

        now = int(self._clock() // 60)
        async with self._redis.pipeline(transaction=False) as pipe:
            for minute in range(now - self._minutes + 1, now + 1):
                pipe.hgetall(self._key(minute))
            buckets = await pipe.execute()

        rows = 0
        counts = {
            name: np.zeros(len(feature.edges) + 2)
            for name, feature in self._reference.features.items()
        }
        for bucket in buckets:
            for field, value in bucket.items():
                if field == ROWS:
                    rows += int(value)
                else:
                    name, index = field.rsplit("|", 1)
                    counts[name][int(index)] += int(value)
        return rows, counts


def _number(value: float | bool | None) -> float:
    """A missing input becomes NaN, which lands in the bin kept for it."""

    return np.nan if value is None else float(value)
