"""Tests for the window of observations kept in Redis.

The window is bin counts, not rows. That is what lets every replica add to one
window, lets a restart lose nothing, and keeps memory fixed under any load.
"""

import unittest

import fakeredis
import numpy as np

from fraudstream_api.drift_detection.psi import bin_counts
from fraudstream_api.drift_detection.reference import build_reference
from fraudstream_api.drift_detection.window import DriftWindow

REFERENCE = build_reference(
    {
        "amount": np.arange(1000.0),
        "customer_features_available": np.array([0.0] * 900 + [1.0] * 100),
        "txn_count_7d": np.where(np.arange(1000.0) % 2 == 0, np.arange(1000.0), np.nan),
    },
    model_name="fraud-detection",
    model_version="2",
    data_snapshot_id="3023861480409485916",
)


class Clock:
    """A clock the test moves by hand."""

    def __init__(self, now: float = 1_000_000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def forward(self, minutes: int) -> None:
        self.now += minutes * 60


def observations(count: int, amount: float = 500.0, missing: bool = False) -> list[dict]:
    return [
        {
            "amount": amount,
            "customer_features_available": 1.0,
            "txn_count_7d": None if missing else 10.0,
        }
        for _ in range(count)
    ]


def window_on(redis, clock=None, minutes: int = 60) -> DriftWindow:
    return DriftWindow(redis, REFERENCE, minutes=minutes, clock=clock or Clock())


class DriftWindowTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.server = fakeredis.FakeServer()

    def redis(self):
        return fakeredis.FakeAsyncRedis(server=self.server, decode_responses=True)

    async def test_counts_add_up_across_batches(self):
        window = window_on(self.redis())

        await window.add(observations(30))
        await window.add(observations(20))

        rows, _ = await window.totals()
        self.assertEqual(50, rows)

    async def test_two_replicas_share_one_window(self):
        """Every pod adds to the same numbers, which is why the counts live in Redis."""

        clock = Clock()
        first = window_on(self.redis(), clock)
        second = window_on(self.redis(), clock)

        await first.add(observations(10))
        await second.add(observations(15))

        rows, _ = await first.totals()
        self.assertEqual(25, rows)

    async def test_a_restart_keeps_the_window(self):
        clock = Clock()
        await window_on(self.redis(), clock).add(observations(12))

        rows, _ = await window_on(self.redis(), clock).totals()

        self.assertEqual(12, rows)

    async def test_the_window_forgets_rows_older_than_its_length(self):
        clock = Clock()
        window = window_on(self.redis(), clock, minutes=60)
        await window.add(observations(10))

        clock.forward(61)

        rows, _ = await window.totals()
        self.assertEqual(0, rows)

    async def test_totals_match_bin_counts_on_the_same_rows(self):
        clock = Clock()
        window = window_on(self.redis(), clock)
        rows = observations(20, amount=100.0) + observations(30, amount=900.0, missing=True)
        await window.add(rows)

        counted, totals = await window.totals()

        self.assertEqual(50, counted)
        for name, feature in REFERENCE.features.items():
            values = np.array(
                [np.nan if row[name] is None else row[name] for row in rows], dtype=float
            )
            with self.subTest(feature=name):
                np.testing.assert_array_equal(bin_counts(values, feature.edges), totals[name])

    async def test_every_minute_key_expires_on_its_own(self):
        redis = self.redis()
        clock = Clock()
        await window_on(redis, clock, minutes=60).add(observations(5))

        keys = await redis.keys("drift:*")

        self.assertEqual(1, len(keys))
        self.assertGreater(await redis.ttl(keys[0]), 60 * 60)

    async def test_the_key_names_the_model_version(self):
        redis = self.redis()
        await window_on(redis).add(observations(5))

        keys = await redis.keys("drift:*")

        self.assertTrue(keys[0].startswith("drift:v2:"), keys[0])


if __name__ == "__main__":
    unittest.main()
