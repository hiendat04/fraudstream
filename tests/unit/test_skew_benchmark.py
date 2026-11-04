"""Unit tests for the skew benchmark: the salted join, each strategy's settings, and how the skew is measured."""

from __future__ import annotations

import importlib.util
from unittest import TestCase, main, skipUnless

from fraudstream.jobs.gold.skew_benchmark import stage_spread, strategy_conf


@skipUnless(importlib.util.find_spec("pyspark"), "PySpark is not installed")
class SaltedJoinTest(TestCase):
    def test_salted_join_matches_plain_join(self) -> None:
        from pyspark.sql import SparkSession

        from fraudstream.jobs.gold.skew_benchmark import salted_join

        spark = SparkSession.builder.master("local[2]").appName("salted-join-test").getOrCreate()
        self.addCleanup(spark.stop)
        cities = ["New York"] * 150 + ["Boston", "Miami", "Denver", "Dallas", "Unknown"] * 10
        left = spark.createDataFrame([(f"t{index}", city) for index, city in enumerate(cities)], ["transaction_id", "city"])
        right = spark.createDataFrame([(1, "New York"), (2, "Boston"), (3, "Miami"), (4, "Denver"), (5, "Dallas")], ["city_key", "city"])

        plain = sorted(left.join(right, "city", "left").collect())
        salted = salted_join(left, right, "city", buckets=4)

        self.assertEqual(salted.columns, ["city", "transaction_id", "city_key"])
        self.assertEqual(sorted(salted.collect()), plain)
        self.assertEqual(len(plain), 200)


class StrategyConfTest(TestCase):
    def test_baseline_disables_adaptive_execution_and_broadcast(self) -> None:
        conf = strategy_conf("baseline")

        self.assertEqual(conf["spark.sql.adaptive.enabled"], "false")
        self.assertEqual(conf["spark.sql.autoBroadcastJoinThreshold"], "-1")

    def test_only_adaptive_turns_on_the_skew_join(self) -> None:
        self.assertEqual(strategy_conf("adaptive")["spark.sql.adaptive.skewJoin.enabled"], "true")
        self.assertEqual(strategy_conf("salted")["spark.sql.adaptive.enabled"], "false")
        self.assertNotIn("spark.sql.autoBroadcastJoinThreshold", strategy_conf("broadcast"))

    def test_every_strategy_uses_the_same_shuffle_partitions(self) -> None:
        partitions = {strategy_conf(name)["spark.sql.shuffle.partitions"] for name in ("baseline", "adaptive", "salted", "broadcast")}

        self.assertEqual(len(partitions), 1)

    def test_unknown_strategy_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            strategy_conf("magic")


class StageSpreadTest(TestCase):
    def test_stage_spread_picks_the_largest_shuffle_stage(self) -> None:
        stages = [
            _stage(1, tasks=8, shuffle_read=0, input_bytes=900, run_ms=[100, 120], records=[0, 0], input_records=[50, 60]),
            _stage(2, tasks=200, shuffle_read=5_000, input_bytes=0, run_ms=[40, 9_000], records=[1_000, 250_000], input_records=[0, 0]),
            _stage(3, tasks=200, shuffle_read=100, input_bytes=0, run_ms=[5, 6], records=[10, 12], input_records=[0, 0]),
        ]

        self.assertEqual(
            stage_spread(stages),
            {"stage_id": 2, "tasks": 200, "median_task_ms": 40, "max_task_ms": 9_000, "median_records": 1_000, "max_records": 250_000},
        )

    def test_without_a_shuffle_the_largest_scan_is_used(self) -> None:
        stages = [
            _stage(1, tasks=8, shuffle_read=0, input_bytes=900, run_ms=[100, 120], records=[0, 0], input_records=[50, 60]),
            _stage(2, tasks=4, shuffle_read=0, input_bytes=10, run_ms=[1, 2], records=[0, 0], input_records=[1, 2]),
        ]

        self.assertEqual(stage_spread(stages)["stage_id"], 1)
        self.assertEqual(stage_spread(stages)["max_records"], 60)


def _stage(stage_id, *, tasks, shuffle_read, input_bytes, run_ms, records, input_records) -> dict:
    """One entry of Spark's /stages endpoint with its taskSummary (median and max) attached."""

    return {
        "stageId": stage_id,
        "numTasks": tasks,
        "shuffleReadBytes": shuffle_read,
        "inputBytes": input_bytes,
        "taskSummary": {
            "quantiles": [0.5, 1.0],
            "executorRunTime": run_ms,
            "shuffleReadMetrics": {"readRecords": records},
            "inputMetrics": {"recordsRead": input_records},
        },
    }


if __name__ == "__main__":
    main()
