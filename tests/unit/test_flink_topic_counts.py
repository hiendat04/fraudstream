"""Tests for counting the Flink job's output topics against the replayed source."""

from __future__ import annotations

import json
import sys
from types import ModuleType, SimpleNamespace
from unittest import TestCase, main
from unittest.mock import patch

from fraudstream.jobs.flink.topic_counts import (
    CONSUMER_SETTINGS,
    count_committed,
    count_corrections,
    reconcile,
)

PARTITION_EOF = -191


class FlinkTopicCountsTest(TestCase):
    def test_reconcile_balances_source_against_topics(self) -> None:
        result = reconcile({"clean": 500_000, "duplicate": 12_500, "invalid": 0, "late": 900}, 512_500)

        self.assertTrue(result["balanced"])
        self.assertEqual(result["gap"], 0)
        self.assertEqual(result["accounted"], 512_500)

    def test_reconcile_flags_a_gap(self) -> None:
        result = reconcile({"clean": 499_990, "duplicate": 12_500, "invalid": 0}, 512_500)

        self.assertFalse(result["balanced"])
        self.assertEqual(result["gap"], 10)

    def test_count_committed_reads_to_the_end(self) -> None:
        consumer = FakeConsumer(
            {
                ("clean", 0): ["a", "b"],
                ("clean", 1): ["c"],
                ("late", 0): [],
            }
        )

        with patch.dict(sys.modules, {"confluent_kafka": _fake_kafka_module()}):
            counts = count_committed(consumer, ["clean", "late"])

        self.assertEqual(counts, {"clean": 3, "late": 0})
        self.assertEqual(consumer.subscribed, ["clean", "late"])
        self.assertEqual(CONSUMER_SETTINGS["isolation.level"], "read_committed")
        self.assertTrue(CONSUMER_SETTINGS["enable.partition.eof"])

    def test_count_corrections_counts_only_rows_that_replace_a_window(self) -> None:
        rows = [json.dumps({"is_correction": flag}) for flag in (False, True, True)]
        consumer = FakeConsumer({("customer", 0): rows, ("merchant", 0): [json.dumps({"is_correction": True})]})

        with patch.dict(sys.modules, {"confluent_kafka": _fake_kafka_module()}):
            self.assertEqual(count_corrections(consumer, ["customer", "merchant"]), 3)

    def test_a_missing_topic_is_an_error_not_zero(self) -> None:
        consumer = FakeConsumer({("clean", 0): ["a"]})

        with patch.dict(sys.modules, {"confluent_kafka": _fake_kafka_module()}), self.assertRaisesRegex(RuntimeError, "clean_baseline"):
            count_committed(consumer, ["clean_baseline"])


class FakeMessage:
    def __init__(self, topic: str, partition: int, value: str | None = None, eof: bool = False) -> None:
        self._topic, self._partition, self._value, self._eof = topic, partition, value, eof

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def value(self) -> bytes:
        return (self._value or "").encode("utf-8")

    def error(self) -> object | None:
        return SimpleNamespace(code=lambda: PARTITION_EOF) if self._eof else None


class FakeConsumer:
    """Serves each partition's records, then one end-of-partition event."""

    def __init__(self, partitions: dict[tuple[str, int], list[str]]) -> None:
        self.partitions = partitions
        self.subscribed: list[str] = []
        self.queue: list[FakeMessage] = []

    def list_topics(self, topic: str, timeout: float) -> object:
        del timeout
        numbers = [number for name, number in self.partitions if name == topic]
        error = None if numbers else f"unknown topic {topic}"
        return SimpleNamespace(topics={topic: SimpleNamespace(partitions={n: None for n in numbers}, error=error)})

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = list(topics)
        self.queue = [
            message
            for (topic, partition), values in self.partitions.items()
            if topic in topics
            for message in [*(FakeMessage(topic, partition, value) for value in values), FakeMessage(topic, partition, eof=True)]
        ]

    def poll(self, timeout: float) -> FakeMessage | None:
        del timeout
        return self.queue.pop(0) if self.queue else None

    def unsubscribe(self) -> None:
        return None


def _fake_kafka_module() -> ModuleType:
    module = ModuleType("confluent_kafka")
    module.KafkaError = SimpleNamespace(_PARTITION_EOF=PARTITION_EOF)  # type: ignore[attr-defined]
    return module


if __name__ == "__main__":
    main()
