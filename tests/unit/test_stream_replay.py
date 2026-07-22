"""Unit tests for replaying generated stream events to Kafka."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType
from unittest import TestCase, main

from fraudstream.producers.stream_replay import ReplayConfig, replay_stream


class StreamReplayProducerTest(TestCase):
    """Tests for Kafka replay behavior."""

    def tearDown(self) -> None:
        """Remove fake Kafka modules after each test."""

        sys.modules.pop("confluent_kafka", None)

    def test_replay_publishes_events_to_kafka_with_keys_and_headers(self) -> None:
        """The replay producer should preserve source order and Kafka metadata."""

        fake_module, produced_messages = _install_fake_kafka_module()
        del fake_module

        with TemporaryDirectory() as tmp_dir:
            source_path = Path(tmp_dir) / "events.jsonl"
            _write_source_events(source_path, event_count=3)

            summary = replay_stream(
                ReplayConfig(
                    source_path=source_path,
                    topic="financial_transactions_test",
                    bootstrap_servers="localhost:9092",
                    events_per_second=0,
                    max_events=2,
                    progress_interval=0,
                )
            )

        self.assertEqual(summary["sink_type"], "kafka")
        self.assertEqual(summary["published_count"], 2)
        self.assertEqual(summary["first_source_line"], 1)
        self.assertEqual(summary["last_source_line"], 2)
        self.assertEqual(len(produced_messages), 2)
        self.assertEqual(produced_messages[0]["topic"], "financial_transactions_test")
        self.assertEqual(produced_messages[0]["key"], b"cust_stream_00000001")
        self.assertIn(("event_type", b'"transaction.created"'), produced_messages[0]["headers"])

        value = json.loads(produced_messages[0]["value"].decode("utf-8"))
        self.assertEqual(value["topic"], "financial_transactions_test")
        self.assertEqual(value["source_sequence"], 1)

    def test_replay_can_start_from_offset(self) -> None:
        """Replay should support resuming from a source-log offset."""

        _fake_module, produced_messages = _install_fake_kafka_module()

        with TemporaryDirectory() as tmp_dir:
            source_path = Path(tmp_dir) / "events.jsonl"
            _write_source_events(source_path, event_count=5)

            summary = replay_stream(
                ReplayConfig(
                    source_path=source_path,
                    events_per_second=0,
                    max_events=2,
                    start_offset=3,
                    progress_interval=0,
                )
            )

        records = [
            json.loads(message["value"].decode("utf-8"))
            for message in produced_messages
        ]

        self.assertEqual(summary["published_count"], 2)
        self.assertEqual(summary["first_source_line"], 4)
        self.assertEqual(records[0]["source_sequence"], 4)
        self.assertEqual(records[1]["source_sequence"], 5)

    def test_config_validation_rejects_invalid_rate(self) -> None:
        """Replay config should fail fast on invalid pacing values."""

        config = ReplayConfig(events_per_second=-1)

        with self.assertRaisesRegex(ValueError, "events_per_second"):
            config.validate()

    def test_replay_requires_existing_source_path(self) -> None:
        """Replay should fail clearly when the JSONL source does not exist."""

        with self.assertRaises(FileNotFoundError):
            replay_stream(
                ReplayConfig(
                    source_path=Path("/tmp/fraudstream_missing_events.jsonl"),
                    events_per_second=0,
                )
            )


def _install_fake_kafka_module() -> tuple[ModuleType, list[dict]]:
    """Install a fake confluent_kafka module for unit tests."""

    produced_messages: list[dict] = []
    fake_module = ModuleType("confluent_kafka")

    class FakeProducer:
        def __init__(self, config: dict) -> None:
            self.config = config

        def produce(
            self,
            topic: str,
            key: bytes,
            value: bytes,
            headers: list[tuple[str, bytes]],
            on_delivery: object,
        ) -> None:
            produced_messages.append(
                {
                    "topic": topic,
                    "key": key,
                    "value": value,
                    "headers": headers,
                }
            )
            on_delivery(None, object())  # type: ignore[operator]

        def poll(self, timeout: float) -> None:
            del timeout

        def flush(self) -> None:
            return None

    fake_module.Producer = FakeProducer  # type: ignore[attr-defined]
    sys.modules["confluent_kafka"] = fake_module
    return fake_module, produced_messages


def _write_source_events(source_path: Path, event_count: int) -> None:
    """Write a small JSONL event log for replay tests."""

    with source_path.open("w", encoding="utf-8") as file:
        for index in range(event_count):
            record = {
                "topic": "financial_transactions",
                "partition": index % 2,
                "partition_key": f"cust_stream_{index + 1:08d}",
                "source_sequence": index + 1,
                "produced_at": f"2026-07-01T00:00:{index:02d}",
                "headers": {
                    "event_type": "transaction.created",
                    "schema_version": "stream_v1",
                    "producer": "fraudstream_streaming_generator",
                    "problem_flags": [],
                },
                "value": {
                    "event_id": f"evt_stream_{index + 1:012d}",
                    "transaction_id": f"txn_stream_{index + 1:012d}",
                    "customer_id": f"cust_stream_{index + 1:08d}",
                    "event_timestamp": f"2026-07-01T00:00:{index:02d}",
                },
            }
            file.write(json.dumps(record, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
