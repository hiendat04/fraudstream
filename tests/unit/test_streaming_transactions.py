"""Unit tests for the streaming transaction generator."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

from fraudstream.generators.streaming_transactions import (
    StreamingGeneratorConfig,
    generate_streaming_transactions,
)


class StreamingTransactionGeneratorTest(TestCase):
    """Tests for Kafka-like local stream generation."""

    def test_generator_simulates_streaming_problems(self) -> None:
        """The generator should emit a local topic log and stream problem evidence."""

        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "streaming_transactions"
            config = _test_config(output_dir=output_dir)

            summary = generate_streaming_transactions(config)
            event_log_path = output_dir / "topic=financial_transactions_test" / "events.jsonl"

            self.assertEqual(summary["base_event_count"], 500)
            self.assertEqual(summary["record_count_after_duplicates"], 520)
            self.assertEqual(summary["duplicate_record_count"], 20)
            self.assertGreater(summary["stream_problems"]["late_event_count"], 0)
            self.assertGreater(summary["stream_problems"]["out_of_order_flag_count"], 0)
            self.assertGreater(summary["stream_problems"]["observed_out_of_order_event_count"], 0)
            self.assertGreater(summary["stream_problems"]["burst_event_count"], 0)
            self.assertEqual(summary["event_time_windows"]["window_minutes"], 5)
            self.assertTrue(event_log_path.exists())
            self.assertTrue((output_dir / "_manifest.json").exists())
            self.assertTrue((output_dir / "_stream_summary.json").exists())

            first_record = json.loads(event_log_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(first_record["topic"], "financial_transactions_test")
            self.assertIn("partition_key", first_record)
            self.assertIn("headers", first_record)
            self.assertIn("value", first_record)
            self.assertIn("event_window_start", first_record["value"])
            self.assertIn("event_window_end", first_record["value"])

    def test_config_validation_rejects_inconsistent_timing_rules(self) -> None:
        """Late-event thresholds must leave room for out-of-order backdating."""

        config = replace(
            _test_config(output_dir=Path("/tmp/fraudstream_unused")),
            window_minutes=30,
            late_event_threshold_minutes=30,
        )

        with self.assertRaisesRegex(ValueError, "late_event_threshold_minutes"):
            config.validate()


def _test_config(output_dir: Path) -> StreamingGeneratorConfig:
    """Return a small deterministic config for unit tests."""

    return StreamingGeneratorConfig(
        random_seed=11,
        n_events=500,
        n_customers=300,
        n_merchants=120,
        start_timestamp=datetime(2026, 7, 1, 0, 0, 0),
        duration_minutes=240,
        currency="USD",
        topic="financial_transactions_test",
        n_partitions=4,
        sink_type="local_jsonl",
        window_minutes=5,
        late_event_rate=0.08,
        out_of_order_rate=0.08,
        duplicate_rate=0.04,
        burst_window_count=4,
        burst_event_ratio=0.30,
        late_event_threshold_minutes=30,
        max_late_minutes=120,
        output_dir=output_dir,
    )


if __name__ == "__main__":
    main()
