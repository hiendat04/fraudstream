"""Unit tests for measured Flink watermark latency profiles."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

from fraudstream.jobs.flink.transactions import StreamingFeatureConfig
from fraudstream.jobs.flink.watermark_calibration import (
    load_watermark_latency_profile,
    measure_watermark_latency,
)


class FlinkWatermarkCalibrationTest(TestCase):
    """Checks that watermark delay comes from first-seen source measurements."""

    def test_measurement_uses_unique_first_arrivals_and_nearest_rank_p95(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            source_path = Path(temporary_directory) / "events.jsonl"
            records = [_envelope(f"event-{index}", index) for index in range(20)]
            records.extend(
                [
                    _envelope("event-0", 500),
                    {"not": "a transaction envelope"},
                    _envelope("negative-delay", -1),
                ]
            )
            source_path.write_text(
                "".join(f"{json.dumps(record)}\n" for record in records),
                encoding="utf-8",
            )

            profile = measure_watermark_latency(
                source_path,
                environment="production",
                measured_at=datetime(2026, 7, 14, tzinfo=UTC),
            )

            self.assertEqual(profile.records_scanned, 23)
            self.assertEqual(profile.unique_events_measured, 20)
            self.assertEqual(profile.duplicate_records_excluded, 1)
            self.assertEqual(profile.invalid_records_excluded, 1)
            self.assertEqual(profile.negative_delay_records_excluded, 1)
            self.assertEqual(profile.p50_latency_seconds, 9)
            self.assertEqual(profile.p95_latency_seconds, 18)
            self.assertEqual(profile.p99_latency_seconds, 19)
            self.assertEqual(profile.recommended_watermark_delay_seconds, 18)

    def test_config_loads_persisted_p95_instead_of_a_numeric_default(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            source_path = temporary_path / "events.jsonl"
            profile_path = temporary_path / "latency-profile.json"
            source_path.write_text(
                "".join(
                    f"{json.dumps(_envelope(f'event-{index}', index))}\n"
                    for index in range(20)
                ),
                encoding="utf-8",
            )
            profile = measure_watermark_latency(source_path, environment="production")
            profile_path.write_text(
                json.dumps(profile.to_dict(), indent=2) + "\n",
                encoding="utf-8",
            )

            loaded = load_watermark_latency_profile(profile_path)
            resolved = StreamingFeatureConfig(
                watermark_latency_profile=profile_path
            ).resolve_watermark_delay()

            self.assertEqual(loaded.p95_latency_seconds, 18)
            self.assertEqual(resolved.watermark_delay_seconds, 18)
            self.assertEqual(
                resolved.to_dict()["watermark"]["delay_basis"],
                "measured_p95_source_event_time_latency",
            )

    def test_loader_rejects_a_delay_that_does_not_match_measured_p95(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            source_path = temporary_path / "events.jsonl"
            profile_path = temporary_path / "latency-profile.json"
            source_path.write_text(
                f"{json.dumps(_envelope('event-1', 30))}\n",
                encoding="utf-8",
            )
            persisted = measure_watermark_latency(
                source_path,
                environment="production",
            ).to_dict()
            persisted["watermark"]["delay_seconds"] = 5
            profile_path.write_text(
                json.dumps(persisted, indent=2) + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "does not match"):
                load_watermark_latency_profile(profile_path)

    def test_runtime_config_rejects_a_programmatic_guessed_delay(self) -> None:
        config = StreamingFeatureConfig(watermark_delay_seconds=5)

        with self.assertRaisesRegex(ValueError, "must match the p95"):
            config.validate()


def _envelope(event_id: str, delay_seconds: int) -> dict:
    event_timestamp = datetime(2026, 7, 1, tzinfo=UTC)
    produced_at = event_timestamp + timedelta(seconds=delay_seconds)
    return {
        "produced_at": produced_at.isoformat().replace("+00:00", "Z"),
        "value": {
            "event_id": event_id,
            "event_timestamp": event_timestamp.isoformat().replace("+00:00", "Z"),
        },
    }


if __name__ == "__main__":
    main()
