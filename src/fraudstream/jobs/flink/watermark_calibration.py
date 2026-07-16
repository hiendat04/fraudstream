"""Measure source event-time latency and persist a p95 watermark profile."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence


PROFILE_SCHEMA_VERSION = "watermark_latency_profile_v1"
DEFAULT_SOURCE_PATH = Path(
    "data/raw_stream/transactions/topic=financial_transactions/events.jsonl"
)
DEFAULT_PROFILE_PATH = Path("configs/flink/streaming_latency_profile.json")
DEFAULT_ENVIRONMENT = "local_generated_source"
PERCENTILE_METHOD = "nearest_rank"


@dataclass(frozen=True)
class WatermarkLatencyProfile:
    """Validated measurement used to configure Flink out-of-orderness."""

    environment: str
    source_path: str
    source_sha256: str
    measured_at: str
    measurement_start: str
    measurement_end: str
    records_scanned: int
    unique_events_measured: int
    duplicate_records_excluded: int
    invalid_records_excluded: int
    negative_delay_records_excluded: int
    p50_latency_seconds: int
    p95_latency_seconds: int
    p99_latency_seconds: int
    max_latency_seconds: int

    @property
    def recommended_watermark_delay_seconds(self) -> int:
        """Return the measured p95 rounded up to a whole second."""

        return max(1, math.ceil(self.p95_latency_seconds))

    def validate(self) -> None:
        """Raise when the profile cannot safely configure a watermark."""

        if not self.environment.strip():
            raise ValueError("environment must not be blank")
        if not self.source_path.strip():
            raise ValueError("source_path must not be blank")
        if len(self.source_sha256) != 64:
            raise ValueError("source_sha256 must be a SHA-256 hex digest")
        if self.records_scanned <= 0 or self.unique_events_measured <= 0:
            raise ValueError("latency profile must contain measured events")
        if self.unique_events_measured > self.records_scanned:
            raise ValueError("unique_events_measured cannot exceed records_scanned")
        latency_values = (
            self.p50_latency_seconds,
            self.p95_latency_seconds,
            self.p99_latency_seconds,
            self.max_latency_seconds,
        )
        if any(value < 0 for value in latency_values):
            raise ValueError("latency percentiles must not be negative")
        if latency_values != tuple(sorted(latency_values)):
            raise ValueError("latency percentiles must be monotonically increasing")

    def to_dict(self) -> dict[str, Any]:
        """Return the persisted engineering evidence contract."""

        self.validate()
        return {
            "schema_version": PROFILE_SCHEMA_VERSION,
            "environment": self.environment,
            "source": {
                "path": self.source_path,
                "sha256": self.source_sha256,
                "event_timestamp_field": "value.event_timestamp",
                "arrival_timestamp_field": "produced_at",
            },
            "measurement": {
                "measured_at": self.measured_at,
                "measurement_start": self.measurement_start,
                "measurement_end": self.measurement_end,
                "records_scanned": self.records_scanned,
                "unique_events_measured": self.unique_events_measured,
                "duplicate_records_excluded": self.duplicate_records_excluded,
                "invalid_records_excluded": self.invalid_records_excluded,
                "negative_delay_records_excluded": self.negative_delay_records_excluded,
                "percentile_method": PERCENTILE_METHOD,
            },
            "latency_seconds": {
                "p50": self.p50_latency_seconds,
                "p95": self.p95_latency_seconds,
                "p99": self.p99_latency_seconds,
                "max": self.max_latency_seconds,
            },
            "watermark": {
                "basis": "p95_source_event_time_latency",
                "delay_seconds": self.recommended_watermark_delay_seconds,
                "delay_minutes_rounded_up": math.ceil(
                    self.recommended_watermark_delay_seconds / 60
                ),
            },
        }


def measure_watermark_latency(
    source_path: Path,
    *,
    environment: str,
    measured_at: datetime | None = None,
) -> WatermarkLatencyProfile:
    """Measure first-arrival latency for unique events in a JSONL source export."""

    if not source_path.is_file():
        raise FileNotFoundError(f"stream source does not exist: {source_path}")
    if not environment.strip():
        raise ValueError("environment must not be blank")

    seen_event_ids: set[str] = set()
    delays_seconds: list[int] = []
    source_digest = hashlib.sha256()
    records_scanned = 0
    duplicate_records_excluded = 0
    invalid_records_excluded = 0
    negative_delay_records_excluded = 0
    first_produced_at: datetime | None = None
    last_produced_at: datetime | None = None

    with source_path.open("rb") as source_file:
        for raw_line in source_file:
            source_digest.update(raw_line)
            records_scanned += 1
            try:
                envelope = json.loads(raw_line)
                event_id = _required_non_blank_string(envelope["value"]["event_id"])
                event_timestamp = _parse_utc_timestamp(envelope["value"]["event_timestamp"])
                produced_at = _parse_utc_timestamp(envelope["produced_at"])
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                invalid_records_excluded += 1
                continue

            first_produced_at = (
                min(first_produced_at, produced_at) if first_produced_at else produced_at
            )
            last_produced_at = max(last_produced_at, produced_at) if last_produced_at else produced_at
            if event_id in seen_event_ids:
                duplicate_records_excluded += 1
                continue
            seen_event_ids.add(event_id)

            delay_seconds = math.ceil((produced_at - event_timestamp).total_seconds())
            if delay_seconds < 0:
                negative_delay_records_excluded += 1
                continue
            delays_seconds.append(delay_seconds)

    if not delays_seconds or first_produced_at is None or last_produced_at is None:
        raise ValueError("source contains no valid non-negative event latency observations")

    delays_seconds.sort()
    profile = WatermarkLatencyProfile(
        environment=environment.strip(),
        source_path=str(source_path),
        source_sha256=source_digest.hexdigest(),
        measured_at=_to_utc_string(measured_at or datetime.now(UTC)),
        measurement_start=_to_utc_string(first_produced_at),
        measurement_end=_to_utc_string(last_produced_at),
        records_scanned=records_scanned,
        unique_events_measured=len(delays_seconds),
        duplicate_records_excluded=duplicate_records_excluded,
        invalid_records_excluded=invalid_records_excluded,
        negative_delay_records_excluded=negative_delay_records_excluded,
        p50_latency_seconds=_nearest_rank(delays_seconds, 50),
        p95_latency_seconds=_nearest_rank(delays_seconds, 95),
        p99_latency_seconds=_nearest_rank(delays_seconds, 99),
        max_latency_seconds=delays_seconds[-1],
    )
    profile.validate()
    return profile


def load_watermark_latency_profile(path: Path) -> WatermarkLatencyProfile:
    """Load and validate a persisted watermark latency profile."""

    if not path.is_file():
        raise FileNotFoundError(
            f"watermark latency profile does not exist: {path}. "
            "Run 'python -m fraudstream.jobs.flink.watermark_calibration' first."
        )
    with path.open("r", encoding="utf-8") as profile_file:
        raw = json.load(profile_file)
    if raw.get("schema_version") != PROFILE_SCHEMA_VERSION:
        raise ValueError(f"unsupported watermark profile schema: {raw.get('schema_version')}")
    if raw.get("measurement", {}).get("percentile_method") != PERCENTILE_METHOD:
        raise ValueError("watermark latency profile must use nearest_rank percentiles")
    if raw.get("watermark", {}).get("basis") != "p95_source_event_time_latency":
        raise ValueError("watermark latency profile must be based on p95 source latency")

    source = raw["source"]
    measurement = raw["measurement"]
    latency = raw["latency_seconds"]
    profile = WatermarkLatencyProfile(
        environment=str(raw["environment"]),
        source_path=str(source["path"]),
        source_sha256=str(source["sha256"]),
        measured_at=str(measurement["measured_at"]),
        measurement_start=str(measurement["measurement_start"]),
        measurement_end=str(measurement["measurement_end"]),
        records_scanned=int(measurement["records_scanned"]),
        unique_events_measured=int(measurement["unique_events_measured"]),
        duplicate_records_excluded=int(measurement["duplicate_records_excluded"]),
        invalid_records_excluded=int(measurement["invalid_records_excluded"]),
        negative_delay_records_excluded=int(measurement["negative_delay_records_excluded"]),
        p50_latency_seconds=int(latency["p50"]),
        p95_latency_seconds=int(latency["p95"]),
        p99_latency_seconds=int(latency["p99"]),
        max_latency_seconds=int(latency["max"]),
    )
    profile.validate()
    persisted_delay = int(raw["watermark"]["delay_seconds"])
    if persisted_delay != profile.recommended_watermark_delay_seconds:
        raise ValueError("watermark delay does not match the measured p95 latency")
    return profile


def build_parser() -> argparse.ArgumentParser:
    """Build the watermark calibration CLI parser."""

    parser = argparse.ArgumentParser(
        description="Measure source event-time latency and write a p95 watermark profile."
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_PROFILE_PATH)
    parser.add_argument("--environment", default=DEFAULT_ENVIRONMENT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Measure and persist one watermark latency profile."""

    try:
        args = build_parser().parse_args(argv)
        profile = measure_watermark_latency(
            args.source,
            environment=args.environment,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(profile.to_dict(), indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(profile.to_dict(), indent=2))
    except (FileNotFoundError, KeyError, TypeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _nearest_rank(sorted_values: list[int], percentile: int) -> int:
    if not sorted_values:
        raise ValueError("percentile requires at least one observation")
    if not 0 < percentile <= 100:
        raise ValueError("percentile must be greater than 0 and at most 100")
    rank = max(1, math.ceil(percentile / 100 * len(sorted_values)))
    return sorted_values[rank - 1]


def _required_non_blank_string(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("identifier must be a non-blank string")
    return value.strip()


def _parse_utc_timestamp(raw_value: Any) -> datetime:
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise ValueError("timestamp must be a non-blank string")
    parsed = datetime.fromisoformat(raw_value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _to_utc_string(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
