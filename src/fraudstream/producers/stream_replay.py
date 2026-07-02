"""Replay generated transaction events to Kafka for Flink consumption.

The streaming generator creates a deterministic JSONL topic log. This module
publishes that log to Kafka so a Flink job can consume the same event contract
used by the local source data.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator, Literal


StreamRecord = dict[str, Any]
TimeMode = Literal["fixed_rate", "produced_at"]

DEFAULT_SOURCE_PATH = Path("data/raw_stream/transactions/topic=financial_transactions/events.jsonl")
DEFAULT_TOPIC = "financial_transactions"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
SUPPORTED_TIME_MODES = {"fixed_rate", "produced_at"}


@dataclass(frozen=True)
class ReplayConfig:
    """Runtime settings for replaying generated stream events to Kafka."""

    source_path: Path = DEFAULT_SOURCE_PATH
    topic: str = DEFAULT_TOPIC
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
    events_per_second: float = 1_000.0
    time_mode: TimeMode = "fixed_rate"
    speed_factor: float = 3_600.0
    max_sleep_seconds: float = 1.0
    max_events: int | None = None
    start_offset: int = 0
    progress_interval: int = 10_000
    message_timeout_ms: int = 10_000
    client_id: str = "fraudstream-replay-producer"

    def validate(self) -> None:
        """Raise ValueError when replay settings are not usable."""

        if self.time_mode not in SUPPORTED_TIME_MODES:
            raise ValueError(f"time_mode must be one of {sorted(SUPPORTED_TIME_MODES)}")
        if self.events_per_second < 0:
            raise ValueError("events_per_second must be greater than or equal to 0")
        if self.speed_factor <= 0:
            raise ValueError("speed_factor must be greater than 0")
        if self.max_sleep_seconds < 0:
            raise ValueError("max_sleep_seconds must be greater than or equal to 0")
        if self.max_events is not None and self.max_events <= 0:
            raise ValueError("max_events must be greater than 0 when provided")
        if self.start_offset < 0:
            raise ValueError("start_offset must be greater than or equal to 0")
        if self.progress_interval < 0:
            raise ValueError("progress_interval must be greater than or equal to 0")
        if self.message_timeout_ms <= 0:
            raise ValueError("message_timeout_ms must be greater than 0")
        if not self.topic:
            raise ValueError("topic must not be empty")
        if not self.bootstrap_servers:
            raise ValueError("bootstrap_servers must not be empty")
        if not self.client_id:
            raise ValueError("client_id must not be empty")


@dataclass(frozen=True)
class SourceEvent:
    """One parsed source event plus its source-log position."""

    line_number: int
    record: StreamRecord


class ReplayPacer:
    """Apply replay timing between published events."""

    def __init__(self, config: ReplayConfig) -> None:
        self._config = config
        self._next_fixed_publish = time.monotonic()
        self._fixed_rate_started = False
        self._previous_produced_at: datetime | None = None

    def wait_before_publish(self, record: StreamRecord) -> None:
        """Sleep if the configured replay mode requires pacing."""

        if self._config.time_mode == "fixed_rate":
            self._wait_for_fixed_rate()
            return

        self._wait_for_produced_at_gap(record)

    def _wait_for_fixed_rate(self) -> None:
        if self._config.events_per_second == 0:
            return
        if not self._fixed_rate_started:
            self._fixed_rate_started = True
            return

        self._next_fixed_publish += 1 / self._config.events_per_second
        sleep_seconds = self._next_fixed_publish - time.monotonic()
        if sleep_seconds > 0:
            time.sleep(min(sleep_seconds, self._config.max_sleep_seconds))

    def _wait_for_produced_at_gap(self, record: StreamRecord) -> None:
        produced_at = _parse_timestamp(record["produced_at"])
        if self._previous_produced_at is None:
            self._previous_produced_at = produced_at
            return

        gap_seconds = max(0.0, (produced_at - self._previous_produced_at).total_seconds())
        sleep_seconds = min(
            gap_seconds / self._config.speed_factor,
            self._config.max_sleep_seconds,
        )
        self._previous_produced_at = produced_at
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


class KafkaPublisher:
    """Publish JSONL stream records to Kafka."""

    def __init__(self, config: ReplayConfig) -> None:
        self._config = config
        self._producer: Any | None = None
        self._delivery_errors: list[str] = []

    def open(self) -> None:
        """Create the Kafka producer."""

        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise RuntimeError(
                "Kafka replay requires the optional package 'confluent-kafka'. "
                "Run 'uv sync --extra kafka' before using this command."
            ) from exc

        self._producer = Producer(
            {
                "bootstrap.servers": self._config.bootstrap_servers,
                "client.id": self._config.client_id,
                "message.timeout.ms": self._config.message_timeout_ms,
            }
        )

    def publish(self, record: StreamRecord) -> None:
        """Publish one stream record to Kafka."""

        if self._producer is None:
            raise RuntimeError("Kafka producer is not open")

        encoded_record = json.dumps(record, separators=(",", ":"))
        self._producer.produce(
            self._config.topic,
            key=str(record["partition_key"]).encode("utf-8"),
            value=encoded_record.encode("utf-8"),
            headers=_encode_headers(record),
            on_delivery=self._capture_delivery_result,
        )
        self._producer.poll(0)

    def close(self) -> None:
        """Flush pending records and raise if Kafka rejected any deliveries."""

        if self._producer is not None:
            self._producer.flush()
        if self._delivery_errors:
            sample = "; ".join(self._delivery_errors[:3])
            raise RuntimeError(f"Kafka delivery failed: {sample}")

    def metadata(self) -> dict[str, Any]:
        """Return Kafka target metadata for the replay summary."""

        return {
            "bootstrap_servers": self._config.bootstrap_servers,
            "topic": self._config.topic,
            "client_id": self._config.client_id,
        }

    def _capture_delivery_result(self, error: Any, message: Any) -> None:
        del message
        if error is not None:
            self._delivery_errors.append(str(error))


def replay_stream(config: ReplayConfig) -> dict[str, Any]:
    """Replay source events to Kafka and return a run summary."""

    config.validate()
    if not config.source_path.exists():
        raise FileNotFoundError(f"source_path does not exist: {config.source_path}")

    publisher = KafkaPublisher(config)
    pacer = ReplayPacer(config)
    started_at = _utc_now()
    published_count = 0
    first_source_line: int | None = None
    last_source_line: int | None = None
    first_produced_at: str | None = None
    last_produced_at: str | None = None

    try:
        publisher.open()
        for source_event in _iter_source_events(config):
            record = _prepare_record_for_replay(source_event.record, config.topic)

            pacer.wait_before_publish(record)
            publisher.publish(record)

            published_count += 1
            first_source_line = first_source_line or source_event.line_number
            last_source_line = source_event.line_number
            first_produced_at = first_produced_at or record["produced_at"]
            last_produced_at = record["produced_at"]
            _report_progress(config, published_count)
    finally:
        publisher.close()

    return {
        "source_path": str(config.source_path),
        "sink_type": "kafka",
        "topic": config.topic,
        "published_count": published_count,
        "start_offset": config.start_offset,
        "max_events": config.max_events,
        "first_source_line": first_source_line,
        "last_source_line": last_source_line,
        "first_produced_at": first_produced_at,
        "last_produced_at": last_produced_at,
        "time_mode": config.time_mode,
        "events_per_second": config.events_per_second,
        "speed_factor": config.speed_factor,
        "started_at": started_at,
        "completed_at": _utc_now(),
        "sink": publisher.metadata(),
    }


def _iter_source_events(config: ReplayConfig) -> Iterator[SourceEvent]:
    """Yield parsed source events from the JSONL replay log."""

    yielded_count = 0
    with config.source_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if line_number <= config.start_offset:
                continue
            if config.max_events is not None and yielded_count >= config.max_events:
                break
            if not line.strip():
                continue

            record = json.loads(line)
            _validate_source_record(record, line_number)
            yielded_count += 1
            yield SourceEvent(line_number=line_number, record=record)


def _prepare_record_for_replay(record: StreamRecord, topic: str) -> StreamRecord:
    """Return a record copy with the replay topic applied."""

    replay_record = dict(record)
    replay_record["topic"] = topic
    return replay_record


def _validate_source_record(record: StreamRecord, line_number: int) -> None:
    """Validate the minimum envelope fields required for replay."""

    required_fields = ["topic", "partition_key", "produced_at", "headers", "value"]
    missing_fields = [field for field in required_fields if field not in record]
    if missing_fields:
        raise ValueError(f"line {line_number} is missing required fields: {missing_fields}")


def _encode_headers(record: StreamRecord) -> list[tuple[str, bytes]]:
    """Encode JSON record headers as Kafka headers."""

    return [
        (key, json.dumps(value, separators=(",", ":")).encode("utf-8"))
        for key, value in record.get("headers", {}).items()
    ]


def _report_progress(config: ReplayConfig, published_count: int) -> None:
    """Write lightweight progress messages for long replay runs."""

    if config.progress_interval == 0:
        return
    if published_count % config.progress_interval == 0:
        print(f"replayed {published_count} events", file=sys.stderr)


def _parse_timestamp(value: str) -> datetime:
    """Parse a timestamp generated by the streaming transaction generator."""

    return datetime.fromisoformat(value)


def _utc_now() -> str:
    """Return a compact UTC timestamp for summary artifacts."""

    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for the Kafka replay producer."""

    parser = argparse.ArgumentParser(
        description="Replay generated transaction events to Kafka for Flink."
    )
    parser.add_argument("--source", default=DEFAULT_SOURCE_PATH, type=Path)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--events-per-second", default=1_000.0, type=float)
    parser.add_argument(
        "--time-mode",
        choices=sorted(SUPPORTED_TIME_MODES),
        default="fixed_rate",
    )
    parser.add_argument("--speed-factor", default=3_600.0, type=float)
    parser.add_argument("--max-sleep-seconds", default=1.0, type=float)
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--start-offset", default=0, type=int)
    parser.add_argument("--progress-interval", default=10_000, type=int)
    parser.add_argument("--message-timeout-ms", default=10_000, type=int)
    parser.add_argument("--client-id", default="fraudstream-replay-producer")
    return parser


def config_from_args(args: argparse.Namespace) -> ReplayConfig:
    """Convert parsed CLI args into a ReplayConfig."""

    return ReplayConfig(
        source_path=args.source,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        events_per_second=args.events_per_second,
        time_mode=args.time_mode,
        speed_factor=args.speed_factor,
        max_sleep_seconds=args.max_sleep_seconds,
        max_events=args.max_events,
        start_offset=args.start_offset,
        progress_interval=args.progress_interval,
        message_timeout_ms=args.message_timeout_ms,
        client_id=args.client_id,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the Kafka replay producer from the command line."""

    args = build_parser().parse_args(argv)
    summary = replay_stream(config_from_args(args))
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
