"""Build real-time customer and merchant fraud features from Kafka events."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Sequence

from fraudstream.jobs.flink.watermark_calibration import (
    DEFAULT_PROFILE_PATH,
    load_watermark_latency_profile,
)


APP_NAME = "FraudStreamStreamingFeatures"
DEFAULT_FLINK_UI_PORT = 8081
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_SOURCE_TOPIC = "financial_transactions"
DEFAULT_GROUP_ID = "fraudstream-flink-features-v1"
DEFAULT_CONNECTOR_JAR = Path("flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar")
DEFAULT_CHECKPOINT_DIR = Path("data/flink/checkpoints/streaming_features")

DEFAULT_CLEAN_TOPIC = "financial_transactions_clean"
DEFAULT_INVALID_TOPIC = "financial_transactions_invalid"
DEFAULT_DUPLICATE_TOPIC = "financial_transactions_duplicate"
DEFAULT_LATE_TOPIC = "financial_transactions_late"
DEFAULT_CUSTOMER_FEATURE_TOPIC = "fraud_features_customer_5m"
DEFAULT_MERCHANT_FEATURE_TOPIC = "fraud_features_merchant_5m"
DEFAULT_ALERT_TOPIC = "fraud_alerts"

EXPECTED_EVENT_TYPE = "transaction.created"
EXPECTED_SCHEMA_VERSION = "stream_v1"
MILLISECONDS_PER_SECOND = 1_000
SECONDS_PER_MINUTE = 60
BENCHMARK_PROFILES = {"none", "baseline", "chained", "optimized"}


@dataclass(frozen=True)
class StreamingFeatureConfig:
    """Runtime settings for the Flink Kafka feature job."""

    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
    source_topic: str = DEFAULT_SOURCE_TOPIC
    group_id: str = DEFAULT_GROUP_ID
    connector_jar: Path = DEFAULT_CONNECTOR_JAR
    checkpoint_dir: Path = DEFAULT_CHECKPOINT_DIR
    clean_topic: str = DEFAULT_CLEAN_TOPIC
    invalid_topic: str = DEFAULT_INVALID_TOPIC
    duplicate_topic: str = DEFAULT_DUPLICATE_TOPIC
    late_topic: str = DEFAULT_LATE_TOPIC
    customer_feature_topic: str = DEFAULT_CUSTOMER_FEATURE_TOPIC
    merchant_feature_topic: str = DEFAULT_MERCHANT_FEATURE_TOPIC
    alert_topic: str = DEFAULT_ALERT_TOPIC
    parallelism: int = 4
    operator_chaining_enabled: bool = True
    flink_ui_enabled: bool = False
    flink_ui_port: int = DEFAULT_FLINK_UI_PORT
    benchmark_profile: str = "none"
    checkpoint_interval_seconds: int = 30
    checkpoint_min_pause_seconds: int = 10
    checkpoint_timeout_seconds: int = 120
    watermark_latency_profile: Path = DEFAULT_PROFILE_PATH
    watermark_delay_seconds: int | None = None
    allowed_lateness_minutes: int = 40
    idle_partition_timeout_seconds: int = 60
    deduplication_ttl_hours: int = 24
    window_minutes: int = 5
    customer_alert_txn_count: int = 5
    customer_alert_amount: Decimal = Decimal("2000.00")
    merchant_alert_txn_count: int = 20

    def validate(self, *, require_connector_jar: bool = False) -> None:
        """Raise when a runtime setting cannot produce a safe Flink job."""

        string_settings = {
            "bootstrap_servers": self.bootstrap_servers,
            "source_topic": self.source_topic,
            "group_id": self.group_id,
            "clean_topic": self.clean_topic,
            "invalid_topic": self.invalid_topic,
            "duplicate_topic": self.duplicate_topic,
            "late_topic": self.late_topic,
            "customer_feature_topic": self.customer_feature_topic,
            "merchant_feature_topic": self.merchant_feature_topic,
            "alert_topic": self.alert_topic,
        }
        for setting_name, value in string_settings.items():
            if not value.strip():
                raise ValueError(f"{setting_name} must not be blank")

        output_topics = tuple(string_settings[name] for name in string_settings if name.endswith("topic"))
        if len(output_topics) != len(set(output_topics)):
            raise ValueError("source and output Kafka topics must be unique")

        positive_settings = {
            "parallelism": self.parallelism,
            "checkpoint_interval_seconds": self.checkpoint_interval_seconds,
            "checkpoint_min_pause_seconds": self.checkpoint_min_pause_seconds,
            "checkpoint_timeout_seconds": self.checkpoint_timeout_seconds,
            "idle_partition_timeout_seconds": self.idle_partition_timeout_seconds,
            "deduplication_ttl_hours": self.deduplication_ttl_hours,
            "window_minutes": self.window_minutes,
            "customer_alert_txn_count": self.customer_alert_txn_count,
            "merchant_alert_txn_count": self.merchant_alert_txn_count,
        }
        for setting_name, value in positive_settings.items():
            if value <= 0:
                raise ValueError(f"{setting_name} must be greater than zero")
        if self.benchmark_profile not in BENCHMARK_PROFILES:
            raise ValueError(
                f"benchmark_profile must be one of {sorted(BENCHMARK_PROFILES)}"
            )
        if not 1 <= self.flink_ui_port <= 65_535:
            raise ValueError("flink_ui_port must be between 1 and 65535")
        if self.watermark_delay_seconds is not None and self.watermark_delay_seconds <= 0:
            raise ValueError("watermark_delay_seconds must be greater than zero")
        if self.watermark_delay_seconds is not None:
            measured_delay = load_watermark_latency_profile(
                self.watermark_latency_profile
            ).recommended_watermark_delay_seconds
            if self.watermark_delay_seconds != measured_delay:
                raise ValueError(
                    "watermark_delay_seconds must match the p95 in the measured "
                    "latency profile"
                )
        if require_connector_jar and self.watermark_delay_seconds is None:
            raise ValueError("watermark delay must be resolved from a measured latency profile")
        if self.allowed_lateness_minutes < 0:
            raise ValueError("allowed_lateness_minutes must be zero or greater")
        if self.customer_alert_amount <= 0:
            raise ValueError("customer_alert_amount must be greater than zero")
        if self.connector_jar.suffix != ".jar":
            raise ValueError("connector_jar must point to a .jar file")
        if require_connector_jar and not self.connector_jar.is_file():
            raise FileNotFoundError(
                f"Kafka connector JAR does not exist: {self.connector_jar}. "
                "Run the connector setup command from docs/07_flink_streaming_pipeline.md."
            )

    def resolve_watermark_delay(self) -> "StreamingFeatureConfig":
        """Load the measured source p95 when no programmatic value is present."""

        if self.watermark_delay_seconds is not None:
            return self
        profile = load_watermark_latency_profile(self.watermark_latency_profile)
        return replace(
            self,
            watermark_delay_seconds=profile.recommended_watermark_delay_seconds,
        )

    def apply_benchmark_profile(self) -> "StreamingFeatureConfig":
        """Apply a reproducible local benchmark profile."""

        if self.benchmark_profile not in BENCHMARK_PROFILES:
            raise ValueError(
                f"benchmark_profile must be one of {sorted(BENCHMARK_PROFILES)}"
            )
        if self.benchmark_profile == "none":
            return self
        profile_settings = {
            "baseline": {"parallelism": 1, "operator_chaining_enabled": False},
            "chained": {"parallelism": 1, "operator_chaining_enabled": True},
            "optimized": {"parallelism": 4, "operator_chaining_enabled": True},
        }
        return replace(
            self,
            **profile_settings[self.benchmark_profile],
            flink_ui_enabled=True,
        )

    @property
    def job_name(self) -> str:
        """Return a UI-friendly job name."""

        if self.benchmark_profile == "none":
            return APP_NAME
        return f"{APP_NAME}-{self.benchmark_profile}"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable runtime configuration."""

        return {
            "app_name": self.job_name,
            "bootstrap_servers": self.bootstrap_servers,
            "source_topic": self.source_topic,
            "group_id": self.group_id,
            "connector_jar": str(self.connector_jar),
            "checkpoint_dir": str(self.checkpoint_dir),
            "output_topics": {
                "clean": self.clean_topic,
                "invalid": self.invalid_topic,
                "duplicate": self.duplicate_topic,
                "late": self.late_topic,
                "customer_features": self.customer_feature_topic,
                "merchant_features": self.merchant_feature_topic,
                "alerts": self.alert_topic,
            },
            "parallelism": self.parallelism,
            "operator_chaining_enabled": self.operator_chaining_enabled,
            "benchmark": {
                "profile": self.benchmark_profile,
                "flink_ui_enabled": self.flink_ui_enabled,
                "flink_ui_url": (
                    f"http://localhost:{self.flink_ui_port}"
                    if self.flink_ui_enabled
                    else None
                ),
            },
            "checkpoint_interval_seconds": self.checkpoint_interval_seconds,
            "watermark": {
                "strategy": "bounded_out_of_orderness",
                "delay_basis": "measured_p95_source_event_time_latency",
                "latency_profile": str(self.watermark_latency_profile),
                "delay_seconds": self.watermark_delay_seconds,
            },
            "allowed_lateness_minutes": self.allowed_lateness_minutes,
            "idle_partition_timeout_seconds": self.idle_partition_timeout_seconds,
            "deduplication_ttl_hours": self.deduplication_ttl_hours,
            "window_minutes": self.window_minutes,
            "alert_thresholds": {
                "customer_txn_count": self.customer_alert_txn_count,
                "customer_amount": str(self.customer_alert_amount),
                "merchant_txn_count": self.merchant_alert_txn_count,
            },
        }


@dataclass(frozen=True)
class ParseResult:
    """Typed result of validating one Kafka JSON value."""

    event: dict[str, Any] | None
    error_codes: tuple[str, ...]
    raw_payload: str

    @property
    def is_valid(self) -> bool:
        """Return whether the record can enter the feature pipeline."""

        return self.event is not None and not self.error_codes


def parse_transaction_message(raw_payload: str) -> ParseResult:
    """Validate and normalize one generated transaction envelope."""

    try:
        envelope = json.loads(raw_payload)
    except (json.JSONDecodeError, TypeError):
        return ParseResult(None, ("invalid_json",), raw_payload)
    if not isinstance(envelope, dict):
        return ParseResult(None, ("invalid_envelope",), raw_payload)

    errors: list[str] = []
    headers = envelope.get("headers")
    value = envelope.get("value")
    if not isinstance(headers, dict):
        errors.append("invalid_headers")
        headers = {}
    if not isinstance(value, dict):
        errors.append("invalid_value")
        value = {}

    required_envelope_fields = ("topic", "partition", "partition_key", "source_sequence", "produced_at")
    required_value_fields = (
        "event_id",
        "transaction_id",
        "account_id",
        "customer_id",
        "merchant_id",
        "merchant_category",
        "amount",
        "currency",
        "channel",
        "transaction_status",
        "is_fraud",
        "event_timestamp",
        "created_ts",
        "device_id",
        "ip_address",
    )
    for field_name in required_envelope_fields:
        if envelope.get(field_name) is None:
            errors.append(f"missing_envelope_{field_name}")
    for field_name in required_value_fields:
        if value.get(field_name) is None:
            errors.append(f"missing_value_{field_name}")

    if headers.get("event_type") != EXPECTED_EVENT_TYPE:
        errors.append("unsupported_event_type")
    if headers.get("schema_version") != EXPECTED_SCHEMA_VERSION:
        errors.append("unsupported_schema_version")
    if errors:
        return ParseResult(None, tuple(sorted(set(errors))), raw_payload)

    identifier_fields = (
        "event_id",
        "transaction_id",
        "account_id",
        "customer_id",
        "merchant_id",
        "currency",
        "channel",
        "transaction_status",
        "device_id",
        "ip_address",
    )
    for field_name in identifier_fields:
        if not isinstance(value[field_name], str) or not value[field_name].strip():
            errors.append(f"invalid_{field_name}")
    if str(envelope["partition_key"]).strip() != str(value["customer_id"]).strip():
        errors.append("partition_key_customer_mismatch")

    try:
        amount = Decimal(str(value["amount"]))
        if not amount.is_finite() or amount < 0 or amount.as_tuple().exponent < -2:
            raise InvalidOperation
    except (InvalidOperation, ValueError):
        amount = Decimal("0")
        errors.append("invalid_amount")

    try:
        event_timestamp = _parse_utc_timestamp(value["event_timestamp"])
    except (TypeError, ValueError):
        event_timestamp = datetime.fromtimestamp(0, UTC)
        errors.append("invalid_event_timestamp")
    try:
        produced_at = _parse_utc_timestamp(envelope["produced_at"])
    except (TypeError, ValueError):
        produced_at = datetime.fromtimestamp(0, UTC)
        errors.append("invalid_produced_at")
    if not isinstance(value["is_fraud"], bool):
        errors.append("invalid_is_fraud")

    try:
        source_partition = int(envelope["partition"])
        source_sequence = int(envelope["source_sequence"])
        if source_partition < 0 or source_sequence <= 0:
            raise ValueError
    except (TypeError, ValueError):
        source_partition = 0
        source_sequence = 0
        errors.append("invalid_source_position")

    problem_flags = headers.get("problem_flags", [])
    if not isinstance(problem_flags, list) or not all(isinstance(flag, str) for flag in problem_flags):
        errors.append("invalid_problem_flags")
        problem_flags = []

    if errors:
        return ParseResult(None, tuple(sorted(set(errors))), raw_payload)

    normalized_event = {
        "event_id": value["event_id"].strip(),
        "transaction_id": value["transaction_id"].strip(),
        "account_id": value["account_id"].strip(),
        "customer_id": value["customer_id"].strip(),
        "merchant_id": value["merchant_id"].strip(),
        "merchant_category": str(value["merchant_category"]).strip(),
        "amount": f"{amount:.2f}",
        "amount_cents": int(amount * 100),
        "currency": value["currency"].strip().upper(),
        "city": _optional_string(value.get("city")),
        "channel": value["channel"].strip().lower(),
        "transaction_status": value["transaction_status"].strip().lower(),
        "evaluation_is_fraud": value["is_fraud"],
        "event_timestamp": _to_utc_string(event_timestamp),
        "event_timestamp_ms": _to_epoch_milliseconds(event_timestamp),
        "produced_at": _to_utc_string(produced_at),
        "arrival_delay_seconds": int((produced_at - event_timestamp).total_seconds()),
        "device_id": value["device_id"].strip(),
        "ip_address": value["ip_address"].strip(),
        "authentication_method": _optional_string(value.get("authentication_method")),
        "schema_version": headers["schema_version"],
        "problem_flags": sorted(set(problem_flags)),
        "source_topic": str(envelope["topic"]),
        "source_partition": source_partition,
        "source_sequence": source_sequence,
        "partition_key": str(envelope["partition_key"]),
    }
    return ParseResult(normalized_event, (), raw_payload)


def build_invalid_record(result: ParseResult, observed_at: datetime | None = None) -> dict[str, Any]:
    """Build an auditable invalid-event output from a failed parse result."""

    return {
        "error_codes": list(result.error_codes),
        "raw_payload": result.raw_payload,
        "observed_at": _to_utc_string(observed_at or datetime.now(UTC)),
    }


def extract_event_timestamp_ms(raw_payload: str) -> int:
    """Extract event time for the post-source watermark assigner."""

    try:
        envelope = json.loads(raw_payload)
        return _to_epoch_milliseconds(_parse_utc_timestamp(envelope["value"]["event_timestamp"]))
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return 0


def event_business_fingerprint(event: dict[str, Any]) -> str:
    """Hash stable business fields while ignoring replay metadata."""

    business_fields = (
        "event_id",
        "transaction_id",
        "account_id",
        "customer_id",
        "merchant_id",
        "merchant_category",
        "amount",
        "currency",
        "city",
        "channel",
        "transaction_status",
        "evaluation_is_fraud",
        "event_timestamp",
        "device_id",
        "ip_address",
        "authentication_method",
        "schema_version",
    )
    canonical = {field_name: event.get(field_name) for field_name in business_fields}
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def create_feature_accumulator(entity_type: str) -> dict[str, Any]:
    """Create an incremental accumulator for a customer or merchant window."""

    if entity_type not in {"customer", "merchant"}:
        raise ValueError("entity_type must be customer or merchant")
    return {
        "txn_count": 0,
        "amount_sum_cents": 0,
        "amount_max_cents": 0,
        "declined_txn_count": 0,
        "merchant_ids": set(),
        "customer_ids": set(),
        "device_ids": set(),
        "last_event_timestamp_ms": 0,
        "merchant_category": None,
    }


def add_event_to_accumulator(
    accumulator: dict[str, Any],
    event: dict[str, Any],
) -> dict[str, Any]:
    """Increment one window accumulator with a normalized event."""

    amount_cents = int(event["amount_cents"])
    accumulator["txn_count"] += 1
    accumulator["amount_sum_cents"] += amount_cents
    accumulator["amount_max_cents"] = max(accumulator["amount_max_cents"], amount_cents)
    accumulator["declined_txn_count"] += int(event["transaction_status"] == "declined")
    accumulator["merchant_ids"].add(event["merchant_id"])
    accumulator["customer_ids"].add(event["customer_id"])
    accumulator["device_ids"].add(event["device_id"])
    accumulator["last_event_timestamp_ms"] = max(
        accumulator["last_event_timestamp_ms"],
        int(event["event_timestamp_ms"]),
    )
    accumulator["merchant_category"] = accumulator["merchant_category"] or event.get("merchant_category")
    return accumulator


def merge_feature_accumulators(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    """Merge partial accumulators produced by parallel window aggregation."""

    merged = create_feature_accumulator("customer")
    merged["txn_count"] = left["txn_count"] + right["txn_count"]
    merged["amount_sum_cents"] = left["amount_sum_cents"] + right["amount_sum_cents"]
    merged["amount_max_cents"] = max(left["amount_max_cents"], right["amount_max_cents"])
    merged["declined_txn_count"] = left["declined_txn_count"] + right["declined_txn_count"]
    merged["merchant_ids"] = set(left["merchant_ids"]) | set(right["merchant_ids"])
    merged["customer_ids"] = set(left["customer_ids"]) | set(right["customer_ids"])
    merged["device_ids"] = set(left["device_ids"]) | set(right["device_ids"])
    merged["last_event_timestamp_ms"] = max(
        left["last_event_timestamp_ms"],
        right["last_event_timestamp_ms"],
    )
    merged["merchant_category"] = left.get("merchant_category") or right.get("merchant_category")
    return merged


def build_feature_record(
    *,
    entity_type: str,
    entity_id: str,
    window_start_ms: int,
    window_end_ms: int,
    accumulator: dict[str, Any],
    is_correction: bool,
    emitted_at: datetime | None = None,
) -> dict[str, Any]:
    """Materialize one deterministic feature record from a window accumulator."""

    txn_count = int(accumulator["txn_count"])
    amount_sum_cents = int(accumulator["amount_sum_cents"])
    feature_type = "customer_velocity_5m" if entity_type == "customer" else "merchant_activity_5m"
    feature_id = f"{feature_type}:{entity_id}:{window_start_ms}"
    record: dict[str, Any] = {
        "feature_id": feature_id,
        "feature_type": feature_type,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "window_start": _milliseconds_to_utc_string(window_start_ms),
        "window_end": _milliseconds_to_utc_string(window_end_ms),
        "window_size_minutes": round((window_end_ms - window_start_ms) / 60_000),
        "txn_count": txn_count,
        "amount_sum": _cents_to_decimal_string(amount_sum_cents),
        "amount_avg": _cents_to_decimal_string(round(amount_sum_cents / txn_count) if txn_count else 0),
        "amount_max": _cents_to_decimal_string(int(accumulator["amount_max_cents"])),
        "declined_txn_count": int(accumulator["declined_txn_count"]),
        "last_event_timestamp": _milliseconds_to_utc_string(accumulator["last_event_timestamp_ms"]),
        "is_correction": is_correction,
        "emitted_at": _to_utc_string(emitted_at or datetime.now(UTC)),
    }
    if entity_type == "customer":
        record.update(
            {
                "customer_id": entity_id,
                "distinct_merchant_count": len(accumulator["merchant_ids"]),
                "distinct_device_count": len(accumulator["device_ids"]),
            }
        )
    elif entity_type == "merchant":
        record.update(
            {
                "merchant_id": entity_id,
                "merchant_category": accumulator.get("merchant_category"),
                "distinct_customer_count": len(accumulator["customer_ids"]),
            }
        )
    else:
        raise ValueError("entity_type must be customer or merchant")
    return record


def build_alert_records(
    feature: dict[str, Any],
    config: StreamingFeatureConfig,
    detected_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Create deterministic velocity or burst alerts from one feature record."""

    alerts: list[dict[str, Any]] = []
    if feature["entity_type"] == "customer":
        if feature["txn_count"] >= config.customer_alert_txn_count:
            alerts.append(
                _build_alert(
                    feature,
                    rule_name="customer_velocity_count",
                    threshold=str(config.customer_alert_txn_count),
                    observed=str(feature["txn_count"]),
                    severity=_threshold_severity(feature["txn_count"], config.customer_alert_txn_count),
                    detected_at=detected_at,
                )
            )
        amount_sum = Decimal(feature["amount_sum"])
        if amount_sum >= config.customer_alert_amount:
            alerts.append(
                _build_alert(
                    feature,
                    rule_name="customer_velocity_amount",
                    threshold=str(config.customer_alert_amount),
                    observed=str(amount_sum),
                    severity="high" if amount_sum >= config.customer_alert_amount * 2 else "medium",
                    detected_at=detected_at,
                )
            )
    elif feature["entity_type"] == "merchant" and feature["txn_count"] >= config.merchant_alert_txn_count:
        alerts.append(
            _build_alert(
                feature,
                rule_name="merchant_burst_count",
                threshold=str(config.merchant_alert_txn_count),
                observed=str(feature["txn_count"]),
                severity=_threshold_severity(feature["txn_count"], config.merchant_alert_txn_count),
                detected_at=detected_at,
            )
        )
    return alerts


def build_late_record(
    event: dict[str, Any],
    *,
    window_name: str,
    window_minutes: int,
    recorded_at: datetime | None = None,
) -> dict[str, Any]:
    """Describe a valid event rejected after a window's cleanup horizon."""

    window_start_ms, window_end_ms = event_time_window(int(event["event_timestamp_ms"]), window_minutes)
    return {
        "late_event_id": (
            f"{event['event_id']}:{window_name}:{event['source_partition']}:{event['source_sequence']}"
        ),
        "event_id": event["event_id"],
        "transaction_id": event["transaction_id"],
        "window_name": window_name,
        "window_start": _milliseconds_to_utc_string(window_start_ms),
        "window_end": _milliseconds_to_utc_string(window_end_ms),
        "event_timestamp": event["event_timestamp"],
        "produced_at": event["produced_at"],
        "arrival_delay_seconds": event["arrival_delay_seconds"],
        "source_topic": event["source_topic"],
        "source_partition": event["source_partition"],
        "source_sequence": event["source_sequence"],
        "reason": "window_state_expired",
        "recorded_at": _to_utc_string(recorded_at or datetime.now(UTC)),
    }


def event_time_window(event_timestamp_ms: int, window_minutes: int) -> tuple[int, int]:
    """Return the half-open tumbling window containing one event timestamp."""

    window_size_ms = window_minutes * SECONDS_PER_MINUTE * MILLISECONDS_PER_SECOND
    window_start_ms = event_timestamp_ms - event_timestamp_ms % window_size_ms
    return window_start_ms, window_start_ms + window_size_ms


def json_dumps(value: dict[str, Any]) -> str:
    """Encode one deterministic compact Kafka JSON value."""

    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def run_streaming_feature_job(config: StreamingFeatureConfig) -> None:
    """Load PyFlink lazily and execute the unbounded Kafka job."""

    config = config.apply_benchmark_profile()
    config = config.resolve_watermark_delay()
    config.validate(require_connector_jar=True)
    if sys.version_info >= (3, 13):
        raise RuntimeError(
            "PyFlink 2.2 requires Python 3.12 for this project. Run the job with "
            "'uv run --project flink --python 3.12'."
        )
    try:
        from fraudstream.jobs.flink.runtime import execute_streaming_feature_job
    except ImportError as exc:
        raise RuntimeError(
            "PyFlink is not installed in the active environment. Run "
            "'UV_CACHE_DIR=/tmp/fraudstream-uv-cache uv sync --project flink --python 3.12'."
        ) from exc
    if config.flink_ui_enabled:
        print(f"Flink UI: http://localhost:{config.flink_ui_port}")
    execute_streaming_feature_job(config)


def build_parser() -> argparse.ArgumentParser:
    """Build the streaming feature-job CLI parser."""

    parser = argparse.ArgumentParser(
        description="Consume transaction events with Flink and emit real-time features and alerts."
    )
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--source-topic", default=DEFAULT_SOURCE_TOPIC)
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID)
    parser.add_argument("--connector-jar", type=Path, default=DEFAULT_CONNECTOR_JAR)
    parser.add_argument("--checkpoint-dir", type=Path, default=DEFAULT_CHECKPOINT_DIR)
    parser.add_argument("--parallelism", type=int, default=4)
    parser.add_argument(
        "--operator-chaining",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable operator chaining; use --no-operator-chaining for diagnostics.",
    )
    parser.add_argument("--flink-ui", action="store_true")
    parser.add_argument("--flink-ui-port", type=int, default=DEFAULT_FLINK_UI_PORT)
    parser.add_argument(
        "--benchmark-profile",
        choices=sorted(BENCHMARK_PROFILES),
        default="none",
        help="Apply a reproducible baseline, chaining-only, or optimized profile.",
    )
    parser.add_argument("--checkpoint-interval-seconds", type=int, default=30)
    parser.add_argument("--latency-profile", type=Path, default=DEFAULT_PROFILE_PATH)
    parser.add_argument("--allowed-lateness-minutes", type=int, default=40)
    parser.add_argument("--idle-partition-timeout-seconds", type=int, default=60)
    parser.add_argument("--deduplication-ttl-hours", type=int, default=24)
    parser.add_argument("--window-minutes", type=int, default=5)
    parser.add_argument("--customer-alert-txn-count", type=int, default=5)
    parser.add_argument("--customer-alert-amount", type=Decimal, default=Decimal("2000.00"))
    parser.add_argument("--merchant-alert-txn-count", type=int, default=20)
    parser.add_argument("--clean-topic", default=DEFAULT_CLEAN_TOPIC)
    parser.add_argument("--invalid-topic", default=DEFAULT_INVALID_TOPIC)
    parser.add_argument("--duplicate-topic", default=DEFAULT_DUPLICATE_TOPIC)
    parser.add_argument("--late-topic", default=DEFAULT_LATE_TOPIC)
    parser.add_argument("--customer-feature-topic", default=DEFAULT_CUSTOMER_FEATURE_TOPIC)
    parser.add_argument("--merchant-feature-topic", default=DEFAULT_MERCHANT_FEATURE_TOPIC)
    parser.add_argument("--alert-topic", default=DEFAULT_ALERT_TOPIC)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print the resolved configuration without starting Flink.",
    )
    return parser


def config_from_args(args: argparse.Namespace) -> StreamingFeatureConfig:
    """Build a validated configuration from parsed CLI arguments."""

    config = StreamingFeatureConfig(
        bootstrap_servers=args.bootstrap_servers,
        source_topic=args.source_topic,
        group_id=args.group_id,
        connector_jar=args.connector_jar,
        checkpoint_dir=args.checkpoint_dir,
        clean_topic=args.clean_topic,
        invalid_topic=args.invalid_topic,
        duplicate_topic=args.duplicate_topic,
        late_topic=args.late_topic,
        customer_feature_topic=args.customer_feature_topic,
        merchant_feature_topic=args.merchant_feature_topic,
        alert_topic=args.alert_topic,
        parallelism=args.parallelism,
        operator_chaining_enabled=args.operator_chaining,
        flink_ui_enabled=args.flink_ui,
        flink_ui_port=args.flink_ui_port,
        benchmark_profile=args.benchmark_profile,
        checkpoint_interval_seconds=args.checkpoint_interval_seconds,
        watermark_latency_profile=args.latency_profile,
        allowed_lateness_minutes=args.allowed_lateness_minutes,
        idle_partition_timeout_seconds=args.idle_partition_timeout_seconds,
        deduplication_ttl_hours=args.deduplication_ttl_hours,
        window_minutes=args.window_minutes,
        customer_alert_txn_count=args.customer_alert_txn_count,
        customer_alert_amount=args.customer_alert_amount,
        merchant_alert_txn_count=args.merchant_alert_txn_count,
    )
    config = config.apply_benchmark_profile().resolve_watermark_delay()
    config.validate()
    return config


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Flink streaming feature job from the command line."""

    try:
        args = build_parser().parse_args(argv)
        config = config_from_args(args)
        if args.dry_run:
            print(json.dumps(config.to_dict(), indent=2))
            return 0
        run_streaming_feature_job(config)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _build_alert(
    feature: dict[str, Any],
    *,
    rule_name: str,
    threshold: str,
    observed: str,
    severity: str,
    detected_at: datetime | None,
) -> dict[str, Any]:
    alert_id = f"{rule_name}:{feature['feature_id']}"
    return {
        "alert_id": alert_id,
        "rule_name": rule_name,
        "severity": severity,
        "entity_type": feature["entity_type"],
        "entity_id": feature["entity_id"],
        "window_start": feature["window_start"],
        "window_end": feature["window_end"],
        "threshold": threshold,
        "observed": observed,
        "source_feature_id": feature["feature_id"],
        "is_correction": feature["is_correction"],
        "detected_at": _to_utc_string(detected_at or datetime.now(UTC)),
    }


def _threshold_severity(observed: int, threshold: int) -> str:
    return "high" if observed >= threshold * 2 else "medium"


def _parse_utc_timestamp(raw_value: Any) -> datetime:
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise ValueError("timestamp must be a non-blank string")
    parsed = datetime.fromisoformat(raw_value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _to_epoch_milliseconds(value: datetime) -> int:
    return math.floor(value.timestamp() * MILLISECONDS_PER_SECOND)


def _to_utc_string(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _milliseconds_to_utc_string(value: int) -> str:
    return _to_utc_string(datetime.fromtimestamp(value / MILLISECONDS_PER_SECOND, UTC))


def _cents_to_decimal_string(value: int) -> str:
    return f"{Decimal(value) / Decimal(100):.2f}"


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


if __name__ == "__main__":
    raise SystemExit(main())
