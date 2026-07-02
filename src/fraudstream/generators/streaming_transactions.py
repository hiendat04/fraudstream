"""Generate Kafka-like streaming transaction events with realistic stream problems."""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import random
import shutil
from collections import Counter
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence


StreamRecord = dict[str, Any]

DEFAULT_CONFIG_PATH = Path("configs/generator/streaming_transactions.json")
LOCAL_JSONL_SINK = "local_jsonl"
STREAM_EVENT_TYPE = "transaction.created"
STREAM_SCHEMA_VERSION = "stream_v1"
STREAM_PRODUCER = "fraudstream_streaming_generator"
SUMMARY_ARTIFACTS = ("_manifest.json", "_stream_summary.json", "_stream_summary.csv")
SECONDS_PER_MINUTE = 60

CITY_POOL = [
    "New York",
    "Chicago",
    "Detroit",
    "Los Angeles",
    "Miami",
    "Seattle",
    "Atlanta",
    "Dallas",
    "Denver",
    "Phoenix",
    "Boston",
    "Charlotte",
]

MERCHANT_CATEGORY_POOL = [
    "grocery",
    "fuel",
    "restaurant",
    "travel",
    "electronics",
    "healthcare",
    "cash_transfer",
    "online_marketplace",
]

CHANNEL_WEIGHTS = {
    "card_present": 44,
    "online": 36,
    "mobile_wallet": 16,
    "atm": 4,
}

STATUS_WEIGHTS = {
    "approved": 92,
    "declined": 6,
    "reversed": 2,
}

CUSTOMER_SEGMENT_WEIGHTS = {
    "everyday": 72,
    "traveler": 8,
    "high_value": 7,
    "new_account": 8,
    "digital_only": 5,
}

MERCHANT_RISK_WEIGHTS = {
    "low": 78,
    "medium": 17,
    "high": 5,
}

FRAUD_RING_CATEGORIES = {"electronics", "cash_transfer", "online_marketplace", "travel"}


@dataclass(frozen=True)
class StreamCustomer:
    """Synthetic customer profile used by the streaming generator."""

    customer_id: str
    account_id: str
    home_city: str
    segment: str
    risk_tier: str


@dataclass(frozen=True)
class StreamMerchant:
    """Synthetic merchant profile used by the streaming generator."""

    merchant_id: str
    category: str
    city: str
    country: str
    risk_tier: str


@dataclass(frozen=True)
class StreamFraudRing:
    """Reusable device and IP pair for coordinated streaming fraud behavior."""

    device_id: str
    ip_address: str


@dataclass(frozen=True)
class EventTiming:
    """Publish time, event time, and stream-problem flags for one event."""

    produced_at: datetime
    event_timestamp: datetime
    window_start: datetime
    window_end: datetime
    problem_flags: tuple[str, ...]


@dataclass(frozen=True)
class StreamingContext:
    """Reusable profiles and stream scenario controls for one generator run."""

    customers: Sequence[StreamCustomer]
    merchants: Sequence[StreamMerchant]
    fraud_rings: Sequence[StreamFraudRing]
    burst_windows: Sequence[datetime]


@dataclass(frozen=True)
class StreamingGeneratorConfig:
    """Validated runtime settings for the streaming transaction generator."""

    random_seed: int
    n_events: int
    n_customers: int
    n_merchants: int
    start_timestamp: datetime
    duration_minutes: int
    currency: str
    topic: str
    n_partitions: int
    sink_type: str
    window_minutes: int
    late_event_rate: float
    out_of_order_rate: float
    duplicate_rate: float
    burst_window_count: int
    burst_event_ratio: float
    late_event_threshold_minutes: int
    max_late_minutes: int
    output_dir: Path

    @classmethod
    def from_json(cls, path: Path) -> "StreamingGeneratorConfig":
        """Load generator configuration from a JSON file."""

        with path.open("r", encoding="utf-8") as file:
            raw = json.load(file)

        config = cls(
            random_seed=int(raw["random_seed"]),
            n_events=int(raw["n_events"]),
            n_customers=int(raw["n_customers"]),
            n_merchants=int(raw["n_merchants"]),
            start_timestamp=datetime.fromisoformat(raw["start_timestamp"]),
            duration_minutes=int(raw["duration_minutes"]),
            currency=str(raw["currency"]),
            topic=str(raw["topic"]),
            n_partitions=int(raw["n_partitions"]),
            sink_type=str(raw["sink_type"]),
            window_minutes=int(raw["window_minutes"]),
            late_event_rate=float(raw["late_event_rate"]),
            out_of_order_rate=float(raw["out_of_order_rate"]),
            duplicate_rate=float(raw["duplicate_rate"]),
            burst_window_count=int(raw["burst_window_count"]),
            burst_event_ratio=float(raw["burst_event_ratio"]),
            late_event_threshold_minutes=int(raw["late_event_threshold_minutes"]),
            max_late_minutes=int(raw["max_late_minutes"]),
            output_dir=Path(raw["output_dir"]),
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Raise ValueError when the streaming configuration is not usable."""

        positive_integer_fields = {
            "n_events": self.n_events,
            "n_customers": self.n_customers,
            "n_merchants": self.n_merchants,
            "duration_minutes": self.duration_minutes,
            "n_partitions": self.n_partitions,
            "window_minutes": self.window_minutes,
            "late_event_threshold_minutes": self.late_event_threshold_minutes,
            "max_late_minutes": self.max_late_minutes,
        }
        for field_name, field_value in positive_integer_fields.items():
            if field_value <= 0:
                raise ValueError(f"{field_name} must be greater than 0")

        ratio_fields = {
            "late_event_rate": self.late_event_rate,
            "out_of_order_rate": self.out_of_order_rate,
            "duplicate_rate": self.duplicate_rate,
            "burst_event_ratio": self.burst_event_ratio,
        }
        for field_name, field_value in ratio_fields.items():
            if not 0 <= field_value <= 1:
                raise ValueError(f"{field_name} must be between 0 and 1")

        if self.burst_window_count < 0:
            raise ValueError("burst_window_count must be greater than or equal to 0")
        if self.sink_type != LOCAL_JSONL_SINK:
            raise ValueError(f"sink_type currently supports only {LOCAL_JSONL_SINK}")
        if self.late_event_rate + self.out_of_order_rate > 1:
            raise ValueError(
                "late_event_rate and out_of_order_rate must not exceed 1 when combined"
            )
        if self.late_event_threshold_minutes <= self.window_minutes:
            raise ValueError("late_event_threshold_minutes must be greater than window_minutes")
        if self.max_late_minutes <= self.late_event_threshold_minutes:
            raise ValueError("max_late_minutes must be greater than late_event_threshold_minutes")


def generate_streaming_transactions(config: StreamingGeneratorConfig) -> dict[str, Any]:
    """Generate a Kafka-like local event log plus manifest and stream summary."""

    config.validate()
    rng = random.Random(config.random_seed)
    _prepare_output_dir(config.output_dir)

    context = _build_streaming_context(config, rng)
    base_records = [
        _generate_record(index, config, context, rng)
        for index in range(config.n_events)
    ]
    duplicate_records = _duplicate_records(base_records, config, rng)
    all_records = _order_records_for_publish([*base_records, *duplicate_records])
    _assign_publish_metadata(all_records, config)

    event_log_path = _write_local_jsonl_sink(all_records, config)
    summary = _build_stream_summary(base_records, all_records, config, context, event_log_path)
    _write_summary_artifacts(summary, config.output_dir)
    _write_manifest(config, summary, event_log_path)
    return summary


def _prepare_output_dir(output_dir: Path) -> None:
    """Remove prior streaming artifacts without deleting unrelated local files."""

    output_dir.mkdir(parents=True, exist_ok=True)
    for artifact in SUMMARY_ARTIFACTS:
        artifact_path = output_dir / artifact
        if artifact_path.exists():
            artifact_path.unlink()

    for topic_dir in output_dir.glob("topic=*"):
        if topic_dir.is_dir():
            shutil.rmtree(topic_dir)


def _build_streaming_context(
    config: StreamingGeneratorConfig,
    rng: random.Random,
) -> StreamingContext:
    """Build reusable entity pools and burst windows for one run."""

    return StreamingContext(
        customers=_generate_customers(config, rng),
        merchants=_generate_merchants(config, rng),
        fraud_rings=_generate_fraud_rings(rng),
        burst_windows=_generate_burst_windows(config, rng),
    )


def _generate_customers(
    config: StreamingGeneratorConfig,
    rng: random.Random,
) -> list[StreamCustomer]:
    """Create streaming customer profiles."""

    customers: list[StreamCustomer] = []
    for index in range(config.n_customers):
        segment = _sample_weighted_value(CUSTOMER_SEGMENT_WEIGHTS, rng)
        customers.append(
            StreamCustomer(
                customer_id=f"cust_stream_{index + 1:08d}",
                account_id=f"acct_stream_{rng.randint(1, config.n_customers * 2):08d}",
                home_city="New York" if rng.random() < 0.42 else rng.choice(CITY_POOL),
                segment=segment,
                risk_tier="elevated" if segment in {"new_account", "digital_only"} else "standard",
            )
        )
    return customers


def _generate_merchants(
    config: StreamingGeneratorConfig,
    rng: random.Random,
) -> list[StreamMerchant]:
    """Create streaming merchant profiles."""

    merchants: list[StreamMerchant] = []
    for index in range(config.n_merchants):
        category = (
            "online_marketplace"
            if rng.random() < 0.36
            else rng.choice(MERCHANT_CATEGORY_POOL)
        )
        merchants.append(
            StreamMerchant(
                merchant_id=f"merch_stream_{index + 1:08d}",
                category=category,
                city=rng.choice(CITY_POOL),
                country="US" if rng.random() > 0.03 else rng.choice(["CA", "MX", "GB"]),
                risk_tier=_sample_weighted_value(MERCHANT_RISK_WEIGHTS, rng),
            )
        )
    return merchants


def _generate_fraud_rings(rng: random.Random) -> list[StreamFraudRing]:
    """Create suspicious device and IP pairs for stream events."""

    return [
        StreamFraudRing(
            device_id=f"dev_stream_ring_{index + 1:04d}",
            ip_address=f"172.20.{index}.{rng.randint(1, 254)}",
        )
        for index in range(8)
    ]


def _generate_burst_windows(config: StreamingGeneratorConfig, rng: random.Random) -> list[datetime]:
    """Pick event-time windows that receive a larger share of stream volume."""

    if config.burst_window_count == 0:
        return []

    window_count = max(1, config.duration_minutes // config.window_minutes)
    selected_window_indexes = rng.sample(
        range(window_count),
        min(config.burst_window_count, window_count),
    )
    return sorted(
        config.start_timestamp + timedelta(minutes=window_index * config.window_minutes)
        for window_index in selected_window_indexes
    )


def _generate_record(
    index: int,
    config: StreamingGeneratorConfig,
    context: StreamingContext,
    rng: random.Random,
) -> StreamRecord:
    """Create one Kafka-like transaction event record."""

    customer = rng.choice(context.customers)
    merchant = rng.choice(context.merchants)
    timing = _sample_event_timing(config, context.burst_windows, rng)
    amount = _sample_amount(merchant.category, rng)
    channel = _sample_weighted_value(CHANNEL_WEIGHTS, rng)
    city = _sample_transaction_city(customer, merchant, channel, rng)
    is_ring = _is_fraud_ring_event(merchant, channel, context.fraud_rings, rng)
    is_fraud = _sample_fraud_label(
        amount,
        customer,
        merchant,
        channel,
        city,
        is_ring,
        timing.event_timestamp,
        rng,
    )
    device_id, ip_address = _sample_device_and_ip(customer, context.fraud_rings, is_ring, rng)
    event_id = f"evt_stream_{index + 1:012d}"

    value = _build_event_value(
        index=index,
        event_id=event_id,
        config=config,
        customer=customer,
        merchant=merchant,
        timing=timing,
        amount=amount,
        city=city,
        channel=channel,
        is_fraud=is_fraud,
        device_id=device_id,
        ip_address=ip_address,
        rng=rng,
    )
    return {
        "topic": config.topic,
        "partition": 0,
        "partition_key": customer.customer_id,
        "source_sequence": 0,
        "produced_at": _format_timestamp(timing.produced_at),
        "headers": _build_event_headers(timing.problem_flags),
        "value": value,
    }


def _sample_event_timing(
    config: StreamingGeneratorConfig,
    burst_windows: Sequence[datetime],
    rng: random.Random,
) -> EventTiming:
    """Sample publish time, event time, event window, and stream problem flags."""

    produced_at, is_burst = _sample_produced_at(config, burst_windows, rng)
    event_timestamp, flags = _sample_event_timestamp(produced_at, is_burst, config, rng)
    if is_burst:
        flags.append("burst")
    window_start, window_end = _event_window(event_timestamp, config.window_minutes)
    return EventTiming(
        produced_at=produced_at,
        event_timestamp=event_timestamp,
        window_start=window_start,
        window_end=window_end,
        problem_flags=tuple(flags),
    )


def _build_event_headers(problem_flags: Sequence[str]) -> dict[str, Any]:
    """Build metadata that would map naturally to Kafka message headers."""

    return {
        "event_type": STREAM_EVENT_TYPE,
        "schema_version": STREAM_SCHEMA_VERSION,
        "producer": STREAM_PRODUCER,
        "problem_flags": list(problem_flags),
    }


def _build_event_value(
    index: int,
    event_id: str,
    config: StreamingGeneratorConfig,
    customer: StreamCustomer,
    merchant: StreamMerchant,
    timing: EventTiming,
    amount: Decimal,
    city: str,
    channel: str,
    is_fraud: bool,
    device_id: str,
    ip_address: str,
    rng: random.Random,
) -> dict[str, Any]:
    """Build the business payload carried by one stream event."""

    return {
        "event_id": event_id,
        "transaction_id": f"txn_stream_{index + 1:012d}",
        "account_id": customer.account_id,
        "customer_id": customer.customer_id,
        "merchant_id": merchant.merchant_id,
        "merchant_category": merchant.category,
        "amount": str(amount),
        "currency": config.currency,
        "city": city,
        "channel": channel,
        "transaction_status": _sample_weighted_value(STATUS_WEIGHTS, rng),
        "is_fraud": is_fraud,
        "event_timestamp": _format_timestamp(timing.event_timestamp),
        "created_ts": _format_timestamp(timing.produced_at),
        "event_window_start": _format_timestamp(timing.window_start),
        "event_window_end": _format_timestamp(timing.window_end),
        "event_window_minutes": config.window_minutes,
        "device_id": device_id,
        "ip_address": ip_address,
        "authentication_method": _sample_authentication_method(channel, is_fraud, rng),
        "risk_signal_version": STREAM_SCHEMA_VERSION,
    }


def _sample_transaction_city(
    customer: StreamCustomer,
    merchant: StreamMerchant,
    channel: str,
    rng: random.Random,
) -> str:
    """Choose the transaction city from channel and entity context."""

    if channel in {"online", "mobile_wallet"}:
        return customer.home_city
    return rng.choice([customer.home_city, merchant.city])


def _sample_produced_at(
    config: StreamingGeneratorConfig,
    burst_windows: Sequence[datetime],
    rng: random.Random,
) -> tuple[datetime, bool]:
    """Sample publish time with optional burst windows."""

    if burst_windows and rng.random() < config.burst_event_ratio:
        window_start = rng.choice(burst_windows)
        produced_at = window_start + timedelta(
            seconds=rng.randrange(config.window_minutes * SECONDS_PER_MINUTE)
        )
        return produced_at, True

    produced_at = config.start_timestamp + timedelta(
        seconds=rng.randrange(config.duration_minutes * SECONDS_PER_MINUTE)
    )
    return produced_at, False


def _sample_event_timestamp(
    produced_at: datetime,
    is_burst: bool,
    config: StreamingGeneratorConfig,
    rng: random.Random,
) -> tuple[datetime, list[str]]:
    """Sample event time relative to publish time and attach stream problem flags."""

    flags: list[str] = []
    if rng.random() < config.late_event_rate:
        delay_minutes = rng.randint(
            config.late_event_threshold_minutes + 1,
            config.max_late_minutes,
        )
        flags.append("late")
    elif rng.random() < config.out_of_order_rate:
        delay_minutes = rng.randint(config.window_minutes + 1, config.late_event_threshold_minutes)
        flags.append("out_of_order")
    else:
        delay_minutes = 0 if is_burst else rng.randint(0, 2)

    event_timestamp = produced_at - timedelta(minutes=delay_minutes, seconds=rng.randrange(60))
    return event_timestamp, flags


def _sample_amount(merchant_category: str, rng: random.Random) -> Decimal:
    """Sample a realistic amount for a transaction category."""

    multipliers = {
        "travel": 7.5,
        "electronics": 4.2,
        "cash_transfer": 3.8,
        "online_marketplace": 2.4,
        "healthcare": 2.1,
    }
    cents = int(rng.lognormvariate(3.2, 0.85) * multipliers.get(merchant_category, 1.0) * 100)
    return Decimal(max(cents, 100)) / Decimal("100")


def _sample_fraud_label(
    amount: Decimal,
    customer: StreamCustomer,
    merchant: StreamMerchant,
    channel: str,
    city: str,
    is_ring: bool,
    event_timestamp: datetime,
    rng: random.Random,
) -> bool:
    """Sample a rare fraud label using transaction and entity risk traits."""

    probability = 0.004
    if amount >= Decimal("500"):
        probability += 0.025
    if merchant.category in {"cash_transfer", "electronics", "online_marketplace"}:
        probability += 0.006
    if channel in {"online", "mobile_wallet"}:
        probability += 0.003
    if customer.risk_tier == "elevated":
        probability += 0.004
    if merchant.risk_tier == "high":
        probability += 0.006
    if merchant.country != "US":
        probability += 0.012
    if city in {"Miami", "Los Angeles"}:
        probability += 0.004
    if event_timestamp.hour <= 5:
        probability += 0.005
    if is_ring:
        probability += 0.35
    return rng.random() < probability


def _is_fraud_ring_event(
    merchant: StreamMerchant,
    channel: str,
    fraud_rings: Sequence[StreamFraudRing],
    rng: random.Random,
) -> bool:
    """Return true when an event should reuse suspicious fraud infrastructure."""

    if not fraud_rings:
        return False
    if channel not in {"online", "mobile_wallet"}:
        return False
    if merchant.category not in FRAUD_RING_CATEGORIES:
        return False
    return rng.random() < 0.04


def _sample_device_and_ip(
    customer: StreamCustomer,
    fraud_rings: Sequence[StreamFraudRing],
    is_ring: bool,
    rng: random.Random,
) -> tuple[str, str]:
    """Sample normal or fraud-ring device/IP identifiers."""

    if is_ring and fraud_rings:
        ring = rng.choice(fraud_rings)
        return ring.device_id, ring.ip_address

    customer_suffix = customer.customer_id.rsplit("_", maxsplit=1)[-1]
    device_id = f"dev_stream_{customer_suffix}_{rng.randint(1, 3):02d}"
    ip_address = f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
    return device_id, ip_address


def _sample_authentication_method(channel: str, is_fraud: bool, rng: random.Random) -> str:
    """Sample authentication behavior for a stream event."""

    if channel == "card_present":
        return rng.choice(["chip", "pin", "tap"])
    if is_fraud:
        return _sample_weighted_value({"none": 45, "3ds": 25, "otp": 15, "biometric": 15}, rng)
    return _sample_weighted_value({"3ds": 35, "otp": 25, "biometric": 25, "none": 15}, rng)


def _event_window(event_timestamp: datetime, window_minutes: int) -> tuple[datetime, datetime]:
    """Return event-time window boundaries for a timestamp."""

    minute = (event_timestamp.minute // window_minutes) * window_minutes
    window_start = event_timestamp.replace(minute=minute, second=0, microsecond=0)
    return window_start, window_start + timedelta(minutes=window_minutes)


def _duplicate_records(
    records: Sequence[StreamRecord],
    config: StreamingGeneratorConfig,
    rng: random.Random,
) -> list[StreamRecord]:
    """Create duplicate stream messages by replaying selected records."""

    duplicate_count = min(len(records), round(len(records) * config.duplicate_rate))
    duplicates: list[StreamRecord] = []
    for record in rng.sample(list(records), duplicate_count):
        duplicate = copy.deepcopy(record)
        produced_at = datetime.fromisoformat(record["produced_at"]) + timedelta(
            seconds=rng.randint(1, 600)
        )
        duplicate["produced_at"] = _format_timestamp(produced_at)
        duplicate["value"]["created_ts"] = duplicate["produced_at"]
        problem_flags = set(duplicate["headers"]["problem_flags"])
        problem_flags.add("duplicate")
        duplicate["headers"]["problem_flags"] = sorted(problem_flags)
        duplicate["headers"]["duplicate_of_event_id"] = record["value"]["event_id"]
        duplicates.append(duplicate)
    return duplicates


def _order_records_for_publish(records: Sequence[StreamRecord]) -> list[StreamRecord]:
    """Return records in the same order a source producer would publish them."""

    return sorted(records, key=lambda record: (record["produced_at"], record["value"]["event_id"]))


def _assign_publish_metadata(
    records: Sequence[StreamRecord],
    config: StreamingGeneratorConfig,
) -> None:
    """Assign sequence numbers and deterministic partitions after publish ordering."""

    for source_sequence, record in enumerate(records, start=1):
        record["source_sequence"] = source_sequence
        record["partition"] = _stable_partition(record["partition_key"], config.n_partitions)


def _write_local_jsonl_sink(
    records: Sequence[StreamRecord],
    config: StreamingGeneratorConfig,
) -> Path:
    """Write stream records as a Kafka-like local JSONL topic log."""

    topic_dir = config.output_dir / f"topic={config.topic}"
    topic_dir.mkdir(parents=True, exist_ok=True)
    event_log_path = topic_dir / "events.jsonl"
    with event_log_path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, separators=(",", ":")) + "\n")
    return event_log_path


def _build_stream_summary(
    base_records: Sequence[StreamRecord],
    all_records: Sequence[StreamRecord],
    config: StreamingGeneratorConfig,
    context: StreamingContext,
    event_log_path: Path,
) -> dict[str, Any]:
    """Build stream evidence metrics for generated events."""

    total_records = len(all_records)
    event_id_counts = Counter(record["value"]["event_id"] for record in all_records)
    duplicate_count = sum(count - 1 for count in event_id_counts.values() if count > 1)
    flag_counts = Counter(
        flag
        for record in all_records
        for flag in record["headers"]["problem_flags"]
    )
    window_counts = Counter(record["value"]["event_window_start"] for record in all_records)
    partition_counts = Counter(str(record["partition"]) for record in all_records)
    out_of_order_count = _count_observed_out_of_order_events(all_records, config.window_minutes)

    return {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "sink_type": config.sink_type,
        "topic": config.topic,
        "event_log_path": str(event_log_path),
        "base_event_count": len(base_records),
        "record_count_after_duplicates": total_records,
        "duplicate_record_count": duplicate_count,
        "duplicate_rate_actual": _rate(duplicate_count, total_records),
        "n_partitions": config.n_partitions,
        "partition_distribution_pct": _to_percentages(partition_counts, total_records),
        "stream_problems": {
            "late_event_count": flag_counts["late"],
            "late_event_rate_actual": _rate(flag_counts["late"], total_records),
            "out_of_order_flag_count": flag_counts["out_of_order"],
            "observed_out_of_order_event_count": out_of_order_count,
            "duplicate_flag_count": flag_counts["duplicate"],
            "burst_event_count": flag_counts["burst"],
            "burst_event_rate_actual": _rate(flag_counts["burst"], total_records),
        },
        "event_time_windows": {
            "window_minutes": config.window_minutes,
            "window_count": len(window_counts),
            "max_records_in_window": max(window_counts.values(), default=0),
            "burst_windows": [
                _format_timestamp(window)
                for window in context.burst_windows
            ],
        },
        "schema": {
            "record_fields": [
                "topic",
                "partition",
                "partition_key",
                "source_sequence",
                "produced_at",
                "headers",
                "value",
            ],
            "message_key": "customer_id",
            "event_type": STREAM_EVENT_TYPE,
            "event_timestamp_field": "value.event_timestamp",
            "arrival_timestamp_field": "produced_at",
        },
    }


def _count_observed_out_of_order_events(
    records: Sequence[StreamRecord],
    window_minutes: int,
) -> int:
    """Count records where event time moves meaningfully backward in publish order."""

    max_event_timestamp: datetime | None = None
    out_of_order_count = 0
    tolerance = timedelta(minutes=window_minutes)
    for record in records:
        event_timestamp = datetime.fromisoformat(record["value"]["event_timestamp"])
        if max_event_timestamp and event_timestamp < max_event_timestamp - tolerance:
            out_of_order_count += 1
        max_event_timestamp = (
            event_timestamp
            if max_event_timestamp is None
            else max(max_event_timestamp, event_timestamp)
        )
    return out_of_order_count


def _write_summary_artifacts(summary: dict[str, Any], output_dir: Path) -> None:
    """Write JSON and CSV summary artifacts for generated stream events."""

    with (output_dir / "_stream_summary.json").open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2)
        file.write("\n")

    rows = [
        ("row_count.base", summary["base_event_count"]),
        ("row_count.after_duplicates", summary["record_count_after_duplicates"]),
        ("duplicates.count", summary["duplicate_record_count"]),
        ("duplicates.rate_actual", summary["duplicate_rate_actual"]),
        ("late_events.count", summary["stream_problems"]["late_event_count"]),
        ("late_events.rate_actual", summary["stream_problems"]["late_event_rate_actual"]),
        ("out_of_order.flag_count", summary["stream_problems"]["out_of_order_flag_count"]),
        (
            "out_of_order.observed_count",
            summary["stream_problems"]["observed_out_of_order_event_count"],
        ),
        ("burst_events.count", summary["stream_problems"]["burst_event_count"]),
        ("burst_events.rate_actual", summary["stream_problems"]["burst_event_rate_actual"]),
        ("windows.count", summary["event_time_windows"]["window_count"]),
        ("windows.max_records", summary["event_time_windows"]["max_records_in_window"]),
    ]
    rows.extend(
        (f"partition.{key}.pct", value)
        for key, value in summary["partition_distribution_pct"].items()
    )

    with (output_dir / "_stream_summary.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["metric", "value"])
        writer.writerows(rows)


def _write_manifest(
    config: StreamingGeneratorConfig,
    summary: dict[str, Any],
    event_log_path: Path,
) -> None:
    """Write a manifest describing the generated local topic log."""

    manifest = {
        "dataset": "streaming_transactions",
        "source_system": STREAM_PRODUCER,
        "created_at": summary["generated_at"],
        "sink_type": config.sink_type,
        "topic": config.topic,
        "message_key": "customer_id",
        "event_log_path": str(event_log_path),
        "summary_path": str(config.output_dir / "_stream_summary.json"),
        "config": {
            "random_seed": config.random_seed,
            "n_events": config.n_events,
            "duplicate_rate": config.duplicate_rate,
            "late_event_rate": config.late_event_rate,
            "out_of_order_rate": config.out_of_order_rate,
            "burst_window_count": config.burst_window_count,
            "burst_event_ratio": config.burst_event_ratio,
            "window_minutes": config.window_minutes,
        },
    }
    with (config.output_dir / "_manifest.json").open("w", encoding="utf-8") as file:
        json.dump(manifest, file, indent=2)
        file.write("\n")


def _stable_partition(partition_key: str, n_partitions: int) -> int:
    """Map a message key to a stable partition number."""

    digest = hashlib.sha256(partition_key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % n_partitions


def _sample_weighted_value(weights: Mapping[str, int], rng: random.Random) -> str:
    """Sample one mapping key using the mapping values as relative weights."""

    return rng.choices(list(weights), weights=list(weights.values()))[0]


def _rate(count: int, total: int) -> float:
    """Return a rounded count-over-total rate."""

    return round(count / total, 4) if total else 0.0


def _to_percentages(counts: Counter[str], total: int) -> dict[str, float]:
    """Convert count values into sorted percentage values."""

    return {
        key: round((value / total) * 100, 2)
        for key, value in sorted(counts.items(), key=lambda item: _summary_key(item[0]))
    }


def _summary_key(value: str) -> tuple[int, int | str]:
    """Sort numeric summary keys numerically and all other keys lexically."""

    return (0, int(value)) if value.isdigit() else (1, value)


def _format_timestamp(value: datetime) -> str:
    """Format timestamps consistently across stream records and artifacts."""

    return value.isoformat(timespec="seconds")


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for the streaming generator."""

    parser = argparse.ArgumentParser(
        description="Generate Kafka-like streaming transaction events."
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        type=Path,
        help="Path to the JSON streaming generator configuration file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Optional output directory override. Defaults to the value in the config file.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the streaming transaction generator from the command line."""

    args = build_parser().parse_args(argv)
    config = StreamingGeneratorConfig.from_json(args.config)
    if args.output_dir:
        config = replace(config, output_dir=args.output_dir)
    summary = generate_streaming_transactions(config)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
