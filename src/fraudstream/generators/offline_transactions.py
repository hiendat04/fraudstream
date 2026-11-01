"""Generate raw offline transaction data with intentional data quality problems.

This module creates source-style financial transaction CSV files that can be
ingested into a future Bronze zone. It intentionally simulates practical
offline data problems: skew, high-cardinality identifiers, schema evolution,
duplicate records, late arrivals, bursty traffic, missing values, inconsistent
formats, and fraud-ring behavior.

Each day is written on its own, as one gzipped CSV, a chunk of rows at a time,
so memory stays flat however many rows are asked for, and days can be
generated in parallel.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import multiprocessing
import random
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from functools import lru_cache
from itertools import repeat
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Mapping, Sequence

from fraudstream import storage
from fraudstream.jobs.warehouse import (
    WarehouseConfig,
    add_warehouse_arguments,
    warehouse_config_from_args,
)


TransactionRow = dict[str, str]

DEFAULT_RAW_URI = "s3a://fraudstream/raw/offline_transactions"
DEFAULT_LABELS_URI = "s3a://fraudstream/raw/transaction_labels"

BASE_COLUMNS = [
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
    "is_fraud",
    "event_timestamp",
    "created_ts",
]

EVOLVED_COLUMNS = [
    *BASE_COLUMNS,
    "device_id",
    "ip_address",
    "authentication_method",
    "risk_signal_version",
]

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
]

CHANNEL_WEIGHTS = {
    "card_present": 45,
    "online": 35,
    "mobile_wallet": 15,
    "atm": 5,
}

TRANSACTION_STATUS_WEIGHTS = {
    "approved": 92,
    "declined": 6,
    "reversed": 2,
}

EVOLVED_ONLY_COLUMNS = [
    "device_id",
    "ip_address",
    "authentication_method",
    "risk_signal_version",
]

PEAK_HOUR_WEIGHTS = {
    0: 2,
    1: 1,
    2: 1,
    3: 1,
    4: 1,
    5: 2,
    6: 4,
    7: 7,
    8: 8,
    9: 6,
    10: 5,
    11: 6,
    12: 8,
    13: 7,
    14: 5,
    15: 5,
    16: 6,
    17: 9,
    18: 10,
    19: 10,
    20: 8,
    21: 6,
    22: 4,
    23: 3,
}
PEAK_HOUR_LABEL_WEIGHTS = {str(hour): weight for hour, weight in PEAK_HOUR_WEIGHTS.items()}

CUSTOMER_SEGMENT_WEIGHTS = {
    "everyday": 72,
    "traveler": 8,
    "high_value": 7,
    "new_account": 8,
    "digital_only": 5,
}

DIGITAL_ONLY_CHANNEL_WEIGHTS = {
    "online": 55,
    "mobile_wallet": 35,
    "card_present": 8,
    "atm": 2,
}

TRAVELER_CHANNEL_WEIGHTS = {
    "card_present": 50,
    "online": 25,
    "mobile_wallet": 20,
    "atm": 5,
}

FRAUD_AUTHENTICATION_WEIGHTS = {
    "none": 45,
    "3ds": 25,
    "otp": 15,
    "biometric": 15,
}

NORMAL_AUTHENTICATION_WEIGHTS = {
    "3ds": 35,
    "otp": 25,
    "biometric": 25,
    "none": 15,
}

RAW_FORMAT_ISSUES = ["city_case", "city_padding", "currency_case", "status_case"]

MERCHANT_RISK_WEIGHTS = {
    "low": 78,
    "medium": 17,
    "high": 5,
}

FRAUD_RING_CATEGORIES = {"electronics", "cash_transfer", "online_marketplace", "travel"}
LATE_ARRIVAL_THRESHOLD_MINUTES = 60
LABEL_COLUMNS = ["transaction_id", "is_fraud", "event_timestamp"]

# Share of all traffic that lands on burst days, split evenly between them.
BURST_TRAFFIC_SHARE = 0.18

# Rows generated, duplicated and shuffled together before they are written.
CHUNK_ROWS = 100_000

# gzip's default level 9 is several times slower for files only a few percent smaller.
GZIP_LEVEL = 6


@dataclass(frozen=True)
class CustomerProfile:
    """Synthetic customer attributes used to generate transactions."""

    customer_id: str
    account_id: str
    home_city: str
    segment: str
    risk_tier: str
    home_country: str = "US"


@dataclass(frozen=True)
class MerchantProfile:
    """Synthetic merchant attributes used to generate transactions."""

    merchant_id: str
    merchant_category: str
    merchant_city: str
    merchant_country: str
    merchant_risk: str


@dataclass(frozen=True)
class FraudRing:
    """Reusable device and IP pair for coordinated fraud simulation."""

    device_id: str
    ip_address: str


@dataclass(frozen=True)
class GenerationContext:
    """Reusable entities and scenario controls for one generator run."""

    customers: Sequence[CustomerProfile]
    merchants: Sequence[MerchantProfile]
    burst_dates: Sequence[date]
    fraud_rings: Sequence[FraudRing]


@dataclass(frozen=True)
class DayPlan:
    """Which day to generate, and which slice of transaction ids it owns."""

    event_date: date
    first_index: int
    row_count: int


@dataclass
class DayStats:
    """Counts describing one day's written rows; days add up into the run's quality summary."""

    base_rows: int = 0
    rows: int = 0
    fraud_rows: int = 0
    late_rows: int = 0
    max_delay_minutes: float = 0.0
    missing_value_rows: int = 0
    inconsistent_format_rows: int = 0
    ring_rows: int = 0
    ring_devices: set[str] = field(default_factory=set)
    amount_total: float = 0.0
    cities: Counter[str] = field(default_factory=Counter)
    categories: Counter[str] = field(default_factory=Counter)
    hours: Counter[str] = field(default_factory=Counter)

    def add(self, base_rows: Sequence[TransactionRow], written_rows: Sequence[TransactionRow], currency: str) -> None:
        """Count one chunk: its generated rows and the rows written, duplicates included."""

        self.base_rows += len(base_rows)
        self.rows += len(written_rows)
        for row in written_rows:
            delay = _arrival_delay_minutes(row)
            self.fraud_rows += row["is_fraud"] == "1"
            self.late_rows += delay > LATE_ARRIVAL_THRESHOLD_MINUTES
            self.max_delay_minutes = max(self.max_delay_minutes, delay)
            self.missing_value_rows += _has_missing_values(row)
            self.inconsistent_format_rows += bool(_has_inconsistent_format(row, currency))
            if row.get("device_id", "").startswith("dev_ring_"):
                self.ring_rows += 1
                self.ring_devices.add(row["device_id"])
            self.amount_total += float(row["amount"])
            self.cities[row["city"]] += 1
            self.categories[row["merchant_category"]] += 1
            self.hours[row["event_timestamp"][11:13]] += 1

    def merge(self, other: "DayStats") -> None:
        """Add another day's counts to these."""

        for name in ("base_rows", "rows", "fraud_rows", "late_rows", "missing_value_rows",
                     "inconsistent_format_rows", "ring_rows", "amount_total"):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        self.max_delay_minutes = max(self.max_delay_minutes, other.max_delay_minutes)
        self.ring_devices |= other.ring_devices
        self.cities += other.cities
        self.categories += other.categories
        self.hours += other.hours


@dataclass(frozen=True)
class DayResult:
    """One generated day: where its files went, and what was in them."""

    event_date: date
    schema_version: str
    transactions_uri: str
    labels_uri: str
    stats: DayStats


@dataclass(frozen=True)
class OfflineGeneratorConfig:
    """Validated runtime settings for the offline transaction generator.

    `output_dir` is local -- it only holds `_manifest.json` and the
    `_quality_summary.*` evidence files, the same "local = evidence only"
    convention every downstream layer's own summary JSON already follows.
    The generated CSV partitions themselves are the actual raw source data
    and live in MinIO under `raw_uri` (see `fraudstream.storage`).

    `drift_start_date` / `drift_amount_multiplier_end` simulate gradual data
    drift: from `drift_start_date` to the end of the history window, sampled
    `amount` values are scaled by a factor ramping linearly from 1.0 to
    `drift_amount_multiplier_end`. `None` (the default) disables drift
    entirely -- existing configs are unaffected.
    """

    random_seed: int
    n_transactions: int
    n_customers: int
    n_accounts: int
    n_merchants: int
    start_date: date
    days_history: int
    currency: str
    skew_city: str
    skew_city_ratio: float
    skew_merchant_category: str
    skew_merchant_category_ratio: float
    duplicate_rate: float
    schema_change_date: date
    output_dir: Path
    raw_uri: str = DEFAULT_RAW_URI
    warehouse: WarehouseConfig = field(default_factory=WarehouseConfig)
    late_arrival_rate: float = 0.04
    missing_value_rate: float = 0.015
    inconsistent_format_rate: float = 0.01
    burst_day_count: int = 8
    fraud_ring_count: int = 6
    drift_start_date: date | None = None
    drift_amount_multiplier_end: float = 1.0
    labels_uri: str = DEFAULT_LABELS_URI

    @classmethod
    def from_json(cls, path: Path) -> "OfflineGeneratorConfig":
        """Load generator configuration from a JSON file."""

        with path.open("r", encoding="utf-8") as file:
            raw = json.load(file)

        config = cls(
            random_seed=int(raw["random_seed"]),
            n_transactions=int(raw["n_transactions"]),
            n_customers=int(raw["n_customers"]),
            n_accounts=int(raw["n_accounts"]),
            n_merchants=int(raw["n_merchants"]),
            start_date=date.fromisoformat(raw["start_date"]),
            days_history=int(raw["days_history"]),
            currency=str(raw["currency"]),
            skew_city=str(raw["skew_city"]),
            skew_city_ratio=float(raw["skew_city_ratio"]),
            skew_merchant_category=str(raw["skew_merchant_category"]),
            skew_merchant_category_ratio=float(raw["skew_merchant_category_ratio"]),
            duplicate_rate=float(raw["duplicate_rate"]),
            schema_change_date=date.fromisoformat(raw["schema_change_date"]),
            output_dir=Path(raw["output_dir"]),
            raw_uri=str(raw.get("raw_uri", DEFAULT_RAW_URI)),
            late_arrival_rate=float(raw.get("late_arrival_rate", 0.04)),
            missing_value_rate=float(raw.get("missing_value_rate", 0.015)),
            inconsistent_format_rate=float(raw.get("inconsistent_format_rate", 0.01)),
            burst_day_count=int(raw.get("burst_day_count", 8)),
            fraud_ring_count=int(raw.get("fraud_ring_count", 6)),
            drift_start_date=(
                date.fromisoformat(raw["drift_start_date"]) if raw.get("drift_start_date") else None
            ),
            drift_amount_multiplier_end=float(raw.get("drift_amount_multiplier_end", 1.0)),
            labels_uri=str(raw.get("labels_uri", DEFAULT_LABELS_URI)),
        )
        config.validate()
        return config

    def validate(self) -> None:
        """Raise ValueError when the generator configuration is not usable."""

        positive_integer_fields = {
            "n_transactions": self.n_transactions,
            "n_customers": self.n_customers,
            "n_accounts": self.n_accounts,
            "n_merchants": self.n_merchants,
            "days_history": self.days_history,
        }
        for field_name, field_value in positive_integer_fields.items():
            if field_value <= 0:
                raise ValueError(f"{field_name} must be greater than 0")

        ratio_fields = {
            "skew_city_ratio": self.skew_city_ratio,
            "skew_merchant_category_ratio": self.skew_merchant_category_ratio,
            "duplicate_rate": self.duplicate_rate,
            "late_arrival_rate": self.late_arrival_rate,
            "missing_value_rate": self.missing_value_rate,
            "inconsistent_format_rate": self.inconsistent_format_rate,
        }
        for field_name, field_value in ratio_fields.items():
            if not 0 <= field_value <= 1:
                raise ValueError(f"{field_name} must be between 0 and 1")

        if self.burst_day_count < 0:
            raise ValueError("burst_day_count must be greater than or equal to 0")
        if self.fraud_ring_count < 0:
            raise ValueError("fraud_ring_count must be greater than or equal to 0")

        history_end_date = self.start_date + timedelta(days=self.days_history)
        if not self.start_date <= self.schema_change_date < history_end_date:
            raise ValueError("schema_change_date must fall inside the generated history window")

        if self.drift_amount_multiplier_end <= 0:
            raise ValueError("drift_amount_multiplier_end must be greater than 0")
        if self.drift_start_date is not None:
            if not self.start_date <= self.drift_start_date < history_end_date:
                raise ValueError("drift_start_date must fall inside the generated history window")


def generate_offline_transactions(config: OfflineGeneratorConfig, workers: int = 1) -> dict[str, Any]:
    """Generate one gzipped transactions file and one label file per day, plus the evidence files.

    `workers` days are generated at once. Every day draws from its own random
    stream, seeded by its date, so the data is the same whatever the worker count.
    """

    config.validate()
    _prepare_output_dir(config.output_dir)
    storage.delete_prefix(config.warehouse, config.raw_uri)
    storage.delete_prefix(config.warehouse, config.labels_uri)

    burst_dates = _generation_context(config).burst_dates
    plans = _plan_days(config, burst_dates)
    if workers == 1:
        results = [_generate_day(config, plan) for plan in plans]
    else:
        with ProcessPoolExecutor(workers, mp_context=multiprocessing.get_context("spawn")) as pool:
            results = list(pool.map(_generate_day, repeat(config), plans))

    summary = _build_quality_summary(config, results, burst_dates)
    _write_summary_artifacts(summary, config.output_dir)
    _write_manifest(config, summary, results)
    return summary


def _prepare_output_dir(output_dir: Path) -> None:
    """Remove prior local evidence artifacts without deleting unrelated local files.

    The raw CSV partitions themselves live in MinIO now (see
    `storage.delete_prefix(config.warehouse, config.raw_uri)` in
    `generate_offline_transactions`) -- this only clears the local
    `_manifest.json` / `_quality_summary.*` evidence files.
    """

    output_dir.mkdir(parents=True, exist_ok=True)
    for artifact in ["_manifest.json", "_quality_summary.json", "_quality_summary.csv"]:
        artifact_path = output_dir / artifact
        if artifact_path.exists():
            artifact_path.unlink()


@lru_cache(maxsize=1)
def _generation_context(config: OfflineGeneratorConfig) -> GenerationContext:
    """Build the run's customers, merchants, burst days and rings once per process, from the seed."""

    return _build_generation_context(config, random.Random(config.random_seed))


def _build_generation_context(config: OfflineGeneratorConfig, rng: random.Random) -> GenerationContext:
    """Create reusable entities and scenario controls for one generator run."""

    return GenerationContext(
        customers=_generate_customers(config, rng),
        merchants=_generate_merchants(config, rng),
        burst_dates=_generate_burst_dates(config, rng),
        fraud_rings=_generate_fraud_rings(config, rng),
    )


def _generate_customers(config: OfflineGeneratorConfig, rng: random.Random) -> list[CustomerProfile]:
    """Create high-cardinality customer and account identifiers."""

    customers: list[CustomerProfile] = []
    for index in range(config.n_customers):
        segment = _sample_weighted_value(CUSTOMER_SEGMENT_WEIGHTS, rng)
        customers.append(
            CustomerProfile(
                customer_id=f"cust_{index + 1:08d}",
                account_id=f"acct_{rng.randint(1, config.n_accounts):08d}",
                home_city=config.skew_city if rng.random() < config.skew_city_ratio else rng.choice(CITY_POOL),
                segment=segment,
                risk_tier="elevated" if segment in {"new_account", "digital_only"} else "standard",
            )
        )
    return customers


def _generate_merchants(config: OfflineGeneratorConfig, rng: random.Random) -> list[MerchantProfile]:
    """Create high-cardinality merchant identifiers with category skew."""

    merchants: list[MerchantProfile] = []
    for index in range(config.n_merchants):
        category = (
            config.skew_merchant_category
            if rng.random() < config.skew_merchant_category_ratio
            else rng.choice(MERCHANT_CATEGORY_POOL)
        )
        merchants.append(
            MerchantProfile(
                merchant_id=f"merch_{index + 1:08d}",
                merchant_category=category,
                merchant_city=rng.choice(CITY_POOL),
                merchant_country="US" if rng.random() > 0.03 else rng.choice(["CA", "MX", "GB"]),
                merchant_risk=_sample_weighted_value(MERCHANT_RISK_WEIGHTS, rng),
            )
        )
    return merchants


def _generate_burst_dates(config: OfflineGeneratorConfig, rng: random.Random) -> list[date]:
    """Pick a small set of days with unusually high traffic."""

    if config.burst_day_count == 0:
        return []

    available_days = [config.start_date + timedelta(days=offset) for offset in range(config.days_history)]
    burst_day_count = min(config.burst_day_count, len(available_days))
    return sorted(rng.sample(available_days, burst_day_count))


def _generate_fraud_rings(config: OfflineGeneratorConfig, rng: random.Random) -> list[FraudRing]:
    """Create reusable device/IP pairs that mimic coordinated fraud rings."""

    rings: list[FraudRing] = []
    for index in range(config.fraud_ring_count):
        rings.append(
            FraudRing(
                device_id=f"dev_ring_{index + 1:04d}",
                ip_address=f"172.16.{index % 255}.{rng.randint(1, 254)}",
            )
        )
    return rings


def _plan_days(config: OfflineGeneratorConfig, burst_dates: Sequence[date]) -> list[DayPlan]:
    """Share the transactions out over the days, with burst days getting extra traffic."""

    days = [config.start_date + timedelta(days=offset) for offset in range(config.days_history)]
    normal_share = 1 - BURST_TRAFFIC_SHARE if burst_dates else 1.0
    shares = [
        normal_share / len(days) + (BURST_TRAFFIC_SHARE / len(burst_dates) if day in burst_dates else 0.0)
        for day in days
    ]
    counts = [int(config.n_transactions * share) for share in shares]
    # Rounding down leaves a few transactions over; the first days take one each.
    for position in range(config.n_transactions - sum(counts)):
        counts[position % len(counts)] += 1

    plans: list[DayPlan] = []
    first_index = 0
    for day, count in zip(days, counts, strict=True):
        plans.append(DayPlan(day, first_index, count))
        first_index += count
    return plans


def _generate_day(config: OfflineGeneratorConfig, plan: DayPlan) -> DayResult:
    """Write one day's transactions and labels as gzipped CSVs, a chunk at a time, then upload them."""

    context = _generation_context(config)
    rng = random.Random(f"{config.random_seed}:{plan.event_date}")
    schema_version = "v2" if plan.event_date >= config.schema_change_date else "v1"
    columns = EVOLVED_COLUMNS if schema_version == "v2" else BASE_COLUMNS
    stats = DayStats()
    transactions_uri = storage.join_uri(
        config.raw_uri,
        f"schema_version={schema_version}",
        f"transaction_date={plan.event_date}",
        "transactions.csv.gz",
    )
    labels_uri = storage.join_uri(config.labels_uri, f"transaction_date={plan.event_date}", "transaction_labels.csv.gz")

    with TemporaryDirectory() as tmp_dir:
        transactions_path = Path(tmp_dir, "transactions.csv.gz")
        labels_path = Path(tmp_dir, "transaction_labels.csv.gz")
        with (
            gzip.open(transactions_path, "wt", compresslevel=GZIP_LEVEL, encoding="utf-8", newline="") as transactions_file,
            gzip.open(labels_path, "wt", compresslevel=GZIP_LEVEL, encoding="utf-8", newline="") as labels_file,
        ):
            transactions = csv.DictWriter(transactions_file, fieldnames=columns, extrasaction="ignore")
            labels = csv.writer(labels_file)
            transactions.writeheader()
            labels.writerow(LABEL_COLUMNS)
            for chunk_start in range(0, plan.row_count, CHUNK_ROWS):
                chunk_end = min(chunk_start + CHUNK_ROWS, plan.row_count)
                rows = [
                    _generate_transaction(plan.first_index + offset, plan.event_date, config, context, rng)
                    for offset in range(chunk_start, chunk_end)
                ]
                labels.writerows([row[column] for column in LABEL_COLUMNS] for row in rows)
                written_rows = [*rows, *_duplicate_rows(rows, config.duplicate_rate, rng)]
                rng.shuffle(written_rows)
                transactions.writerows(written_rows)
                stats.add(rows, written_rows, config.currency)

        storage.put_file(config.warehouse, transactions_uri, transactions_path)
        storage.put_file(config.warehouse, labels_uri, labels_path)
    return DayResult(plan.event_date, schema_version, transactions_uri, labels_uri, stats)


def _generate_transaction(
    index: int,
    event_date: date,
    config: OfflineGeneratorConfig,
    context: GenerationContext,
    rng: random.Random,
) -> TransactionRow:
    """Create one synthetic transaction row on the given day."""

    customer = rng.choice(context.customers)
    merchant = rng.choice(context.merchants)
    event_time = _sample_event_time(event_date, rng)
    created_ts = _sample_created_timestamp(event_time, config.late_arrival_rate, rng)
    drift_multiplier = _drift_multiplier(event_date, config)
    amount = _sample_amount(merchant.merchant_category, rng, drift_multiplier)
    channel = _sample_channel(customer, rng)
    transaction_city = _sample_transaction_city(customer, merchant, channel, rng)
    is_ring_transaction = _is_fraud_ring_transaction(merchant, channel, context.fraud_rings, rng)
    is_fraud = _sample_fraud_label(
        amount=amount,
        merchant_category=merchant.merchant_category,
        city=transaction_city,
        channel=channel,
        customer=customer,
        merchant=merchant,
        is_ring_transaction=is_ring_transaction,
        event_time=event_time,
        rng=rng,
    )

    row = {
        "transaction_id": f"txn_{index + 1:012d}",
        "account_id": customer.account_id,
        "customer_id": customer.customer_id,
        "merchant_id": merchant.merchant_id,
        "merchant_category": merchant.merchant_category,
        "amount": str(amount),
        "currency": config.currency,
        "city": transaction_city,
        "channel": channel,
        "transaction_status": _sample_weighted_value(TRANSACTION_STATUS_WEIGHTS, rng),
        "is_fraud": "1" if is_fraud else "0",
        "event_timestamp": event_time.isoformat(timespec="seconds"),
        "created_ts": created_ts.isoformat(timespec="seconds"),
    }

    if event_date >= config.schema_change_date:
        device_id = f"dev_{rng.randint(1, config.n_customers * 2):010d}"
        ip_address = f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
        if is_ring_transaction and context.fraud_rings:
            ring = rng.choice(context.fraud_rings)
            device_id = ring.device_id
            ip_address = ring.ip_address

        row.update(
            {
                "device_id": device_id,
                "ip_address": ip_address,
                "authentication_method": _sample_authentication_method(channel, is_fraud, rng),
                "risk_signal_version": "v2",
            }
        )

    _inject_raw_quality_issues(row, config, event_date, rng)
    return row


def _sample_event_time(event_date: date, rng: random.Random) -> datetime:
    """Sample a time of day with daily seasonality and peak hours."""

    hour = int(_sample_weighted_value(PEAK_HOUR_LABEL_WEIGHTS, rng))
    minute = rng.randrange(60)
    second = rng.randrange(60)
    return datetime.combine(event_date, datetime.min.time()) + timedelta(hours=hour, minutes=minute, seconds=second)


def _sample_created_timestamp(event_time: datetime, late_arrival_rate: float, rng: random.Random) -> datetime:
    """Sample source creation time after the business event occurred."""

    if rng.random() < late_arrival_rate:
        delay_minutes = rng.randint(LATE_ARRIVAL_THRESHOLD_MINUTES + 1, 72 * 60)
    else:
        delay_minutes = rng.randint(0, 20)
    return event_time + timedelta(minutes=delay_minutes, seconds=rng.randrange(60))


def _sample_channel(customer: CustomerProfile, rng: random.Random) -> str:
    """Bias channels by customer segment while keeping global channel skew."""

    if customer.segment == "digital_only":
        return _sample_weighted_value(DIGITAL_ONLY_CHANNEL_WEIGHTS, rng)
    if customer.segment == "traveler":
        return _sample_weighted_value(TRAVELER_CHANNEL_WEIGHTS, rng)
    return _sample_weighted_value(CHANNEL_WEIGHTS, rng)


def _sample_transaction_city(
    customer: CustomerProfile,
    merchant: MerchantProfile,
    channel: str,
    rng: random.Random,
) -> str:
    """Choose the transaction city from home behavior, merchant location, and travel."""

    if channel in {"online", "mobile_wallet"} and rng.random() < 0.70:
        return customer.home_city
    if customer.segment == "traveler" and rng.random() < 0.45:
        return merchant.merchant_city
    if rng.random() < 0.10:
        return rng.choice(CITY_POOL)
    return customer.home_city


def _sample_amount(merchant_category: str, rng: random.Random, drift_multiplier: float = 1.0) -> Decimal:
    """Sample a realistic transaction amount for the merchant category."""

    category_multipliers = {
        "travel": 7.5,
        "electronics": 4.2,
        "cash_transfer": 3.8,
        "online_marketplace": 2.4,
        "healthcare": 2.1,
    }
    multiplier = category_multipliers.get(merchant_category, 1.0)
    cents = int(rng.lognormvariate(3.2, 0.85) * multiplier * drift_multiplier * 100)
    return Decimal(max(cents, 100)) / Decimal("100")


def _drift_multiplier(event_date: date, config: OfflineGeneratorConfig) -> float:
    """Return the linear amount-drift multiplier active on a given event date.

    Ramps linearly from 1.0 at `drift_start_date` to `drift_amount_multiplier_end`
    by the end of the generated history window, so `amount` (and everything
    downstream that aggregates it) drifts gradually rather than stepping
    instantly.
    """

    if config.drift_start_date is None or event_date < config.drift_start_date:
        return 1.0

    history_end_date = config.start_date + timedelta(days=config.days_history)
    total_drift_days = max((history_end_date - config.drift_start_date).days, 1)
    elapsed_days = min((event_date - config.drift_start_date).days, total_drift_days)
    progress = elapsed_days / total_drift_days
    return 1.0 + (config.drift_amount_multiplier_end - 1.0) * progress


def _sample_weighted_value(weights: Mapping[str, int], rng: random.Random) -> str:
    """Sample one dictionary key using the dictionary values as relative weights."""

    return rng.choices(list(weights), weights=list(weights.values()))[0]


def _sample_authentication_method(channel: str, is_fraud: bool, rng: random.Random) -> str:
    """Sample authentication in a way that makes risky transactions inspectable."""

    if channel == "card_present":
        return rng.choice(["chip", "pin", "tap"])
    if is_fraud:
        return _sample_weighted_value(FRAUD_AUTHENTICATION_WEIGHTS, rng)
    return _sample_weighted_value(NORMAL_AUTHENTICATION_WEIGHTS, rng)


def _is_fraud_ring_transaction(
    merchant: MerchantProfile,
    channel: str,
    fraud_rings: Sequence[FraudRing],
    rng: random.Random,
) -> bool:
    """Return true for a small number of transactions tied to reusable fraud infrastructure."""

    if not fraud_rings:
        return False
    if channel not in {"online", "mobile_wallet"}:
        return False
    if merchant.merchant_category not in FRAUD_RING_CATEGORIES:
        return False
    return rng.random() < 0.035


def _sample_fraud_label(
    amount: Decimal,
    merchant_category: str,
    city: str,
    channel: str,
    customer: CustomerProfile,
    merchant: MerchantProfile,
    is_ring_transaction: bool,
    event_time: datetime,
    rng: random.Random,
) -> bool:
    """Sample a rare fraud label with higher risk for selected transaction traits."""

    probability = 0.004
    if amount >= Decimal("500"):
        probability += 0.025
    if merchant_category in {"cash_transfer", "electronics", "online_marketplace"}:
        probability += 0.006
    if city in {"Miami", "Los Angeles"}:
        probability += 0.004
    if channel in {"online", "mobile_wallet"}:
        probability += 0.003
    if customer.risk_tier == "elevated":
        probability += 0.004
    if merchant.merchant_risk == "high":
        probability += 0.006
    if merchant.merchant_country != customer.home_country:
        probability += 0.012
    if event_time.hour <= 5:
        probability += 0.005
    if is_ring_transaction:
        probability += 0.35
    return rng.random() < probability


def _inject_raw_quality_issues(
    row: TransactionRow,
    config: OfflineGeneratorConfig,
    event_date: date,
    rng: random.Random,
) -> None:
    """Inject light source-system messiness that downstream cleaning can solve."""

    if rng.random() < config.missing_value_rate:
        candidates = ["city", "merchant_id"]
        if event_date >= config.schema_change_date:
            candidates.extend(["device_id", "ip_address", "authentication_method"])
        row[rng.choice(candidates)] = ""

    if rng.random() < config.inconsistent_format_rate:
        issue_type = rng.choice(RAW_FORMAT_ISSUES)
        if issue_type == "city_case":
            row["city"] = row["city"].upper()
        elif issue_type == "city_padding":
            row["city"] = f" {row['city']} "
        elif issue_type == "currency_case":
            row["currency"] = row["currency"].lower()
        elif issue_type == "status_case":
            row["transaction_status"] = row["transaction_status"].upper()


def _duplicate_rows(
    rows: list[TransactionRow],
    duplicate_rate: float,
    rng: random.Random,
) -> list[TransactionRow]:
    """Repeat each row with probability `duplicate_rate`, so the rate holds however small the chunk."""

    return [dict(row) for row in rows if rng.random() < duplicate_rate]


def _build_quality_summary(
    config: OfflineGeneratorConfig,
    results: Sequence[DayResult],
    burst_dates: Sequence[date],
) -> dict[str, Any]:
    """Add the days' counts up into the evidence metrics for the generated source data."""

    total = DayStats()
    for result in results:
        total.merge(result.stats)
    total_rows = total.rows
    duplicate_rows = total.rows - total.base_rows
    burst_rows = sum(result.stats.rows for result in results if result.event_date in burst_dates)
    new_partition_rows = sum(result.stats.rows for result in results if result.schema_version == "v2")

    return {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "output_dir": str(config.output_dir),
        "raw_uri": config.raw_uri,
        "data_format": "partitioned_csv_gzip",
        "written_file_count": len(results),
        "base_row_count": total.base_rows,
        "row_count_after_duplicates": total_rows,
        "duplicate_row_count": duplicate_rows,
        "duplicate_rate_actual": _rate(duplicate_rows, total_rows),
        "skew": {
            "city_distribution_pct": _to_percentages(total.cities, total_rows),
            "merchant_category_distribution_pct": _to_percentages(total.categories, total_rows),
        },
        "fraud": {
            "fraud_row_count": total.fraud_rows,
            "fraud_rate_actual": _rate(total.fraud_rows, total_rows),
            "fraud_ring_row_count": total.ring_rows,
            "fraud_ring_device_count": len(total.ring_devices),
        },
        "drift": _build_drift_summary(config, results),
        "traffic_patterns": {
            "burst_dates": [item.isoformat() for item in sorted(burst_dates)],
            "burst_row_count": burst_rows,
            "burst_row_pct": _percentage(burst_rows, total_rows),
            "hour_distribution_pct": _to_percentages(total.hours, total_rows),
        },
        "late_arrivals": {
            "late_arrival_threshold_minutes": LATE_ARRIVAL_THRESHOLD_MINUTES,
            "late_arrival_row_count": total.late_rows,
            "late_arrival_rate_actual": _rate(total.late_rows, total_rows),
            "max_arrival_delay_hours": round(total.max_delay_minutes / 60, 2),
            "file_order_note": "Rows are shuffled before writing, so source file order is not event-time order.",
        },
        "raw_quality_issues": {
            "missing_value_row_count": total.missing_value_rows,
            "missing_value_rate_actual": _rate(total.missing_value_rows, total_rows),
            "inconsistent_format_row_count": total.inconsistent_format_rows,
            "inconsistent_format_rate_actual": _rate(total.inconsistent_format_rows, total_rows),
            "examples": [
                "blank device_id/ip/city/merchant_id",
                "uppercase or padded city",
                "lowercase currency",
                "uppercase status",
            ],
        },
        # Every generated transaction has its own id. Distinct customers, accounts,
        # merchants and devices are measured from the stored files by the profile job.
        "high_cardinality": {"approx_count_distinct_transaction_id": total.base_rows},
        "schema_evolution": {
            "schema_change_date": config.schema_change_date.isoformat(),
            "old_partition_row_count": total_rows - new_partition_rows,
            "new_partition_row_count": new_partition_rows,
            "old_partition_missing_columns": EVOLVED_ONLY_COLUMNS,
            "new_partition_added_columns": EVOLVED_ONLY_COLUMNS,
        },
        "label_table": {
            "uri": config.labels_uri,
            "row_count": total.base_rows,
            "columns": LABEL_COLUMNS,
        },
        "bronze_ingestion_notes": {
            "dedup_key": "transaction_id",
            "partition_columns": ["schema_version", "transaction_date"],
            "recommended_raw_table": "raw_transactions",
            "recommended_silver_fixes": [
                "deduplicate by transaction_id",
                "standardize city, currency, and transaction_status formats",
                "handle missing evolved columns from v1 partitions",
                "use event_timestamp for business time and created_ts for arrival time",
            ],
        },
    }


def _build_drift_summary(config: OfflineGeneratorConfig, results: Sequence[DayResult]) -> dict[str, Any]:
    """Report mean amount before and after the drift start date, or that drift is off."""

    if config.drift_start_date is None:
        return {"enabled": False}

    def mean_amount(days: list[DayResult]) -> float | None:
        rows = sum(day.stats.rows for day in days)
        return round(sum(day.stats.amount_total for day in days) / rows, 2) if rows else None

    return {
        "enabled": True,
        "drift_start_date": config.drift_start_date.isoformat(),
        "drift_amount_multiplier_end": config.drift_amount_multiplier_end,
        "mean_amount_before_drift": mean_amount([r for r in results if r.event_date < config.drift_start_date]),
        "mean_amount_after_drift": mean_amount([r for r in results if r.event_date >= config.drift_start_date]),
    }


def _arrival_delay_minutes(row: TransactionRow) -> float:
    """Calculate source arrival delay in minutes."""

    event_ts = datetime.fromisoformat(row["event_timestamp"])
    created_ts = datetime.fromisoformat(row["created_ts"])
    return (created_ts - event_ts).total_seconds() / 60


def _rate(count: int, total: int) -> float:
    """Return a rounded count-over-total rate."""

    return round(count / total, 4) if total else 0.0


def _percentage(count: int, total: int) -> float:
    """Return a rounded count-over-total percentage."""

    return round(_rate(count, total) * 100, 2)


def _has_missing_values(row: TransactionRow) -> bool:
    """Return true when a row has blank values in business-critical fields."""

    fields = ["merchant_id", "city", "device_id", "ip_address", "authentication_method"]
    return any(field in row and row[field] == "" for field in fields)


def _has_inconsistent_format(row: TransactionRow, expected_currency: str) -> bool | str | Any:
    """Return true for easy-to-clean source formatting issues."""

    city = row["city"]
    return (
        city != city.strip()
        or (city and city != city.title())
        or row["currency"] != expected_currency
        or row["transaction_status"] not in TRANSACTION_STATUS_WEIGHTS
    )


def _to_percentages(counts: Counter[str], total: int) -> dict[str, float]:
    """Convert count values into sorted percentage values."""

    return {key: round((value / total) * 100, 2) for key, value in counts.most_common()}


def _write_summary_artifacts(summary: dict[str, Any], output_dir: Path) -> None:
    """Write JSON and CSV summary artifacts next to the generated source files."""

    with (output_dir / "_quality_summary.json").open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2)

    rows = [
        ("row_count.base", summary["base_row_count"]),
        ("row_count.after_duplicates", summary["row_count_after_duplicates"]),
        ("duplicates.count", summary["duplicate_row_count"]),
        ("duplicates.rate_actual", summary["duplicate_rate_actual"]),
        ("fraud.count", summary["fraud"]["fraud_row_count"]),
        ("fraud.rate_actual", summary["fraud"]["fraud_rate_actual"]),
        ("fraud.ring_rows", summary["fraud"]["fraud_ring_row_count"]),
        ("traffic.burst_row_count", summary["traffic_patterns"]["burst_row_count"]),
        ("late_arrivals.count", summary["late_arrivals"]["late_arrival_row_count"]),
        ("late_arrivals.rate_actual", summary["late_arrivals"]["late_arrival_rate_actual"]),
        ("raw_quality.missing_value_rows", summary["raw_quality_issues"]["missing_value_row_count"]),
        ("raw_quality.inconsistent_format_rows", summary["raw_quality_issues"]["inconsistent_format_row_count"]),
        ("schema.old_partition_row_count", summary["schema_evolution"]["old_partition_row_count"]),
        ("schema.new_partition_row_count", summary["schema_evolution"]["new_partition_row_count"]),
        ("drift.mean_amount_before", summary["drift"].get("mean_amount_before_drift")),
        ("drift.mean_amount_after", summary["drift"].get("mean_amount_after_drift")),
        ("label_table.row_count", summary["label_table"]["row_count"]),
    ]
    rows.extend((f"skew.city.{key}.pct", value) for key, value in summary["skew"]["city_distribution_pct"].items())
    rows.extend(
        (f"skew.merchant_category.{key}.pct", value)
        for key, value in summary["skew"]["merchant_category_distribution_pct"].items()
    )
    rows.extend((f"cardinality.{key}", value) for key, value in summary["high_cardinality"].items())

    with (output_dir / "_quality_summary.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["metric", "value"])
        writer.writerows(rows)


def _write_manifest(config: OfflineGeneratorConfig, summary: dict[str, Any], results: Sequence[DayResult]) -> None:
    """Write a manifest that future Bronze ingestion jobs can consume."""

    manifest = {
        "dataset": "offline_transactions",
        "source_system": "fraudstream_generator",
        "created_at": summary["generated_at"],
        "config": {
            "random_seed": config.random_seed,
            "n_transactions": config.n_transactions,
            "duplicate_rate": config.duplicate_rate,
            "late_arrival_rate": config.late_arrival_rate,
            "missing_value_rate": config.missing_value_rate,
            "inconsistent_format_rate": config.inconsistent_format_rate,
            "burst_day_count": config.burst_day_count,
            "fraud_ring_count": config.fraud_ring_count,
            "schema_change_date": config.schema_change_date.isoformat(),
            "drift_start_date": config.drift_start_date.isoformat() if config.drift_start_date else None,
            "drift_amount_multiplier_end": config.drift_amount_multiplier_end,
            "labels_uri": config.labels_uri,
        },
        "files": [result.transactions_uri for result in results],
        "label_files": [result.labels_uri for result in results],
    }
    with (config.output_dir / "_manifest.json").open("w", encoding="utf-8") as file:
        json.dump(manifest, file, indent=2)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for the offline generator."""

    parser = argparse.ArgumentParser(description="Generate raw offline transaction source data.")
    parser.add_argument(
        "--config",
        default="configs/generator/offline_transactions.json",
        type=Path,
        help="Path to the JSON generator configuration file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Optional local output directory override for manifest/summary evidence files. "
        "Defaults to the value in the config file.",
    )
    parser.add_argument(
        "--raw-uri",
        help=f"Optional MinIO URI override for the raw CSV partitions. Defaults to {DEFAULT_RAW_URI} "
        "(or the config file's raw_uri, if set).",
    )
    parser.add_argument(
        "--labels-uri",
        help=f"Optional MinIO URI override for the id/label training table. Defaults to {DEFAULT_LABELS_URI} "
        "(or the config file's labels_uri, if set).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Days generated at once, one process each. Defaults to 1.",
    )
    add_warehouse_arguments(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the offline transaction generator from the command line."""

    args = build_parser().parse_args(argv)
    config = OfflineGeneratorConfig.from_json(args.config)
    if args.output_dir:
        config = replace(config, output_dir=args.output_dir)
    if args.raw_uri:
        config = replace(config, raw_uri=args.raw_uri)
    if args.labels_uri:
        config = replace(config, labels_uri=args.labels_uri)
    config = replace(config, warehouse=warehouse_config_from_args(args))
    summary = generate_offline_transactions(config, workers=args.workers)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
