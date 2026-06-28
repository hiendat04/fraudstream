"""Generate raw offline transaction data with intentional data quality problems.

This module creates source-style financial transaction CSV files that can be
ingested into a future Bronze zone. It intentionally simulates practical
offline data problems: skew, high-cardinality identifiers, schema evolution,
duplicate records, late arrivals, bursty traffic, missing values, inconsistent
formats, and fraud-ring behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
from collections import Counter
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence


TransactionRow = dict[str, str]

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
class OfflineGeneratorConfig:
    """Validated runtime settings for the offline transaction generator."""

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
    late_arrival_rate: float = 0.04
    missing_value_rate: float = 0.015
    inconsistent_format_rate: float = 0.01
    burst_day_count: int = 8
    fraud_ring_count: int = 6

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
            late_arrival_rate=float(raw.get("late_arrival_rate", 0.04)),
            missing_value_rate=float(raw.get("missing_value_rate", 0.015)),
            inconsistent_format_rate=float(raw.get("inconsistent_format_rate", 0.01)),
            burst_day_count=int(raw.get("burst_day_count", 8)),
            fraud_ring_count=int(raw.get("fraud_ring_count", 6)),
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


def generate_offline_transactions(config: OfflineGeneratorConfig) -> dict[str, Any]:
    """Generate partitioned raw CSV files and data-quality summary artifacts."""

    config.validate()
    rng = random.Random(config.random_seed)
    _prepare_output_dir(config.output_dir)

    context = _build_generation_context(config, rng)
    rows = [_generate_transaction(index, config, context, rng) for index in range(config.n_transactions)]
    duplicated_rows = _duplicate_rows(rows, config.duplicate_rate, rng)
    all_rows = [*rows, *duplicated_rows]
    rng.shuffle(all_rows)

    written_files = _write_partitioned_csv(all_rows, config)
    summary = _build_quality_summary(rows, all_rows, config, written_files, context.burst_dates)
    _write_summary_artifacts(summary, config.output_dir)
    _write_manifest(config, summary, written_files)
    return summary


def _prepare_output_dir(output_dir: Path) -> None:
    """Remove prior generator artifacts without deleting unrelated local files."""

    output_dir.mkdir(parents=True, exist_ok=True)
    for artifact in ["_manifest.json", "_quality_summary.json", "_quality_summary.csv"]:
        artifact_path = output_dir / artifact
        if artifact_path.exists():
            artifact_path.unlink()

    for partition_dir in output_dir.glob("schema_version=*"):
        if partition_dir.is_dir():
            shutil.rmtree(partition_dir)


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


def _generate_transaction(
    index: int,
    config: OfflineGeneratorConfig,
    context: GenerationContext,
    rng: random.Random,
) -> TransactionRow:
    """Create one synthetic transaction row."""

    customer = rng.choice(context.customers)
    merchant = rng.choice(context.merchants)
    event_time = _sample_event_time(config, context.burst_dates, rng)
    event_date = event_time.date()
    created_ts = _sample_created_timestamp(event_time, config.late_arrival_rate, rng)
    amount = _sample_amount(merchant.merchant_category, rng)
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


def _sample_event_time(config: OfflineGeneratorConfig, burst_dates: Sequence[date], rng: random.Random) -> datetime:
    """Sample event time with daily seasonality, peaks, and occasional burst days."""

    if burst_dates and rng.random() < 0.18:
        event_date = rng.choice(burst_dates)
    else:
        event_date = config.start_date + timedelta(days=rng.randrange(config.days_history))

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


def _sample_amount(merchant_category: str, rng: random.Random) -> Decimal:
    """Sample a realistic transaction amount for the merchant category."""

    category_multipliers = {
        "travel": 7.5,
        "electronics": 4.2,
        "cash_transfer": 3.8,
        "online_marketplace": 2.4,
        "healthcare": 2.1,
    }
    multiplier = category_multipliers.get(merchant_category, 1.0)
    cents = int(rng.lognormvariate(3.2, 0.85) * multiplier * 100)
    return Decimal(max(cents, 100)) / Decimal("100")


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
    """Create duplicate source records by repeating sampled transaction rows."""

    duplicate_count = round(len(rows) * duplicate_rate)
    return [dict(row) for row in rng.sample(rows, duplicate_count)]


def _write_partitioned_csv(rows: list[TransactionRow], config: OfflineGeneratorConfig) -> list[str]:
    """Write raw rows partitioned by schema version and transaction date."""

    partitions: dict[tuple[str, str], list[TransactionRow]] = {}
    for row in rows:
        event_date = row["event_timestamp"][:10]
        schema_version = "v2" if date.fromisoformat(event_date) >= config.schema_change_date else "v1"
        partitions.setdefault((schema_version, event_date), []).append(row)

    written_files: list[str] = []
    for (schema_version, event_date), partition_rows in sorted(partitions.items()):
        partition_dir = config.output_dir / f"schema_version={schema_version}" / f"transaction_date={event_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = partition_dir / "transactions.csv"
        columns = EVOLVED_COLUMNS if schema_version == "v2" else BASE_COLUMNS
        with output_path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(partition_rows)
        written_files.append(str(output_path))
    return written_files


def _build_quality_summary(
    base_rows: list[TransactionRow],
    all_rows: list[TransactionRow],
    config: OfflineGeneratorConfig,
    written_files: list[str],
    burst_dates: Sequence[date],
) -> dict[str, Any]:
    """Calculate evidence metrics for generated source data."""

    total_rows = len(all_rows)
    event_dates = [_event_date(row) for row in all_rows]
    burst_date_set = set(burst_dates)
    new_rows = [
        row
        for row, event_date in zip(all_rows, event_dates, strict=True)
        if event_date >= config.schema_change_date
    ]
    old_partition_row_count = total_rows - len(new_rows)

    city_counts = Counter(row["city"] for row in all_rows)
    category_counts = Counter(row["merchant_category"] for row in all_rows)
    hour_counts = Counter(row["event_timestamp"][11:13] for row in all_rows)
    transaction_counts = Counter(row["transaction_id"] for row in all_rows)

    duplicate_rows = sum(count - 1 for count in transaction_counts.values() if count > 1)
    fraud_row_count = sum(1 for row in all_rows if row["is_fraud"] == "1")
    arrival_delays = [_arrival_delay_minutes(row) for row in all_rows]
    late_arrival_row_count = sum(delay > LATE_ARRIVAL_THRESHOLD_MINUTES for delay in arrival_delays)
    burst_row_count = sum(event_date in burst_date_set for event_date in event_dates)
    missing_value_rows = sum(1 for row in all_rows if _has_missing_values(row))
    inconsistent_format_rows = sum(1 for row in all_rows if _has_inconsistent_format(row, config.currency))
    fraud_ring_rows = sum(1 for row in new_rows if row.get("device_id", "").startswith("dev_ring_"))
    fraud_ring_devices = {
        row.get("device_id")
        for row in new_rows
        if row.get("device_id", "").startswith("dev_ring_")
    }

    return {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "output_dir": str(config.output_dir),
        "data_format": "partitioned_csv",
        "written_file_count": len(written_files),
        "base_row_count": len(base_rows),
        "row_count_after_duplicates": total_rows,
        "duplicate_row_count": duplicate_rows,
        "duplicate_rate_actual": _rate(duplicate_rows, total_rows),
        "skew": {
            "city_distribution_pct": _to_percentages(city_counts, total_rows),
            "merchant_category_distribution_pct": _to_percentages(category_counts, total_rows),
        },
        "fraud": {
            "fraud_row_count": fraud_row_count,
            "fraud_rate_actual": _rate(fraud_row_count, total_rows),
            "fraud_ring_row_count": fraud_ring_rows,
            "fraud_ring_device_count": len(fraud_ring_devices),
        },
        "traffic_patterns": {
            "burst_dates": [item.isoformat() for item in sorted(burst_dates)],
            "burst_row_count": burst_row_count,
            "burst_row_pct": _percentage(burst_row_count, total_rows),
            "hour_distribution_pct": _to_percentages(hour_counts, total_rows),
        },
        "late_arrivals": {
            "late_arrival_threshold_minutes": LATE_ARRIVAL_THRESHOLD_MINUTES,
            "late_arrival_row_count": late_arrival_row_count,
            "late_arrival_rate_actual": _rate(late_arrival_row_count, total_rows),
            "max_arrival_delay_hours": round(max(arrival_delays, default=0) / 60, 2),
            "file_order_note": "Rows are shuffled before writing, so source file order is not event-time order.",
        },
        "raw_quality_issues": {
            "missing_value_row_count": missing_value_rows,
            "missing_value_rate_actual": _rate(missing_value_rows, total_rows),
            "inconsistent_format_row_count": inconsistent_format_rows,
            "inconsistent_format_rate_actual": _rate(inconsistent_format_rows, total_rows),
            "examples": [
                "blank device_id/ip/city/merchant_id",
                "uppercase or padded city",
                "lowercase currency",
                "uppercase status",
            ],
        },
        "high_cardinality": {
            "approx_count_distinct_transaction_id": len(transaction_counts),
            "approx_count_distinct_customer_id": _count_distinct_nonblank(all_rows, "customer_id"),
            "approx_count_distinct_account_id": _count_distinct_nonblank(all_rows, "account_id"),
            "approx_count_distinct_merchant_id": _count_distinct_nonblank(all_rows, "merchant_id"),
            "approx_count_distinct_device_id": _count_distinct_nonblank(new_rows, "device_id"),
        },
        "schema_evolution": {
            "schema_change_date": config.schema_change_date.isoformat(),
            "old_partition_row_count": old_partition_row_count,
            "new_partition_row_count": len(new_rows),
            "old_partition_missing_columns": EVOLVED_ONLY_COLUMNS,
            "new_partition_added_columns": EVOLVED_ONLY_COLUMNS,
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


def _event_date(row: TransactionRow) -> date:
    """Extract the business event date from a transaction row."""

    return date.fromisoformat(row["event_timestamp"][:10])


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


def _count_distinct_nonblank(rows: Sequence[TransactionRow], field_name: str) -> int:
    """Count distinct nonblank values for a field."""

    return len({row[field_name] for row in rows if row.get(field_name)})


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


def _write_manifest(config: OfflineGeneratorConfig, summary: dict[str, Any], written_files: list[str]) -> None:
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
        },
        "files": written_files,
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
        help="Optional output directory override. Defaults to the value in the config file.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the offline transaction generator from the command line."""

    args = build_parser().parse_args(argv)
    config = OfflineGeneratorConfig.from_json(args.config)
    if args.output_dir:
        config = replace(config, output_dir=args.output_dir)
    summary = generate_offline_transactions(config)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
