import argparse
import json
import random
from collections.abc import Iterable

from data.generate_explanations import generate_explanation
from data.models import AlertRecord
from data.sample_alerts import default_dsn, fetch_alert_rows

PROMPT_TEMPLATE = (
    "Transaction: ${amount:,.2f} via {channel} at a {merchant_category} merchant "
    "in {city}. Customer 7-day transaction count: {customer_txn_count_7d}. "
    "Merchant burst ratio: {merchant_burst_ratio_1d_to_prior_30d}. "
    "Device shared with {device_distinct_customer_count_1d} other customers today. "
    "Explain the fraud risk."
)


def build_training_example(record: AlertRecord, rng: random.Random) -> dict:
    prompt = PROMPT_TEMPLATE.format(
        amount=record.amount,
        channel=record.channel,
        merchant_category=record.merchant_category or "unknown",
        city=record.city or "unknown",
        customer_txn_count_7d=record.customer_txn_count_7d,
        merchant_burst_ratio_1d_to_prior_30d=record.merchant_burst_ratio_1d_to_prior_30d,
        device_distinct_customer_count_1d=record.device_distinct_customer_count_1d,
    )
    completion = generate_explanation(record, rng)
    return {
        "messages": [
            {"role": "user", "content": prompt},
            {"role": "assistant", "content": completion},
        ]
    }


def split_train_valid(
    examples: list[dict], valid_fraction: float, rng: random.Random
) -> tuple[list[dict], list[dict]]:
    shuffled = examples.copy()
    rng.shuffle(shuffled)
    split_index = max(1, int(len(shuffled) * (1 - valid_fraction)))
    return shuffled[:split_index], shuffled[split_index:]


def write_jsonl(examples: Iterable[dict], path: str) -> None:
    with open(path, "w") as f:
        for example in examples:
            f.write(json.dumps(example) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=300)
    parser.add_argument("--valid-fraction", type=float, default=0.15)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out-dir", default="data")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    records = fetch_alert_rows(default_dsn(), args.limit)
    examples = [build_training_example(r, rng) for r in records]
    train, valid = split_train_valid(examples, args.valid_fraction, rng)

    write_jsonl(train, f"{args.out_dir}/train.jsonl")
    write_jsonl(valid, f"{args.out_dir}/valid.jsonl")
    print(f"Wrote {len(train)} train / {len(valid)} valid examples to {args.out_dir}")


if __name__ == "__main__":
    main()
