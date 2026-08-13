import random

from data.build_dataset import build_training_example, split_train_valid
from data.models import AlertRecord


def make_record(**overrides) -> AlertRecord:
    defaults = dict(
        transaction_id="txn_1",
        event_timestamp="2026-01-05T03:14:00+00:00",
        amount=9000.0,
        channel="online",
        merchant_category="electronics",
        city="Austin",
        customer_txn_count_7d=2,
        customer_amount_sum_30d=1500.0,
        merchant_fraud_rate_1d=0.1,
        merchant_burst_ratio_1d_to_prior_30d=1.0,
        device_distinct_customer_count_1d=1,
        ip_distinct_account_count_1d=1,
    )
    defaults.update(overrides)
    return AlertRecord(**defaults)


def test_build_training_example_has_user_and_assistant_messages():
    record = make_record()

    example = build_training_example(record, random.Random(1))

    roles = [m["role"] for m in example["messages"]]
    assert roles == ["user", "assistant"]
    assert "$9,000.00" in example["messages"][0]["content"]
    assert len(example["messages"][1]["content"]) > 0


def test_build_training_example_handles_none_numeric_fields():
    record = make_record(
        customer_txn_count_7d=None,
        merchant_burst_ratio_1d_to_prior_30d=None,
        device_distinct_customer_count_1d=None,
    )

    example = build_training_example(record, random.Random(1))

    prompt_content = example["messages"][0]["content"]
    assert "None" not in prompt_content
    assert "Customer 7-day transaction count: 0." in prompt_content
    assert "Merchant burst ratio: 0." in prompt_content
    assert "Device shared with 0 other customers today." in prompt_content


def test_split_train_valid_respects_fraction():
    examples = [{"i": i} for i in range(100)]

    train, valid = split_train_valid(examples, valid_fraction=0.2, rng=random.Random(1))

    assert len(train) == 80
    assert len(valid) == 20
    assert {e["i"] for e in train} | {e["i"] for e in valid} == set(range(100))


def test_split_train_valid_keeps_at_least_one_train_example():
    examples = [{"i": 0}, {"i": 1}]

    train, valid = split_train_valid(examples, valid_fraction=0.9, rng=random.Random(1))

    assert len(train) >= 1
