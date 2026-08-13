import random

from data.generate_explanations import generate_explanation
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


def test_same_seed_produces_identical_output():
    record = make_record()

    first = generate_explanation(record, random.Random(42))
    second = generate_explanation(record, random.Random(42))

    assert first == second


def test_different_seeds_can_produce_different_phrasing():
    record = make_record()

    outputs = {generate_explanation(record, random.Random(seed)) for seed in range(10)}

    assert len(outputs) > 1


def test_burst_signal_included_when_ratio_above_threshold():
    record = make_record(merchant_burst_ratio_1d_to_prior_30d=3.4)

    explanation = generate_explanation(record, random.Random(1))

    assert "3.4x" in explanation


def test_burst_signal_omitted_when_ratio_below_threshold():
    record = make_record(merchant_burst_ratio_1d_to_prior_30d=1.0)

    explanation = generate_explanation(record, random.Random(1))

    assert "burst" not in explanation.lower()
    assert "spiked" not in explanation.lower()


def test_device_reuse_signal_included_above_threshold():
    record = make_record(device_distinct_customer_count_1d=5)

    explanation = generate_explanation(record, random.Random(1))

    assert "5" in explanation
