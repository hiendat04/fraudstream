from datetime import datetime, timezone

from data.sample_alerts import format_alert_record


def test_format_alert_record_shapes_row_into_alert_record():
    row = {
        "transaction_id": "txn_123",
        "event_timestamp": datetime(2026, 1, 5, 3, 14, tzinfo=timezone.utc),
        "amount": 9000,
        "channel": "online",
        "merchant_category": "electronics",
        "city": "Austin",
        "customer_txn_count_7d": 2,
        "customer_amount_sum_30d": 1500.50,
        "merchant_fraud_rate_1d": 0.12,
        "merchant_burst_ratio_1d_to_prior_30d": 3.4,
        "device_distinct_customer_count_1d": 5,
        "ip_distinct_account_count_1d": 3,
    }

    record = format_alert_record(row)

    assert record.transaction_id == "txn_123"
    assert record.event_timestamp == "2026-01-05T03:14:00+00:00"
    assert record.amount == 9000.0
    assert record.customer_amount_sum_30d == 1500.50


def test_format_alert_record_handles_null_optional_fields():
    row = {
        "transaction_id": "txn_456",
        "event_timestamp": datetime(2026, 1, 5, 3, 14, tzinfo=timezone.utc),
        "amount": 42,
        "channel": "pos",
        "merchant_category": None,
        "city": None,
        "customer_txn_count_7d": None,
        "customer_amount_sum_30d": None,
        "merchant_fraud_rate_1d": None,
        "merchant_burst_ratio_1d_to_prior_30d": None,
        "device_distinct_customer_count_1d": None,
        "ip_distinct_account_count_1d": None,
    }

    record = format_alert_record(row)

    assert record.merchant_category is None
    assert record.customer_amount_sum_30d is None
