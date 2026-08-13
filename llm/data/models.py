from dataclasses import dataclass


@dataclass(frozen=True)
class AlertRecord:
    transaction_id: str
    event_timestamp: str
    amount: float
    channel: str
    merchant_category: str | None
    city: str | None
    customer_txn_count_7d: int | None
    customer_amount_sum_30d: float | None
    merchant_fraud_rate_1d: float | None
    merchant_burst_ratio_1d_to_prior_30d: float | None
    device_distinct_customer_count_1d: int | None
    ip_distinct_account_count_1d: int | None
