import os

import psycopg
from psycopg.rows import dict_row

from data.models import AlertRecord

SAMPLE_QUERY = """
    SELECT
        f.transaction_id,
        f.event_timestamp,
        f.amount,
        f.channel,
        ft.merchant_category,
        ft.city,
        f.customer_txn_count_7d,
        f.customer_amount_sum_30d,
        f.merchant_fraud_rate_1d,
        f.merchant_burst_ratio_1d_to_prior_30d,
        f.device_distinct_customer_count_1d,
        f.ip_distinct_account_count_1d
    FROM gold.feat_transaction_training f
    JOIN gold.fact_transactions ft ON ft.transaction_id = f.transaction_id
    WHERE f.is_fraud = true
    ORDER BY random()
    LIMIT %(limit)s;
"""


def format_alert_record(row: dict) -> AlertRecord:
    return AlertRecord(
        transaction_id=row["transaction_id"],
        event_timestamp=row["event_timestamp"].isoformat(),
        amount=float(row["amount"]),
        channel=row["channel"],
        merchant_category=row["merchant_category"],
        city=row["city"],
        customer_txn_count_7d=row["customer_txn_count_7d"],
        customer_amount_sum_30d=(
            float(row["customer_amount_sum_30d"])
            if row["customer_amount_sum_30d"] is not None
            else None
        ),
        merchant_fraud_rate_1d=row["merchant_fraud_rate_1d"],
        merchant_burst_ratio_1d_to_prior_30d=row["merchant_burst_ratio_1d_to_prior_30d"],
        device_distinct_customer_count_1d=row["device_distinct_customer_count_1d"],
        ip_distinct_account_count_1d=row["ip_distinct_account_count_1d"],
    )


def fetch_alert_rows(dsn: str, limit: int) -> list[AlertRecord]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(SAMPLE_QUERY, {"limit": limit}).fetchall()
    return [format_alert_record(row) for row in rows]


def default_dsn() -> str:
    return os.environ.get(
        "FRAUDSTREAM_PG_DSN",
        "postgresql://fraudstream:fraudstream_local_password@localhost:5432/fraudstream",
    )
