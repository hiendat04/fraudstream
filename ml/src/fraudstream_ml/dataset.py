"""Build the labeled training frame from the Feast offline store."""

from __future__ import annotations

from datetime import datetime
from typing import Any


FACT_TABLE = "iceberg.gold.fact_transactions"
LABEL_TABLE = "iceberg.gold.transaction_labels"

BATCH_FEATURE_VIEWS: dict[str, tuple[str, ...]] = {
    "customer_rolling_features": (
        "txn_count_7d",
        "txn_count_30d",
        "amount_sum_7d",
        "amount_sum_30d",
        "amount_avg_7d",
        "amount_avg_30d",
        "distinct_merchant_count_7d",
        "distinct_merchant_count_30d",
        "declined_txn_count_7d",
        "fraud_txn_count_30d",
    ),
    "customer_orders_90d_features": ("total_orders_90d",),
    "merchant_risk_features": (
        "merchant_category",
        "merchant_txn_count_1d",
        "merchant_txn_count_7d",
        "merchant_txn_count_30d",
        "merchant_amount_sum_30d",
        "merchant_amount_avg_30d",
        "merchant_distinct_customer_count_1d",
        "merchant_declined_txn_count_1d",
        "merchant_fraud_rate_1d",
        "merchant_prior_fraud_rate_30d",
        "merchant_burst_ratio_1d_to_prior_30d",
        "merchant_vs_category_amount_ratio_30d",
    ),
}

BATCH_FEATURE_REFS: tuple[str, ...] = tuple(
    f"{view}:{feature}"
    for view, features in BATCH_FEATURE_VIEWS.items()
    for feature in features
)


def _timestamp_literal(value: str | datetime) -> str:
    """Normalize a window bound into a Spark SQL timestamp literal."""

    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def entity_dataframe_sql(start: str | datetime, end: str | datetime) -> str:
    """Return the entity-dataframe SQL joining each transaction to its label."""

    start_literal = _timestamp_literal(start)
    end_literal = _timestamp_literal(end)
    if end_literal <= start_literal:
        raise ValueError(f"end ({end_literal}) must be after start ({start_literal})")

    return f"""
        SELECT
            f.transaction_id,
            f.customer_id,
            f.merchant_dim_id AS merchant_id,
            f.event_time AS event_timestamp,
            l.is_fraud
        FROM {FACT_TABLE} f
        JOIN {LABEL_TABLE} l ON l.transaction_id = f.transaction_id
        WHERE f.event_time >= TIMESTAMP '{start_literal}'
          AND f.event_time < TIMESTAMP '{end_literal}'
    """


def load_training_frame(
    repo_path: str,
    start: str | datetime,
    end: str | datetime,
    *,
    feature_refs: tuple[str, ...] = BATCH_FEATURE_REFS,
) -> Any:
    """Retrieve point-in-time-correct features for every labeled transaction in the window."""

    from feast import FeatureStore

    store = FeatureStore(repo_path=repo_path)
    retrieval = store.get_historical_features(
        entity_df=entity_dataframe_sql(start, end),
        features=list(feature_refs),
    )
    return retrieval.to_df()


def coverage_report(frame: Any) -> dict[str, float]:
    """Return the share of rows that joined to each batch feature view."""

    report: dict[str, float] = {}
    for view, features in BATCH_FEATURE_VIEWS.items():
        columns = [name for name in features if name in frame.columns]
        if not columns:
            raise ValueError(f"frame carries no columns for feature view {view!r}")
        report[view] = float(frame[columns].notna().any(axis=1).mean()) if len(frame) else 0.0
    return report
