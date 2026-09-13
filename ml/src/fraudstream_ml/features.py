"""Turn the retrieved frame into model-ready feature columns."""

from __future__ import annotations

from typing import Any

from fraudstream_ml.dataset import BATCH_FEATURE_VIEWS


IDENTIFIER_COLUMNS = ("transaction_id", "customer_id", "merchant_id", "event_timestamp")
LABEL_COLUMN = "is_fraud"
CATEGORICAL_COLUMN = "merchant_category"

MERCHANT_CATEGORIES = (
    "cash_transfer",
    "electronics",
    "fuel",
    "grocery",
    "healthcare",
    "online_marketplace",
    "restaurant",
    "travel",
)

AVAILABILITY_FLAGS: dict[str, str] = {
    "customer_rolling_features": "customer_features_available",
    "customer_orders_90d_features": "customer_orders_90d_available",
    "merchant_risk_features": "merchant_features_available",
}


def prepare_features(
    frame: Any,
    *,
    categories: tuple[str, ...] = MERCHANT_CATEGORIES,
) -> tuple[Any, list[str]]:
    """Add availability flags and encoded categories, returning the frame and its feature columns.

    Nulls are left untouched. Most transactions have no customer snapshot
    inside the view's TTL, and both dropping and imputing those rows would
    destroy information: dropping biases the set toward high-activity
    customers, imputing hides that the customer has no recent history. The
    availability flags make that absence explicit instead.
    """

    prepared = frame.copy() 
    feature_names: list[str] = []

    for view, view_features in BATCH_FEATURE_VIEWS.items():
        columns = [name for name in view_features if name in prepared.columns]
        if not columns:
            raise ValueError(f"frame carries no columns for feature view {view!r}")
        prepared[AVAILABILITY_FLAGS[view]] = prepared[columns].notna().any(axis=1)
        feature_names.extend(name for name in columns if name != CATEGORICAL_COLUMN)

    feature_names.extend(AVAILABILITY_FLAGS[view] for view in BATCH_FEATURE_VIEWS)

    for category in categories:
        column = f"{CATEGORICAL_COLUMN}_{category}"
        prepared[column] = (prepared[CATEGORICAL_COLUMN] == category).astype(int)
        feature_names.append(column)

    return prepared, feature_names
