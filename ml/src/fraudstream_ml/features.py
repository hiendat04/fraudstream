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

CHANNELS = ("atm", "card_present", "mobile_wallet", "online")

CITIES = (
    "Atlanta",
    "Boston",
    "Charlotte",
    "Chicago",
    "Dallas",
    "Denver",
    "Detroit",
    "Los Angeles",
    "Miami",
    "New York",
    "Phoenix",
    "Seattle",
)

CATEGORICAL_ENCODINGS: dict[str, tuple[str, ...]] = {
    CATEGORICAL_COLUMN: MERCHANT_CATEGORIES,
    "channel": CHANNELS,
    "city": CITIES,
}

# Attributes of the transaction being scored rather than aggregates of its
# entities' past, so they are known at scoring time and carry no leakage.
REQUEST_TIME_NUMERIC = ("amount", "event_hour")

AVAILABILITY_FLAGS: dict[str, str] = {
    "customer_rolling_features": "customer_features_available",
    "customer_orders_90d_features": "customer_orders_90d_available",
    "merchant_risk_features": "merchant_features_available",
}


def _slug(value: str) -> str:
    """Turn a category value into a column-name-safe suffix."""

    return value.strip().lower().replace(" ", "_")


def prepare_features(
    frame: Any,
    *,
    encodings: dict[str, tuple[str, ...]] | None = None,
) -> tuple[Any, list[str]]:
    """Add availability flags, request-time features, and encoded categories.

    Nulls are left untouched. Most transactions have no customer snapshot
    inside the view's TTL, and both dropping and imputing those rows would
    destroy information: dropping biases the set toward high-activity
    customers, imputing hides that the customer has no recent history. The
    availability flags make that absence explicit instead.
    """

    categorical = CATEGORICAL_ENCODINGS if encodings is None else encodings
    prepared = frame.copy()
    feature_names: list[str] = []

    for view, view_features in BATCH_FEATURE_VIEWS.items():
        columns = [name for name in view_features if name in prepared.columns]
        if not columns:
            raise ValueError(f"frame carries no columns for feature view {view!r}")
        prepared[AVAILABILITY_FLAGS[view]] = prepared[columns].notna().any(axis=1)
        feature_names.extend(name for name in columns if name not in categorical)

    if "event_timestamp" in prepared.columns:
        prepared["event_hour"] = prepared["event_timestamp"].dt.hour

    feature_names.extend(name for name in REQUEST_TIME_NUMERIC if name in prepared.columns)
    feature_names.extend(AVAILABILITY_FLAGS[view] for view in BATCH_FEATURE_VIEWS)

    for column, categories in categorical.items():
        if column not in prepared.columns:
            continue
        for category in categories:
            encoded = f"{column}_{_slug(category)}"
            prepared[encoded] = (prepared[column] == category).astype(int)
            feature_names.append(encoded)

    return prepared, feature_names
