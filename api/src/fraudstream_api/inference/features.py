"""The model's 51 inputs for one payment, built with the training code itself."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from fraudstream_ml.dataset import BATCH_FEATURE_VIEWS
from fraudstream_ml.features import AVAILABILITY_FLAGS, prepare_features

from fraudstream_api.inference.schemas import Transaction


@dataclass(frozen=True)
class OnlineFeatures:
    """What the online store holds for one customer and one merchant."""

    values: dict[str, Any]
    event_times: dict[str, datetime | None]


def usable_features(
    online: OnlineFeatures, event_time: datetime, ttls: dict[str, timedelta]
) -> dict[str, Any]:
    """Drop every view that training would not have used for this payment.

    Training only joined a value that became true before the payment and
    within the view's TTL. The online store keeps whatever it last received,
    however old, so the same rule is applied here.
    """

    usable = dict(online.values)
    for view, names in BATCH_FEATURE_VIEWS.items():
        became_true = online.event_times.get(view)
        fresh = became_true is not None and timedelta(0) <= event_time - became_true <= ttls[view]
        if not fresh:
            for name in names:
                usable[name] = None
    return usable


def model_inputs(
    transaction: Transaction, features: dict[str, Any]
) -> dict[str, float | bool | None]:
    """Return the model's 51 named inputs, prepared exactly as in training."""

    row = {
        "transaction_id": transaction.transaction_id,
        "customer_id": transaction.customer_id,
        "merchant_id": transaction.merchant_id,
        "event_timestamp": pd.Timestamp(transaction.event_timestamp),
        "amount": transaction.amount,
        "channel": transaction.channel,
        "city": transaction.city,
        **features,
    }
    prepared, names = prepare_features(pd.DataFrame([row]))
    first = prepared.iloc[0]
    return {name: plain(first[name]) for name in names}


def history_found(inputs: dict[str, float | bool | None]) -> dict[str, bool]:
    """Say which of the three feature views had usable history."""

    return {flag: bool(inputs[flag]) for flag in AVAILABILITY_FLAGS.values()}


def plain(value: Any) -> float | bool | None:
    """Turn a pandas or NumPy value into something JSON can carry."""

    if value is None or pd.isna(value):
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    return float(value)
