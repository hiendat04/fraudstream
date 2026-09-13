"""Chronological train/validation/test split of the labeled training frame."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
from pandas import Timestamp

TRAIN_END = "2026-05-05"
VALIDATION_END = "2026-05-30"

SPLIT_NAMES = ("train", "validation", "test")


def _boundary(value: str | datetime, timestamps: Any) -> Timestamp:
    """Match a boundary to the timestamp column's timezone awareness."""

    stamp = pd.Timestamp(value)
    column_tz = getattr(timestamps.dtype, "tz", None)
    if column_tz is not None and stamp.tz is None:
        return stamp.tz_localize(column_tz)
    if column_tz is None and stamp.tz is not None:
        return stamp.tz_convert("UTC").tz_localize(None)
    return stamp


def chronological_split(
    frame: Any,
    train_end: str | datetime = TRAIN_END,
    validation_end: str | datetime = VALIDATION_END,
    *,
    timestamp_column: str = "event_timestamp",
    label_column: str = "is_fraud",
    require_both_classes: bool = True,
) -> dict[str, Any]:
    """Split a labeled frame into train/validation/test by event time.

    Bounds are half-open: a row exactly on a boundary belongs to the later
    split. The split is by time rather than at random because the features
    are rolling aggregates of an entity's own history, so a randomly placed
    future row would let the model learn from behaviour that had not happened
    yet at the moment the earlier rows were scored.
    """

    timestamps = frame[timestamp_column]
    train_bound = _boundary(train_end, timestamps)
    validation_bound = _boundary(validation_end, timestamps)
    if validation_bound <= train_bound:
        raise ValueError(
            f"validation_end ({validation_bound}) must be after train_end ({train_bound})"
        )

    splits = {
        "train": frame[timestamps < train_bound].copy(),
        "validation": frame[(timestamps >= train_bound) & (timestamps < validation_bound)].copy(),
        "test": frame[timestamps >= validation_bound].copy(),
    }

    for name in SPLIT_NAMES:
        split = splits[name]
        if split.empty:
            raise ValueError(
                f"{name} split is empty for boundaries {train_bound} / {validation_bound}"
            )
        if require_both_classes and split[label_column].nunique() < 2:
            raise ValueError(f"{name} split carries a single class of {label_column}")
    return splits


def split_summary(splits: dict[str, Any], *, label_column: str = "is_fraud") -> Any:
    """Return per-split row counts, date ranges, and fraud rates for reporting."""

    return pd.DataFrame(
        [
            {
                "split": name,
                "rows": len(splits[name]),
                "start": splits[name]["event_timestamp"].min(),
                "end": splits[name]["event_timestamp"].max(),
                "fraud_rows": int(splits[name][label_column].sum()),
                "fraud_rate": float(splits[name][label_column].mean()),
            }
            for name in SPLIT_NAMES
        ]
    )
