from datetime import date

import numpy as np
import pandas as pd
import pytest

from fraudstream_api.drift_detection.reference import build_reference
from fraudstream_monitoring.drift import resolve_window, summarize

FLAGS = ("customer_features_available", "customer_orders_90d_available", "merchant_features_available")
START, END = date(2026, 6, 23), date(2026, 6, 30)


def columns(rows: int, *, scale: float = 1.0, seed: int = 0) -> dict[str, np.ndarray]:
    rng = np.random.default_rng(seed)
    data = {"amount": rng.lognormal(3.5, 0.8, rows) * scale}
    for flag in FLAGS:
        data[flag] = (rng.random(rows) < 0.6).astype(float)
    return data


REFERENCE = build_reference(
    columns(20000, seed=1), model_name="fraud-detection", model_version="2", data_snapshot_id="1"
)


def window(rows: int, **kwargs) -> pd.DataFrame:
    return pd.DataFrame(columns(rows, **kwargs))


def check(frame: pd.DataFrame, **kwargs):
    return summarize(frame, REFERENCE, window_start=START, window_end=END, **kwargs)


def test_a_window_like_training_is_stable():
    result = check(window(5000))
    assert result.status == "stable"
    assert result.rows == 5000
    assert {one["name"] for one in result.features} == {"amount", *FLAGS}


def test_bigger_amounts_are_drift_and_come_first():
    result = check(window(5000, scale=1.6))
    assert result.status == "drift"
    assert result.features[0]["name"] == "amount"
    assert result.features[0]["psi"] >= 0.25


def test_the_numbers_are_the_apis_numbers():
    from fraudstream_api.drift_detection.psi import bin_counts
    from fraudstream_api.drift_detection.reference import compare

    frame = window(5000, scale=1.3)
    counts = {name: bin_counts(frame[name].to_numpy(), ref.edges) for name, ref in REFERENCE.features.items()}
    expected = {one.name: one.psi for one in compare(REFERENCE, counts, len(frame))}
    assert {one["name"]: one["psi"] for one in check(frame).features} == expected


def test_too_few_rows_are_not_judged():
    result = check(window(999))
    assert (result.status, result.rows, result.features) == ("not_enough_data", 999, [])


def test_the_floor_itself_is_judged():
    assert check(window(1000)).status != "not_enough_data"


def test_an_empty_window_reports_zero_rows():
    result = check(window(0))
    assert (result.status, result.rows) == ("not_enough_data", 0)
    assert result.available == {flag: 0.0 for flag in FLAGS}


def test_history_shares_are_reported():
    frame = window(2000)
    frame["customer_features_available"] = [1.0] * 500 + [0.0] * 1500
    assert check(frame).available["customer_features_available"] == 0.25


def test_a_missing_model_input_is_an_error():
    with pytest.raises(ValueError, match="amount"):
        check(window(2000).drop(columns=["amount"]))


def test_the_window_ends_the_day_after_the_newest_payment():
    assert resolve_window(date(2026, 6, 29), "", 7) == (date(2026, 6, 23), date(2026, 6, 30))
    assert resolve_window(date(2026, 6, 29), None, 7) == (date(2026, 6, 23), date(2026, 6, 30))


def test_a_given_end_is_used_as_is():
    assert resolve_window(date(2026, 6, 29), "2026-03-01", 7) == (date(2026, 2, 22), date(2026, 3, 1))


def test_a_window_is_at_least_one_day():
    assert resolve_window(date(2026, 6, 29), "", 1) == (date(2026, 6, 29), date(2026, 6, 30))
    with pytest.raises(ValueError):
        resolve_window(date(2026, 6, 29), "", 0)
