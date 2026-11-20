import numpy as np
import pandas as pd
import pytest

from fraudstream_monitoring.model import ServedModel
from fraudstream_monitoring.performance import COLUMNS, scored

TIMES = pd.to_datetime(["2026-06-01", "2026-06-02", "2026-06-03"], utc=True)


def fixed_scores(scores, threshold=0.626, feature_names=("amount", "event_hour"), seen=None):
    def predict_proba(inputs):
        if seen is not None:
            seen.append(inputs)
        return np.asarray(scores)

    return ServedModel(version="4", feature_names=list(feature_names), threshold=threshold, predict_proba=predict_proba)


def payments(**extra):
    return pd.DataFrame({"event_timestamp": TIMES, "amount": [10.0, 20.0, 30.0], "event_hour": [1, 2, 3], **extra})


def test_flagged_follows_the_threshold():
    result = scored(payments(), fixed_scores([0.2, 0.626, 0.9]))

    assert result["flagged"].tolist() == [0, 1, 1]
    assert result["score"].tolist() == [0.2, 0.626, 0.9]


def test_scoring_uses_the_model_s_own_inputs():
    seen = []
    frame = payments(device_customer_count_30d=[1.0, 300.0, 2.0])
    model = fixed_scores([0.1, 0.2, 0.3], feature_names=("event_hour", "amount"), seen=seen)

    scored(frame, model)

    assert list(seen[0].columns) == ["event_hour", "amount"]


def test_an_input_the_model_needs_but_the_rows_lack_is_named():
    with pytest.raises(ValueError, match="device_customer_count_30d"):
        scored(payments(), fixed_scores([0.1, 0.2, 0.3], feature_names=("amount", "device_customer_count_30d")))


def test_labels_pass_through_when_present():
    result = scored(payments(is_fraud=[0, 1, 0]), fixed_scores([0.1, 0.9, 0.3]))

    assert list(result.columns) == COLUMNS
    assert result["is_fraud"].tolist() == [0, 1, 0]
    assert result["event_timestamp"].tolist() == TIMES.tolist()


def test_no_label_column_without_labels():
    result = scored(payments(), fixed_scores([0.1, 0.9, 0.3]))

    assert list(result.columns) == COLUMNS[:-1]


def test_a_version_without_a_threshold_is_refused():
    with pytest.raises(ValueError, match="threshold"):
        scored(payments(), fixed_scores([0.1, 0.9, 0.3], threshold=None))


def run_reference(tmp_path, monkeypatch, model):
    from fraudstream_api.drift_detection import snapshot
    from fraudstream_ml.dataset import BATCH_FEATURE_VIEWS

    from fraudstream_monitoring import model as model_module
    from fraudstream_monitoring.performance import reference_main

    rows = payments(is_fraud=[0, 1, 0], channel=["online"] * 3, city=["Boston"] * 3)
    # Like an older snapshot: every view but the device one, which came later.
    for view, names in BATCH_FEATURE_VIEWS.items():
        if view != "device_ring_features":
            for name in names:
                rows[name] = None
    asked = {}

    def fake_rows(*args, **kwargs):
        asked["rows"] = args
        return rows

    def fake_model(uri, tracking_uri):
        asked["model"] = uri
        return model

    monkeypatch.setattr(snapshot, "snapshot_rows", fake_rows)
    monkeypatch.setattr(model_module, "load_model", fake_model)
    out = tmp_path / "reference.parquet"

    reference_main(["--snapshot", "42", "--start", "2026-05-30", "--end", "2026-06-30",
                    "--model-version", "4", "--tracking-uri", "http://mlflow", "--out", str(out)])
    return asked, out


def test_the_reference_is_the_version_s_test_window_scored_with_labels(tmp_path, monkeypatch):
    asked, out = run_reference(tmp_path, monkeypatch, fixed_scores([0.1, 0.9, 0.7]))

    saved = pd.read_parquet(out)
    assert asked == {"rows": ("42", "2026-05-30", "2026-06-30"), "model": "models:/fraud-detection/4"}
    assert list(saved.columns) == COLUMNS
    assert saved[["flagged", "is_fraud"]].values.tolist() == [[0, 0], [1, 1], [1, 0]]


def test_a_version_needing_a_column_the_snapshot_lacks_is_refused(tmp_path, monkeypatch):
    model = fixed_scores([0.1, 0.9, 0.7], feature_names=("amount", "device_customer_count_30d"))

    with pytest.raises(ValueError, match="device_customer_count_30d"):
        run_reference(tmp_path, monkeypatch, model)
