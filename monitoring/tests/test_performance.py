import sys
from datetime import date

import numpy as np
import pandas as pd
import pytest
from fraudstream_ml.dataset import BATCH_FEATURE_VIEWS

from fraudstream_monitoring.model import ServedModel
from fraudstream_monitoring.performance import COLUMNS, scored

TIMES = pd.to_datetime(["2026-06-01", "2026-06-02", "2026-06-03"], utc=True)


def fixed_scores(scores, threshold=0.626, feature_names=("amount", "event_hour"), seen=None, version="4"):
    def predict_proba(inputs):
        if seen is not None:
            seen.append(inputs)
        return np.asarray(scores)

    return ServedModel(version=version, feature_names=list(feature_names), threshold=threshold, predict_proba=predict_proba)


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


THRESHOLD = 0.5
WEEK = 20_000


def weeks(count, start, seed, *, squash=1.0, fraud_shift=1.0, rows=None):
    """Payments at a 1.6% fraud rate whose scores separate fraud from the rest, like the model's."""

    rng = np.random.default_rng(seed)
    total = rows or WEEK * count
    is_fraud = (rng.random(total) < 0.016).astype(int)
    raw = np.where(is_fraud == 1, rng.beta(5, 2, total) * fraud_shift, rng.beta(1, 8, total))
    score = raw**squash
    times = pd.Timestamp(start, tz="UTC") + pd.to_timedelta(np.linspace(0, 7 * count, total, endpoint=False), unit="D")
    return pd.DataFrame({"event_timestamp": times, "score": score,
                         "flagged": (score >= THRESHOLD ** squash).astype(int), "is_fraud": is_fraud})


def realized_recall(frame):
    frauds = frame[frame["is_fraud"] == 1]
    return frauds["flagged"].mean()


def estimated(reference, analysis):
    from fraudstream_monitoring.performance import estimate

    return estimate(reference, analysis.drop(columns="is_fraud"), model_version="4",
                    window_start="2026-07-06", window_end="2026-07-13")


def test_the_estimate_is_near_the_known_recall():
    reference, analysis = weeks(4, "2026-05-04", 1), weeks(1, "2026-07-06", 2)

    summary = estimated(reference, analysis)

    recall = summary.metrics["recall"]
    assert summary.status == "ok"
    assert abs(recall.value - realized_recall(analysis)) < 0.05
    assert recall.lower <= realized_recall(analysis) <= recall.upper
    assert set(summary.metrics) == {"recall", "precision", "roc_auc"}


def test_uncalibrated_scores_are_still_estimated():
    reference, analysis = weeks(4, "2026-05-04", 1, squash=0.3), weeks(1, "2026-07-06", 2, squash=0.3)

    summary = estimated(reference, analysis)

    assert abs(summary.metrics["recall"].value - realized_recall(analysis)) < 0.05


def test_shifted_scores_raise_an_alert():
    reference, analysis = weeks(4, "2026-05-04", 1), weeks(1, "2026-07-06", 2, fraud_shift=0.4)

    summary = estimated(reference, analysis)

    assert summary.status == "alert"
    assert any(one.alert for one in summary.metrics.values())


def test_too_few_frauds_is_not_enough_data():
    summary = estimated(weeks(4, "2026-05-04", 1), weeks(1, "2026-07-06", 2, rows=500))

    assert (summary.status, summary.metrics, summary.rows) == ("not_enough_data", {}, 500)


def test_the_summary_survives_the_file_between_tasks():
    from fraudstream_monitoring.performance import PerformanceSummary

    summary = estimated(weeks(4, "2026-05-04", 1), weeks(1, "2026-07-06", 2))

    assert PerformanceSummary.from_json(summary.to_json()) == summary


def test_metrics_carry_their_bands():
    from prometheus_client import generate_latest

    from fraudstream_monitoring.performance import Estimate, PerformanceSummary, performance_metrics

    summary = PerformanceSummary(
        model_name="fraud-detection", model_version="4", window_start="2026-07-06", window_end="2026-07-13",
        rows=20_000, status="alert", metrics={"recall": Estimate(value=0.8, lower=0.75, upper=0.85, alert=True)},
    )

    text = generate_latest(performance_metrics(summary)).decode()

    labels = 'metric="recall",model_name="fraud-detection",model_version="4"'
    for line in (f"fraud_estimated_performance{{{labels}}} 0.8", f"fraud_estimated_performance_lower{{{labels}}} 0.75",
                 f"fraud_estimated_performance_upper{{{labels}}} 0.85", f"fraud_estimated_performance_alert{{{labels}}} 1.0",
                 'fraud_estimated_performance_status{model_name="fraud-detection",model_version="4"} 1.0'):
        assert line in text, line


def test_not_enough_data_publishes_only_the_status():
    from prometheus_client import generate_latest

    from fraudstream_monitoring.performance import PerformanceSummary, performance_metrics

    summary = PerformanceSummary(model_name="fraud-detection", model_version="4", window_start="2026-07-06",
                                 window_end="2026-07-13", rows=500, status="not_enough_data")

    text = generate_latest(performance_metrics(summary)).decode()

    assert 'fraud_estimated_performance_status{model_name="fraud-detection",model_version="4"} -1.0' in text
    assert "fraud_estimated_performance{" not in text


def run_estimate(tmp_path, monkeypatch, *, version="4", window=None, reference=True):
    """estimate_main with MLflow, the newest-date lookup and Feast faked."""

    import types

    from fraudstream_ml import dataset

    from fraudstream_monitoring import drift
    from fraudstream_monitoring import model as model_module
    from fraudstream_monitoring.performance import estimate_main

    asked = {}
    errors = types.ModuleType("feast.errors")
    errors.EntitySQLEmptyResults = type("EntitySQLEmptyResults", (Exception,), {})
    monkeypatch.setitem(sys.modules, "feast", types.ModuleType("feast"))
    monkeypatch.setitem(sys.modules, "feast.errors", errors)

    def fake_model(uri, tracking_uri):
        asked["model"] = uri
        return fixed_scores([0.9] * 3, version=version)

    def fake_frame(repo_path, start, end, **kwargs):
        asked["frame"] = (start, end, kwargs)
        if window is None:
            raise errors.EntitySQLEmptyResults()
        return window

    monkeypatch.setattr(model_module, "load_model", fake_model)
    monkeypatch.setattr(drift, "newest_payment_date", lambda repo_path: date(2026, 7, 12))
    monkeypatch.setattr(dataset, "load_training_frame", fake_frame)
    if reference:
        weeks(4, "2026-05-04", 1).to_parquet(tmp_path / f"fraud-detection-v{version}-performance.parquet")
    out = tmp_path / "summary.json"
    estimate_main(["--repo-path", "repo", "--reference-dir", str(tmp_path), "--window-days", "7",
                   "--tracking-uri", "http://mlflow", "--out", str(out)])
    return asked, out


def test_a_missing_reference_names_the_file(tmp_path, monkeypatch):
    with pytest.raises(FileNotFoundError, match="fraud-detection-v5-performance.parquet"):
        run_estimate(tmp_path, monkeypatch, version="5", reference=False)


def test_the_daily_estimate_reads_no_labels(tmp_path, monkeypatch):
    from fraudstream_monitoring.performance import PerformanceSummary

    asked, out = run_estimate(tmp_path, monkeypatch)

    summary = PerformanceSummary.from_json(out.read_text())
    assert asked["model"] == "models:/fraud-detection@production"
    assert asked["frame"] == ("2026-07-06", "2026-07-13", {"labels": False, "sample_percent": 2})
    assert (summary.model_version, summary.status, summary.rows) == ("4", "not_enough_data", 0)


def test_the_daily_estimate_scores_the_window_with_the_production_model(tmp_path, monkeypatch):
    from fraudstream_monitoring.performance import PerformanceSummary

    rows = payments(channel=["online"] * 3, city=["Boston"] * 3)
    for name in (name for names in BATCH_FEATURE_VIEWS.values() for name in names):
        rows[name] = None

    _, out = run_estimate(tmp_path, monkeypatch, window=rows)

    summary = PerformanceSummary.from_json(out.read_text())
    assert (summary.window_start, summary.window_end, summary.rows) == ("2026-07-06", "2026-07-13", 3)
