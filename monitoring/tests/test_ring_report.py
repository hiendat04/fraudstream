import math
import sys
import types

import numpy as np
import pandas as pd

from fraudstream_monitoring.model import ServedModel, load_model
from fraudstream_monitoring.ring_report import CatchRate, catch_rates, report


def payments(rows):
    return pd.DataFrame(rows, columns=["device_id", "is_fraud"])


def test_catch_rate_counts_only_fraud():
    frame = payments([("dev_ring_0001", 1)] * 4 + [("dev_ring_0002", 0), ("dev_0000000001", 1), (None, 1)])
    scores = np.array([0.9, 0.1, 0.8, 0.2, 0.99, 0.7, 0.1])

    rates = catch_rates(frame, scores, threshold=0.5)

    assert rates == {"ring device": CatchRate(4, 0.5), "everywhere else": CatchRate(2, 0.5)}


def test_a_score_at_the_threshold_is_caught():
    rates = catch_rates(payments([("dev_ring_0001", 1)]), np.array([0.5]), threshold=0.5)
    assert rates["ring device"] == CatchRate(1, 1.0)


def test_a_frame_without_devices_puts_everything_elsewhere():
    frame = pd.DataFrame({"is_fraud": [1, 1, 0]})

    rates = catch_rates(frame, np.array([0.9, 0.1, 0.9]), threshold=0.5)

    assert rates["everywhere else"] == CatchRate(2, 0.5)
    assert rates["ring device"].payments == 0 and math.isnan(rates["ring device"].caught)


def test_each_version_scores_its_own_inputs_in_its_own_order():
    frame = pd.DataFrame({"device_id": ["dev_ring_0001", "dev_0000000001"], "is_fraud": [1, 1],
                          "a": [1.0, 0.0], "b": [0.0, 1.0]})
    seen = []

    def by_first_column(inputs):
        seen.append(list(inputs.columns))
        return inputs.iloc[:, 0].to_numpy()

    models = [ServedModel("4", ["a"], 0.5, by_first_column), ServedModel("5", ["b", "a"], 0.5, by_first_column)]

    table = report(frame, frame, models)

    assert seen == [["a"], ["b", "a"]]
    assert table.splitlines() == [
        "| version | threshold | ring device frauds | ring device caught | everywhere else frauds | everywhere else caught |",
        "|---|---|---|---|---|---|",
        "| 4 | 0.500 | 1 | 100.0% | 1 | 0.0% |",
        "| 5 | 0.500 | 1 | 0.0% | 1 | 100.0% |",
    ]


def test_load_model_takes_the_threshold_from_the_versions_run(monkeypatch):
    classifier = types.SimpleNamespace(
        get_booster=lambda: types.SimpleNamespace(feature_names=["a", "b"]),
        predict_proba=lambda inputs: np.array([[0.3, 0.7]] * len(inputs)),
    )
    client = types.SimpleNamespace(
        get_model_version=lambda name, version: types.SimpleNamespace(run_id=f"run-{name}-{version}"),
        get_run=lambda run_id: types.SimpleNamespace(data=types.SimpleNamespace(metrics={"threshold": 0.62})),
    )
    loaded = []
    mlflow = types.ModuleType("mlflow")
    mlflow.MlflowClient = lambda tracking_uri: client
    mlflow.set_tracking_uri = lambda uri: None
    mlflow.xgboost = types.SimpleNamespace(load_model=lambda uri: loaded.append(uri) or classifier)
    monkeypatch.setitem(sys.modules, "mlflow", mlflow)

    model = load_model("models:/fraud-detection/5", "http://mlflow")

    assert (model.version, model.feature_names, model.threshold) == ("5", ["a", "b"], 0.62)
    assert loaded == ["models:/fraud-detection/5"]
    assert model.predict_proba(pd.DataFrame({"a": [1], "b": [2]})).tolist() == [0.7]


def test_load_model_resolves_an_alias_to_its_version(monkeypatch):
    classifier = types.SimpleNamespace(get_booster=lambda: types.SimpleNamespace(feature_names=["a"]))
    client = types.SimpleNamespace(
        get_model_version_by_alias=lambda name, alias: types.SimpleNamespace(version=4),
        get_model_version=lambda name, version: types.SimpleNamespace(run_id="run"),
        get_run=lambda run_id: types.SimpleNamespace(data=types.SimpleNamespace(metrics={"threshold": 0.63})),
    )
    loaded = []
    mlflow = types.ModuleType("mlflow")
    mlflow.MlflowClient = lambda tracking_uri: client
    mlflow.set_tracking_uri = lambda uri: None
    mlflow.xgboost = types.SimpleNamespace(load_model=lambda uri: loaded.append(uri) or classifier)
    monkeypatch.setitem(sys.modules, "mlflow", mlflow)

    model = load_model("models:/fraud-detection@production", "http://mlflow")

    assert (model.version, loaded) == ("4", ["models:/fraud-detection/4"])
