"""Tests for packaging a registered model so it can ship inside the serving image.

A small model is registered in a local SQLite MLflow store, fetched into a folder,
and loaded back from that folder with no tracking server configured, which is how
the container runs.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import mlflow
import pytest

from fraudstream_serving.fetch import CARD_FILE, fetch
from fraudstream_serving.predictor import FraudPredictor
from test_predictor import _register_model


@pytest.fixture
def registry(tmp_path):
    uri = f"sqlite:///{tmp_path / 'mlflow.db'}"
    _register_model(uri, threshold=0.7)
    _register_model(uri, threshold=0.65)
    yield uri
    mlflow.set_tracking_uri(None)


def test_latest_resolves_to_the_newest_version(registry, tmp_path):
    card = fetch("models:/test-fraud/latest", tmp_path / "model", tracking_uri=registry)

    assert card["version"] == "2"
    assert card["threshold"] == 0.65


def test_a_fresh_process_reads_only_the_given_registry(registry, tmp_path):
    """CI runs fetch as its own command; MLflow must not fall back to a local mlflow.db."""

    src = Path(__file__).parents[1] / "src"
    workdir = tmp_path / "ci"
    workdir.mkdir()
    subprocess.run(
        [sys.executable, "-m", "fraudstream_serving.fetch", "models:/test-fraud/1", "model",
         "--tracking-uri", registry],
        cwd=workdir, check=True,
        # set_tracking_uri exports MLFLOW_TRACKING_URI; CI has no such variable.
        env={**{k: v for k, v in os.environ.items() if not k.startswith("MLFLOW_")}, "PYTHONPATH": str(src)},
    )

    assert json.loads((workdir / "model" / CARD_FILE).read_text())["version"] == "1"
    assert not (workdir / "mlflow.db").exists()


def test_a_numbered_version_is_fetched_as_asked(registry, tmp_path):
    card = fetch("models:/test-fraud/1", tmp_path / "model", tracking_uri=registry)

    assert card["version"] == "1"
    assert card["threshold"] == 0.7


def test_the_card_is_written_next_to_the_model(registry, tmp_path):
    out = tmp_path / "model"
    card = fetch("models:/test-fraud/latest", out, tracking_uri=registry)

    assert (out / "MLmodel").is_file()
    assert json.loads((out / CARD_FILE).read_text()) == card
    assert card["name"] == "test-fraud"
    assert card["run_id"]


def test_the_packaged_model_serves_without_a_registry(registry, tmp_path):
    """The image has no MLflow to ask, so version and threshold come from the card."""

    out = tmp_path / "model"
    fetch("models:/test-fraud/1", out, tracking_uri=registry)
    mlflow.set_tracking_uri(f"sqlite:///{tmp_path / 'unused.db'}")

    predictor = FraudPredictor("fraud-detection", str(out))
    predictor.load()

    assert (predictor.version, predictor.threshold) == ("1", 0.7)
    row = {"amount": 0.9, "customer_txn_count_30d": 0.1, "event_hour": 0.5}
    assert predictor.predict({"instances": [row]})["model_version"] == "1"


def test_only_registered_models_can_be_packaged(tmp_path):
    with pytest.raises(ValueError, match="models:/"):
        fetch("runs:/abc/model", tmp_path / "model")



def test_the_command_line_prints_what_it_packaged(registry, tmp_path, monkeypatch, capsys):
    from fraudstream_serving import fetch as module

    out = tmp_path / "model"
    monkeypatch.setattr("sys.argv", ["fetch", "models:/test-fraud/latest", str(out), "--tracking-uri", registry])
    module.main()

    assert json.loads(capsys.readouterr().out) == {"name": "test-fraud", "version": "2", "threshold": 0.65}
    assert (out / CARD_FILE).is_file()
