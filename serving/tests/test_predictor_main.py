"""Tests for the container's entry point, with KServe's server mocked.

ModelServer is replaced where the predictor module looks it up, and loading is
replaced too, so neither a port nor a registry is needed.
"""

from unittest.mock import MagicMock

import pytest

from fraudstream_serving import predictor


@pytest.fixture
def server(monkeypatch):
    """The mocked ModelServer class, and every predictor main() loads."""

    loaded = []
    monkeypatch.setattr(predictor.FraudPredictor, "load", lambda self: loaded.append(self))
    server_class = MagicMock()
    monkeypatch.setattr(predictor, "ModelServer", server_class)
    server_class.loaded = loaded
    return server_class


@pytest.fixture
def environment(monkeypatch):
    monkeypatch.setenv("MODEL_URI", "models:/fraud-detection/2")
    monkeypatch.setenv("MODEL_NAME", "fraud-detection-test")
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")


def test_main_reads_the_model_address_from_the_environment(server, environment):
    predictor.main()

    [loaded] = server.loaded
    assert loaded.name == "fraud-detection-test"
    assert loaded.model_uri == "models:/fraud-detection/2"
    assert loaded.tracking_uri == "http://mlflow:5000"


def test_main_starts_the_server_with_the_loaded_predictor(server, environment):
    predictor.main()

    server.return_value.start.assert_called_once_with(server.loaded)


def test_the_model_name_and_tracking_uri_have_defaults(server, monkeypatch):
    monkeypatch.setenv("MODEL_URI", "models:/fraud-detection/2")
    monkeypatch.delenv("MODEL_NAME", raising=False)
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)

    predictor.main()

    [loaded] = server.loaded
    assert loaded.name == "fraud-detection"
    assert loaded.tracking_uri is None


def test_main_refuses_to_start_without_a_model_uri(server, monkeypatch):
    monkeypatch.delenv("MODEL_URI", raising=False)

    with pytest.raises(KeyError, match="MODEL_URI"):
        predictor.main()

    server.return_value.start.assert_not_called()
