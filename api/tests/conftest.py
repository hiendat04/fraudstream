"""Shared fixtures for the API tests.

Each fake stands in for something outside this code: Feast, KServe, Redis or
the drift detection API.
"""

import os

import fakeredis
import pytest
from fastapi.testclient import TestClient
from hypothesis import settings as hypothesis_settings

from fraudstream_api.drift_detection.app import create_app as create_drift_app
from fraudstream_api.drift_detection.settings import Settings as DriftSettings
from fraudstream_api.inference.app import create_app as create_inference_app
from fraudstream_api.inference.settings import Settings as InferenceSettings

from tests.test_drift_app import reference_json
from tests.test_inference_app import PAYMENT, FakeDrift, FakeModel, FakeReader

hypothesis_settings.register_profile("dev", max_examples=100, deadline=None)
hypothesis_settings.register_profile(
    "thorough", max_examples=1000, deadline=None, derandomize=True
)
hypothesis_settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))


@pytest.fixture
def payment() -> dict:
    return dict(PAYMENT)


@pytest.fixture
def inference_settings() -> InferenceSettings:
    return InferenceSettings(postgres_user="u", postgres_password="p", app_version="test")


@pytest.fixture
def fake_reader() -> FakeReader:
    return FakeReader()


@pytest.fixture
def fake_model() -> FakeModel:
    return FakeModel()


@pytest.fixture
def fake_drift() -> FakeDrift:
    return FakeDrift()


@pytest.fixture
def inference_client(inference_settings, fake_reader, fake_model, fake_drift):
    app = create_inference_app(
        inference_settings, reader=fake_reader, model=fake_model, drift=fake_drift
    )
    with TestClient(app) as client:
        yield client


@pytest.fixture
def drift_reference_path(tmp_path):
    path = tmp_path / "reference.json"
    path.write_text(reference_json())
    return path


@pytest.fixture
def fake_redis_server() -> fakeredis.FakeServer:
    return fakeredis.FakeServer()


@pytest.fixture
def drift_client(drift_reference_path, fake_redis_server):
    settings = DriftSettings(
        reference_path=str(drift_reference_path), min_observations=50, app_version="test"
    )
    redis = fakeredis.FakeAsyncRedis(server=fake_redis_server, decode_responses=True)
    with TestClient(create_drift_app(settings, redis=redis)) as client:
        yield client
