"""Shared test settings: async tests run on asyncio, and Hypothesis has a quick and a thorough profile."""

import os

import pytest
from hypothesis import settings as hypothesis_settings

hypothesis_settings.register_profile("dev", max_examples=100, deadline=None)
hypothesis_settings.register_profile("thorough", max_examples=1000, deadline=None, derandomize=True)
hypothesis_settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))


@pytest.fixture
def anyio_backend():
    return "asyncio"
