"""The model gives the same score every time, for any rows Hypothesis invents.

A score that moved between calls, between loads, or with the order or number of
rows sent would be a silent wrong answer, not a visible failure.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings, strategies as st

from fraudstream_serving.predictor import FraudPredictor

from tests.test_predictor import COLUMNS, _register_model

VALUES = st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False)
ROWS = st.lists(st.lists(VALUES, min_size=3, max_size=3), min_size=1, max_size=8)


@pytest.fixture(scope="module")
def registered(tmp_path_factory):
    tracking_uri = f"sqlite:///{tmp_path_factory.mktemp('mlflow') / 'mlflow.db'}"
    return tracking_uri, _register_model(tracking_uri)


@pytest.fixture(scope="module")
def predictor(registered):
    tracking_uri, model_uri = registered
    one = FraudPredictor(name="test-fraud", model_uri=model_uri, tracking_uri=tracking_uri)
    one.load()
    return one


@pytest.fixture(scope="module")
def reloaded(registered):
    """A second predictor, loaded separately from the same registry."""

    tracking_uri, model_uri = registered
    other = FraudPredictor(name="test-fraud", model_uri=model_uri, tracking_uri=tracking_uri)
    other.load()
    return other


def scores(model: FraudPredictor, rows: list[list[float]]) -> list[float]:
    instances = [dict(zip(COLUMNS, row)) for row in rows]
    return model.predict({"instances": instances})["predictions"]


@settings(max_examples=50, deadline=None)
@given(rows=ROWS)
def test_the_same_rows_score_the_same_twice(predictor, rows):
    assert scores(predictor, rows) == scores(predictor, rows)


@settings(max_examples=50, deadline=None)
@given(rows=ROWS)
def test_a_separately_loaded_model_agrees(predictor, reloaded, rows):
    assert scores(predictor, rows) == scores(reloaded, rows)


@settings(max_examples=50, deadline=None)
@given(rows=ROWS, order=st.permutations(COLUMNS))
def test_the_order_of_the_fields_does_not_matter(predictor, rows, order):
    instances = [dict(zip(COLUMNS, row)) for row in rows]
    shuffled = [{name: row[name] for name in order} for row in instances]

    assert predictor.predict({"instances": shuffled})["predictions"] == scores(predictor, rows)


@settings(max_examples=50, deadline=None)
@given(rows=ROWS)
def test_scoring_rows_together_equals_scoring_them_alone(predictor, rows):
    together = scores(predictor, rows)
    alone = [scores(predictor, [row])[0] for row in rows]

    assert together == alone
