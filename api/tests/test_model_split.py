"""Tests for the check that the versions answering match the InferenceService's split."""

import json
from collections import Counter

from tools.model_split import deployed, problems, tally


def isvc(version="4", canary=None):
    predictor = {"containers": [{"env": [
        {"name": "MODEL_URI", "value": f"models:/fraud-detection/{version}"},
        {"name": "MLFLOW_TRACKING_URI", "value": "http://mlflow:5000"},
    ]}]}
    if canary is not None:
        predictor["canaryTrafficPercent"] = canary
    return {"spec": {"predictor": predictor}}


def answer(version):
    return json.dumps(
        {"transaction_id": "t", "fraud_probability": 0.1, "model_version": version, "history_found": {}}
    )


def test_the_manifest_gives_the_version_and_split():
    assert deployed(isvc("4", 20)) == ("4", 20)
    assert deployed(isvc("2")) == ("2", None)


def test_answers_are_counted_by_version_and_failures_apart():
    seen, unanswered = tally(
        [answer("2"), answer("4"), answer("2"), "", '{"detail": "the model did not answer in time"}']
    )
    assert seen == Counter({"2": 2, "4": 1})
    assert unanswered == 2


def test_a_healthy_canary_passes():
    assert problems("4", 20, Counter({"2": 24, "4": 6}), 0) == []


def test_a_silent_canary_is_a_problem():
    found = problems("4", 20, Counter({"2": 30}), 0)
    assert any("answered none" in line for line in found)


def test_a_canary_with_one_version_is_a_problem():
    found = problems("4", 20, Counter({"4": 30}), 0)
    assert any("left in after a promotion" in line for line in found)


def test_unanswered_payments_are_a_problem():
    assert any("no answer" in line for line in problems("4", 20, Counter({"2": 20, "4": 5}), 5))


def test_a_promoted_version_answers_alone():
    assert problems("4", None, Counter({"4": 30}), 0) == []
    assert problems("4", 100, Counter({"4": 30}), 0) == []
    assert problems("4", None, Counter({"4": 28, "2": 2}), 0) != []


def test_a_rollback_that_still_answers_is_a_problem():
    assert problems("4", 0, Counter({"2": 30}), 0) == []
    assert any("rollback" in line for line in problems("4", 0, Counter({"2": 25, "4": 5}), 0))


def test_nothing_answered_is_a_problem():
    assert problems("4", None, Counter(), 30) != []
