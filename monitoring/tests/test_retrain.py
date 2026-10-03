import json
from datetime import UTC, date, datetime, timedelta

import httpx
import pytest

from fraudstream_monitoring.kubeflow import Pipelines
from fraudstream_monitoring.retrain import (
    COOLDOWN,
    MustWait,
    reason_to_wait,
    retrain,
    retrain_metrics,
    training_window,
)

from tests.test_summary import summary

NOW = datetime(2026, 9, 27, 3, 0, tzinfo=UTC)


def run(name, state, age, run_id="r1"):
    created = (NOW - age).isoformat().replace("+00:00", "Z")
    return {"run_id": run_id, "display_name": name, "state": state, "created_at": created}


class FakeKubeflow:
    """Answers the few v2beta1 calls the retrain step makes, and records what it was asked."""

    def __init__(self, *, pipelines=True, versions=True, experiment=True, runs=()):
        self.pipelines = pipelines
        self.versions = versions
        self.experiment = experiment
        self.runs = list(runs)
        self.created = []
        self.calls = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path, params = request.url.path, dict(request.url.params)
        self.calls.append((request.method, path, params))
        if request.method == "GET" and path == "/apis/v2beta1/pipelines":
            found = [{"pipeline_id": "p1", "display_name": "fraud-training"}] if self.pipelines else []
            return httpx.Response(200, json={"pipelines": found} if found else {})
        if request.method == "GET" and path == "/apis/v2beta1/pipelines/p1/versions":
            found = [{"pipeline_version_id": "v-newest"}] if self.versions else []
            return httpx.Response(200, json={"pipeline_versions": found} if found else {})
        if request.method == "GET" and path == "/apis/v2beta1/experiments":
            return httpx.Response(200, json={"experiments": [{"experiment_id": "e1"}]} if self.experiment else {})
        if request.method == "POST" and path == "/apis/v2beta1/experiments":
            self.experiment = True
            return httpx.Response(200, json={"experiment_id": "e-new"})
        if request.method == "GET" and path == "/apis/v2beta1/runs":
            return httpx.Response(200, json={"runs": self.runs} if self.runs else {})
        if request.method == "POST" and path == "/apis/v2beta1/runs":
            self.created.append(json.loads(request.content))
            return httpx.Response(200, json={"run_id": "new-run"})
        return httpx.Response(404)


def pipelines_for(fake: FakeKubeflow) -> Pipelines:
    return Pipelines(httpx.Client(base_url="https://pipelines.test", transport=httpx.MockTransport(fake)))


def test_the_window_matches_the_first_training_run():
    assert training_window(date(2026, 6, 30)) == {
        "start_date": "2026-01-01",
        "train_end": "2026-05-05",
        "validation_end": "2026-05-30",
        "end_date": "2026-06-30",
    }


def test_an_active_run_blocks_a_retrain():
    for state in ("PENDING", "RUNNING", "PAUSED", "CANCELING"):
        assert reason_to_wait([run("fraud-training nightly", state, timedelta(hours=1))], now=NOW)


def test_a_recent_drift_retrain_blocks_another():
    reason = reason_to_wait([run("drift-retrain-2026-06-30", "SUCCEEDED", timedelta(days=2))], now=NOW)
    assert "drift-retrain-2026-06-30" in reason


def test_a_failed_drift_retrain_still_counts():
    assert reason_to_wait([run("drift-retrain-2026-06-30", "FAILED", timedelta(days=2))], now=NOW)


def test_the_cooldown_ends_after_seven_days():
    assert COOLDOWN == timedelta(days=7)
    assert reason_to_wait([run("drift-retrain-2026-06-30", "SUCCEEDED", COOLDOWN)], now=NOW) is None
    assert reason_to_wait([run("drift-retrain-2026-06-30", "SUCCEEDED", COOLDOWN - timedelta(minutes=1))], now=NOW)


def test_other_finished_runs_never_block():
    assert reason_to_wait([run("fraud-training b3fabea", "SUCCEEDED", timedelta(hours=1))], now=NOW) is None
    assert reason_to_wait([], now=NOW) is None


def test_starts_the_newest_version_with_the_slid_window():
    fake = FakeKubeflow()
    run_id = retrain(pipelines_for(fake), summary("drift"), now=NOW)
    assert run_id == "new-run"
    body = fake.created[0]
    assert body["display_name"] == "drift-retrain-2026-06-30"
    assert body["experiment_id"] == "e1"
    assert body["pipeline_version_reference"] == {"pipeline_id": "p1", "pipeline_version_id": "v-newest"}
    assert body["runtime_config"]["parameters"] == {**training_window(date(2026, 6, 30)), "num_nodes": 2}


def test_asks_for_the_pipeline_and_experiment_by_name():
    fake = FakeKubeflow()
    retrain(pipelines_for(fake), summary("drift"), now=NOW)
    filters = [json.loads(params["filter"]) for method, _, params in fake.calls if "filter" in params]
    names = {predicate["stringValue"] for f in filters for predicate in f["predicates"]}
    assert names == {"fraud-training", "fraud-model-training"}
    assert all(p["key"] == "display_name" and p["operation"] == 1 for f in filters for p in f["predicates"])


def test_the_newest_version_is_asked_for():
    fake = FakeKubeflow()
    retrain(pipelines_for(fake), summary("drift"), now=NOW)
    versions = [params for _, path, params in fake.calls if path.endswith("/versions")][0]
    assert (versions["sort_by"], versions["page_size"]) == ("created_at desc", "1")


def test_creates_the_experiment_when_missing():
    fake = FakeKubeflow(experiment=False)
    retrain(pipelines_for(fake), summary("drift"), now=NOW)
    assert fake.created[0]["experiment_id"] == "e-new"


def test_waits_instead_of_starting_a_second_run():
    fake = FakeKubeflow(runs=[run("drift-retrain-2026-06-23", "RUNNING", timedelta(minutes=5))])
    with pytest.raises(MustWait, match="running"):
        retrain(pipelines_for(fake), summary("drift"), now=NOW)
    assert fake.created == []


def test_a_missing_pipeline_is_a_clear_error():
    with pytest.raises(LookupError, match="fraud-training"):
        retrain(pipelines_for(FakeKubeflow(pipelines=False)), summary("drift"), now=NOW)


def test_a_pipeline_without_versions_is_a_clear_error():
    fake = FakeKubeflow(versions=False)
    with pytest.raises(LookupError, match="no versions"):
        retrain(pipelines_for(fake), summary("drift"), now=NOW)
    assert fake.created == []


def test_the_start_time_is_published():
    from prometheus_client.exposition import generate_latest
    from prometheus_client.utils import floatToGoString

    body = generate_latest(retrain_metrics(summary("drift"), NOW)).decode()
    started = floatToGoString(NOW.timestamp())
    assert f'fraud_retrain_started_timestamp_seconds{{model_name="fraud-detection",model_version="2"}} {started}' in body
