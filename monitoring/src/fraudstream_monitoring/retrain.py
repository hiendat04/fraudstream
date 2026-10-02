"""Start the training pipeline when the drift check found drift.

It starts the newest version CI registered, trained on a window that ends where
the checked window ends. The new model is registered in MLflow like any other.
Deciding to serve it stays with a person.

    python -m fraudstream_monitoring.retrain --summary /tmp/drift.json [--force]
Needs PIPELINES_URL, PUSHGATEWAY_URL, GATEWAY_USER, GATEWAY_PASSWORD and GATEWAY_CA.
Exits 99 when there is nothing to do or a retrain must wait. Airflow shows that as skipped.
"""

import argparse
import os
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from prometheus_client import CollectorRegistry, Gauge

from fraudstream_monitoring.gateway import gateway_client
from fraudstream_monitoring.kubeflow import Pipelines
from fraudstream_monitoring.publish import push
from fraudstream_monitoring.summary import DriftSummary, should_retrain

PIPELINE = "fraud-training"
EXPERIMENT = "fraud-model-training"
RUN_PREFIX = "drift-retrain"
# Drift that lasts is found again by every daily check. One retrain a week is enough.
COOLDOWN = timedelta(days=7)
ACTIVE = frozenset({"PENDING", "RUNNING", "PAUSED", "CANCELING"})
SKIPPED = 99


class MustWait(Exception):
    """A run is still going, or a drift retrain started less than a week ago."""


def training_window(end: date) -> dict[str, str]:
    """The pipeline's dates, slid to end with the checked window.

    The lengths match the first training run: 180 days in all, the last 56 for
    validation and test.
    """

    return {
        "start_date": (end - timedelta(days=180)).isoformat(),
        "train_end": (end - timedelta(days=56)).isoformat(),
        "validation_end": (end - timedelta(days=31)).isoformat(),
        "end_date": end.isoformat(),
    }


def reason_to_wait(runs: list[dict], *, now: datetime) -> str | None:
    for run in runs:
        state = run.get("state", "")
        if state in ACTIVE:
            return f"run {run['run_id']} ({run.get('display_name', '')}) is still {state.lower()}"
        if run.get("display_name", "").startswith(RUN_PREFIX):
            started = datetime.fromisoformat(run["created_at"])
            if now - started < COOLDOWN:
                return f"{run['display_name']} started {started:%Y-%m-%d %H:%M} UTC, less than {COOLDOWN.days} days ago"
    return None


def retrain(pipelines: Pipelines, summary: DriftSummary, *, now: datetime) -> str:
    experiment = pipelines.experiment_id(EXPERIMENT)
    reason = reason_to_wait(pipelines.recent_runs(experiment), now=now)
    if reason:
        raise MustWait(reason)
    pipeline = pipelines.pipeline_id(PIPELINE)
    version = pipelines.newest_version_id(pipeline)
    return pipelines.start_run(
        name=f"{RUN_PREFIX}-{summary.window_end}",
        experiment_id=experiment,
        pipeline_id=pipeline,
        version_id=version,
        parameters={**training_window(date.fromisoformat(summary.window_end)), "num_nodes": 2},
    )


def retrain_metrics(summary: DriftSummary, now: datetime) -> CollectorRegistry:
    registry = CollectorRegistry()
    Gauge(
        "fraud_retrain_started_timestamp_seconds",
        "When the drift pipeline last started a retrain, as a Unix time.",
        ["model_name", "model_version"],
        registry=registry,
    ).labels(summary.model_name, summary.model_version).set(now.timestamp())
    return registry


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--summary", required=True)
    parser.add_argument("--force", action="store_true", help="start a run even without drift")
    arguments = parser.parse_args()

    summary = DriftSummary.from_json(Path(arguments.summary).read_text())
    if not should_retrain(summary, force=arguments.force):
        print(f"status {summary.status}: nothing to retrain")
        return SKIPPED

    now = datetime.now(UTC)
    with gateway_client(os.environ["PIPELINES_URL"]) as client:
        try:
            run_id = retrain(Pipelines(client), summary, now=now)
        except MustWait as wait:
            print(f"the retrain waits: {wait}")
            return SKIPPED
    with gateway_client(os.environ["PUSHGATEWAY_URL"]) as client:
        push(client, retrain_metrics(summary, now), job="fraud_retrain", grouping={})
    print(f"started run {run_id} ({RUN_PREFIX}-{summary.window_end})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
