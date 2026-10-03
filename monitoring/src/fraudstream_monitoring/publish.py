"""Send one drift check's numbers to the PushGateway, where Prometheus collects them.

Each window is its own group, so a later check never hides an earlier one, and a
second check of the same window replaces the first.

    python -m fraudstream_monitoring.publish --summary /tmp/drift.json
Needs PUSHGATEWAY_URL, GATEWAY_USER and GATEWAY_PASSWORD, and GATEWAY_CA for the local CA.
"""

import argparse
import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

import httpx
from prometheus_client import CollectorRegistry, Gauge
from prometheus_client.exposition import CONTENT_TYPE_LATEST, generate_latest

from fraudstream_monitoring.gateway import gateway_client
from fraudstream_monitoring.summary import DriftSummary

JOB = "fraud_drift"
STATUS_CODES = {"not_enough_data": -1, "stable": 0, "warning": 1, "drift": 2}
MODEL_LABELS = ["model_name", "model_version"]


def drift_metrics(summary: DriftSummary) -> CollectorRegistry:
    registry = CollectorRegistry()
    model = [summary.model_name, summary.model_version]

    def gauge(name: str, text: str, value: float) -> None:
        Gauge(name, text, MODEL_LABELS, registry=registry).labels(*model).set(value)

    gauge("fraud_drift_status", "Worst input: -1 not enough data, 0 stable, 1 warning, 2 drift.", STATUS_CODES[summary.status])
    gauge("fraud_drift_rows", "Payments in the window.", summary.rows)
    gauge("fraud_drift_features_drifted", "Inputs with a PSI of 0.25 or more.", summary.count("drift"))
    gauge("fraud_drift_features_warning", "Inputs with a PSI from 0.10 to 0.25.", summary.count("warning"))
    gauge(
        "fraud_drift_window_end_timestamp_seconds",
        "End of the window, as a Unix time.",
        datetime.fromisoformat(summary.window_end).replace(tzinfo=UTC).timestamp(),
    )

    psi = Gauge("fraud_drift_feature_psi", "PSI of one input against the training data.", [*MODEL_LABELS, "feature"], registry=registry)
    for feature in summary.features:
        psi.labels(*model, feature["name"]).set(feature["psi"])

    history = Gauge(
        "fraud_drift_history_available_ratio",
        "Share of the window's payments with history in one feature view.",
        [*MODEL_LABELS, "flag"],
        registry=registry,
    )
    for flag, share in summary.available.items():
        history.labels(*model, flag).set(share)
    return registry


def group_path(job: str, grouping: dict[str, str]) -> str:
    parts = [f"/metrics/job/{quote(job, safe='')}"]
    parts += [f"/{quote(key, safe='')}/{quote(value, safe='')}" for key, value in grouping.items()]
    return "".join(parts)


def push(client: httpx.Client, registry: CollectorRegistry, *, job: str, grouping: dict[str, str]) -> None:
    """Replace everything the group holds with this registry's metrics."""

    answer = client.put(
        group_path(job, grouping),
        content=generate_latest(registry),
        headers={"Content-Type": CONTENT_TYPE_LATEST},
    )
    answer.raise_for_status()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--summary", required=True)
    arguments = parser.parse_args()

    summary = DriftSummary.from_json(Path(arguments.summary).read_text())
    with gateway_client(os.environ["PUSHGATEWAY_URL"]) as client:
        push(client, drift_metrics(summary), job=JOB, grouping={"window_end": summary.window_end})
    print(f"published {summary.status} for the window ending {summary.window_end}, {len(summary.features)} inputs")


if __name__ == "__main__":
    main()
