"""Estimate the fraud detection model's recall, precision and ROC AUC before any label exists.

Fraud is confirmed weeks late. NannyML's confidence-based performance estimation
(CBPE) learns, on a window whose labels are known, how the model's scores relate
to being right, then reads the newest window's scores alone.

A reference is one version's own test window, scored by that version, with the labels.
Build it once per version, from the repo root:

    PYTHONPATH=monitoring/src:api/src:ml/src uv run --project monitoring --group scoring python -m fraudstream_monitoring.performance reference --snapshot <snapshot> --start <validation_end> --end <end_date> --model-version 4 --tracking-uri http://localhost:18085 --out api/reference/fraud-detection-v4-performance.parquet

The daily estimate (`estimate`) and its publishing (`publish`) run in the drift DAG.
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pandas as pd
from prometheus_client import CollectorRegistry, Gauge

from fraudstream_monitoring.model import ServedModel

COLUMNS = ["event_timestamp", "score", "flagged", "is_fraud"]
METRICS = ["recall", "precision", "roc_auc"]
# Fewer expected frauds than this and the band is too wide to mean anything. The rollout uses the same floor.
MIN_EXPECTED_FRAUDS = 100
JOB = "fraud_detection_performance"
STATUS_CODES = {"not_enough_data": -1, "ok": 0, "alert": 1}
MODEL_LABELS = ["model_name", "model_version"]


@dataclass(frozen=True)
class Estimate:
    value: float
    lower: float
    upper: float
    # Outside the range the reference's own weeks moved in.
    alert: bool


@dataclass(frozen=True)
class PerformanceSummary:
    model_name: str
    model_version: str
    window_start: str
    window_end: str
    rows: int
    status: str
    metrics: dict[str, Estimate] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=1)

    @classmethod
    def from_json(cls, text: str) -> PerformanceSummary:
        values = json.loads(text)
        values["metrics"] = {name: Estimate(**one) for name, one in values["metrics"].items()}
        return cls(**values)


def scored(frame: pd.DataFrame, model: ServedModel) -> pd.DataFrame:
    """Each payment's time, score and flag, and its label when the frame has one.

    The model scores its own inputs in its own order, as the model server does, so a
    column it was not trained on never reaches it.
    """

    if model.threshold is None:
        raise ValueError(f"version {model.version} logged no threshold, so nothing can be flagged")
    missing = [name for name in model.feature_names if name not in frame.columns]
    if missing:
        raise ValueError(f"version {model.version} needs inputs these rows lack: {', '.join(missing)}")

    # A column of only nulls comes back as text; as numbers they are missing values.
    scores = model.predict_proba(frame[model.feature_names].apply(pd.to_numeric))
    result = pd.DataFrame({"event_timestamp": frame["event_timestamp"].to_numpy(), "score": scores})
    result["score"] = result["score"].astype(float)
    result["flagged"] = (result["score"] >= model.threshold).astype(int)
    if "is_fraud" in frame:
        result["is_fraud"] = frame["is_fraud"].astype(int).to_numpy()
    return result


def estimate(
    reference: pd.DataFrame,
    analysis: pd.DataFrame,
    *,
    model_version: str,
    window_start: str,
    window_end: str,
    model_name: str = "fraud-detection",
) -> PerformanceSummary:
    """The analysis window's estimated performance, fitted on the reference."""

    described = {"model_name": model_name, "model_version": model_version, "window_start": window_start,
                 "window_end": window_end, "rows": len(analysis)}
    if reference["is_fraud"].mean() * len(analysis) < MIN_EXPECTED_FRAUDS:
        return PerformanceSummary(**described, status="not_enough_data")

    os.environ.setdefault("NML_DISABLE_USAGE_LOGGING", "1")
    import nannyml

    # The window is one chunk whatever weekday it starts on; the reference is cut into
    # chunks of the same size, so the alert range matches what one window can vary by.
    estimator = nannyml.CBPE(
        problem_type="classification_binary", y_pred_proba="score", y_pred="flagged", y_true="is_fraud",
        timestamp_column_name="event_timestamp", metrics=METRICS, chunk_size=len(analysis),
    )
    estimator.fit(reference)
    newest = estimator.estimate(analysis).filter(period="analysis").to_df().iloc[-1]
    metrics = {
        name: Estimate(
            value=float(newest[(name, "value")]),
            lower=float(newest[(name, "lower_confidence_boundary")]),
            upper=float(newest[(name, "upper_confidence_boundary")]),
            alert=bool(newest[(name, "alert")]),
        )
        for name in METRICS
    }
    status = "alert" if any(one.alert for one in metrics.values()) else "ok"
    return PerformanceSummary(**described, status=status, metrics=metrics)


def performance_metrics(summary: PerformanceSummary) -> CollectorRegistry:
    registry = CollectorRegistry()
    model = [summary.model_name, summary.model_version]
    Gauge("fraud_estimated_performance_status", "-1 not enough data, 0 ok, 1 an estimate is out of its usual range.",
          MODEL_LABELS, registry=registry).labels(*model).set(STATUS_CODES[summary.status])

    gauges = {
        "value": Gauge("fraud_estimated_performance", "Estimated value of one metric.", [*MODEL_LABELS, "metric"], registry=registry),
        "lower": Gauge("fraud_estimated_performance_lower", "Lower edge of its confidence band.", [*MODEL_LABELS, "metric"], registry=registry),
        "upper": Gauge("fraud_estimated_performance_upper", "Upper edge of its confidence band.", [*MODEL_LABELS, "metric"], registry=registry),
        "alert": Gauge("fraud_estimated_performance_alert", "1 when the estimate left its usual range.", [*MODEL_LABELS, "metric"], registry=registry),
    }
    for name, one in summary.metrics.items():
        for part, gauge in gauges.items():
            gauge.labels(*model, name).set(float(getattr(one, part)))
    return registry


def reference_main(argv: Sequence[str] | None = None) -> None:
    """Score one version's test window with that version and save it with its labels."""

    from fraudstream_api.drift_detection.snapshot import snapshot_rows
    from fraudstream_ml.dataset import BATCH_FEATURE_VIEWS
    from fraudstream_ml.features import prepare_features

    from fraudstream_monitoring.model import load_model

    parser = argparse.ArgumentParser(prog="performance reference", description=reference_main.__doc__)
    parser.add_argument("--snapshot", required=True, help="the Iceberg snapshot of iceberg.ml.training_data")
    parser.add_argument("--start", required=True, help="the version's validation_end")
    parser.add_argument("--end", required=True, help="the version's end_date")
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--model-name", default="fraud-detection")
    parser.add_argument("--tracking-uri", required=True)
    parser.add_argument("--out", required=True)
    arguments = parser.parse_args(argv)

    rows = snapshot_rows(arguments.snapshot, arguments.start, arguments.end)
    model = load_model(f"models:/{arguments.model_name}/{arguments.model_version}", arguments.tracking_uri)
    # An older snapshot predates later feature views; nulls are fine as long as the model never reads them.
    absent = [name for names in BATCH_FEATURE_VIEWS.values()
                for name in names if name not in rows.columns]
    needed = [name for name in absent if name in model.feature_names]
    if needed:
        raise ValueError(f"version {model.version} needs columns snapshot {arguments.snapshot} lacks: {', '.join(needed)}")
    prepared, _ = prepare_features(rows.assign(**dict.fromkeys(absent)))
    reference = scored(prepared, model)
    reference.to_parquet(arguments.out, index=False)
    print(
        f"{len(reference):,} payments, fraud rate {reference['is_fraud'].mean():.2%}, "
        f"flagged {reference['flagged'].mean():.2%}, saved to {arguments.out}"
    )


def estimate_main(argv: Sequence[str] | None = None) -> None:
    """Estimate the production version's performance on the newest window, without a label."""

    from fraudstream_ml.dataset import load_training_frame
    from fraudstream_ml.features import prepare_features

    from fraudstream_monitoring.drift import SAMPLE_PERCENT, newest_payment_date, resolve_window
    from fraudstream_monitoring.model import load_model

    parser = argparse.ArgumentParser(prog="performance estimate", description=estimate_main.__doc__)
    parser.add_argument("--repo-path", required=True)
    parser.add_argument("--reference-dir", required=True, help="where each version's performance reference lives")
    parser.add_argument("--window-days", type=int, default=7)
    parser.add_argument("--window-end", default="", help="YYYY-MM-DD, not included; empty means the day after the newest payment")
    parser.add_argument("--model-name", default="fraud-detection")
    parser.add_argument("--tracking-uri", required=True)
    parser.add_argument("--sample-percent", type=int, default=SAMPLE_PERCENT, help="the share of the window's payments to score")
    parser.add_argument("--out", required=True)
    arguments = parser.parse_args(argv)

    from feast.errors import EntitySQLEmptyResults

    model = load_model(f"models:/{arguments.model_name}@production", arguments.tracking_uri)
    reference_path = Path(arguments.reference_dir) / f"{arguments.model_name}-v{model.version}-performance.parquet"
    if not reference_path.exists():
        raise FileNotFoundError(f"version {model.version} serves but has no performance reference at {reference_path}; build it with `performance reference`")
    reference = pd.read_parquet(reference_path)

    start, end = resolve_window(newest_payment_date(arguments.repo_path), arguments.window_end, arguments.window_days)
    try:
        frame = load_training_frame(
            arguments.repo_path, start.isoformat(), end.isoformat(), labels=False, sample_percent=arguments.sample_percent
        )
    except EntitySQLEmptyResults:
        analysis = pd.DataFrame(columns=COLUMNS[:-1])
    else:
        frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
        analysis = scored(prepare_features(frame)[0], model)

    summary = estimate(reference, analysis, model_version=model.version, window_start=start.isoformat(),
                       window_end=end.isoformat(), model_name=arguments.model_name)
    out = Path(arguments.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(summary.to_json() + "\n")
    shown = ", ".join(f"{name} {one.value:.3f} [{one.lower:.3f}, {one.upper:.3f}]" for name, one in summary.metrics.items())
    print(f"version {model.version}, {summary.rows:,} payments from {start} to {end}: {summary.status}. {shown}")


def publish_main(argv: Sequence[str] | None = None) -> None:
    """Send one estimate to the PushGateway. Needs PUSHGATEWAY_URL, GATEWAY_USER and GATEWAY_PASSWORD."""

    from fraudstream_monitoring.gateway import gateway_client
    from fraudstream_monitoring.publish import push

    parser = argparse.ArgumentParser(prog="performance publish", description=publish_main.__doc__)
    parser.add_argument("--summary", required=True)
    arguments = parser.parse_args(argv)

    summary = PerformanceSummary.from_json(Path(arguments.summary).read_text())
    # One group, replaced each day, so Prometheus keeps the estimate as a time series.
    with gateway_client(os.environ["PUSHGATEWAY_URL"]) as client:
        push(client, performance_metrics(summary), job=JOB, grouping={})
    print(f"published {summary.status} for version {summary.model_version}, window ending {summary.window_end}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["reference", "estimate", "publish"])
    command, rest = parser.parse_known_args()
    {"reference": reference_main, "estimate": estimate_main, "publish": publish_main}[command.command](rest)


if __name__ == "__main__":
    main()
