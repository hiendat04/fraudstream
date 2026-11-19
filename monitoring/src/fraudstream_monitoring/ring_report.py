"""How much fraud each model version catches on fraud-ring devices, and everywhere else.

A ring device is one many customers pay from; the generator names them dev_ring_.
Run from the repo root, on the test window of the newest version's data:

    PYTHONPATH=monitoring/src:api/src:ml/src uv run --project monitoring --group scoring python -m fraudstream_monitoring.ring_report --snapshot <snapshot> --start <validation_end> --end <end_date> --model-version 4 --model-version 5 --tracking-uri http://localhost:18085
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

import numpy as np
import pandas as pd

from fraudstream_monitoring.model import ServedModel

RING_PREFIX = "dev_ring_"
GROUPS = ("ring device", "everywhere else")


@dataclass(frozen=True)
class CatchRate:
    payments: int  # fraud payments in the group
    caught: float  # the share of them scored at or above the threshold; NaN for none


def catch_rates(frame: pd.DataFrame, scores: np.ndarray, threshold: float) -> dict[str, CatchRate]:
    """The catch rate on ring devices and everywhere else. Only fraud rows count."""

    devices = frame["device_id"] if "device_id" in frame else pd.Series("", index=frame.index)
    ring = devices.fillna("").astype(str).str.startswith(RING_PREFIX).to_numpy()
    fraud = frame["is_fraud"].astype(int).to_numpy() == 1
    caught = np.asarray(scores) >= threshold
    rates = {}
    for group, members in zip(GROUPS, (ring, ~ring)):
        chosen = fraud & members
        count = int(chosen.sum())
        rates[group] = CatchRate(count, float(caught[chosen].mean()) if count else float("nan"))
    return rates


def report(rows: pd.DataFrame, inputs: pd.DataFrame, models: list[ServedModel]) -> str:
    """A Markdown table: one row per version, scored on its own inputs at its own threshold."""

    header = ["version", "threshold"] + [f"{group} {part}" for group in GROUPS for part in ("frauds", "caught")]
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for model in models:
        rates = catch_rates(rows, model.predict_proba(inputs[model.feature_names]), model.threshold)
        cells = [model.version, f"{model.threshold:.3f}"]
        cells += [cell for group in GROUPS for cell in (str(rates[group].payments), f"{rates[group].caught:.1%}")]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def main() -> None:
    from fraudstream_api.drift_detection.snapshot import snapshot_rows
    from fraudstream_ml.features import prepare_features

    from fraudstream_monitoring.model import load_model

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--snapshot", required=True, help="the Iceberg snapshot of iceberg.ml.training_data")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--model-version", action="append", required=True, dest="versions")
    parser.add_argument("--model-name", default="fraud-detection")
    parser.add_argument("--tracking-uri", required=True)
    arguments = parser.parse_args()

    rows = snapshot_rows(arguments.snapshot, arguments.start, arguments.end)
    prepared, names = prepare_features(rows)
    models = [load_model(f"models:/{arguments.model_name}/{version}", arguments.tracking_uri)
              for version in arguments.versions]
    print(f"{len(rows):,} payments from {arguments.start} to {arguments.end}\n")
    print(report(rows, prepared[names].astype(float), models))


if __name__ == "__main__":
    main()
