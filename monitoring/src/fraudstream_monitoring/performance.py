"""The fraud detection model's scores on a set of payments, the input to estimating its performance.

A reference is one version's own test window, scored by that version, with the labels.
Build it once per version, from the repo root:

    PYTHONPATH=monitoring/src:api/src:ml/src uv run --project monitoring --group scoring python -m fraudstream_monitoring.performance reference --snapshot <snapshot> --start <validation_end> --end <end_date> --model-version 4 --tracking-uri http://localhost:18085 --out api/reference/fraud-detection-v4-performance.parquet
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

import pandas as pd

from fraudstream_monitoring.model import ServedModel

COLUMNS = ["event_timestamp", "score", "flagged", "is_fraud"]


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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["reference"])
    _, rest = parser.parse_known_args()
    reference_main(rest)


if __name__ == "__main__":
    main()
