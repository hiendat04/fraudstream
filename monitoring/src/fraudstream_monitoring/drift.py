"""Check one window of the offline feature store against the served model's training data.

Nothing here reads a label. Nobody knows yet which of these payments were fraud,
so the check can only say whether the model's inputs still look like the rows it
learned from. It scores them with the drift detection API's own compare().

    python -m fraudstream_monitoring.drift --repo-path feature_store/feature_repo \\
        --reference api/reference/fraud-detection-v2.json --window-days 7 --out /tmp/drift.json
"""

import argparse
from datetime import date, timedelta
from pathlib import Path

from fraudstream_api.drift_detection.psi import bin_counts
from fraudstream_api.drift_detection.reference import Reference, compare
from fraudstream_ml.features import AVAILABILITY_FLAGS
from fraudstream_monitoring.summary import DriftSummary

# Below this, a PSI says more about chance than about drift. The API uses the same floor.
MIN_ROWS = 1000
SEVERITY = {"stable": 0, "warning": 1, "drift": 2}


def resolve_window(newest: date, window_end: str | None, days: int) -> tuple[date, date]:
    """The days to check: `days` days up to window_end, which is not included.

    With no window_end, the window ends the day after the newest payment. The
    stored data lives in the past, so the calendar date would find nothing.
    """

    if days < 1:
        raise ValueError(f"a window is at least one day, not {days}")
    end = date.fromisoformat(window_end) if window_end else newest + timedelta(days=1)
    return end - timedelta(days=days), end


def summarize(
    frame,
    reference: Reference,
    *,
    window_start: date,
    window_end: date,
    min_rows: int = MIN_ROWS,
) -> DriftSummary:
    """Score every model input of the window against the reference."""

    missing = [name for name in reference.features if name not in frame.columns]
    if missing:
        raise ValueError(f"the window has no column for these model inputs: {', '.join(missing)}")

    rows = len(frame)
    described = {
        "model_name": reference.model_name,
        "model_version": reference.model_version,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "rows": rows,
        "available": {
            flag: round(float(frame[flag].mean()), 4) if rows else 0.0
            for flag in AVAILABILITY_FLAGS.values()
        },
    }
    if rows < min_rows:
        return DriftSummary(**described, status="not_enough_data")

    counts = {
        name: bin_counts(frame[name].to_numpy(dtype=float), feature.edges)
        for name, feature in reference.features.items()
    }
    drifts = compare(reference, counts, rows)
    worst = max(drifts, key=lambda one: SEVERITY[one.status]).status
    return DriftSummary(
        **described,
        status=worst,
        features=[{"name": one.name, "psi": one.psi, "status": one.status} for one in drifts],
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--repo-path", required=True)
    parser.add_argument("--reference", required=True, help="the served model's training reference")
    parser.add_argument("--window-days", type=int, default=7)
    parser.add_argument("--window-end", default="", help="YYYY-MM-DD, not included; empty means the day after the newest payment")
    parser.add_argument("--out", required=True)
    arguments = parser.parse_args()

    import pandas as pd
    from feast import FeatureStore
    from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
        get_spark_session_or_start_new_with_repoconfig,
    )

    from fraudstream_ml.dataset import FACT_TABLE, load_training_frame
    from fraudstream_ml.features import prepare_features

    reference = Reference.from_json(Path(arguments.reference).read_text())
    store = FeatureStore(repo_path=arguments.repo_path)
    spark = get_spark_session_or_start_new_with_repoconfig(store.config.offline_store)
    newest = spark.sql(f"SELECT max(event_time) FROM {FACT_TABLE}").first()[0].date()
    start, end = resolve_window(newest, arguments.window_end, arguments.window_days)

    frame = load_training_frame(arguments.repo_path, start.isoformat(), end.isoformat(), labels=False)
    frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
    prepared, names = prepare_features(frame)
    summary = summarize(prepared[names].astype(float), reference, window_start=start, window_end=end)

    out = Path(arguments.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(summary.to_json() + "\n")
    print(
        f"{summary.rows:,} payments from {start} to {end}: {summary.status}, "
        f"{summary.count('drift')} inputs in drift, {summary.count('warning')} in warning"
    )


if __name__ == "__main__":
    main()
