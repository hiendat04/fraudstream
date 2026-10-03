"""The one decision the drift DAG makes in Python: is there anything to retrain?"""

from pathlib import Path

from fraudstream_monitoring.summary import DriftSummary, should_retrain


def drift_found(summary_path: str, params: dict) -> bool:
    summary = DriftSummary.from_json(Path(summary_path).read_text())
    return should_retrain(summary, force=bool(params["force_retrain"]))
