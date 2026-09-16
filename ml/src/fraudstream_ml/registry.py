"""Save a trained model, its settings and its scores to MLflow.

Each run also records the Iceberg snapshot id of the data it used. That id lets
you go from a model version back to the exact rows it was trained on.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from fraudstream_ml.versioning import TRAINING_TABLE

EXPERIMENT = "fraud-model-training"
REGISTERED_MODEL = "fraud-detection"
CHAMPION_ALIAS = "champion"


@dataclass(frozen=True)
class RecordedRun:
    """The run id and model version that a training run produced."""

    run_id: str
    model_version: int


def log_training_run(
    model: Any,
    *,
    params: Mapping[str, Any],
    metrics: Mapping[str, float],
    data_snapshot_id: int,
    data_table: str = TRAINING_TABLE,
    experiment: str = EXPERIMENT,
    registered_model_name: str = REGISTERED_MODEL,
    tracking_uri: str | None = None,
    run_name: str | None = None,
) -> RecordedRun:
    """Save one training run and register the model it produced.

    Training only creates a new model version. It does not automatically make
    that model the production model. Someone must decide which version should
    be used for predictions.
    """

    import mlflow

    if tracking_uri is not None:
        mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)

    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_params(
            {
                **{name: value for name, value in params.items()},
                "data_snapshot_id": data_snapshot_id,
                "data_table": data_table,
            }
        )
        mlflow.log_metrics({name: float(value) for name, value in metrics.items()})

        info = mlflow.xgboost.log_model(
            model,
            name="model",
            registered_model_name=registered_model_name,
        )

    return RecordedRun(run_id=run.info.run_id, model_version=int(info.registered_model_version))


def promote(
    version: int,
    *,
    registered_model_name: str = REGISTERED_MODEL,
    alias: str = CHAMPION_ALIAS,
    tracking_uri: str | None = None,
) -> None:
    """Mark a model version as the one to use for predictions.

    Run this yourself after looking at the scores.
    """

    from mlflow import MlflowClient

    MlflowClient(tracking_uri=tracking_uri).set_registered_model_alias(
        registered_model_name, alias, str(version)
    )
