"""Save a trained model, its settings and its scores to MLflow.

Each run also records the Iceberg snapshot id of the data it used. That id lets
you go from a model version back to the exact rows it was trained on.

Two names point at versions. `models:/fraud-detection/latest` is MLflow's own pointer
to the newest version, so every new version is `latest` the moment it registers;
MLflow reserves the name, so nobody can set it by hand. A person starts a new
version as a canary. The `production` alias moves automatically only when a
version serves all traffic, so it always names the model actually answering.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from fraudstream_ml.versioning import TRAINING_TABLE

EXPERIMENT = "fraud-model-training"
REGISTERED_MODEL = "fraud-detection"
PRODUCTION_ALIAS = "production"


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
    """Save one training run and register the model it produced, which becomes `latest`.

    Training never moves `production`. A new version reaches traffic only through
    a canary, and becomes `production` when it serves everything.
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
    alias: str = PRODUCTION_ALIAS,
    tracking_uri: str | None = None,
) -> None:
    """Point an alias at a model version: `production` by default."""

    from mlflow import MlflowClient

    MlflowClient(tracking_uri=tracking_uri).set_registered_model_alias(
        registered_model_name, alias, str(version)
    )
