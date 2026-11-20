"""One registered version of the fraud detection model, loaded to score rows offline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class ServedModel:
    version: str
    feature_names: list[str]
    threshold: float | None
    # The fraud probability of each row of a frame holding feature_names.
    predict_proba: Callable


def load_model(model_uri: str, tracking_uri: str) -> ServedModel:
    """A models:/<name>/<version> or models:/<name>@<alias> model, with the threshold training logged on its run."""

    # This MLflow server fails every multipart download through its proxy.
    os.environ.setdefault("MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD", "false")
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    client = mlflow.MlflowClient(tracking_uri)
    path = model_uri.removeprefix("models:/")
    if "@" in path:
        name, alias = path.split("@", 1)
        version = str(client.get_model_version_by_alias(name, alias).version)
    else:
        name, version = path.rsplit("/", 1)
    # The xgboost loader gives back the classifier, which reports how likely fraud is.
    classifier = mlflow.xgboost.load_model(f"models:/{name}/{version}")
    run = client.get_run(client.get_model_version(name, version).run_id)
    return ServedModel(
        version=version,
        feature_names=list(classifier.get_booster().feature_names),
        threshold=run.data.metrics.get("threshold"),
        predict_proba=lambda inputs: classifier.predict_proba(inputs)[:, 1],
    )
