"""Serves the registered fraud model behind KServe.

KServe's own runtimes cannot serve this model on an Apple Silicon machine. The
MLflow runtime has no image for it, and the XGBoost runtime sends columns with
no names, which the model refuses because it remembers the 51 names it was
trained on.

This predictor keeps the names. Fields are matched by name and put back in the
model's own order, so a request that lists its fields in a different order still
gets the right score.
"""

from __future__ import annotations

import os
from typing import Any

from kserve import Model, ModelServer
from kserve.errors import InvalidInput

MODEL_NAME = "fraud-detection"
UNKNOWN_VERSION = "unknown"


def registry_reference(model_uri: str) -> tuple[str, str] | None:
    """The registered name and version (or "@alias") a models:/ URI points at."""

    if not model_uri.startswith("models:/"):
        return None
    path = model_uri.removeprefix("models:/")
    if "@" in path:
        name, alias = path.split("@", 1)
        return name, f"@{alias}"
    if "/" not in path:
        # models:/m-<id> names a logged model, not a registered version.
        return None
    name, version = path.rsplit("/", 1)
    return name, version


class FraudPredictor(Model):
    """Loads the model from the MLflow registry and scores requests with it."""

    def __init__(self, name: str, model_uri: str, tracking_uri: str | None = None):
        super().__init__(name)
        self.model_uri = model_uri
        self.tracking_uri = tracking_uri
        self.model: Any = None
        self.feature_names: list[str] = []
        self.version = UNKNOWN_VERSION
        self.threshold: float | None = None

    def load(self) -> bool:
        """Fetch the model from the registry and remember the fields it expects."""

        import mlflow

        if self.tracking_uri:
            mlflow.set_tracking_uri(self.tracking_uri)

        # The xgboost loader gives back the classifier, which can report how
        # likely fraud is. The general loader only returns yes or no.
        self.model = mlflow.xgboost.load_model(self.model_uri)
        self.feature_names = list(self.model.get_booster().feature_names)

        reference = registry_reference(self.model_uri)
        if reference is not None:
            # The threshold training chose is logged on the version's run. Each
            # version keeps its own, so its flag rate is measured the way it decides.
            client = mlflow.MlflowClient(self.tracking_uri)
            name, version = reference
            if version.startswith("@"):
                version = client.get_model_version_by_alias(name, version[1:]).version
            self.version = str(version)
            run_id = client.get_model_version(name, self.version).run_id
            self.threshold = client.get_run(run_id).data.metrics.get("threshold")

        self.ready = True
        return self.ready

    def predict(self, payload: dict, headers: dict[str, str] | None = None) -> dict:
        """Return how likely each payment is to be fraud, from 0 to 1, and who answered."""

        import pandas as pd

        instances = payload["instances"]
        frame = pd.DataFrame(instances)

        missing = [name for name in self.feature_names if name not in frame.columns]
        if missing:
            # InvalidInput answers 400. A plain error would answer 500, which
            # tells the caller the server broke rather than the request.
            raise InvalidInput(f"these fields are missing from the request: {', '.join(missing)}")

        # A payment without history sends nulls. pandas makes a column of only
        # nulls into text, which the model refuses; as numbers they are missing values.
        ordered = frame[self.feature_names].apply(pd.to_numeric)
        scores = self.model.predict_proba(ordered)[:, 1]
        return {
            "predictions": [float(score) for score in scores],
            "model_version": self.version,
            "threshold": self.threshold,
        }


def main() -> None:
    """Entry point for the container."""

    predictor = FraudPredictor(
        name=os.environ.get("MODEL_NAME", MODEL_NAME),
        model_uri=os.environ["MODEL_URI"],
        tracking_uri=os.environ.get("MLFLOW_TRACKING_URI"),
    )
    predictor.load()
    ModelServer().start([predictor])


if __name__ == "__main__":
    main()
