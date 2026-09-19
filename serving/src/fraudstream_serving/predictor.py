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


class FraudPredictor(Model):
    """Loads the model from the MLflow registry and scores requests with it."""

    def __init__(self, name: str, model_uri: str, tracking_uri: str | None = None):
        super().__init__(name)
        self.model_uri = model_uri
        self.tracking_uri = tracking_uri
        self.model: Any = None
        self.feature_names: list[str] = []

    def load(self) -> bool:
        """Fetch the model from the registry and remember the fields it expects."""

        import mlflow

        if self.tracking_uri:
            mlflow.set_tracking_uri(self.tracking_uri)

        # The xgboost loader gives back the classifier, which can report how
        # likely fraud is. The general loader only returns yes or no.
        self.model = mlflow.xgboost.load_model(self.model_uri)
        self.feature_names = list(self.model.get_booster().feature_names)
        self.ready = True
        return self.ready

    def predict(self, payload: dict, headers: dict[str, str] | None = None) -> dict:
        """Return how likely each payment is to be fraud, from 0 to 1."""

        import pandas as pd

        instances = payload["instances"]
        frame = pd.DataFrame(instances)

        missing = [name for name in self.feature_names if name not in frame.columns]
        if missing:
            # InvalidInput answers 400. A plain error would answer 500, which
            # tells the caller the server broke rather than the request.
            raise InvalidInput(f"these fields are missing from the request: {', '.join(missing)}")

        ordered = frame[self.feature_names]
        scores = self.model.predict_proba(ordered)[:, 1]
        return {"predictions": [float(score) for score in scores]}


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
