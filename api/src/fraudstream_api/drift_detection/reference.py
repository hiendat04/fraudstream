"""What the model's inputs looked like in training, and how far traffic has moved from it."""

import json
from dataclasses import dataclass

import numpy as np

from fraudstream_api.drift_detection.psi import bin_counts, bin_edges, psi, status


@dataclass(frozen=True)
class FeatureReference:
    """One input's bins, and the share of training rows that fell in each."""

    edges: np.ndarray
    proportions: np.ndarray


@dataclass(frozen=True)
class FeatureDrift:
    name: str
    psi: float
    status: str


@dataclass(frozen=True)
class Reference:
    """The training distribution of every model input, for one model version."""

    model_name: str
    model_version: str
    data_snapshot_id: str
    rows: int
    features: dict[str, FeatureReference]

    def to_json(self) -> str:
        return json.dumps(
            {
                "model_name": self.model_name,
                "model_version": self.model_version,
                "data_snapshot_id": self.data_snapshot_id,
                "rows": self.rows,
                "features": {
                    name: {
                        "edges": feature.edges.tolist(),
                        "proportions": feature.proportions.tolist(),
                    }
                    for name, feature in self.features.items()
                },
            },
            indent=1,
        )

    @classmethod
    def from_json(cls, text: str) -> "Reference":
        raw = json.loads(text)
        features = {
            name: FeatureReference(
                np.array(feature["edges"], dtype=float),
                np.array(feature["proportions"], dtype=float),
            )
            for name, feature in raw["features"].items()
        }
        return cls(
            raw["model_name"], raw["model_version"], raw["data_snapshot_id"], raw["rows"], features
        )


def build_reference(
    columns: dict[str, np.ndarray], *, model_name: str, model_version: str, data_snapshot_id: str
) -> Reference:
    """Bin every input of the training rows. Column order is the model's input order."""

    rows = len(next(iter(columns.values())))
    features = {}
    for name, values in columns.items():
        edges = bin_edges(values)
        features[name] = FeatureReference(edges, bin_counts(values, edges) / rows)
    return Reference(model_name, model_version, data_snapshot_id, rows, features)


def compare(
    reference: Reference, counts: dict[str, np.ndarray], rows: int
) -> list[FeatureDrift]:
    """Score every input against the reference, largest shift first.

    The API and every offline check call this, so their numbers cannot differ.
    """

    drifts = []
    for name, expected in reference.features.items():
        value = psi(expected.proportions, counts[name] / rows)
        drifts.append(FeatureDrift(name, round(value, 4), status(value)))
    return sorted(drifts, key=lambda drift: drift.psi, reverse=True)
