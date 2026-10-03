"""The result of one drift check, as the Airflow tasks hand it to each other."""

import json
from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class DriftSummary:
    model_name: str
    model_version: str
    window_start: str
    window_end: str
    rows: int
    status: str
    features: list[dict] = field(default_factory=list)
    available: dict[str, float] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=1)

    @classmethod
    def from_json(cls, text: str) -> "DriftSummary":
        return cls(**json.loads(text))

    def count(self, status: str) -> int:
        return sum(1 for feature in self.features if feature["status"] == status)


def should_retrain(summary: DriftSummary, *, force: bool = False) -> bool:
    """Retrain when at least one model input is in the drift band, or when asked to."""

    return force or summary.status == "drift"
