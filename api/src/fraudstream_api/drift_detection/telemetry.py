"""What the drift detection API measures beyond requests."""

from prometheus_client import Counter

ACCEPTED = Counter("drift_observations_accepted_total", "Model input rows added to the drift window.")
REJECTED = Counter(
    "drift_batches_rejected_total",
    "Batches refused because their fields did not match the model's inputs.",
)
