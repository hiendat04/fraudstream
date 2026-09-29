"""What the inference API measures beyond requests: the model, the online store, the scores."""

import logging

from prometheus_client import Counter, Histogram

from fraudstream_api.metrics import LATENCY_BUCKETS

log = logging.getLogger(__name__)

MODEL_DURATION = Histogram(
    "fraud_model_request_duration_seconds",
    "Time the model took to answer, failures included.",
    buckets=LATENCY_BUCKETS,
)
MODEL_FAILURES = Counter("fraud_model_failures_total", "Model calls that failed, by reason.", ["reason"])
ONLINE_STORE_FAILURES = Counter(
    "fraud_online_store_failures_total", "Payments refused because the online store did not answer."
)
SCORES = Histogram(
    "fraud_score",
    "Fraud probability given to each scored payment.",
    buckets=tuple(round(0.05 * step, 2) for step in range(1, 20)),
)
HISTORY_MISSING = Counter(
    "fraud_history_missing_total",
    "Scored payments with no usable history in one feature view.",
    ["flag"],
)


def model_failed(reason: str, error: Exception) -> None:
    """Count and log one failed model call."""

    MODEL_FAILURES.labels(reason).inc()
    log.warning("model call failed (%s): %s", reason, error)


def scored(probability: float, found: dict[str, bool]) -> None:
    """Record one answered payment: its score, and which history it lacked."""

    SCORES.observe(probability)
    for flag, present in found.items():
        if not present:
            HISTORY_MISSING.labels(flag).inc()
