"""What the inference API measures beyond requests: the model, the online store, the scores."""

import logging

from opentelemetry import trace
from prometheus_client import Counter, Gauge, Histogram

from fraudstream_api.inference.model import UNKNOWN_VERSION, ModelAnswer
from fraudstream_api.metrics import LATENCY_BUCKETS

log = logging.getLogger(__name__)

MODEL_DURATION = Histogram(
    "fraud_model_request_duration_seconds",
    "Time the model took to answer, failures included, by the version that answered.",
    ["model_version"],
    buckets=LATENCY_BUCKETS,
)
MODEL_FAILURES = Counter("fraud_model_failures_total", "Model calls that failed, by reason.", ["reason"])
ONLINE_STORE_FAILURES = Counter(
    "fraud_online_store_failures_total", "Payments refused because the online store did not answer."
)
SCORES = Histogram(
    "fraud_score",
    "Fraud probability given to each scored payment, by the version that gave it.",
    ["model_version"],
    buckets=tuple(round(0.05 * step, 2) for step in range(1, 20)),
)
FLAGGED = Counter(
    "fraud_flagged_total",
    "Scored payments at or above the answering version's own threshold.",
    ["model_version"],
)
THRESHOLD = Gauge(
    "fraud_model_threshold",
    "Decision threshold the answering version was trained with.",
    ["model_version"],
)
HISTORY_MISSING = Counter(
    "fraud_history_missing_total",
    "Scored payments with no usable history in one feature view.",
    ["flag"],
)


def model_failed(reason: str, error: Exception, seconds: float) -> None:
    """Count, time and log one failed model call. A failure has no version."""

    MODEL_DURATION.labels(UNKNOWN_VERSION).observe(seconds)
    MODEL_FAILURES.labels(reason).inc()
    log.warning("model call failed (%s): %s", reason, error)


def scored(answer: ModelAnswer, seconds: float, found: dict[str, bool]) -> None:
    """Record one answered payment: who answered, how fast, the score, and missing history."""

    MODEL_DURATION.labels(answer.version).observe(seconds)
    SCORES.labels(answer.version).observe(answer.probability)
    trace.get_current_span().set_attribute("model.version", answer.version)
    if answer.threshold is not None:
        THRESHOLD.labels(answer.version).set(answer.threshold)
        if answer.probability >= answer.threshold:
            FLAGGED.labels(answer.version).inc()
    for flag, present in found.items():
        if not present:
            HISTORY_MISSING.labels(flag).inc()
