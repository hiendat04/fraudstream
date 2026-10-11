"""Decide, from live metrics, whether a canary may take more traffic.

No ground truth exists while a canary runs, so health alone rolls back:
failures and model latency are not a matter of opinion. Behavior only
pauses: a model that flags more or scores differently is not shown to be
worse, so a person looks before it takes more traffic.
"""

from dataclasses import dataclass, field
from enum import Enum


class Verdict(str, Enum):
    PASS = "pass"
    INCONCLUSIVE = "inconclusive"
    HOLD = "hold"
    FAIL = "fail"


@dataclass(frozen=True)
class Thresholds:
    min_answers: int = 100
    max_failure_ratio: float = 0.01
    latency_ratio: float = 1.2
    latency_slack_seconds: float = 0.05
    max_flag_rate_gap: float = 0.02
    max_median_shift: float = 0.10


@dataclass(frozen=True)
class VersionStats:
    answers: float
    p95_seconds: float | None
    median_score: float | None
    flag_rate: float | None


@dataclass(frozen=True)
class WindowMetrics:
    candidate: VersionStats
    baseline: VersionStats
    failures: float


@dataclass(frozen=True)
class Judgement:
    verdict: Verdict
    reasons: list[str] = field(default_factory=list)


def judge_health(metrics: WindowMetrics, thresholds: Thresholds) -> Judgement:
    """Failures are judged on any traffic; latency only on enough of it to be stable."""

    reasons = []
    total = metrics.candidate.answers + metrics.baseline.answers + metrics.failures
    if total and metrics.failures / total > thresholds.max_failure_ratio:
        reasons.append(f"failures {metrics.failures / total:.2%} of payments, limit {thresholds.max_failure_ratio:.0%}")
    candidate, served = metrics.candidate.p95_seconds, metrics.baseline.p95_seconds
    if (
        metrics.candidate.answers >= thresholds.min_answers
        and candidate is not None
        and served is not None
        and candidate > served * thresholds.latency_ratio + thresholds.latency_slack_seconds
    ):
        reasons.append(f"candidate model p95 {candidate * 1000:.0f} ms against {served * 1000:.0f} ms")
    return Judgement(Verdict.FAIL, reasons) if reasons else Judgement(Verdict.PASS)


def judge_step(metrics: WindowMetrics, thresholds: Thresholds) -> Judgement:
    health = judge_health(metrics, thresholds)
    if health.verdict is Verdict.FAIL:
        return health
    if metrics.candidate.answers < thresholds.min_answers:
        return Judgement(
            Verdict.INCONCLUSIVE,
            [f"candidate answered {metrics.candidate.answers:.0f} payments, {thresholds.min_answers} needed"],
        )
    reasons = []
    flag, served_flag = metrics.candidate.flag_rate, metrics.baseline.flag_rate
    if flag is not None and served_flag is not None and abs(flag - served_flag) > thresholds.max_flag_rate_gap:
        reasons.append(f"flag rate {flag:.2%} against {served_flag:.2%}")
    median, served_median = metrics.candidate.median_score, metrics.baseline.median_score
    if median is not None and served_median is not None and abs(median - served_median) > thresholds.max_median_shift:
        reasons.append(f"median score {median:.3f} against {served_median:.3f}")
    return Judgement(Verdict.HOLD, reasons) if reasons else Judgement(Verdict.PASS)
