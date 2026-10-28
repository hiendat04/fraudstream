"""Judging an A/B split from Prometheus: per variant, its calls, its failures, its p95 and its golden set.

The LLM split's variants are the versions behind fraud-detection-llm, counted per backend
at the LLM gateway for the benchmark's calls. The risk agent's variants are the two agents,
counted from their own spans, with the golden-set results check_agent pushes.
"""

import math
from dataclasses import dataclass
from enum import Enum

import httpx

MIN_CALLS = {"llm": 20, "agent": 10}
MAX_FAILURE_GAP = 0.02
MAX_UNNAMED_FAILURES = 0.01
LATENCY_RATIO = 1.2
SPLITS = {"llm": ("qwen3-4b", "qwen3-1.7b"), "agent": ("risk_agent", "risk_agent_b")}
LLM_BACKENDS = {"llm/qwen3-4b": "qwen3-4b", "llm/qwen3-1-7b": "qwen3-1.7b"}


@dataclass(frozen=True)
class Variant:
    name: str
    calls: float
    failures: float
    p95_seconds: float | None
    golden_passed: int | None = None
    golden_asked: int | None = None
    golden_pushed: float | None = None


class Verdict(str, Enum):
    PASS = "pass"
    INCONCLUSIVE = "inconclusive"
    FAIL = "fail"


@dataclass(frozen=True)
class Judgement:
    verdict: Verdict
    reasons: list[str]


def failure_rate(variant: Variant) -> float:
    return variant.failures / variant.calls if variant.calls else 0.0


def judge(baseline: Variant, candidate: Variant, *, min_calls: int, unnamed_failures: float = 0.0) -> Judgement:
    """Fail the candidate on failures and the golden set at any traffic, on p95 only once it has min_calls calls."""

    reasons = []
    if failure_rate(candidate) - failure_rate(baseline) > MAX_FAILURE_GAP:
        reasons.append(f"{candidate.name} failed {failure_rate(candidate):.1%} of calls, {baseline.name} {failure_rate(baseline):.1%}")
    total = baseline.calls + candidate.calls + unnamed_failures
    if total and unnamed_failures / total > MAX_UNNAMED_FAILURES:
        reasons.append(f"{unnamed_failures:.0f} of {total:.0f} calls failed before reaching a variant")
    if baseline.golden_asked and candidate.golden_asked and \
            candidate.golden_passed / candidate.golden_asked < baseline.golden_passed / baseline.golden_asked:
        reasons.append(f"{candidate.name} passed {candidate.golden_passed}/{candidate.golden_asked} golden questions, "
                       f"{baseline.name} {baseline.golden_passed}/{baseline.golden_asked}")
    if candidate.calls >= min_calls and baseline.p95_seconds is not None and candidate.p95_seconds is not None \
            and candidate.p95_seconds > LATENCY_RATIO * baseline.p95_seconds:
        reasons.append(f"{candidate.name}'s p95 is {candidate.p95_seconds:.1f} s, {baseline.name}'s {baseline.p95_seconds:.1f} s")
    if reasons:
        return Judgement(Verdict.FAIL, reasons)
    if candidate.calls < min_calls:
        return Judgement(Verdict.INCONCLUSIVE, [f"{candidate.name} answered {candidate.calls:.0f}, {min_calls} needed"])
    return Judgement(Verdict.PASS, [])


def series(client: httpx.Client, promql: str, *labels: str) -> dict[tuple[str, ...], float]:
    """An instant query's values by the labels named; NaN, as a quantile of no traffic is, is left out."""

    response = client.get("/api/v1/query", params={"query": promql})
    response.raise_for_status()
    values = {tuple(item["metric"].get(label, "") for label in labels): float(item["value"][1])
              for item in response.json()["data"]["result"]}
    return {key: value for key, value in values.items() if not math.isnan(value)}


def read_variants(client: httpx.Client, kind: str, window: str) -> tuple[Variant, Variant, float]:
    """The baseline and the candidate over the window, and the failures that reached neither."""

    if kind == "llm":
        requests = f'agentgateway_requests_total{{gateway="llm/llm-gateway", consumer="benchmark"'
        calls = series(client, f"sum by (backend) (increase({requests}}}[{window}]))", "backend")
        failed = series(client, f'sum by (backend) (increase({requests}, status!~"2.."}}[{window}]))', "backend")
        p95 = series(client, "histogram_quantile(0.95, sum by (le, gen_ai_response_model) (rate("
                             f'agentgateway_gen_ai_server_request_duration_bucket{{consumer="benchmark"}}[{window}])))',
                     "gen_ai_response_model")
        by_name = {name: (backend,) for backend, name in LLM_BACKENDS.items()}
        variants = [Variant(name, calls.get(by_name[name], 0.0), failed.get(by_name[name], 0.0), p95.get((name,)))
                for name in SPLITS["llm"]]
        unnamed = sum(value for (backend,), value in failed.items() if backend not in LLM_BACKENDS)
        return variants[0], variants[1], unnamed
    if kind == "agent":
        agents = 'gen_ai_agent_name=~"risk_agent|risk_agent_b"'
        calls = series(client, f"sum by (gen_ai_agent_name) (increase(agent_calls_total{{{agents}}}[{window}]))", "gen_ai_agent_name")
        failed = series(client, f'sum by (gen_ai_agent_name) (increase(agent_calls_total{{{agents}, status_code="STATUS_CODE_ERROR"}}[{window}]))',
                        "gen_ai_agent_name")
        p95 = series(client, "histogram_quantile(0.95, sum by (le, gen_ai_agent_name) (rate("
                             f"agent_duration_seconds_bucket{{{agents}}}[{window}])))", "gen_ai_agent_name")
        golden = series(client, 'sum by (agent, outcome) (agent_golden_questions{job="agent_golden"})', "agent", "outcome")
        pushed = series(client, 'max by (agent) (push_time_seconds{job="agent_golden"})', "agent")

        def agent_variant(name: str) -> Variant:
            # The golden set is pushed under the agent's Kubernetes name, its spans carry the name with underscores.
            agent = name.replace("_", "-")
            passed, failed_golden = golden.get((agent, "passed")), golden.get((agent, "failed"))
            asked = None if passed is None or failed_golden is None else int(passed + failed_golden)
            return Variant(name, calls.get((name,), 0.0), failed.get((name,), 0.0), p95.get((name,)),
                       None if asked is None else int(passed), asked, pushed.get((agent,)))

        return agent_variant(SPLITS["agent"][0]), agent_variant(SPLITS["agent"][1]), 0.0
    raise ValueError(f"unknown kind {kind!r}, not llm or agent")
