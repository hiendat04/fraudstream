"""Judging an A/B split: when the candidate passes, fails or has too few calls to say, and reading the variants from Prometheus."""

from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from fraudstream_agents.ab_testing import Variant, Verdict, judge, read_variants
from tools import ab_testing_report


def variant(name="B", calls=100.0, failures=0.0, p95=10.0, passed=None, asked=None):
    return Variant(name, calls, failures, p95, golden_passed=passed, golden_asked=asked)


A = variant("A")


@pytest.mark.parametrize("calls, verdict", [(0, Verdict.INCONCLUSIVE), (9, Verdict.INCONCLUSIVE),
                                            (10, Verdict.PASS), (11, Verdict.PASS)])
def test_the_candidate_needs_min_calls_to_pass(calls, verdict):
    judgement = judge(A, variant(calls=calls), min_calls=10)
    assert judgement.verdict is verdict
    assert judgement.reasons == ([f"B answered {calls}, 10 needed"] if verdict is Verdict.INCONCLUSIVE else [])


@pytest.mark.parametrize("failures, verdict", [(2, Verdict.PASS), (3, Verdict.FAIL)])
def test_failing_more_than_2_points_more_often_fails(failures, verdict):
    assert judge(variant("A", failures=0), variant(failures=failures), min_calls=10).verdict is verdict


def test_failures_are_judged_on_any_traffic():
    judgement = judge(A, variant(calls=1, failures=1), min_calls=10)
    assert judgement.verdict is Verdict.FAIL
    assert judgement.reasons == ["B failed 100.0% of calls, A 0.0%"]


def test_a_baseline_with_no_calls_has_no_failures():
    assert judge(variant("A", calls=0, failures=0, p95=None), variant(), min_calls=10).verdict is Verdict.PASS


@pytest.mark.parametrize("unnamed, verdict", [(1, Verdict.PASS), (2, Verdict.FAIL)])
def test_failures_naming_no_arm_fail_above_1_percent(unnamed, verdict):
    judgement = judge(variant("A", calls=50), variant(calls=100 - 50 - unnamed), min_calls=10, unnamed_failures=unnamed)
    assert judgement.verdict is verdict
    if verdict is Verdict.FAIL:
        assert judgement.reasons == ["2 of 100 calls failed before reaching a variant"]


@pytest.mark.parametrize("p95, verdict", [(12.0, Verdict.PASS), (12.1, Verdict.FAIL), (None, Verdict.PASS)])
def test_a_p95_more_than_1_2_times_the_baselines_fails(p95, verdict):
    judgement = judge(A, variant(p95=p95), min_calls=10)
    assert judgement.verdict is verdict
    if verdict is Verdict.FAIL:
        assert judgement.reasons == ["B's p95 is 12.1 s, A's 10.0 s"]


def test_latency_is_judged_from_exactly_min_calls():
    assert judge(A, variant(calls=10, p95=60.0), min_calls=10).verdict is Verdict.FAIL


def test_a_slow_candidate_with_too_few_calls_is_inconclusive():
    assert judge(A, variant(calls=9, p95=60.0), min_calls=10).verdict is Verdict.INCONCLUSIVE


@pytest.mark.parametrize("passed, asked, verdict", [(2, 2, Verdict.PASS), (1, 2, Verdict.FAIL), (None, None, Verdict.PASS)])
def test_passing_fewer_golden_questions_fails(passed, asked, verdict):
    judgement = judge(variant("A", passed=2, asked=2), variant(passed=passed, asked=asked), min_calls=10)
    assert judgement.verdict is verdict
    if verdict is Verdict.FAIL:
        assert judgement.reasons == ["B passed 1/2 golden questions, A 2/2"]


def test_every_reason_is_given():
    judgement = judge(variant("A", passed=2, asked=2), variant(failures=50, p95=60.0, passed=0, asked=2), min_calls=10)
    assert judgement.verdict is Verdict.FAIL
    assert len(judgement.reasons) == 3


def prometheus(answers: dict[str, list]):
    """A Prometheus whose query endpoint answers each expression containing a key with that key's series."""

    def handler(request):
        expression = parse_qs(urlparse(str(request.url)).query)["query"][0]
        result = next((series for key, series in answers.items() if key in expression), [])
        return httpx.Response(200, json={"status": "success", "data": {"resultType": "vector", "result": result}})

    return httpx.Client(base_url="http://prometheus", transport=httpx.MockTransport(handler))


def series(value, **labels):
    return {"metric": labels, "value": [0, str(value)]}


def test_the_llm_arms_are_read_per_backend():
    client = prometheus({
        'status!~"2.."': [series(1, backend="llm/qwen3-1-7b"), series(3, backend="unknown")],
        "agentgateway_requests_total": [series(80, backend="llm/qwen3-4b"), series(20, backend="llm/qwen3-1-7b"),
                                        series(3, backend="unknown")],
        "request_duration_bucket": [series(2.5, gen_ai_response_model="qwen3-4b"), series(1.5, gen_ai_response_model="qwen3-1.7b")],
    })
    baseline, candidate, unnamed = read_variants(client, "llm", "30m")
    assert baseline == Variant("qwen3-4b", 80, 0, 2.5)
    assert candidate == Variant("qwen3-1.7b", 20, 1, 1.5)
    assert unnamed == 3


def test_the_agent_arms_carry_their_golden_results():
    client = prometheus({
        "STATUS_CODE_ERROR": [series(1, gen_ai_agent_name="risk_agent_b")],
        "agent_calls_total": [series(12, gen_ai_agent_name="risk_agent"), series(11, gen_ai_agent_name="risk_agent_b")],
        "agent_duration_seconds_bucket": [series(90, gen_ai_agent_name="risk_agent"), series("NaN", gen_ai_agent_name="risk_agent_b")],
        "agent_golden_questions": [series(2, agent="risk-agent", outcome="passed"), series(0, agent="risk-agent", outcome="failed"),
                                   series(1, agent="risk-agent-b", outcome="passed"), series(1, agent="risk-agent-b", outcome="failed")],
        "push_time_seconds": [series(1000, agent="risk-agent"), series(2000, agent="risk-agent-b")],
    })
    baseline, candidate, unnamed = read_variants(client, "agent", "2h")
    assert baseline == Variant("risk_agent", 12, 0, 90, 2, 2, 1000)
    assert candidate == Variant("risk_agent_b", 11, 1, None, 1, 2, 2000)
    assert unnamed == 0


def test_a_golden_set_missing_an_outcome_is_no_golden_set():
    client = prometheus({"agent_golden_questions": [series(2, agent="risk-agent", outcome="passed")]})
    baseline, _, _ = read_variants(client, "agent", "30m")
    assert (baseline.golden_passed, baseline.golden_asked) == (None, None)


def test_no_traffic_is_no_calls_and_no_p95():
    baseline, candidate, unnamed = read_variants(prometheus({}), "agent", "30m")
    assert baseline == Variant("risk_agent", 0, 0, None)
    assert candidate == Variant("risk_agent_b", 0, 0, None)
    assert unnamed == 0


def test_the_window_is_in_every_rate():
    seen = []

    def handler(request):
        seen.append(parse_qs(urlparse(str(request.url)).query)["query"][0])
        return httpx.Response(200, json={"status": "success", "data": {"result": []}})

    read_variants(httpx.Client(base_url="http://p", transport=httpx.MockTransport(handler)), "llm", "45m")
    assert seen and all("[45m]" in expression for expression in seen)


def test_an_unknown_kind_is_refused():
    with pytest.raises(ValueError, match="unknown kind"):
        read_variants(prometheus({}), "model", "30m")


@pytest.mark.parametrize("verdict, code", [(Verdict.PASS, 0), (Verdict.FAIL, 10), (Verdict.INCONCLUSIVE, 12)])
def test_the_report_exits_by_verdict(monkeypatch, capsys, verdict, code):
    golden = Variant("risk_agent", 12, 0, 90.0, 2, 2, 0.0)
    monkeypatch.setattr(ab_testing_report, "read_variants", lambda client, kind, window: (golden, golden, 0.0))
    monkeypatch.setattr(ab_testing_report, "judge", lambda *args, **kwargs: ab_testing_report.Judgement(verdict, ["why"]))
    assert ab_testing_report.main(["agent", "--prometheus", "http://p"]) == code
    out = capsys.readouterr().out
    assert "risk_agent" in out and "2/2" in out and verdict.value in out and "why" in out


def test_the_report_prints_no_p95_without_traffic(monkeypatch, capsys):
    empty = Variant("qwen3-1.7b", 0, 0, None)
    monkeypatch.setattr(ab_testing_report, "read_variants", lambda client, kind, window: (empty, empty, 0.0))
    assert ab_testing_report.main(["llm", "--prometheus", "http://p"]) == 12
    assert "-" in capsys.readouterr().out
