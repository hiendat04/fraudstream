from fraudstream_monitoring.rollout_analysis import (
    Thresholds,
    VersionStats,
    Verdict,
    WindowMetrics,
    judge_health,
    judge_step,
)

T = Thresholds()


def window(*, answers=1000, p95=0.010, median=0.30, flag=0.05, base_p95=0.010, base_median=0.30, base_flag=0.05, failures=0):
    return WindowMetrics(
        candidate=VersionStats(answers, p95, median, flag),
        baseline=VersionStats(answers * 4, base_p95, base_median, base_flag),
        failures=failures,
    )


def test_a_candidate_like_the_served_version_passes():
    assert judge_step(window(), T).verdict is Verdict.PASS


def test_too_many_failures_fail():
    judged = judge_step(window(failures=100), T)
    assert judged.verdict is Verdict.FAIL
    assert "failures" in judged.reasons[0]


def test_a_slower_candidate_fails():
    assert judge_step(window(p95=0.200, base_p95=0.010), T).verdict is Verdict.FAIL


def test_latency_within_the_slack_passes():
    assert judge_step(window(p95=0.055, base_p95=0.010), T).verdict is Verdict.PASS


def test_too_few_answers_are_inconclusive():
    judged = judge_step(window(answers=40), T)
    assert judged.verdict is Verdict.INCONCLUSIVE


def test_latency_is_not_judged_on_too_few_answers():
    assert judge_health(window(answers=40, p95=0.500), T).verdict is Verdict.PASS


def test_failures_are_judged_on_any_traffic():
    assert judge_health(window(answers=10, failures=5), T).verdict is Verdict.FAIL


def test_a_different_flag_rate_holds_and_never_fails():
    judged = judge_step(window(flag=0.12, base_flag=0.05), T)
    assert judged.verdict is Verdict.HOLD
    assert "flag rate" in judged.reasons[0]


def test_a_shifted_median_score_holds():
    assert judge_step(window(median=0.55, base_median=0.30), T).verdict is Verdict.HOLD


def test_a_missing_flag_rate_is_not_judged():
    assert judge_step(window(flag=None), T).verdict is Verdict.PASS
