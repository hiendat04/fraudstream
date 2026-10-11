from fraudstream_monitoring.rollout import HEALTH_WINDOW_SECONDS, Actions, Policy, TakenOver, Unhealthy, remaining, run
from fraudstream_monitoring.rollout_analysis import Thresholds, VersionStats, WindowMetrics

POLICY = Policy(mode="progressive", steps=[20, 40, 60, 80], step_seconds=600, check_seconds=60, thresholds=Thresholds())


def healthy(answers=1000):
    return WindowMetrics(VersionStats(answers, 0.010, 0.30, 0.05), VersionStats(answers * 4, 0.010, 0.30, 0.05), 0)


class Script:
    """Records what the loop does. measure() answers from a list, then repeats the last answer."""

    def __init__(self, windows, take_over_at=None, unhealthy_at=None):
        self.windows = list(windows)
        self.done = []
        self.take_over_at = take_over_at
        self.unhealthy_at = unhealthy_at

    def actions(self) -> Actions:
        def set_percent(p):
            if p == self.take_over_at:
                raise TakenOver("the manifest changed under the rollout")
            self.done.append(p)
            if p == self.unhealthy_at:
                raise Unhealthy("the split check failed")

        def measure(_seconds):
            return self.windows.pop(0) if len(self.windows) > 1 else self.windows[0]

        return Actions(
            set_percent=set_percent,
            promote=lambda: self.done.append("promote"),
            roll_back=lambda: self.done.append("roll back"),
            measure=measure,
            sleep=lambda _s: None,
            log=lambda _m: None,
        )


def test_the_steps_left_after_the_current_share():
    assert remaining([20, 40, 60, 80], 20) == [40, 60, 80]
    assert remaining([20, 40, 60, 80], 80) == []


def test_a_healthy_canary_walks_every_step_then_is_promoted():
    script = Script([healthy()])
    assert run(POLICY, 20, script.actions()) == "promoted"
    assert script.done == [40, 60, 80, "promote"]


def test_a_failure_mid_step_rolls_back_at_once():
    failing = WindowMetrics(VersionStats(50, 0.010, 0.30, 0.05), VersionStats(200, 0.010, 0.30, 0.05), 50)
    script = Script([healthy(), healthy(), failing])
    assert run(POLICY, 20, script.actions()) == "rolled back"
    assert script.done == [40, "roll back"]


def test_a_step_slow_overall_rolls_back_even_when_each_minute_looked_fine():
    """Two-minute windows can be too thin to judge latency; the whole step is not."""

    thin = WindowMetrics(VersionStats(30, 0.300, 0.30, 0.05), VersionStats(120, 0.010, 0.30, 0.05), 0)
    slow = WindowMetrics(VersionStats(1000, 0.300, 0.30, 0.05), VersionStats(4000, 0.010, 0.30, 0.05), 0)
    script = Script([])
    actions = script.actions()
    actions = Actions(**{**actions.__dict__, "measure": lambda seconds: thin if seconds == HEALTH_WINDOW_SECONDS else slow})
    assert run(POLICY, 20, actions) == "rolled back"
    assert script.done == [40, "roll back"]


def test_a_difference_in_behaviour_holds_and_does_not_roll_back():
    different = WindowMetrics(VersionStats(1000, 0.010, 0.30, 0.15), VersionStats(4000, 0.010, 0.30, 0.05), 0)
    script = Script([different])
    assert run(POLICY, 20, script.actions()) == "held"
    assert script.done == [40]


def test_silence_never_promotes():
    script = Script([healthy(answers=0)])
    assert run(POLICY, 20, script.actions()) == "held"
    assert "promote" not in script.done


def test_too_little_traffic_gets_a_second_look():
    checks = 10 + 1
    script = Script([healthy(answers=10)] * checks + [healthy()])
    assert run(POLICY, 60, script.actions()) == "promoted"


def test_a_split_changed_by_someone_else_stops_the_rollout():
    script = Script([healthy()], take_over_at=60)
    assert run(POLICY, 20, script.actions()) == "stopped"
    assert script.done == [40]


def test_a_step_that_fails_its_split_check_rolls_back():
    """A step applied but answered wrongly must not be left holding more traffic."""

    script = Script([healthy()], unhealthy_at=60)
    assert run(POLICY, 20, script.actions()) == "rolled back"
    assert script.done == [40, 60, "roll back"]


def test_the_policy_reads_minutes_and_thresholds():
    policy = Policy.from_yaml("mode: progressive\nsteps: [40, 20]\nstep_minutes: 2\ncheck_minutes: 0.5\nthresholds:\n  min_answers: 5\n")
    assert (policy.steps, policy.step_seconds, policy.check_seconds) == ([20, 40], 120, 30)
    assert policy.thresholds.min_answers == 5
