"""Walk a model canary through larger shares of traffic, checking live metrics at every step.

Health is checked every check interval and rolls back at once. At the end of a
step the whole step is judged: pass moves on, fail rolls back, hold stops for a
person, and too little traffic gets one more step-long look before holding.
Every change is a commit, so Git always says what the cluster does.

    cd monitoring && PYTHONPATH=src uv run python -m fraudstream_monitoring.rollout \\
        --policy ../k8s/models/rollout.yaml --manifest ../k8s/models/fraud-detection.yaml \\
        --prometheus-url http://localhost:19090
Exits 0 promoted, 10 rolled back, 11 stopped without changing the split, 12 held.
Python itself exits 1 on a crash and 2 on bad arguments, so those mean neither.
"""

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import yaml

from fraudstream_monitoring.rollout_analysis import Thresholds, Verdict, WindowMetrics, judge_health, judge_step
from fraudstream_monitoring.rollout_manifest import (
    ROLLOUT_PREFIX,
    promoted,
    rolled_back,
    should_start,
    split,
    with_canary,
)

PROMOTED, ROLLED_BACK, HELD, STOPPED = "promoted", "rolled back", "held", "stopped"
EXIT = {PROMOTED: 0, ROLLED_BACK: 10, STOPPED: 11, HELD: 12}
# Health looks at the last two minutes: long enough for Prometheus' 15 s scrapes.
HEALTH_WINDOW_SECONDS = 120


class TakenOver(Exception):
    """Someone else changed the split while the rollout ran. Their change wins."""


class Unhealthy(Exception):
    """A step was applied but the split check after it failed."""


@dataclass(frozen=True)
class Policy:
    mode: str
    steps: list[int]
    step_seconds: int
    check_seconds: int
    thresholds: Thresholds

    @classmethod
    def from_yaml(cls, text: str) -> "Policy":
        raw = yaml.safe_load(text)
        return cls(
            mode=raw.get("mode", "manual"),
            steps=sorted(int(step) for step in raw["steps"]),
            step_seconds=int(raw["step_minutes"] * 60),
            check_seconds=int(raw["check_minutes"] * 60),
            thresholds=Thresholds(**raw.get("thresholds", {})),
        )


@dataclass(frozen=True)
class Actions:
    set_percent: Callable[[int], None]
    promote: Callable[[], None]
    roll_back: Callable[[], None]
    measure: Callable[[int], WindowMetrics]
    sleep: Callable[[float], None]
    log: Callable[[str], None]


def remaining(steps: list[int], current: int) -> list[int]:
    return [step for step in steps if step > current]


def _watch(policy: Policy, act: Actions) -> str | None:
    """Watch one step. None means it passed."""

    for _look in range(2):
        waited = 0
        while waited < policy.step_seconds:
            act.sleep(policy.check_seconds)
            waited += policy.check_seconds
            health = judge_health(act.measure(HEALTH_WINDOW_SECONDS), policy.thresholds)
            if health.verdict is Verdict.FAIL:
                act.log("unhealthy: " + "; ".join(health.reasons))
                act.roll_back()
                return ROLLED_BACK
        step = judge_step(act.measure(policy.step_seconds), policy.thresholds)
        act.log(f"step: {step.verdict.value} {'; '.join(step.reasons)}".rstrip())
        if step.verdict is Verdict.PASS:
            return None
        if step.verdict is Verdict.FAIL:
            act.roll_back()
            return ROLLED_BACK
        if step.verdict is Verdict.HOLD:
            return HELD
    return HELD


def run(policy: Policy, current: int, act: Actions) -> str:
    try:
        # The share a person started is judged too, before the first step up.
        for percent in [current, *remaining(policy.steps, current)]:
            if percent != current:
                act.set_percent(percent)
            act.log(f"canary at {percent}%")
            outcome = _watch(policy, act)
            if outcome is not None:
                return outcome
        act.promote()
        act.log("promoted")
        return PROMOTED
    except TakenOver as reason:
        act.log(f"stopped: {reason}")
        return STOPPED
    except Unhealthy as reason:
        act.log(f"unhealthy: {reason}")
        act.roll_back()
        return ROLLED_BACK


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--prometheus-url", default="http://localhost:19090")
    parser.add_argument("--should-start", action="store_true", help="exit 0 if this commit should start a rollout")
    parser.add_argument("--subject", default="", help="the commit subject, with --should-start")
    arguments = parser.parse_args()

    import httpx

    from fraudstream_monitoring.rollout_metrics import window_metrics

    manifest = Path(arguments.manifest)
    policy_text = Path(arguments.policy).read_text()
    if arguments.should_start:
        return 0 if should_start(manifest.read_text(), yaml.safe_load(policy_text), arguments.subject) else 1

    policy = Policy.from_yaml(policy_text)
    version, current = split(manifest.read_text())
    if current is None or not 0 < current < 100:
        print(f"no canary is running (version {version}, canary {current}): nothing to roll out")
        return EXIT[STOPPED]

    repo = manifest.resolve().parents[2]
    expected = {"percent": current}

    def live_percent() -> int | None:
        out = subprocess.run(
            ["kubectl", "-n", "kserve-models", "get", "inferenceservice", "fraud-detection",
             "-o", "jsonpath={.spec.predictor.canaryTrafficPercent}"],
            check=True, capture_output=True, text=True,
        ).stdout.strip()
        return int(out) if out else None

    def ship(new_text: str, message: str, percent: int | None) -> None:
        if live_percent() != expected["percent"]:
            raise TakenOver(f"the live split is {live_percent()}%, the rollout left it at {expected['percent']}%")
        manifest.write_text(new_text)
        subprocess.run(["git", "-C", repo, "commit", "-qam", f"{ROLLOUT_PREFIX} {message}"], check=True)
        if subprocess.run(["git", "-C", repo, "push", "-q", "origin", "HEAD"]).returncode != 0:
            subprocess.run(["git", "-C", repo, "reset", "-q", "--hard", "HEAD~1"], check=True)
            raise TakenOver("the deploy branch moved: someone else changed it")
        subprocess.run([repo / "ci" / "apply_model.sh"], check=True)
        expected["percent"] = percent
        if subprocess.run([repo / "ci" / "check_model_split.sh"]).returncode != 0:
            raise Unhealthy(f"the versions answering do not match the split at {percent}%")

    with httpx.Client(base_url=arguments.prometheus_url, timeout=30) as prometheus:
        outcome = run(policy, current, Actions(
            set_percent=lambda p: ship(with_canary(manifest.read_text(), p), f"version {version} at {p}%", p),
            promote=lambda: ship(promoted(manifest.read_text()), f"promote version {version} to all traffic", None),
            roll_back=lambda: ship(rolled_back(manifest.read_text()), f"roll version {version} back to 0%", 0),
            measure=lambda seconds: window_metrics(prometheus, candidate=version, window_seconds=seconds),
            sleep=time.sleep,
            log=lambda message: print(time.strftime("%H:%M:%S"), message, flush=True),
        ))
    print(f"rollout {outcome}")
    return EXIT[outcome]


if __name__ == "__main__":
    sys.exit(main())
