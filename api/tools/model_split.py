"""Decide whether the model versions that answered match the InferenceService's split.

    python api/tools/model_split.py isvc.json answers.jsonl
isvc.json is `kubectl get inferenceservice -o json`; answers.jsonl holds one
/v1/predict answer per line, an empty line for a payment that got none.
Standard library only, so CI runs it without the API's dependencies.
"""

import json
import sys
from collections import Counter


def deployed(isvc: dict) -> tuple[str, int | None]:
    predictor = isvc["spec"]["predictor"]
    env = {item["name"]: item.get("value", "") for item in predictor["containers"][0].get("env", [])}
    return env["MODEL_URI"].rsplit("/", 1)[-1], predictor.get("canaryTrafficPercent")


def tally(lines: list[str]) -> tuple[Counter, int]:
    seen: Counter = Counter()
    unanswered = 0
    for line in lines:
        try:
            body = json.loads(line)
        except json.JSONDecodeError:
            unanswered += 1
            continue
        if "fraud_probability" not in body:
            unanswered += 1
            continue
        seen[body.get("model_version", "unknown")] += 1
    return seen, unanswered


def problems(version: str, canary: int | None, seen: Counter, unanswered: int) -> list[str]:
    found = [f"{unanswered} payments got no answer"] if unanswered else []
    others = {other: count for other, count in seen.items() if other != version}
    if canary is None or canary >= 100:
        if others:
            found.append(f"only version {version} should answer, but {others} did")
    elif canary <= 0:
        if seen[version]:
            found.append(f"the rollback left version {version} answering {seen[version]} payments")
    else:
        if not seen[version]:
            found.append(f"the canary, version {version}, answered none of {sum(seen.values())} payments")
        if not others:
            found.append(
                f"a canary is set but only version {version} answered: "
                "was canaryTrafficPercent left in after a promotion?"
            )
    if not seen:
        found.append("no payment was answered")
    return found


def main() -> int:
    isvc_path, answers_path = sys.argv[1:3]
    with open(isvc_path) as isvc_file:
        version, canary = deployed(json.load(isvc_file))
    with open(answers_path) as answers_file:
        seen, unanswered = tally(answers_file.read().splitlines())
    split = "no canary" if canary is None else f"canary {canary}%"
    print(f"manifest: version {version}, {split}; answers by version: {dict(seen)}, unanswered: {unanswered}")
    found = problems(version, canary, seen, unanswered)
    for line in found:
        print(f"  PROBLEM: {line}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
