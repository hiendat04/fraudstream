"""Read and change the traffic split in k8s/models/fraud-detection.yaml.

Only canaryTrafficPercent, and minReplicas on promotion, are touched, so a step
moves traffic between the revisions that exist and never adds one.
"""

import yaml

ROLLOUT_PREFIX = "rollout(models):"


class _Dumper(yaml.SafeDumper):
    """Indents list items under their key, the way the manifest is written."""

    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


def _load(text: str) -> list[dict]:
    return list(yaml.safe_load_all(text))


def _dump(documents: list[dict]) -> str:
    return yaml.dump_all(documents, Dumper=_Dumper, sort_keys=False, default_flow_style=False)


def _predictor(documents: list[dict]) -> dict:
    service = next(d for d in documents if d and d.get("kind") == "InferenceService")
    return service["spec"]["predictor"]


def split(text: str) -> tuple[str, int | None]:
    predictor = _predictor(_load(text))
    env = {item["name"]: item.get("value", "") for item in predictor["containers"][0].get("env", [])}
    return env["MODEL_URI"].rsplit("/", 1)[-1], predictor.get("canaryTrafficPercent")


def with_canary(text: str, percent: int) -> str:
    documents = _load(text)
    _predictor(documents)["canaryTrafficPercent"] = percent
    return _dump(documents)


def promoted(text: str) -> str:
    documents = _load(text)
    predictor = _predictor(documents)
    predictor.pop("canaryTrafficPercent", None)
    predictor["minReplicas"] = 0
    return _dump(documents)


def rolled_back(text: str) -> str:
    return with_canary(text, 0)


def should_start(text: str, policy: dict, subject: str) -> bool:
    """A person deployed a canary, the policy says progressive, and this commit is not the job's own."""

    _, canary = split(text)
    return (
        policy.get("mode") == "progressive"
        and canary is not None
        and 0 < canary < 100
        and not subject.startswith(ROLLOUT_PREFIX)
    )
