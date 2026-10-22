"""The agents' system prompts in MLflow's Prompt Registry.

The text lives in git, one file per agent under agents/prompts/. register makes a
new MLflow version only when that text changed, promote points an alias at a
version, and render writes the ConfigMap the kagent Agent reads its system
prompt from.

    PYTHONPATH=src MLFLOW_TRACKING_URI=http://localhost:18085 python -m fraudstream_agents.prompts register context-agent --promote
    PYTHONPATH=src MLFLOW_TRACKING_URI=http://localhost:18085 python -m fraudstream_agents.prompts render context-agent | kubectl apply -f -
"""

import argparse
import json
import re
import sys
from pathlib import Path

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"
# About 1,000 tokens of the model's 8,192, which also hold the tool schemas and the conversation.
MAX_PROMPT_CHARS = 4000
# The prompt is named after its agent, whose Service name must be an RFC 1035 label.
NAME_PATTERN = r"^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$"


def check(name: str, template: str) -> None:
    """Refuse a name that can't be an agent's, and a prompt that is blank, too long or has template syntax."""

    if not re.fullmatch(NAME_PATTERN, name):
        raise ValueError(f"{name!r} is not a usable name: lower case, digits and hyphens, at most 63, starting with a letter")
    if not template.strip():
        raise ValueError("the prompt is blank")
    if len(template) > MAX_PROMPT_CHARS:
        raise ValueError(f"the prompt has {len(template)} characters, more than {MAX_PROMPT_CHARS}")
    # Both MLflow and kagent treat {{ }} as a variable and would rewrite the text.
    if "{{" in template:
        raise ValueError("the prompt contains {{, which MLflow and kagent read as a template variable")


def register(registry, name: str, template: str) -> tuple[int, bool]:
    """The version that holds this text, and whether it was just made. Unchanged text makes no new version."""

    check(name, template)
    latest = registry.load_prompt(f"prompts:/{name}@latest", allow_missing=True, cache_ttl_seconds=0)
    if latest is not None and latest.template == template:
        return int(latest.version), False
    made = registry.register_prompt(name, template, commit_message=f"from agents/prompts/{name}.md")
    return int(made.version), True


def promote(registry, name: str, version: int, alias: str = "production") -> None:
    """Point an alias of the prompt at one version."""

    registry.set_prompt_alias(name, alias, version)


def configmap(name: str, version: int, template: str, namespace: str = "kagent") -> dict:
    """The ConfigMap an Agent reads its system prompt from, saying which MLflow version it holds."""

    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": f"{name}-prompt",
            "namespace": namespace,
            "labels": {"app.kubernetes.io/part-of": "fraudstream-agents"},
            "annotations": {"fraudstream.dev/prompt": f"prompts:/{name}/{version}"},
        },
        "data": {"system-message": template},
    }


def main(argv: list[str] | None = None, registry=None, prompts_dir: Path = PROMPTS_DIR) -> int:
    """register or render one agent's prompt; 0 when done, 1 when the alias has no version."""

    parser = argparse.ArgumentParser(prog="python -m fraudstream_agents.prompts")
    commands = parser.add_subparsers(dest="command", required=True)
    register_command = commands.add_parser("register", help="make a version from agents/prompts/<name>.md")
    register_command.add_argument("name")
    register_command.add_argument("--promote", action="store_true", help="point production at it")
    render_command = commands.add_parser("render", help="print the ConfigMap of one alias")
    render_command.add_argument("name")
    render_command.add_argument("--alias", default="production")
    render_command.add_argument("--namespace", default="kagent")
    args = parser.parse_args(argv)

    if registry is None:
        # Imported here, so the tests never load MLflow.
        import mlflow.genai as registry

    if args.command == "register":
        version, created = register(registry, args.name, (prompts_dir / f"{args.name}.md").read_text())
        line = f"{args.name}: version {version} ({'new' if created else 'unchanged'})"
        if args.promote:
            promote(registry, args.name, version)
            line += ", production"
        print(line)
        return 0

    prompt = registry.load_prompt(f"prompts:/{args.name}@{args.alias}", allow_missing=True, cache_ttl_seconds=0)
    if prompt is None:
        print(f"{args.name} has no {args.alias} version", file=sys.stderr)
        return 1
    print(json.dumps(configmap(args.name, int(prompt.version), prompt.template, args.namespace), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
