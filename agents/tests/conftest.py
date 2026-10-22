"""Shared test settings: Hypothesis's quick and thorough profiles, and MLflow's prompt registry faked in memory."""

import os
import re
from types import SimpleNamespace

import pytest
from hypothesis import settings as hypothesis_settings

hypothesis_settings.register_profile("dev", max_examples=100, deadline=None)
hypothesis_settings.register_profile("thorough", max_examples=1000, deadline=None, derandomize=True)
hypothesis_settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))


class FakeRegistry:
    """MLflow's prompt registry in memory: the templates of each name's versions, and the aliases."""

    def __init__(self):
        self.versions: dict[str, list[str]] = {}
        self.aliases: dict[tuple[str, str], int] = {}
        self.loads: list[dict] = []

    def load_prompt(self, name_or_uri, allow_missing=False, cache_ttl_seconds=None):
        self.loads.append({"allow_missing": allow_missing, "cache_ttl_seconds": cache_ttl_seconds})
        name, alias = re.fullmatch(r"prompts:/(.+)@(.+)", name_or_uri).groups()
        templates = self.versions.get(name, [])
        version = len(templates) if alias == "latest" else self.aliases.get((name, alias), 0)
        if not version:
            if allow_missing:
                return None
            raise LookupError(name_or_uri)
        return SimpleNamespace(template=templates[version - 1], version=version)

    def register_prompt(self, name, template, commit_message=None):
        self.versions.setdefault(name, []).append(template)
        return SimpleNamespace(template=template, version=len(self.versions[name]))

    def set_prompt_alias(self, name, alias, version):
        self.aliases[(name, alias)] = version


@pytest.fixture
def registry():
    return FakeRegistry()
