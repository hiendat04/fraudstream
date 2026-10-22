"""Prompts: which names and texts pass, registering only changed text, promoting, and the ConfigMap."""

import json

import pytest

from fraudstream_agents.prompts import MAX_PROMPT_CHARS, PROMPTS_DIR, check, configmap, main, promote, register


@pytest.mark.parametrize("name", [
    pytest.param("context-agent", id="the-context-agent"),
    pytest.param("a", id="one-letter"),
    pytest.param("a" * 63, id="63-the-longest-service-name"),
])
def test_a_good_name_passes(name):
    check(name, "Answer from your tools.")


@pytest.mark.parametrize("name", [
    pytest.param("a" * 64, id="64-too-long-for-a-service-name"),
    pytest.param("", id="empty"),
    pytest.param("Context-agent", id="upper-case"),
    pytest.param("1agent", id="starts-with-a-digit"),
    pytest.param("agent-", id="ends-with-a-hyphen"),
    pytest.param("context_agent", id="underscore"),
])
def test_a_bad_name_is_refused(name):
    with pytest.raises(ValueError, match="name"):
        check(name, "Answer from your tools.")


@pytest.mark.parametrize(("template", "accepted"), [
    pytest.param("x" * MAX_PROMPT_CHARS, True, id="4000-at-the-limit"),
    pytest.param("x" * (MAX_PROMPT_CHARS + 1), False, id="4001-over-the-limit"),
    pytest.param("x", True, id="one-character"),
    pytest.param("", False, id="empty"),
    pytest.param(" \n\t", False, id="blank"),
    pytest.param("Use {customer_id} as written.", True, id="single-braces-are-text"),
    pytest.param("Hello {{ name }}", False, id="template-syntax"),
])
def test_a_prompt_is_accepted_only_when_it_fits_and_has_no_template_syntax(template, accepted):
    if accepted:
        check("context-agent", template)
    else:
        with pytest.raises(ValueError):
            check("context-agent", template)


@pytest.mark.parametrize(("template", "message"), [
    pytest.param("", "blank", id="blank"),
    pytest.param("x" * (MAX_PROMPT_CHARS + 1), "4000", id="too-long"),
    pytest.param("Hello {{ name }}", "{{", id="template-syntax"),
])
def test_a_refusal_says_what_is_wrong(template, message):
    with pytest.raises(ValueError, match=message):
        check("context-agent", template)


def test_the_first_registration_makes_version_1(registry):
    assert register(registry, "context-agent", "v1 text") == (1, True)


def test_the_same_text_again_makes_no_new_version(registry):
    register(registry, "context-agent", "v1 text")
    assert register(registry, "context-agent", "v1 text") == (1, False)
    assert registry.versions["context-agent"] == ["v1 text"]


def test_changed_text_makes_the_next_version(registry):
    register(registry, "context-agent", "v1 text")
    assert register(registry, "context-agent", "v2 text") == (2, True)


def test_register_reads_the_newest_version_past_the_cache(registry):
    register(registry, "context-agent", "v1 text")
    assert registry.loads[-1] == {"allow_missing": True, "cache_ttl_seconds": 0}


def test_register_checks_the_prompt_first(registry):
    with pytest.raises(ValueError):
        register(registry, "context-agent", "Hello {{ name }}")
    assert registry.versions == {}


def test_promote_points_production_at_the_version(registry):
    register(registry, "context-agent", "v1 text")
    register(registry, "context-agent", "v2 text")
    promote(registry, "context-agent", 1)
    assert registry.aliases[("context-agent", "production")] == 1


def test_the_configmap_carries_the_prompt_and_its_version():
    assert configmap("context-agent", 3, "Answer from your tools.") == {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "context-agent-prompt",
            "namespace": "kagent",
            "labels": {"app.kubernetes.io/part-of": "fraudstream-agents"},
            "annotations": {"fraudstream.dev/prompt": "prompts:/context-agent/3"},
        },
        "data": {"system-message": "Answer from your tools."},
    }


def test_main_registers_promotes_and_says_so(registry, tmp_path, capsys):
    (tmp_path / "context-agent.md").write_text("Answer from your tools.\n")
    assert main(["register", "context-agent", "--promote"], registry=registry, prompts_dir=tmp_path) == 0
    assert capsys.readouterr().out.strip() == "context-agent: version 1 (new), production"
    assert main(["register", "context-agent"], registry=registry, prompts_dir=tmp_path) == 0
    assert capsys.readouterr().out.strip() == "context-agent: version 1 (unchanged)"
    assert registry.aliases == {("context-agent", "production"): 1}


def test_main_renders_the_production_prompt(registry, tmp_path, capsys):
    (tmp_path / "context-agent.md").write_text("Answer from your tools.\n")
    main(["register", "context-agent", "--promote"], registry=registry, prompts_dir=tmp_path)
    capsys.readouterr()
    assert main(["render", "context-agent"], registry=registry, prompts_dir=tmp_path) == 0
    assert json.loads(capsys.readouterr().out) == configmap("context-agent", 1, "Answer from your tools.\n")
    assert registry.loads[-1] == {"allow_missing": True, "cache_ttl_seconds": 0}


def test_main_renders_another_alias_into_another_namespace(registry, capsys):
    register(registry, "context-agent", "v1 text")
    promote(registry, "context-agent", 1, alias="canary")
    assert main(["render", "context-agent", "--alias", "canary", "--namespace", "agents"], registry=registry) == 0
    assert json.loads(capsys.readouterr().out) == configmap("context-agent", 1, "v1 text", namespace="agents")


def test_main_render_without_a_production_version_fails_plainly(registry, capsys):
    assert main(["render", "context-agent"], registry=registry) == 1
    assert "context-agent has no production version" in capsys.readouterr().err


def test_the_context_agent_prompt_in_git_passes_the_checks():
    check("context-agent", (PROMPTS_DIR / "context-agent.md").read_text())


def test_the_context_agent_prompt_names_its_tools_and_no_others():
    text = (PROMPTS_DIR / "context-agent.md").read_text()
    assert all(tool in text for tool in ("get_customer_features", "get_merchant_features", "get_chunk"))
    assert not any(tool in text for tool in ("score_payment", "search_handbook"))
