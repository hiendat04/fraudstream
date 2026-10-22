"""Properties that must hold for any input Hypothesis invents.

CrossHair drives the prompt-length rule, an integer comparison it solves at the
boundaries instead of guessing them. Registration takes any text at all, which
random search covers better.
"""

from hypothesis import given, settings, strategies as st

from fraudstream_agents.prompts import MAX_PROMPT_CHARS, check, register
from tests.conftest import FakeRegistry


@settings(backend="crosshair", max_examples=50, deadline=None)
@given(length=st.integers(min_value=0, max_value=2 * MAX_PROMPT_CHARS))
def test_a_prompt_fits_exactly_when_it_has_1_to_4000_characters(length):
    try:
        check("context-agent", "x" * length)
        accepted = True
    except ValueError:
        accepted = False
    assert accepted == (1 <= length <= MAX_PROMPT_CHARS)


@given(template=st.text(min_size=1, max_size=200).filter(lambda t: t.strip() and "{{" not in t))
def test_registering_the_same_text_twice_is_idempotent(template):
    registry = FakeRegistry()
    first = register(registry, "context-agent", template)
    second = register(registry, "context-agent", template)
    assert second == (first[0], False)
    assert len(registry.versions["context-agent"]) == 1
