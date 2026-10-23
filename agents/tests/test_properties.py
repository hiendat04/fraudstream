"""Properties that must hold for any input Hypothesis invents.

CrossHair drives the prompt-length rule and the 1% rule, integer comparisons it
solves at the boundaries instead of guessing them. It can't see through the regex
that finds numbers in an answer, so the property on whole answers, like the one
on registering any text, is left to random search.
"""

from hypothesis import given, settings, strategies as st

from fraudstream_agents.a2a import close, grounded
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


@settings(backend="crosshair", max_examples=50, deadline=None)
@given(value=st.integers(min_value=1, max_value=10**6), offset=st.integers(min_value=-(10**6), max_value=10**6))
def test_a_number_is_close_exactly_when_it_is_within_1_percent(value, offset):
    assert close(value + offset, value) == (abs(offset) * 100 <= value)


@given(value=st.integers(min_value=1, max_value=10**6), offset=st.integers(min_value=-(10**6), max_value=10**6))
def test_a_quoted_number_counts_exactly_when_it_is_within_1_percent(value, offset):
    assert grounded(f"the tool said {value + offset}", [value]) == (abs(offset) * 100 <= value)
