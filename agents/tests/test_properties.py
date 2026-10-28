"""Properties that must hold for any input Hypothesis invents.

CrossHair drives the prompt-length rule and the 1% rule, integer comparisons it
solves at the boundaries instead of guessing them. It can't see through the regex
that finds numbers in an answer, so the property on whole answers, like the one
on registering any text, is left to random search. So are the decision line and
the cited chunks, both read with a regex. It also drives the A/B judge's
properties, ratios of whole counts it can solve exactly.
"""

from hypothesis import assume, given, settings, strategies as st

from fraudstream_agents.ab_testing import Variant, Verdict, judge
from fraudstream_agents.a2a import SANDBOX_AGENTS, agent_url, close, grounded
from fraudstream_agents.evals import cited_chunks, decision
from fraudstream_agents.prompts import MAX_PROMPT_CHARS, check, main, register
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


@given(before=st.text().filter(lambda t: "decision" not in t.lower()), after=st.text().filter(lambda t: "decision" not in t.lower()),
       flagged=st.booleans())
def test_the_decision_line_is_read_wherever_it_is(before, after, flagged):
    line = "Model decision: " + ("flagged" if flagged else "not flagged")
    assert decision(f"{before}\n{line}\n{after}") is flagged


chunk_ids = st.from_regex(r"[a-z][a-z0-9./-]{0,20}#[a-z0-9.-]{1,10}", fullmatch=True)


@given(ids=st.lists(chunk_ids, max_size=5), filler=st.text(alphabet=st.characters(blacklist_characters="[]"), max_size=30))
def test_the_cited_chunks_are_the_bracketed_ids(ids, filler):
    text = filler.join(f"[{chunk}]" for chunk in ids)
    assert cited_chunks(text) == list(dict.fromkeys(ids))


@given(texts=st.lists(st.text(alphabet="abc xyz", min_size=1, max_size=20).filter(str.strip), min_size=1, max_size=5))
def test_promoting_moves_exactly_one_alias(texts, tmp_path_factory):
    registry = FakeRegistry()
    prompts_dir = tmp_path_factory.mktemp("prompts")
    for text in texts:
        (prompts_dir / "context-agent.md").write_text(text)
        main(["register", "context-agent", "--alias", "candidate"], registry=registry, prompts_dir=prompts_dir)
    main(["promote", "context-agent", "--from", "candidate"], registry=registry)
    changes = 1 + sum(1 for before, after in zip(texts, texts[1:]) if before != after)
    assert registry.aliases == {("context-agent", "production"): changes}


@given(name=st.one_of(st.sampled_from(sorted(SANDBOX_AGENTS)), st.from_regex(r"[a-z-]{1,40}", fullmatch=True)))
def test_agent_url_names_exactly_one_route(name):
    url = agent_url("http://c:8083", name)
    sandboxed = url.startswith("http://c:8083/api/a2a-sandboxes/kagent/")
    assert sandboxed != url.startswith("http://c:8083/api/a2a/kagent/")
    assert sandboxed == (name in SANDBOX_AGENTS)
    assert url.endswith(f"/{name}/")


counts = st.integers(min_value=0, max_value=1000)


@settings(backend="crosshair", max_examples=50, deadline=None)
@given(a_calls=counts, a_failed=counts, b_calls=counts, b_failed=counts, a_asked=counts, a_passed=counts, b_asked=counts,
       b_passed=counts, a_p95=counts, b_p95=counts, min_calls=st.integers(min_value=1, max_value=50))
def test_a_candidate_no_worse_on_every_count_is_never_failed(a_calls, a_failed, b_calls, b_failed, a_asked, a_passed, b_asked,
                                                             b_passed, a_p95, b_p95, min_calls):
    assume(a_failed <= a_calls and b_failed <= b_calls and a_passed <= a_asked and b_passed <= b_asked)
    assume(b_failed * a_calls <= a_failed * b_calls and (a_calls or not b_failed))
    assume(b_passed * a_asked >= a_passed * b_asked and b_p95 <= a_p95)
    a = Variant("A", a_calls, a_failed, a_p95, a_passed, a_asked)
    b = Variant("B", b_calls, b_failed, b_p95, b_passed, b_asked)
    assert judge(a, b, min_calls=min_calls).verdict is not Verdict.FAIL


@settings(backend="crosshair", max_examples=50, deadline=None)
@given(b_calls=counts, min_calls=st.integers(min_value=1, max_value=50))
def test_too_few_calls_never_pass(b_calls, min_calls):
    assume(b_calls < min_calls)
    assert judge(Variant("A", 100, 0, 1.0), Variant("B", b_calls, 0, 1.0), min_calls=min_calls).verdict is not Verdict.PASS
