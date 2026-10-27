"""The agents' check: which bursts its command line accepts, how many questions it has in flight, and which refusals it asks again."""

import asyncio
import sys

import pytest
from hypothesis import given, strategies as st

from tools import check_agent

PORTS = ["--controller", "c", "--context-api", "x", "--inference-api", "i"]


def burst(load: int, concurrency: int | None = None) -> list[str]:
    return [*PORTS, "--agent", "context-agent", "--load", str(load)] + ([] if concurrency is None else ["--concurrency", str(concurrency)])


def test_concurrency_defaults_to_the_whole_burst():
    assert check_agent.parse_args(burst(10)).concurrency == 10


@pytest.mark.parametrize("concurrency", [1, 2, 10])
def test_concurrency_from_one_to_the_burst_is_accepted(concurrency):
    assert check_agent.parse_args(burst(10, concurrency)).concurrency == concurrency


@pytest.mark.parametrize("concurrency", [0, -1, 11])
def test_concurrency_below_one_or_above_the_burst_is_refused(concurrency):
    with pytest.raises(SystemExit):
        check_agent.parse_args(burst(10, concurrency))


def test_concurrency_without_a_burst_is_refused():
    with pytest.raises(SystemExit):
        check_agent.parse_args([*PORTS, "--concurrency", "1"])


@given(load=st.integers(min_value=1, max_value=50), concurrency=st.integers(min_value=-5, max_value=60))
def test_concurrency_is_accepted_exactly_between_one_and_the_burst(load, concurrency):
    try:
        accepted = check_agent.parse_args(burst(load, concurrency)).concurrency == concurrency
    except SystemExit:
        accepted = False
    assert accepted == (1 <= concurrency <= load)


@pytest.mark.anyio
async def test_a_burst_has_at_most_concurrency_questions_in_flight(monkeypatch):
    in_flight = peak = asked = 0

    async def fake_ask_and_check(client, args, golden, judge):
        nonlocal in_flight, peak, asked
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        asked += 1
        return True

    monkeypatch.setattr(check_agent, "ask_and_check", fake_ask_and_check)
    monkeypatch.setattr(sys, "argv", ["check_agent.py", *burst(7, 3)])
    assert await check_agent.main() == 0
    assert (asked, peak) == (7, 3)


def asks(*outcomes):
    """A fake ask that raises or returns each outcome in turn, and the list of sessions it was asked in."""
    called = []

    async def fake_ask(client, url, text, context_id=None):
        called.append(context_id)
        outcome = outcomes[len(called) - 1]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return fake_ask, called


REFUSED = check_agent.A2AError("substrate worker pool has no free workers; try again later or increase WorkerPool replicas")
ANSWER = check_agent.Answer("completed", "done", [])


@pytest.mark.anyio
async def test_a_refusal_for_want_of_a_worker_is_asked_again(monkeypatch):
    fake_ask, called = asks(REFUSED, REFUSED, ANSWER)
    monkeypatch.setattr(check_agent, "ask", fake_ask)
    monkeypatch.setattr(check_agent, "RETRY_SECONDS", 0)
    assert await check_agent.ask_when_free(None, "url", "q") == ANSWER
    # One session for all three: a refused request still leaves its session behind in Substrate.
    assert len(called) == 3 and called[0] and len(set(called)) == 1


@pytest.mark.anyio
async def test_any_other_error_is_not_asked_again(monkeypatch):
    fake_ask, called = asks(check_agent.A2AError("500: boom"), ANSWER)
    monkeypatch.setattr(check_agent, "ask", fake_ask)
    with pytest.raises(check_agent.A2AError, match="boom"):
        await check_agent.ask_when_free(None, "url", "q")
    assert len(called) == 1


@pytest.mark.anyio
async def test_refusals_stop_being_asked_again_at_the_answer_timeout(monkeypatch):
    fake_ask, called = asks(REFUSED, ANSWER)
    monkeypatch.setattr(check_agent, "ask", fake_ask)
    monkeypatch.setattr(check_agent, "ANSWER_TIMEOUT_SECONDS", 0)
    with pytest.raises(check_agent.A2AError, match="no free workers"):
        await check_agent.ask_when_free(None, "url", "q")
    assert len(called) == 1
