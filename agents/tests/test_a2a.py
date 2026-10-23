"""Talking to an agent over A2A, and telling whether its answer quotes the tool's numbers."""

import json

import httpx
import pytest

from fraudstream_agents.a2a import A2AError, answer_text, ask, grounded, message_request, numbers, task_state

pytestmark = pytest.mark.anyio


def test_the_request_is_a_blocking_message_send():
    assert message_request("hi", "r1") == {
        "jsonrpc": "2.0", "id": "r1", "method": "message/send",
        "params": {
            "message": {"kind": "message", "role": "user", "messageId": "r1", "parts": [{"kind": "text", "text": "hi"}]},
            "configuration": {"blocking": True},
        },
    }


@pytest.mark.parametrize(("result", "text"), [
    pytest.param({"kind": "task", "status": {"state": "completed"},
                  "artifacts": [{"parts": [{"kind": "text", "text": "a"}, {"kind": "data", "data": {}}, {"kind": "text", "text": "b"}]}]},
                 "a\nb", id="artifact-text-parts-joined-others-skipped"),
    pytest.param({"kind": "task", "status": {"state": "failed", "message": {"parts": [{"kind": "text", "text": "boom"}]}}},
                 "boom", id="no-artifacts-the-status-message"),
    pytest.param({"kind": "message", "parts": [{"kind": "text", "text": "hello"}]}, "hello", id="a-plain-message"),
    pytest.param({"kind": "task", "status": {"state": "completed"}}, "", id="nothing-to-read"),
])
def test_the_answer_is_the_text_the_agent_sent(result, text):
    assert answer_text(result) == text


@pytest.mark.parametrize(("result", "state"), [
    pytest.param({"kind": "task", "status": {"state": "failed"}}, "failed", id="a-task-has-its-state"),
    pytest.param({"kind": "message", "parts": []}, "completed", id="a-message-is-complete"),
])
def test_the_state_of_an_answer(result, state):
    assert task_state(result) == state


async def test_ask_posts_the_question_and_reads_the_answer():
    seen = {}

    def answer(request):
        seen["url"], seen["body"] = str(request.url), json.loads(request.content)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": seen["body"]["id"], "result": {
            "kind": "task", "status": {"state": "completed"},
            "artifacts": [{"parts": [{"kind": "text", "text": "42 payments"}]}]}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(answer)) as client:
        assert await ask(client, "http://c/api/a2a/kagent/context-agent/", "how many?") == ("completed", "42 payments")
    assert seen["url"] == "http://c/api/a2a/kagent/context-agent/"
    assert seen["body"]["method"] == "message/send"
    assert seen["body"]["params"]["message"]["parts"][0]["text"] == "how many?"
    assert seen["body"]["params"]["message"]["messageId"] == seen["body"]["id"]


async def test_each_question_gets_its_own_id():
    ids = []

    def answer(request):
        ids.append(json.loads(request.content)["id"])
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": ids[-1], "result": {"kind": "message", "parts": []}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(answer)) as client:
        await ask(client, "http://c/", "a")
        await ask(client, "http://c/", "b")
    assert len(set(ids)) == 2


@pytest.mark.parametrize(("response", "message"), [
    pytest.param(httpx.Response(200, json={"jsonrpc": "2.0", "id": "x", "error": {"code": -32001, "message": "agent not found"}}),
                 "agent not found", id="a-json-rpc-error"),
    pytest.param(httpx.Response(502, text="bad gateway"), "502: bad gateway", id="an-http-error"),
])
async def test_ask_raises_what_went_wrong(response, message):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response)) as client:
        with pytest.raises(A2AError, match=message):
            await ask(client, "http://c/", "q")


async def test_a_long_error_body_is_cut_to_200():
    response = httpx.Response(500, text="x" * 500)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response)) as client:
        with pytest.raises(A2AError) as error:
            await ask(client, "http://c/", "q")
    assert str(error.value) == "500: " + "x" * 200


@pytest.mark.parametrize(("text", "found"), [
    pytest.param("12 payments, 1,234.50 dollars, -3 declines", [12.0, 1234.5, -3.0], id="ints-thousands-negatives"),
    pytest.param("customer cust_00090571", [], id="digits-inside-an-id-are-not-numbers"),
    pytest.param("version v2.5", [], id="digits-after-a-letter-are-not-numbers"),
    pytest.param("no numbers here", [], id="none"),
])
def test_numbers_in_an_answer(text, found):
    assert numbers(text) == found


@pytest.mark.parametrize(("answer", "values", "expected"), [
    pytest.param("made 17 payments", [17], True, id="exact"),
    pytest.param("about 202", [200], True, id="1-percent-off-counts"),
    pytest.param("about 198", [200], True, id="1-percent-under-counts"),
    pytest.param("about 203", [200], False, id="just-over-1-percent"),
    pytest.param("spent 1,234.57", [1234.5678], True, id="rounded-to-cents"),
    pytest.param("declined -4", [-4], True, id="a-negative-value"),
    pytest.param("rate 0.5", [0.5], False, id="values-below-1-are-ignored"),
    pytest.param("exactly 1", [1], True, id="1-is-the-smallest-value-counted"),
    pytest.param("nothing stored", [17], False, id="no-numbers-in-the-answer"),
    pytest.param("made 17 payments", [], False, id="no-values-from-the-tool"),
])
def test_an_answer_is_grounded_when_it_quotes_a_tool_value(answer, values, expected):
    assert grounded(answer, values) is expected
