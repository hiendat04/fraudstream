"""Tests for what both MCP servers share. The APIs are httpx mock transports."""

import httpx
import pytest
from mcp.server.mcpserver import MCPServer
from prometheus_client import REGISTRY
from starlette.testclient import TestClient

from fraudstream_mcp.common import (
    FAILED,
    NOT_FOUND,
    OK,
    REJECTED,
    Api,
    ToolCallError,
    add_probes,
    counted,
    detail,
    outcome,
)

pytestmark = pytest.mark.anyio


def api_answering(status: int, body=None, *, seen=None) -> Api:
    def handle(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        return httpx.Response(status, json=body if body is not None else {})

    return Api("context API", "http://context", 5, transport=httpx.MockTransport(handle))


def api_unreachable() -> Api:
    def handle(request):
        raise httpx.ConnectError("refused")

    return Api("context API", "http://context", 5, transport=httpx.MockTransport(handle))


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        pytest.param(199, FAILED, id="below-2xx-is-a-failure"),
        pytest.param(200, OK, id="200-is-ok"),
        pytest.param(299, OK, id="top-of-2xx-is-ok"),
        pytest.param(300, FAILED, id="a-redirect-is-a-failure"),
        pytest.param(399, FAILED, id="top-of-3xx-is-a-failure"),
        pytest.param(400, REJECTED, id="400-is-rejected"),
        pytest.param(404, NOT_FOUND, id="404-is-not-found"),
        pytest.param(422, REJECTED, id="422-is-rejected"),
        pytest.param(499, REJECTED, id="top-of-4xx-is-rejected"),
        pytest.param(500, FAILED, id="500-is-a-failure"),
        pytest.param(503, FAILED, id="503-is-a-failure"),
    ],
)
def test_outcome_partitions_status_codes(status, expected):
    assert outcome(status) == expected


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        pytest.param({"detail": "no chunk x"}, "no chunk x", id="a-string-detail-as-is"),
        pytest.param(
            {"detail": [{"loc": ["body", "amount"], "msg": "Input should be greater than 0"}]},
            "amount: Input should be greater than 0",
            id="a-validation-error-as-field-and-message",
        ),
        pytest.param({"other": 1}, "{'other': 1}", id="no-detail-gives-the-body"),
    ],
)
def test_detail_is_one_line_the_model_can_read(body, expected):
    assert detail(httpx.Response(422, json=body)) == expected


def test_detail_of_a_body_that_is_not_json_is_its_text():
    assert detail(httpx.Response(502, text="bad gateway")) == "bad gateway"


async def test_a_2xx_answer_is_the_tool_result():
    seen = []
    api = api_answering(200, {"hits": []}, seen=seen)
    assert await api.get("/v1/chunks", chunk_id="a#b") == {"hits": []}
    assert seen[0].url.params["chunk_id"] == "a#b"


async def test_a_post_sends_its_body():
    seen = []
    await api_answering(200, {}, seen=seen).post("/v1/search", {"question": "q"})
    assert seen[0].method == "POST" and seen[0].read() == b'{"question":"q"}'


@pytest.mark.parametrize(
    ("status", "expected_outcome", "message"),
    [
        (404, NOT_FOUND, "no chunk x"),
        (422, REJECTED, "no chunk x"),
        (503, FAILED, "the context API failed (503): no chunk x"),
    ],
)
async def test_an_error_answer_becomes_a_tool_error_the_model_can_read(status, expected_outcome, message):
    with pytest.raises(ToolCallError) as raised:
        await api_answering(status, {"detail": "no chunk x"}).get("/v1/chunks")
    assert (raised.value.outcome, str(raised.value)) == (expected_outcome, message)


async def test_an_unreachable_api_is_a_failed_call():
    with pytest.raises(ToolCallError) as raised:
        await api_unreachable().get("/v1/chunks")
    assert (raised.value.outcome, str(raised.value)) == (FAILED, "the context API cannot be reached")


@pytest.mark.parametrize(("api", "ready"), [(api_answering(200), True), (api_answering(503), False)])
async def test_ready_only_when_the_api_says_it_is(api, ready):
    assert await api.is_ready() is ready


async def test_not_ready_when_the_api_cannot_be_reached():
    assert await api_unreachable().is_ready() is False


def calls(tool: str, result: str) -> float:
    return REGISTRY.get_sample_value(
        "mcp_tool_calls_total", {"server": "test", "tool": tool, "outcome": result}
    ) or 0.0


@pytest.mark.parametrize(
    ("raised", "expected"),
    [
        pytest.param(None, OK, id="a-tool-that-returns-is-ok"),
        pytest.param(ToolCallError("gone", NOT_FOUND), NOT_FOUND, id="a-tool-error-keeps-its-outcome"),
        pytest.param(RuntimeError("bug"), FAILED, id="anything-else-is-a-failure"),
    ],
)
def test_every_call_is_counted_once_with_how_it_ended(raised, expected):
    tool = f"tool-{expected}-{type(raised).__name__}"
    before = calls(tool, expected)
    try:
        with counted("test", tool):
            if raised:
                raise raised
    except Exception:
        pass
    assert calls(tool, expected) == before + 1


def probed(api: Api) -> TestClient:
    server = MCPServer("test")
    add_probes(server, [api])
    return TestClient(server.streamable_http_app(stateless_http=True, json_response=True, host="0.0.0.0"))


def test_liveness_checks_nothing_else():
    with probed(api_unreachable()) as client:
        assert client.get("/livez").status_code == 200


@pytest.mark.parametrize(("api", "status"), [(api_answering(200), 200), (api_answering(503), 503)])
def test_readiness_follows_the_api(api, status):
    with probed(api) as client:
        response = client.get("/readyz")
    assert response.status_code == status
