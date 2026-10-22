"""The served app: the right server per MCP_SERVER, plain HTTP with any Host, no session needed."""

import httpx
import pytest
from starlette.testclient import TestClient

import fraudstream_mcp.__main__ as entry
from fraudstream_mcp.common import Settings

HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
    "MCP-Protocol-Version": "2025-06-18",
}
INITIALIZE = {
    "jsonrpc": "2.0", "id": 1, "method": "initialize",
    "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test", "version": "0"}},
}
LIST = {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}


def ready_api():
    return httpx.MockTransport(lambda request: httpx.Response(200, json={"status": "ready"}))


@pytest.mark.parametrize(
    ("server", "tools"),
    [
        ("context", {"get_customer_features", "get_merchant_features", "get_chunk"}),
        ("risk", {"score_payment", "search_handbook"}),
    ],
)
def test_the_service_name_as_host_and_no_session_id_are_answered(server, tools):
    app = entry.create_app(Settings(mcp_server=server), transport=ready_api())
    # agentgateway calls with the Service's name as Host; replicas share no session.
    with TestClient(app, base_url=f"http://fraud-{server}.mcp.svc.cluster.local") as client:
        assert client.post("/mcp", json=INITIALIZE, headers=HEADERS).status_code == 200
        response = client.post("/mcp", json=LIST, headers=HEADERS)
    assert response.status_code == 200
    assert {tool["name"] for tool in response.json()["result"]["tools"]} == tools


@pytest.mark.parametrize(
    ("server", "down", "reason"),
    [
        ("context", "context", "the context API does not answer"),
        ("risk", "context", "the context API does not answer"),
        ("risk", "inference", "the inference API does not answer"),
    ],
)
def test_a_server_is_not_ready_while_an_api_behind_its_tools_is_down(server, down, reason):
    def handle(request):
        return httpx.Response(503 if request.url.host == f"{down}-api.fraudstream-apis.svc.cluster.local" else 200)

    app = entry.create_app(Settings(mcp_server=server), transport=httpx.MockTransport(handle))
    with TestClient(app) as client:
        assert client.get("/readyz", headers={"Host": "localhost"}).json()["reason"] == reason
        assert client.get("/readyz").status_code == 503


@pytest.mark.parametrize("server", ["context", "risk"])
def test_a_server_is_ready_when_its_apis_are(server):
    with TestClient(entry.create_app(Settings(mcp_server=server), transport=ready_api())) as client:
        assert client.get("/readyz").status_code == 200


@pytest.mark.parametrize("server", ["context", "risk"])
def test_each_server_introduces_itself_with_its_name_and_instructions(server):
    app = entry.create_app(Settings(mcp_server=server), transport=ready_api())
    with TestClient(app) as client:
        result = client.post("/mcp", json=INITIALIZE, headers=HEADERS).json()["result"]
    assert result["serverInfo"]["name"] == f"fraud-{server}"
    assert result["instructions"]


def test_main_serves_metrics_and_the_app(monkeypatch):
    started, ran = [], []
    monkeypatch.setenv("MCP_SERVER", "context")
    monkeypatch.setenv("METRICS_PORT", "9000")
    monkeypatch.setattr(entry, "start_http_server", started.append)
    monkeypatch.setattr(entry.uvicorn, "run", lambda app, **options: ran.append((app, options)))
    entry.main()
    assert started == [9000]
    app, options = ran[0]
    assert app is not None
    # Under the chart's 30 s grace period, so open calls finish before the pod is killed.
    assert (options["host"], options["port"], options["timeout_graceful_shutdown"]) == ("0.0.0.0", 8080, 20)
