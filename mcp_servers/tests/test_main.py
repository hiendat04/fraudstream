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


def test_main_serves_metrics_and_the_app(monkeypatch):
    started, ran = [], []
    monkeypatch.setenv("MCP_SERVER", "context")
    monkeypatch.setenv("METRICS_PORT", "9000")
    monkeypatch.setattr(entry, "start_http_server", started.append)
    monkeypatch.setattr(entry.uvicorn, "run", lambda app, **options: ran.append(options))
    entry.main()
    assert started == [9000]
    assert ran[0]["port"] == 8080 and ran[0]["host"] == "0.0.0.0"
