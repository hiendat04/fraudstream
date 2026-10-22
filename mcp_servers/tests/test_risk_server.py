"""The fraud-risk tools, called in process, against mocked context and inference APIs."""

import json

import httpx
import pytest
from mcp import Client

from fraudstream_mcp import risk_server
from fraudstream_mcp.common import Api

pytestmark = pytest.mark.anyio

PAYMENT = {
    "transaction_id": "txn_000000128913",
    "customer_id": "cust_00090571",
    "merchant_id": "merch_00011503",
    "event_timestamp": "2026-06-22T23:59:21Z",
    "amount": 143.92,
    "channel": "online",
    "city": "New York",
}


def recording(status=200, body=None):
    seen = []

    def handle(request):
        seen.append(request)
        return httpx.Response(status, json=body if body is not None else {"ok": True})

    return httpx.MockTransport(handle), seen


async def test_it_offers_the_risk_tools_only():
    transport, _ = recording()
    server = risk_server.build(Api("context API", "http://c", 5, transport), Api("inference API", "http://i", 5, transport))
    async with Client(server) as client:
        names = {tool.name for tool in (await client.list_tools()).tools}
    assert names == {"score_payment", "search_handbook"}


async def test_score_payment_sends_the_payment_to_the_inference_api():
    context, _ = recording()
    inference, seen = recording(body={"fraud_probability": 0.71, "threshold": 0.626, "flagged": True})
    server = risk_server.build(Api("context API", "http://c", 5, context), Api("inference API", "http://i", 5, inference))
    async with Client(server) as client:
        result = await client.call_tool("score_payment", PAYMENT)
    assert result.structured_content["flagged"] is True
    assert seen[0].url.path == "/v1/predict"
    assert json.loads(seen[0].read()) == PAYMENT


async def test_a_payment_the_api_refuses_comes_back_with_what_was_wrong():
    context, _ = recording()
    inference, _ = recording(422, {"detail": [{"loc": ["body", "channel"], "msg": "channel must be one of: atm, online, pos"}]})
    server = risk_server.build(Api("context API", "http://c", 5, context), Api("inference API", "http://i", 5, inference))
    async with Client(server) as client:
        result = await client.call_tool("score_payment", {**PAYMENT, "channel": "web"})
    assert result.is_error
    # The SDK puts the tool's name in front of a ToolError's own text.
    assert result.content[0].text == "Error executing tool score_payment: channel: channel must be one of: atm, online, pos"


async def test_search_handbook_asks_the_context_api_with_top_k():
    context, seen = recording(body={"hits": []})
    inference, _ = recording()
    server = risk_server.build(Api("context API", "http://c", 5, context), Api("inference API", "http://i", 5, inference))
    async with Client(server) as client:
        await client.call_tool("search_handbook", {"query": "card testing", "top_k": 3})
    assert seen[0].url.path == "/v1/search"
    assert json.loads(seen[0].read()) == {"question": "card testing", "top_k": 3}
