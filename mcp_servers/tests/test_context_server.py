"""The fraud-context tools, called in process, against a mocked context API."""

import httpx
import pytest
from mcp import Client

from fraudstream_mcp import context_server
from fraudstream_mcp.common import Api

pytestmark = pytest.mark.anyio


def server_with(status=200, body=None):
    seen = []

    def handle(request):
        seen.append(request)
        return httpx.Response(status, json=body if body is not None else {"ok": True})

    api = Api("context API", "http://context", 5, transport=httpx.MockTransport(handle))
    return context_server.build(api), seen


async def call(server, tool, arguments):
    async with Client(server) as client:
        return await client.call_tool(tool, arguments)


async def test_it_offers_the_context_tools_only():
    server, _ = server_with()
    async with Client(server) as client:
        names = {tool.name for tool in (await client.list_tools()).tools}
    assert names == {"get_customer_features", "get_merchant_features", "get_chunk"}


@pytest.mark.parametrize(
    ("tool", "arguments", "path"),
    [
        ("get_customer_features", {"customer_id": "cust_1"}, "/v1/customers/cust_1"),
        ("get_merchant_features", {"merchant_id": "merch_1"}, "/v1/merchants/merch_1"),
    ],
)
async def test_an_entity_tool_reads_its_own_endpoint(tool, arguments, path):
    server, seen = server_with(body={"entity_id": "x", "views": {}})
    result = await call(server, tool, arguments)
    assert not result.is_error
    assert result.structured_content == {"entity_id": "x", "views": {}}
    assert seen[0].url.path == path


async def test_an_id_with_a_slash_cannot_reach_another_endpoint():
    server, seen = server_with()
    await call(server, "get_customer_features", {"customer_id": "a/../search"})
    assert seen[0].url.raw_path == b"/v1/customers/a%2F..%2Fsearch"


async def test_a_chunk_id_with_a_hash_is_sent_whole():
    server, seen = server_with()
    await call(server, "get_chunk", {"chunk_id": "handbook/typology-card-testing#signs"})
    assert seen[0].url.params["chunk_id"] == "handbook/typology-card-testing#signs"


async def test_not_found_is_an_error_result_with_the_apis_words():
    server, _ = server_with(404, {"detail": "the online store has no features for customer cust_9"})
    result = await call(server, "get_customer_features", {"customer_id": "cust_9"})
    assert result.is_error
    # The SDK puts the tool's name in front of a ToolError's own text.
    assert result.content[0].text == "Error executing tool get_customer_features: the online store has no features for customer cust_9"
