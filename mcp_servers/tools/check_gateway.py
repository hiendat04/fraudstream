"""Checks the MCP gateway from outside, through a port-forward.

    python tools/check_gateway.py http://127.0.0.1:18090/mcp
    python tools/check_gateway.py http://127.0.0.1:18090/mcp --loop 120

Reads CONTEXT_AGENT_KEY, RISK_AGENT_KEY and MCP_CHECK_KEY. Exits 1 if any check
fails. --loop only calls search_handbook every 0.6 s for that many seconds and
counts failures, to watch a rolling update from the caller's side.
The rate-limit burst throttles the risk agent's key for about a minute, so wait
that long before running the checks again.
"""

import argparse
import asyncio
import os
import sys
import time

import httpx
import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

CONTEXT_TOOLS = {"get_customer_features", "get_merchant_features", "get_chunk"}
RISK_TOOLS = {"score_payment", "search_handbook"}
INITIALIZE = {
    "jsonrpc": "2.0", "id": 1, "method": "initialize",
    "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "check", "version": "0"}},
}
HEADERS = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
# Valid arguments, so a refusal can only come from the gateway's policy.
PAYMENT = {
    "transaction_id": "txn_check", "customer_id": "cust_00090571", "merchant_id": "merch_00011503",
    "event_timestamp": "2026-06-22T23:59:21Z", "amount": 10.0, "channel": "online", "city": "New York",
}
# Under the gateway's 120 calls a minute per consumer.
LOOP_PAUSE_SECONDS = 0.6
failed = 0


def expect(what: str, wanted, got) -> None:
    global failed
    ok = wanted == got
    failed += not ok
    print(f"  {'ok  ' if ok else 'FAIL'}  {what}: {got!r}" + ("" if ok else f", wanted {wanted!r}"))


def client(url: str, key: str) -> Client:
    headers = {"Authorization": f"Bearer {key}"}
    return Client(streamable_http_client(url, http_client=httpx2.AsyncClient(headers=headers)), mode="legacy")


async def tools(url: str, key: str) -> set[str] | str:
    try:
        async with client(url, key) as session:
            return {tool.name for tool in (await session.list_tools()).tools}
    except Exception as error:
        return f"no answer: {error!r}"


async def check(url: str, keys: dict[str, str]) -> None:
    post = lambda **headers: httpx.post(url, json=INITIALIZE, headers={**HEADERS, **headers})
    status = lambda **headers: post(**headers).status_code
    expect("no key", 401, status())
    expect("a wrong key", 401, status(Authorization="Bearer wrong"))

    expect("the context agent sees", CONTEXT_TOOLS, await tools(url, keys["context-agent"]))
    expect("the risk agent sees", RISK_TOOLS, await tools(url, keys["risk-agent"]))
    expect("the check key sees", CONTEXT_TOOLS | RISK_TOOLS, await tools(url, keys["mcp-check"]))

    async with client(url, keys["context-agent"]) as session:
        try:
            refused = (await session.call_tool("score_payment", PAYMENT)).is_error
        except Exception:
            refused = True
    expect("the context agent calling a risk tool is refused", True, refused)

    async with client(url, keys["mcp-check"]) as session:
        found = await session.call_tool("search_handbook", {"query": "what do I do about card testing", "top_k": 2})
        expect("search_handbook answers", False, found.is_error)
        chunk_id = found.structured_content["hits"][0]["chunk_id"]
        chunk = await session.call_tool("get_chunk", {"chunk_id": chunk_id})
        expect(f"get_chunk {chunk_id} answers", False, chunk.is_error)

    # A throttled MCP call still gets HTTP 200; the refusal is a JSON-RPC error in the body.
    answers = [post(Authorization=f"Bearer {keys['risk-agent']}") for _ in range(130)]
    throttled = any("rate limit" in answer.text for answer in answers)
    expect("the rate limit refuses some of 130 calls in a burst", True, throttled)


async def loop(url: str, key: str, seconds: float) -> None:
    global failed
    calls = 0
    end = time.monotonic() + seconds
    async with client(url, key) as session:
        while time.monotonic() < end:
            calls += 1
            try:
                failed += (await session.call_tool("search_handbook", {"query": "card testing", "top_k": 1})).is_error
            except Exception as error:
                failed += 1
                print(f"  call {calls}: {error}")
            await asyncio.sleep(LOOP_PAUSE_SECONDS)
    print(f"  {calls} calls, {failed} failed")


def main() -> None:
    parser = argparse.ArgumentParser(description="Check the MCP gateway.")
    parser.add_argument("url")
    parser.add_argument("--loop", type=float, default=None, metavar="SECONDS")
    arguments = parser.parse_args()
    keys = {
        "context-agent": os.environ["CONTEXT_AGENT_KEY"],
        "risk-agent": os.environ["RISK_AGENT_KEY"],
        "mcp-check": os.environ["MCP_CHECK_KEY"],
    }
    if arguments.loop is None:
        asyncio.run(check(arguments.url, keys))
    else:
        asyncio.run(loop(arguments.url, keys["mcp-check"], arguments.loop))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
