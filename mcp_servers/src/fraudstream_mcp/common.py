"""What both MCP servers share: settings, calling a REST API, counting tool calls, probes."""

import time
from contextlib import contextmanager
from typing import Literal

import httpx
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from prometheus_client import Counter, Histogram
from pydantic_settings import BaseSettings
from starlette.requests import Request
from starlette.responses import JSONResponse

OK, NOT_FOUND, REJECTED, FAILED = "ok", "not_found", "rejected", "failed"
READY_TIMEOUT = 1.0

TOOL_CALLS = Counter(
    "mcp_tool_calls_total",
    "Tool calls answered, by server, tool and how they ended.",
    ["server", "tool", "outcome"],
)
TOOL_DURATION = Histogram(
    "mcp_tool_call_duration_seconds",
    "Time taken by one tool call, the API's answer included.",
    ["server", "tool"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)


class Settings(BaseSettings):
    """Which server this pod runs, and where its APIs are."""

    mcp_server: Literal["context", "risk"]
    context_api_url: str = "http://context-api.fraudstream-apis.svc.cluster.local"
    inference_api_url: str = "http://inference-api.fraudstream-apis.svc.cluster.local"
    # Above the inference API's 30 s wait for a model waking from zero.
    api_timeout_seconds: float = 35.0
    port: int = 8080
    metrics_port: int | None = None


class ToolCallError(ToolError):
    """A tool error the model reads, with how the call ended for the counters."""

    def __init__(self, message: str, outcome: str):
        super().__init__(message)
        self.outcome = outcome


def outcome(status: int) -> str:
    """How a tool call ended, from its API's status code."""

    if 200 <= status < 300:
        return OK
    if status == 404:
        return NOT_FOUND
    if 400 <= status < 500:
        return REJECTED
    return FAILED


def detail(response: httpx.Response) -> str:
    """FastAPI's error detail as one line: a string as it is, a validation error as field: message."""

    try:
        body = response.json()
    except ValueError:
        return response.text[:200]
    found = body.get("detail") if isinstance(body, dict) else None
    if isinstance(found, list):
        return "; ".join(
            f"{'.'.join(str(part) for part in item.get('loc', [])[1:])}: {item.get('msg', '')}"
            for item in found
            if isinstance(item, dict)
        )
    return str(body if found is None else found)[:200]


class Api:
    """One REST API behind the tools. Its answers become tool results, its errors messages."""

    def __init__(
        self,
        name: str,
        base_url: str,
        timeout_seconds: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self.name = name
        self._http = httpx.AsyncClient(base_url=base_url, timeout=timeout_seconds, transport=transport)

    async def get(self, path: str, **params: str) -> dict:
        return await self._call("GET", path, params=params or None)

    async def post(self, path: str, body: dict) -> dict:
        return await self._call("POST", path, json=body)

    async def _call(self, method: str, path: str, **kwargs) -> dict:
        try:
            response = await self._http.request(method, path, **kwargs)
        except httpx.TransportError as error:
            raise ToolCallError(f"the {self.name} cannot be reached", FAILED) from error
        result = outcome(response.status_code)
        if result == OK:
            return response.json()
        if result == FAILED:
            raise ToolCallError(
                f"the {self.name} failed ({response.status_code}): {detail(response)}", FAILED
            )
        raise ToolCallError(detail(response), result)

    async def is_ready(self) -> bool:
        try:
            response = await self._http.get("/readyz", timeout=READY_TIMEOUT)
        except httpx.TransportError:
            return False
        return response.status_code == 200

    async def close(self) -> None:
        await self._http.aclose()


@contextmanager
def counted(server: str, tool: str):
    """Count one tool call and time it. MCP tool errors return HTTP 200, so only the server can count them."""

    started = time.perf_counter()
    # What is counted if the tool raises anything other than a ToolCallError.
    result = FAILED
    try:
        yield
        result = OK
    except ToolCallError as error:
        result = error.outcome
        raise
    finally:
        TOOL_CALLS.labels(server, tool, result).inc()
        TOOL_DURATION.labels(server, tool).observe(time.perf_counter() - started)


def add_probes(server: MCPServer, apis: list[Api]) -> None:
    """Liveness checks nothing else; readiness checks every API the tools call."""

    @server.custom_route("/livez", methods=["GET"], include_in_schema=False)
    async def livez(request: Request) -> JSONResponse:
        return JSONResponse({"status": "alive"})

    @server.custom_route("/readyz", methods=["GET"], include_in_schema=False)
    async def readyz(request: Request) -> JSONResponse:
        # A pod that cannot reach its API cannot answer any tool. A release with a wrong
        # API URL never becomes ready, so Helm rolls it back.
        for api in apis:
            if not await api.is_ready():
                return JSONResponse({"status": "not ready", "reason": f"the {api.name} does not answer"}, 503)
        return JSONResponse({"status": "ready"})
