"""Runs one MCP server. MCP_SERVER=context runs fraud-context; MCP_SERVER=risk runs fraud-risk.

    MCP_SERVER=context python -m fraudstream_mcp
"""

import httpx
import uvicorn
from prometheus_client import start_http_server
from starlette.applications import Starlette

from fraudstream_mcp import context_server, risk_server
from fraudstream_mcp.common import Api, Settings


def create_app(
    settings: Settings | None = None, *, transport: httpx.AsyncBaseTransport | None = None
) -> Starlette:
    """Build the server the settings name. Tests pass a transport in place of the real APIs."""

    config = settings or Settings()
    context_api = Api("context API", config.context_api_url, config.api_timeout_seconds, transport)
    if config.mcp_server == "context":
        server = context_server.build(context_api)
    else:
        inference_api = Api("inference API", config.inference_api_url, config.api_timeout_seconds, transport)
        server = risk_server.build(context_api, inference_api)
    # Stateless: no session lives in a pod, so any replica answers and a rolling update
    # breaks no caller. host="0.0.0.0" turns off the SDK's localhost-only Host check,
    # which would refuse agentgateway calling by the Service's name.
    return server.streamable_http_app(stateless_http=True, json_response=True, host="0.0.0.0")


def main() -> None:
    config = Settings()
    if config.metrics_port is not None:
        start_http_server(config.metrics_port)
    uvicorn.run(
        create_app(config), host="0.0.0.0", port=config.port, timeout_graceful_shutdown=20
    )


if __name__ == "__main__":
    main()
