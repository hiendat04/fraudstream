"""An HTTP client for a service behind the gateway: its password, and the local CA."""

import os
import ssl

import httpx


def gateway_client(base_url: str, *, transport: httpx.BaseTransport | None = None) -> httpx.Client:
    """Reads GATEWAY_USER and GATEWAY_PASSWORD, and trusts GATEWAY_CA when it is set."""

    ca = os.environ.get("GATEWAY_CA")
    return httpx.Client(
        base_url=base_url,
        auth=(os.environ["GATEWAY_USER"], os.environ["GATEWAY_PASSWORD"]),
        verify=ssl.create_default_context(cafile=ca) if ca else True,
        timeout=30,
        transport=transport,
    )
