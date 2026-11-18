"""fraud-risk: score a payment with the fraud detection model, see who shares its device, and search the handbook."""

from typing import Annotated, Any
from urllib.parse import quote

from mcp.server.mcpserver import MCPServer
from pydantic import Field

from fraudstream_mcp.common import Api, add_probes, counted

NAME = "fraud-risk"


def build(context_api: Api, inference_api: Api) -> MCPServer:
    server = MCPServer(
        NAME,
        instructions=(
            "Score payments with the production fraud detection model, see who shares a payment's device, "
            "and search the fraud handbook. "
            "The model's probability and threshold decide; never estimate a probability yourself."
        ),
    )

    @server.tool()
    async def score_payment(
        transaction_id: str,
        customer_id: str,
        merchant_id: str,
        event_timestamp: Annotated[str, Field(description="ISO 8601 with a time zone, e.g. 2026-06-22T23:59:21Z")],
        amount: float,
        channel: Annotated[str, Field(description="as written in the alert, e.g. online")],
        city: str,
        device_id: Annotated[str | None, Field(description="the device it was paid on; none for in-store")] = None,
    ) -> dict[str, Any]:
        """Score one payment with the production fraud detection model. Returns the fraud probability, the model version, its threshold, whether the payment is flagged, and which history was found."""
        with counted(NAME, "score_payment"):
            return await inference_api.post(
                "/v1/predict",
                {
                    "transaction_id": transaction_id,
                    "customer_id": customer_id,
                    "merchant_id": merchant_id,
                    "event_timestamp": event_timestamp,
                    "amount": amount,
                    "channel": channel,
                    "city": city,
                    "device_id": device_id,
                },
            )

    @server.tool()
    async def device_neighbours(
        device_id: Annotated[str, Field(description="e.g. dev_ring_0001")],
    ) -> dict[str, Any]:
        """Who paid on one device in the 30 days up to its newest payment: how many customers, how many of them had a fraud payment on it, the largest customer crowd on any IP it used, and up to 10 of the customers, fraud first."""
        with counted(NAME, "device_neighbours"):
            # Encoded, so an id with '/' or '?' can only ever be a wrong id.
            return await context_api.get(f"/v1/devices/{quote(device_id, safe='')}")

    @server.tool()
    async def search_handbook(
        query: str,
        top_k: Annotated[int, Field(description="how many sections, 1 to 10")] = 5,
    ) -> dict[str, Any]:
        """Search the fraud handbook: policies, review playbooks, fraud patterns, model cards and feature definitions. Returns the closest sections with their chunk ids, to cite."""
        with counted(NAME, "search_handbook"):
            return await context_api.post("/v1/search", {"question": query, "top_k": top_k})

    add_probes(server, [context_api, inference_api])
    return server
