"""fraud-risk: score a payment with the fraud detection model, and search the handbook."""

from typing import Annotated, Any

from mcp.server.mcpserver import MCPServer
from pydantic import Field

from fraudstream_mcp.common import Api, add_probes, counted

NAME = "fraud-risk"


def build(context_api: Api, inference_api: Api) -> MCPServer:
    server = MCPServer(
        NAME,
        instructions=(
            "Score payments with the production fraud detection model and search the fraud handbook. "
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
                },
            )

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
