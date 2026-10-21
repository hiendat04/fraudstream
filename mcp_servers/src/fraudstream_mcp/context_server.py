
"""fraud-context: what the platform knows about a customer, a merchant or a handbook chunk."""

from typing import Annotated, Any
from urllib.parse import quote

from mcp.server.mcpserver import MCPServer
from pydantic import Field

from fraudstream_mcp.common import Api, add_probes, counted

NAME = "fraud-context"


def build(context_api: Api) -> MCPServer:
    server = MCPServer(
        NAME,
        instructions="Look up what the platform knows. Use ids exactly as the analyst wrote them.",
    )

    @server.tool()
    async def get_customer_features(
        customer_id: Annotated[str, Field(description="e.g. cust_00090571")],
    ) -> dict[str, Any]:
        """Recent activity of one customer from the online feature store: payment counts, amounts, distinct merchants and declines over 7, 30 and 90 days, with when each group was computed."""
        with counted(NAME, "get_customer_features"):
            # Encoded, so an id with '/' or '?' can only ever be a wrong id.
            return await context_api.get(f"/v1/customers/{quote(customer_id, safe='')}")

    @server.tool()
    async def get_merchant_features(
        merchant_id: Annotated[str, Field(description="e.g. merch_00011503")],
    ) -> dict[str, Any]:
        """Recent activity and risk of one merchant: payment counts, amounts, fraud rate, declines and burst ratio, with when they were computed."""
        with counted(NAME, "get_merchant_features"):
            return await context_api.get(f"/v1/merchants/{quote(merchant_id, safe='')}")

    @server.tool()
    async def get_chunk(
        chunk_id: Annotated[str, Field(description="a chunk id from search_handbook, e.g. handbook/typology-card-testing#signs")],
    ) -> dict[str, Any]:
        """One section of the fraud handbook, a model card or a feature page, by its chunk id, to quote it in full."""
        with counted(NAME, "get_chunk"):
            return await context_api.get("/v1/chunks", chunk_id=chunk_id)

    add_probes(server, [context_api])
    return server
