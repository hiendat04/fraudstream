"""The context API: what the platform knows about a customer, a merchant or a question."""

from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, HTTPException, Path, Query
from fastapi.responses import JSONResponse
from psycopg import Error as PostgresError
from redis.exceptions import RedisError

from fraudstream_api.context.schemas import (
    ENTITY_ID_PATTERN,
    Chunk,
    EntityFeatures,
    SearchAnswer,
    SearchRequest,
)
from fraudstream_api.context.settings import Settings
from fraudstream_api.metrics import RequestMetrics, serve_metrics
from fraudstream_api.tracing import enable_tracing
from fraudstream_api.versioning import VersionHeader

EntityId = Annotated[str, Path(pattern=ENTITY_ID_PATTERN)]
# Chunk ids carry '#' and '/', so they travel as a query parameter, never in the path.
ChunkId = Annotated[str, Query(min_length=1, max_length=200)]


def create_app(settings: Settings | None = None, *, stores=None) -> FastAPI:
    """Build the app. Tests pass their own stores; nothing else does."""

    config = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        from fraudstream_api.context.stores import ContextStores

        app.state.metrics_server = (
            serve_metrics(config.metrics_port) if config.metrics_port is not None else None
        )
        app.state.stores = stores or ContextStores(config)
        # Uvicorn opens its port only after this returns, so no request meets a cold store.
        await app.state.stores.start()
        yield
        await app.state.stores.close()
        if app.state.metrics_server is not None:
            app.state.metrics_server.shutdown()
            app.state.metrics_server.server_close()
        if tracer is not None:
            tracer.shutdown()

    app = FastAPI(title="FraudStream context API", lifespan=lifespan)
    app.add_middleware(VersionHeader, version=config.app_version)
    app.add_middleware(RequestMetrics)
    tracer = enable_tracing(
        app,
        service="context-api",
        version=config.app_version,
        endpoint=config.otel_exporter_otlp_endpoint,
    )

    async def entity(read, kind: str, entity_id: str) -> EntityFeatures:
        try:
            views = await read(entity_id)
        except RedisError as error:
            raise HTTPException(503, "the online store cannot be reached") from error
        if all(view["as_of"] is None for view in views.values()):
            raise HTTPException(404, f"the online store has no features for {kind} {entity_id}")
        return EntityFeatures(entity_id=entity_id, views=views)

    @app.get("/v1/customers/{customer_id}", response_model=EntityFeatures)
    async def customer(customer_id: EntityId) -> EntityFeatures:
        return await entity(app.state.stores.customer, "customer", customer_id)

    @app.get("/v1/merchants/{merchant_id}", response_model=EntityFeatures)
    async def merchant(merchant_id: EntityId) -> EntityFeatures:
        return await entity(app.state.stores.merchant, "merchant", merchant_id)

    @app.get("/v1/chunks", response_model=Chunk)
    async def chunk(chunk_id: ChunkId) -> Chunk:
        try:
            found = await app.state.stores.chunk(chunk_id)
        except PostgresError as error:
            raise HTTPException(503, "the document store cannot be reached") from error
        if found is None:
            raise HTTPException(404, f"no chunk {chunk_id}")
        return Chunk(**found)

    @app.post("/v1/search", response_model=SearchAnswer)
    async def search(request: SearchRequest) -> SearchAnswer:
        try:
            hits = await app.state.stores.search(request.question, request.top_k)
        except PostgresError as error:
            raise HTTPException(503, "the document store cannot be reached") from error
        return SearchAnswer(hits=hits)

    @app.get("/livez")
    async def livez() -> dict[str, str]:
        # Checks nothing else. Restarting this pod cannot fix Redis or pgvector.
        return {"status": "alive"}

    @app.get("/readyz")
    async def readyz():
        if await app.state.stores.is_ready():
            return {"status": "ready"}
        return JSONResponse(
            {"status": "not ready", "reason": "the online store or pgvector does not answer"}, 503
        )

    return app
