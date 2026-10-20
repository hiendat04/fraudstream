"""Reads the fraud features from Redis and the document chunks from pgvector, through Feast."""

import asyncio
from datetime import UTC, datetime

import psycopg
from feast import FeatureStore, RepoConfig
from psycopg.conninfo import make_conninfo
from redis import asyncio as aioredis
from redis.exceptions import RedisError

from fraudstream_ml.dataset import BATCH_FEATURE_VIEWS
from fraudstream_rag.embedding import embed
from fraudstream_rag.online import FEATURE_VIEW, JOIN_KEY
from fraudstream_rag.retrieval import RETURNED, search

from fraudstream_api.context.settings import Settings

CUSTOMER_VIEWS = ("customer_rolling_features", "customer_orders_90d_features")
MERCHANT_VIEWS = ("merchant_risk_features",)
READY_TIMEOUT = 1.0


def views_answer(answer: dict[str, list], views: tuple[str, ...]) -> dict[str, dict]:
    """One entry per view: when the store last received it, and its values.

    Feast reports 0 as the time of a view it holds nothing for.
    """

    found = {}
    for view in views:
        names = BATCH_FEATURE_VIEWS[view]
        # Every feature of a view shares one event time, so its first feature gives it.
        seconds = answer[f"{names[0]}__ts"][0]
        found[view] = {
            "as_of": datetime.fromtimestamp(seconds, UTC) if seconds else None,
            "features": {name: answer[name][0] for name in names} if seconds else {},
        }
    return found


class ContextStores:
    """Two Feast projects with only their registry and online stores. Spark is never loaded."""

    def __init__(self, settings: Settings):
        registry = {
            "registry_type": "sql",
            "path": (
                f"postgresql+psycopg://{settings.postgres_user}:"
                f"{settings.postgres_password.get_secret_value()}"
                f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
            ),
            "cache_ttl_seconds": 60,
            "cache_mode": "thread",
        }
        self._features = FeatureStore(
            config=RepoConfig(
                project="fraudstream",
                provider="local",
                entity_key_serialization_version=3,
                registry=registry,
                online_store={
                    "type": "redis",
                    "connection_string": f"{settings.redis_host}:{settings.redis_port}",
                },
            )
        )
        self._chunks = FeatureStore(
            config=RepoConfig(
                project="fraudstream_rag",
                provider="local",
                entity_key_serialization_version=3,
                registry=registry,
                online_store={
                    "type": "postgres",
                    "host": settings.pgvector_host,
                    "port": settings.pgvector_port,
                    "database": settings.pgvector_db,
                    "db_schema": "rag",
                    "user": settings.pgvector_user,
                    "password": settings.pgvector_password.get_secret_value(),
                    "sslmode": "disable",
                    "vector_enabled": True,
                },
            )
        )
        self._redis = aioredis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            socket_timeout=1,
            socket_connect_timeout=1,
        )
        self._pgvector = make_conninfo(
            host=settings.pgvector_host,
            port=settings.pgvector_port,
            dbname=settings.pgvector_db,
            user=settings.pgvector_user,
            password=settings.pgvector_password.get_secret_value(),
            connect_timeout=1,
        )

    async def start(self) -> None:
        """Load both registries and the embedding model before the first real request."""

        await self.customer("warm-up")
        await self.chunk("warm-up")
        await asyncio.to_thread(embed, ["warm-up"])

    async def customer(self, customer_id: str) -> dict[str, dict]:
        return await self._entity("customer_id", customer_id, CUSTOMER_VIEWS)

    async def merchant(self, merchant_id: str) -> dict[str, dict]:
        return await self._entity("merchant_id", merchant_id, MERCHANT_VIEWS)

    async def _entity(self, key: str, entity_id: str, views: tuple[str, ...]) -> dict[str, dict]:
        response = await self._features.get_online_features_async(
            features=[f"{view}:{name}" for view in views for name in BATCH_FEATURE_VIEWS[view]],
            entity_rows=[{key: entity_id}],
        )
        return views_answer(response.to_dict(include_event_timestamps=True), views)

    async def chunk(self, chunk_id: str) -> dict | None:
        response = await self._chunks.get_online_features_async(
            features=[f"{FEATURE_VIEW}:{name}" for name in RETURNED],
            entity_rows=[{JOIN_KEY: chunk_id}],
        )
        answer = response.to_dict()
        if answer["doc_id"][0] is None:
            return None
        return {"chunk_id": chunk_id, **{name: answer[name][0] for name in RETURNED}}

    async def search(self, question: str, top_k: int) -> list[dict]:
        # Embedding is CPU work and Feast's vector search has no async form.
        return await asyncio.to_thread(search, self._chunks, question, top_k)

    async def is_ready(self) -> bool:
        try:
            await asyncio.wait_for(self._redis.ping(), timeout=READY_TIMEOUT)
            connection = await asyncio.wait_for(
                psycopg.AsyncConnection.connect(self._pgvector), timeout=READY_TIMEOUT
            )
            await connection.close()
        except (RedisError, psycopg.Error, OSError, TimeoutError):
            return False
        return True

    async def close(self) -> None:
        # close(), never teardown(): teardown deletes the feature store's infrastructure.
        await self._features.close()
        await self._chunks.close()
        await self._redis.aclose()
