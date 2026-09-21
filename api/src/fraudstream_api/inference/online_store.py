"""Reads the model's features from the Feast online store without blocking."""

import asyncio
from datetime import UTC, datetime, timedelta

from feast import FeatureStore, RepoConfig
from redis import asyncio as aioredis
from redis.exceptions import RedisError

from fraudstream_ml.dataset import BATCH_FEATURE_REFS, BATCH_FEATURE_VIEWS

from fraudstream_api.inference.features import OnlineFeatures
from fraudstream_api.inference.settings import Settings


READY_TIMEOUT = 1.0


class FeastReader:
    """Feast with only its registry and online store. Spark is never loaded."""

    def __init__(self, settings: Settings):
        registry_url = (
            f"postgresql+psycopg://{settings.postgres_user}:"
            f"{settings.postgres_password.get_secret_value()}"
            f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
        )
        config = RepoConfig(
            project="fraudstream",
            provider="local",
            entity_key_serialization_version=3,
            registry={
                "registry_type": "sql",
                "path": registry_url,
                "cache_ttl_seconds": 60,
                "cache_mode": "thread",
            },
            online_store={
                "type": "redis",
                "connection_string": f"{settings.redis_host}:{settings.redis_port}",
            },
        )
        self._store = FeatureStore(config=config)
        self._ping = aioredis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            socket_timeout=1,
            socket_connect_timeout=1,
        )
        self._ttls: dict[str, timedelta] = {}

    async def start(self) -> None:
        """Load the registry and open Redis before the first real request."""

        self._ttls = {view: self._store.get_feature_view(view).ttl for view in BATCH_FEATURE_VIEWS}
        # The first read loads the registry and takes seconds. Every read after it is fast.
        await self.read("warm-up", "warm-up")

    def ttls(self) -> dict[str, timedelta]:
        return self._ttls

    async def read(self, customer_id: str, merchant_id: str) -> OnlineFeatures:
        response = await self._store.get_online_features_async(
            features=list(BATCH_FEATURE_REFS),
            entity_rows=[{"customer_id": customer_id, "merchant_id": merchant_id}],
        )
        answer = response.to_dict(include_event_timestamps=True)
        values = {
            name: answer[name][0] for names in BATCH_FEATURE_VIEWS.values() for name in names
        }
        # Every feature of a view shares one event time, so its first feature gives it.
        event_times = {
            view: _as_time(answer[f"{names[0]}__ts"][0])
            for view, names in BATCH_FEATURE_VIEWS.items()
        }
        return OnlineFeatures(values=values, event_times=event_times)

    async def is_ready(self) -> bool:
        try:
            # redis-py retries a refused connection, which can take several
            # seconds. The readiness probe gives up well before its own timeout.
            return bool(await asyncio.wait_for(self._ping.ping(), timeout=READY_TIMEOUT))
        except (RedisError, TimeoutError):
            return False

    async def close(self) -> None:
        # close(), never teardown(): teardown deletes the feature store's infrastructure.
        await self._store.close()
        await self._ping.aclose()


def _as_time(epoch_seconds: int) -> datetime | None:
    """Feast reports 0 when nothing is stored."""

    return datetime.fromtimestamp(epoch_seconds, UTC) if epoch_seconds else None
