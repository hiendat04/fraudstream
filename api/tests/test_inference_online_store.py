"""Tests for the Feast reader, with Feast mocked and Redis faked.

FeatureStore is replaced where the reader looks it up, so no registry database
is needed. RepoConfig stays real, so a setting Feast would refuse still fails.
"""

import asyncio
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import fakeredis
import pytest

from fraudstream_ml.dataset import BATCH_FEATURE_REFS, BATCH_FEATURE_VIEWS

from fraudstream_api.inference import online_store
from fraudstream_api.inference.online_store import FeastReader
from fraudstream_api.inference.settings import Settings

STORED_AT = datetime(2026, 6, 29, 16, 46, 40, tzinfo=UTC)
FEATURE_NAMES = [name for names in BATCH_FEATURE_VIEWS.values() for name in names]
TTLS = {
    "customer_rolling_features": timedelta(days=31),
    "customer_orders_90d_features": timedelta(days=91),
    "merchant_risk_features": timedelta(days=31),
}


def feast_answer(stored_at: int) -> dict:
    """What Feast's to_dict(include_event_timestamps=True) gives for one row."""

    answer = {"customer_id": ["c"], "merchant_id": ["m"]}
    for index, name in enumerate(FEATURE_NAMES):
        answer[name] = [float(index)]
        answer[f"{name}__ts"] = [stored_at]
    return answer


@pytest.fixture
def feature_store(monkeypatch):
    """A mocked FeatureStore class. Its instance answers like Feast does."""

    store = MagicMock()
    store.get_feature_view.side_effect = lambda view: SimpleNamespace(ttl=TTLS[view])
    response = MagicMock()
    response.answer = feast_answer(int(STORED_AT.timestamp()))

    def to_dict(include_event_timestamps=False):
        # Feast leaves the __ts columns out unless they are asked for.
        if include_event_timestamps is True:
            return response.answer
        return {name: value for name, value in response.answer.items() if "__ts" not in name}

    response.to_dict.side_effect = to_dict
    store.get_online_features_async = AsyncMock(return_value=response)
    store.close = AsyncMock()
    feature_store_class = MagicMock(return_value=store)
    monkeypatch.setattr(online_store, "FeatureStore", feature_store_class)
    return feature_store_class


@pytest.fixture
def redis_server(monkeypatch):
    """Every Redis the reader opens is a fake one on this server."""

    server = fakeredis.FakeServer()
    opened = []

    def fake_redis(**options):
        opened.append(options)
        return fakeredis.FakeAsyncRedis(server=server)

    monkeypatch.setattr(online_store, "aioredis", SimpleNamespace(Redis=fake_redis))
    server.opened = opened
    return server


@pytest.fixture
def settings():
    return Settings(postgres_user="u", postgres_password="p")


@pytest.fixture
def reader(settings, feature_store, redis_server):
    return FeastReader(settings)


def test_the_registry_is_built_from_the_settings(reader, feature_store):
    config = feature_store.call_args.kwargs["config"]

    assert config.project == "fraudstream"
    assert config.registry.path == "postgresql+psycopg://u:p@postgres:5432/fraudstream"
    assert config.registry.cache_mode == "thread"
    assert config.registry.cache_ttl_seconds == 60
    assert config.online_store.connection_string == "redis:6379"


def test_the_password_is_read_from_the_secret_value(feature_store, redis_server):
    FeastReader(Settings(postgres_user="u", postgres_password="s3cret"))

    path = feature_store.call_args.kwargs["config"].registry.path
    assert ":s3cret@" in path
    assert "*" not in path


def test_the_ping_connection_gives_up_after_one_second(reader, redis_server):
    options = redis_server.opened[0]

    assert options["host"] == "redis"
    assert options["port"] == 6379
    assert options["socket_timeout"] == 1
    assert options["socket_connect_timeout"] == 1


def test_start_reads_every_ttl_and_warms_up(reader, feature_store):
    store = feature_store.return_value

    asyncio.run(reader.start())

    assert reader.ttls() == TTLS
    assert store.get_feature_view.call_count == len(BATCH_FEATURE_VIEWS)
    store.get_online_features_async.assert_awaited_once()


def test_a_read_asks_for_every_feature_of_one_customer_and_merchant(reader, feature_store):
    asyncio.run(reader.read("cust_1", "merch_2"))

    call = feature_store.return_value.get_online_features_async.call_args.kwargs
    assert call["features"] == list(BATCH_FEATURE_REFS)
    assert call["entity_rows"] == [{"customer_id": "cust_1", "merchant_id": "merch_2"}]


def test_a_read_returns_every_feature_and_each_views_time(reader):
    features = asyncio.run(reader.read("cust_1", "merch_2"))

    assert list(features.values) == FEATURE_NAMES
    assert features.values["txn_count_7d"] == 0.0
    assert features.event_times == {view: STORED_AT for view in BATCH_FEATURE_VIEWS}


def test_zero_means_nothing_is_stored(reader, feature_store):
    feature_store.return_value.get_online_features_async.return_value.answer = feast_answer(0)

    features = asyncio.run(reader.read("cust_1", "merch_2"))

    assert features.event_times == {view: None for view in BATCH_FEATURE_VIEWS}


def test_ready_when_redis_answers(reader):
    assert asyncio.run(reader.is_ready()) is True


def test_not_ready_when_redis_is_down(reader, redis_server):
    redis_server.connected = False

    assert asyncio.run(reader.is_ready()) is False


def test_not_ready_when_the_ping_hangs(reader, monkeypatch):
    async def hang():
        await asyncio.sleep(10)

    monkeypatch.setattr(online_store, "READY_TIMEOUT", 0.05)
    monkeypatch.setattr(reader._ping, "ping", hang)

    started = time.monotonic()
    assert asyncio.run(reader.is_ready()) is False
    assert time.monotonic() - started < 1


def test_close_never_tears_the_store_down(reader, feature_store):
    store = feature_store.return_value

    asyncio.run(reader.close())

    store.close.assert_awaited_once()
    store.teardown.assert_not_called()
