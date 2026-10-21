"""Tests for the context API's stores, with Feast mocked, Redis faked and Postgres mocked.

FeatureStore is replaced where the stores look it up, so no registry database is
needed. RepoConfig stays real, so a setting Feast would refuse still fails.
"""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import fakeredis
import psycopg
import pytest

from fraudstream_ml.dataset import BATCH_FEATURE_VIEWS

from fraudstream_api.context import stores
from fraudstream_api.context.settings import Settings
from fraudstream_api.context.stores import (
    CUSTOMER_VIEWS,
    MERCHANT_VIEWS,
    ContextStores,
    views_answer,
)

STORED_AT = datetime(2026, 6, 29, 16, 46, 40, tzinfo=UTC)
CHUNK_ID = "handbook/typology-card-testing#signs"


def feast_answer(views, stored_at: int) -> dict:
    """What Feast's to_dict(include_event_timestamps=True) gives for one entity."""

    answer = {}
    for view in views:
        for index, name in enumerate(BATCH_FEATURE_VIEWS[view]):
            answer[name] = [float(index) if stored_at else None]
            answer[f"{name}__ts"] = [stored_at]
    return answer


def responding(answer: dict) -> MagicMock:
    response = MagicMock()
    response.to_dict.return_value = answer
    return response


@pytest.fixture
def settings() -> Settings:
    return Settings(
        postgres_user="u", postgres_password="p", pgvector_user="v", pgvector_password="w"
    )


@pytest.fixture
def feast(monkeypatch) -> dict:
    """The two mocked Feast stores, by the project each was built for."""

    built = {}

    def build(config):
        store = MagicMock()
        store.close = AsyncMock()
        built[config.project] = store
        return store

    monkeypatch.setattr(stores, "FeatureStore", MagicMock(side_effect=lambda config: build(config)))
    return built


@pytest.fixture
def redis_server(monkeypatch) -> fakeredis.FakeServer:
    server = fakeredis.FakeServer()
    monkeypatch.setattr(stores.aioredis, "Redis", lambda **_: fakeredis.FakeAsyncRedis(server=server))
    return server


@pytest.fixture
def pgvector(monkeypatch) -> AsyncMock:
    connect = AsyncMock(return_value=AsyncMock())
    monkeypatch.setattr(stores.psycopg.AsyncConnection, "connect", connect)
    return connect


@pytest.fixture
def context(settings, feast, redis_server, pgvector) -> ContextStores:
    return ContextStores(settings)


def test_a_view_the_store_holds_has_its_time_and_values():
    views = views_answer(feast_answer(CUSTOMER_VIEWS, int(STORED_AT.timestamp())), CUSTOMER_VIEWS)
    assert set(views) == set(CUSTOMER_VIEWS)
    assert views["customer_rolling_features"]["as_of"] == STORED_AT
    assert views["customer_rolling_features"]["features"]["txn_count_7d"] == 0.0


def test_a_view_the_store_does_not_hold_has_no_time_and_no_values():
    views = views_answer(feast_answer(MERCHANT_VIEWS, 0), MERCHANT_VIEWS)
    assert views == {"merchant_risk_features": {"as_of": None, "features": {}}}


def test_the_chunks_come_from_the_rag_project_in_pgvector(context, feast):
    configs = {call.kwargs["config"].project: call.kwargs["config"] for call in stores.FeatureStore.call_args_list}
    assert configs["fraudstream"].online_store.type == "redis"
    rag = configs["fraudstream_rag"].online_store
    assert (rag.type, rag.host, rag.database, rag.db_schema, rag.vector_enabled) == (
        "postgres", "pgvector", "llm", "rag", True
    )


@pytest.mark.parametrize(
    ("read", "key", "views"),
    [("customer", "customer_id", CUSTOMER_VIEWS), ("merchant", "merchant_id", MERCHANT_VIEWS)],
)
def test_an_entity_read_asks_for_its_own_views_by_its_own_key(context, feast, read, key, views):
    store = feast["fraudstream"]
    store.get_online_features_async = AsyncMock(
        return_value=responding(feast_answer(views, int(STORED_AT.timestamp())))
    )

    answer = asyncio.run(getattr(context, read)("id_1"))

    call = store.get_online_features_async.call_args.kwargs
    assert call["entity_rows"] == [{key: "id_1"}]
    assert {ref.split(":")[0] for ref in call["features"]} == set(views)
    assert set(answer) == set(views)


def test_a_chunk_is_read_by_its_id(context, feast):
    feast["fraudstream_rag"].get_online_features_async = AsyncMock(
        return_value=responding(
            {"chunk_id": [CHUNK_ID], "doc_id": ["handbook/typology-card-testing"],
             "title": ["Card testing"], "heading": ["Signs"], "text": ["Many small payments."]}
        )
    )
    assert asyncio.run(context.chunk(CHUNK_ID)) == {
        "chunk_id": CHUNK_ID, "doc_id": "handbook/typology-card-testing",
        "title": "Card testing", "heading": "Signs", "text": "Many small payments.",
    }
    assert feast["fraudstream_rag"].get_online_features_async.call_args.kwargs["entity_rows"] == [
        {"chunk_id": CHUNK_ID}
    ]


def test_a_chunk_the_store_does_not_hold_is_none(context, feast):
    feast["fraudstream_rag"].get_online_features_async = AsyncMock(
        return_value=responding(
            {"chunk_id": ["nope"], "doc_id": [None], "title": [None], "heading": [None], "text": [None]}
        )
    )
    assert asyncio.run(context.chunk("nope")) is None


def test_search_is_the_pipelines_own_search_on_the_rag_store(context, feast, monkeypatch):
    calls = []
    monkeypatch.setattr(
        stores, "search", lambda store, question, top_k: calls.append((store, question, top_k)) or ["hit"]
    )
    assert asyncio.run(context.search("card testing", 3)) == ["hit"]
    assert calls == [(feast["fraudstream_rag"], "card testing", 3)]


def test_start_warms_both_stores_and_the_model(context, feast, monkeypatch):
    embedded = []
    monkeypatch.setattr(stores, "embed", lambda texts: embedded.append(texts))
    feast["fraudstream"].get_online_features_async = AsyncMock(
        return_value=responding(feast_answer(CUSTOMER_VIEWS, 0))
    )
    feast["fraudstream_rag"].get_online_features_async = AsyncMock(
        return_value=responding(
            {"chunk_id": ["w"], "doc_id": [None], "title": [None], "heading": [None], "text": [None]}
        )
    )
    asyncio.run(context.start())
    assert feast["fraudstream"].get_online_features_async.called
    assert feast["fraudstream_rag"].get_online_features_async.called
    assert embedded == [["warm-up"]]


def test_ready_when_redis_and_pgvector_both_answer(context):
    assert asyncio.run(context.is_ready()) is True


def test_not_ready_when_pgvector_refuses(context, pgvector):
    pgvector.side_effect = psycopg.OperationalError("connection refused")
    assert asyncio.run(context.is_ready()) is False


def test_not_ready_when_redis_is_down(context, redis_server):
    redis_server.connected = False
    assert asyncio.run(context.is_ready()) is False


def test_close_closes_both_stores(context, feast):
    asyncio.run(context.close())
    feast["fraudstream"].close.assert_awaited_once()
    feast["fraudstream_rag"].close.assert_awaited_once()
