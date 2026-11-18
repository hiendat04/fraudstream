"""Tests for the context API's stores, with Feast mocked, Redis faked and Postgres mocked.

FeatureStore is replaced where the stores look it up, so no registry database is
needed. RepoConfig stays real, so a setting Feast would refuse still fails.
"""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import fakeredis
import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict

from fraudstream_ml.dataset import BATCH_FEATURE_VIEWS

from fraudstream_api.context import stores
from fraudstream_api.context.settings import Settings
from fraudstream.graph.ring import RING_FEATURES, device_neighbours_query
from fraudstream_api.context.stores import (
    CUSTOMER_VIEWS,
    MERCHANT_VIEWS,
    NEWEST_PAYMENT,
    WINDOW_SECONDS,
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
    server.options = {}

    def connect(**options):
        server.options = options
        return fakeredis.FakeAsyncRedis(server=server)

    monkeypatch.setattr(stores.aioredis, "Redis", connect)
    return server


@pytest.fixture
def pgvector(monkeypatch) -> AsyncMock:
    connect = AsyncMock(return_value=AsyncMock())
    monkeypatch.setattr(stores.psycopg.AsyncConnection, "connect", connect)
    return connect


NEWEST = 1_782_172_761  # 2026-06-22T23:59:21Z
COUNTS = {"device_id": "dev_ring_0001", "customer_count": 240, "fraud_customer_count": 61, "ip_customer_count": 255}
NEIGHBOURS = [{"customer_id": "cust_00090571", "payments": 4, "fraud": True}]


class FakeGraph:
    """Stands in for the neo4j async driver, its session and its results. Records every query."""

    def __init__(self, newest=NEWEST):
        self.newest = newest
        self.runs = []
        self.closed = False
        self.opened_with = None

    def session(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def run(self, query, **params):
        self.runs.append((query, params))
        rows = {NEWEST_PAYMENT: [{"newest": self.newest}], RING_FEATURES: [COUNTS]}.get(query, NEIGHBOURS)
        return SimpleNamespace(data=AsyncMock(return_value=rows))

    async def close(self):
        self.closed = True


@pytest.fixture
def graph(monkeypatch) -> FakeGraph:
    fake = FakeGraph()

    def driver(uri, **options):
        fake.opened_with = (uri, options)
        return fake

    monkeypatch.setattr(stores.AsyncGraphDatabase, "driver", driver)
    return fake


@pytest.fixture
def context(settings, feast, redis_server, pgvector, graph) -> ContextStores:
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


def test_both_projects_share_one_cached_sql_registry_in_postgres(context, feast):
    for call in stores.FeatureStore.call_args_list:
        config = call.kwargs["config"]
        assert (config.provider, config.entity_key_serialization_version) == ("local", 3)
        registry = config.registry
        assert (registry.registry_type, registry.path, registry.cache_ttl_seconds, registry.cache_mode) == (
            "sql", "postgresql+psycopg://u:p@postgres:5432/fraudstream", 60, "thread"
        )


def test_the_rag_store_logs_in_to_pgvector_without_tls(context, feast):
    rag = next(
        call.kwargs["config"].online_store
        for call in stores.FeatureStore.call_args_list
        if call.kwargs["config"].project == "fraudstream_rag"
    )
    assert (rag.port, rag.user, rag.password, rag.sslmode) == (5432, "v", "w", "disable")


def test_the_readiness_clients_give_up_after_one_second(context, redis_server, pgvector):
    assert redis_server.options == {"host": "redis", "port": 6379, "socket_timeout": 1, "socket_connect_timeout": 1}
    asyncio.run(context.is_ready())
    assert conninfo_to_dict(pgvector.call_args.args[0]) == {
        "host": "pgvector", "port": "5432", "dbname": "llm", "user": "v", "password": "w", "connect_timeout": "1",
    }


@pytest.mark.parametrize("hangs", ["redis", "pgvector"])
def test_not_ready_when_a_store_hangs(context, pgvector, monkeypatch, hangs):
    async def forever(*_):
        await asyncio.sleep(60)

    if hangs == "redis":
        monkeypatch.setattr(context._redis, "ping", forever)
    else:
        pgvector.side_effect = forever
    assert asyncio.run(context.is_ready()) is False


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
    # The view times come from the event timestamps, so they must be asked for.
    store.get_online_features_async.return_value.to_dict.assert_called_once_with(include_event_timestamps=True)
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
    call = feast["fraudstream_rag"].get_online_features_async.call_args.kwargs
    assert call["entity_rows"] == [{"chunk_id": CHUNK_ID}]
    assert call["features"] == [
        f"document_chunks:{name}" for name in ("doc_id", "title", "heading", "text")
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


def test_close_closes_both_stores_and_the_graph(context, feast, graph):
    asyncio.run(context.close())
    feast["fraudstream"].close.assert_awaited_once()
    feast["fraudstream_rag"].close.assert_awaited_once()
    assert graph.closed


def test_the_graph_is_memgraph_from_the_settings_with_a_short_connect_timeout(context, graph):
    uri, options = graph.opened_with
    assert uri == "bolt://memgraph:7687"
    assert options["connection_timeout"] <= 2


def test_a_device_answer_has_its_counts_and_customers(context, graph):
    found = asyncio.run(context.device_neighbours("dev_ring_0001"))
    assert found == {
        **COUNTS,
        "window_end": datetime(2026, 6, 22, 23, 59, 21, tzinfo=UTC),
        "customers": NEIGHBOURS,
    }


def test_the_window_ends_at_the_newest_payment(context, graph):
    """The data is historical: thirty days back from now would find nothing."""

    asyncio.run(context.device_neighbours("dev_ring_0001"))
    window = {"start": NEWEST + 1 - WINDOW_SECONDS, "end": NEWEST + 1}
    assert graph.runs == [
        (NEWEST_PAYMENT, {"device_id": "dev_ring_0001"}),
        (RING_FEATURES, {"device_ids": ["dev_ring_0001"], **window}),
        (device_neighbours_query, {"device_id": "dev_ring_0001", **window}),
    ]


def test_a_device_with_no_payment_is_none(context, graph):
    graph.newest = None
    assert asyncio.run(context.device_neighbours("dev_nobody")) is None
    assert len(graph.runs) == 1


def test_a_graph_that_hangs_gives_up(context, graph, monkeypatch):
    async def hang(query, **params):
        await asyncio.sleep(10)

    monkeypatch.setattr(stores, "GRAPH_TIMEOUT", 0.01)
    graph.run = hang
    with pytest.raises(TimeoutError):
        asyncio.run(context.device_neighbours("dev_ring_0001"))


def test_readiness_does_not_ask_the_graph(context, graph):
    assert asyncio.run(context.is_ready()) is True
    assert graph.runs == []
