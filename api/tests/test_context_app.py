"""Tests for the context API's endpoints and probes, with both stores faked."""

import psycopg
import pytest
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError

from fraudstream_api.context import app as context_app
from fraudstream_api.context.app import create_app

CHUNK = {
    "chunk_id": "handbook/typology-card-testing#signs",
    "doc_id": "handbook/typology-card-testing",
    "title": "Card testing",
    "heading": "Signs",
    "text": "Many small payments on one card in a few minutes.",
}
HELD = {
    "customer_rolling_features": {"as_of": "2026-06-29T16:46:40Z", "features": {"txn_count_7d": 3}},
    "customer_orders_90d_features": {"as_of": None, "features": {}},
}
NOTHING = {"merchant_risk_features": {"as_of": None, "features": {}}}


class FakeStores:
    """Stands in for both Feast stores. Records what it was asked."""

    def __init__(self, *, views=None, chunk=CHUNK, ready=True, error=None):
        self.views = views if views is not None else HELD
        self.found_chunk = chunk
        self.ready = ready
        self.error = error
        self.asked = []

    async def start(self):
        pass

    async def _answer(self, what, value):
        self.asked.append(what)
        if self.error:
            raise self.error
        return value

    async def customer(self, customer_id):
        return await self._answer(("customer", customer_id), self.views)

    async def merchant(self, merchant_id):
        return await self._answer(("merchant", merchant_id), self.views)

    async def chunk(self, chunk_id):
        return await self._answer(("chunk", chunk_id), self.found_chunk)

    async def search(self, question, top_k):
        return await self._answer(("search", question, top_k), [{**CHUNK, "distance": 0.12}][:top_k])

    async def is_ready(self):
        return self.ready

    async def close(self):
        pass


def client_for(context_settings, stores) -> TestClient:
    return TestClient(create_app(context_settings, stores=stores))


def test_a_customer_answer_has_every_view_with_its_time(context_client, fake_stores):
    response = context_client.get("/v1/customers/cust_00090571")
    assert response.status_code == 200
    body = response.json()
    assert body["entity_id"] == "cust_00090571"
    assert body["views"]["customer_rolling_features"]["features"] == {"txn_count_7d": 3}
    assert body["views"]["customer_orders_90d_features"]["as_of"] is None
    assert fake_stores.asked == [("customer", "cust_00090571")]


def test_a_merchant_is_read_as_a_merchant(context_client, fake_stores):
    assert context_client.get("/v1/merchants/merch_00011503").status_code == 200
    assert fake_stores.asked == [("merchant", "merch_00011503")]


def test_an_id_with_nothing_stored_is_not_found_and_says_so(context_settings):
    with client_for(context_settings, FakeStores(views=NOTHING)) as client:
        response = client.get("/v1/merchants/merch_1")
    assert response.status_code == 404
    assert response.json()["detail"] == "the online store has no features for merchant merch_1"


def test_redis_down_is_a_503(context_settings):
    with client_for(context_settings, FakeStores(error=RedisConnectionError("down"))) as client:
        response = client.get("/v1/customers/cust_1")
    assert response.status_code == 503
    assert response.json()["detail"] == "the online store cannot be reached"


def test_a_chunk_id_with_a_hash_and_slashes_is_found(context_client, fake_stores):
    response = context_client.get("/v1/chunks", params={"chunk_id": CHUNK["chunk_id"]})
    assert response.status_code == 200
    assert response.json() == CHUNK
    assert fake_stores.asked == [("chunk", CHUNK["chunk_id"])]


def test_an_unknown_chunk_is_not_found(context_settings):
    with client_for(context_settings, FakeStores(chunk=None)) as client:
        response = client.get("/v1/chunks", params={"chunk_id": "nope"})
    assert response.status_code == 404
    assert response.json()["detail"] == "no chunk nope"


@pytest.mark.parametrize(
    "request_",
    [
        pytest.param(("get", "/v1/chunks", {"params": {"chunk_id": "x"}}), id="chunk"),
        pytest.param(("post", "/v1/search", {"json": {"question": "card testing"}}), id="search"),
    ],
)
def test_pgvector_down_is_a_503_with_a_message(context_settings, request_):
    method, path, kwargs = request_
    with client_for(context_settings, FakeStores(error=psycopg.OperationalError("down"))) as client:
        response = getattr(client, method)(path, **kwargs)
    assert response.status_code == 503
    assert response.json()["detail"] == "the document store cannot be reached"


def test_search_returns_hits_and_asks_for_five_by_default(context_client, fake_stores):
    response = context_client.post("/v1/search", json={"question": "  card testing  "})
    assert response.status_code == 200
    assert response.json()["hits"][0]["chunk_id"] == CHUNK["chunk_id"]
    assert fake_stores.asked == [("search", "card testing", 5)]


def test_search_refuses_fields_it_does_not_know(context_client):
    assert context_client.post("/v1/search", json={"question": "x", "k": 3}).status_code == 422


def test_liveness_checks_nothing_else(context_settings):
    with client_for(context_settings, FakeStores(ready=False)) as client:
        assert client.get("/livez").status_code == 200


@pytest.mark.parametrize(("ready", "status"), [(True, 200), (False, 503)])
def test_readiness_follows_the_stores(context_settings, ready, status):
    with client_for(context_settings, FakeStores(ready=ready)) as client:
        assert client.get("/readyz").status_code == status


def test_the_metrics_port_opens_with_the_app_and_closes_after(context_settings):
    app = create_app(context_settings.model_copy(update={"metrics_port": 0}), stores=FakeStores())
    with TestClient(app):
        assert app.state.metrics_server.server_port > 0
    assert app.state.metrics_server.socket.fileno() == -1


def test_traces_name_the_context_api_and_its_version(context_settings, monkeypatch):
    seen = {}
    monkeypatch.setattr(context_app, "enable_tracing", lambda app, **options: seen.update(options))
    create_app(context_settings, stores=FakeStores())
    assert (seen["service"], seen["version"]) == ("context-api", context_settings.app_version)
