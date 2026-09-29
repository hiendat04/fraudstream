"""Tests for the request metrics both APIs share."""

import urllib.request

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

from fraudstream_api.metrics import RequestMetrics, serve_metrics


def requests(route: str, status: str, method: str = "GET") -> float:
    labels = {"method": method, "route": route, "status": status}
    return REGISTRY.get_sample_value("http_requests_total", labels) or 0.0


def timed(route: str, method: str = "GET") -> float:
    labels = {"method": method, "route": route}
    return REGISTRY.get_sample_value("http_request_duration_seconds_count", labels) or 0.0


def client() -> TestClient:
    app = FastAPI()
    app.add_middleware(RequestMetrics)

    @app.get("/things/{name}")
    async def thing(name: str):
        if name == "missing":
            raise HTTPException(404, "no such thing")
        return {"name": name}

    @app.get("/boom")
    async def boom():
        raise RuntimeError("broken")

    @app.get("/livez")
    async def livez():
        return {"status": "alive"}

    return TestClient(app, raise_server_exceptions=False)


def test_counts_by_route_template_not_by_path():
    before = requests("/things/{name}", "200")
    with client() as http:
        http.get("/things/a")
        http.get("/things/b")
    assert requests("/things/{name}", "200") == before + 2
    assert REGISTRY.get_sample_value(
        "http_requests_total", {"method": "GET", "route": "/things/a", "status": "200"}
    ) is None


def test_counts_the_status_the_route_answered():
    before = requests("/things/{name}", "404")
    with client() as http:
        http.get("/things/missing")
    assert requests("/things/{name}", "404") == before + 1


def test_an_unhandled_error_counts_as_a_500():
    before = requests("/boom", "500")
    with client() as http:
        assert http.get("/boom").status_code == 500
    assert requests("/boom", "500") == before + 1


def test_unknown_paths_share_one_label():
    before = requests("unmatched", "404")
    with client() as http:
        http.get("/wp-admin")
        http.get("/.env")
    assert requests("unmatched", "404") == before + 2


def test_odd_methods_share_one_label():
    before = requests("/things/{name}", "405", method="OTHER")
    with client() as http:
        http.request("PROPFIND", "/things/a")
    assert requests("/things/{name}", "405", method="OTHER") == before + 1


def test_probes_are_not_counted():
    with client() as http:
        http.get("/livez")
    assert requests("/livez", "200") == 0.0


def test_every_counted_request_is_timed():
    before = timed("/things/{name}")
    with client() as http:
        http.get("/things/a")
    assert timed("/things/{name}") == before + 1


def test_nothing_is_left_in_progress():
    with client() as http:
        http.get("/things/a")
        http.get("/boom")
    assert REGISTRY.get_sample_value("http_requests_in_progress") == 0.0


def test_the_metrics_port_serves_the_counters():
    server = serve_metrics(0)
    try:
        port = server.server_port
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as answer:
            body = answer.read().decode()
    finally:
        server.shutdown()
        server.server_close()
    assert "http_requests_total" in body
