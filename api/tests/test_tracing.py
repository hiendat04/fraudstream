"""Tests for tracing: which requests get a span, and how the trace reaches the next service."""

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import fakeredis
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from fraudstream_api.drift_detection.app import create_app as create_drift_app
from fraudstream_api.drift_detection.settings import Settings as DriftSettings
from fraudstream_api.inference.app import create_app as create_inference_app
from fraudstream_api.inference.model import ModelClient
from fraudstream_api.inference.settings import Settings as InferenceSettings
from fraudstream_api.tracing import enable_tracing, traces_url

from tests.test_inference_app import FakeModel, FakeReader

NOWHERE = "http://127.0.0.1:9"


def traced_app():
    app = FastAPI()

    @app.get("/things/{name}")
    async def thing(name: str):
        return {"name": name}

    @app.get("/livez")
    async def livez():
        return {"status": "alive"}

    spans = InMemorySpanExporter()
    provider = enable_tracing(
        app, service="test-api", version="t1", endpoint=None, processor=SimpleSpanProcessor(spans)
    )
    return app, spans, provider


def test_off_without_an_endpoint():
    app = FastAPI()
    assert enable_tracing(app, service="x", version="t", endpoint=None) is None
    assert enable_tracing(app, service="x", version="t", endpoint="") is None


def test_the_traces_path_is_added_once():
    assert traces_url("http://jaeger:4318") == "http://jaeger:4318/v1/traces"
    assert traces_url("http://jaeger:4318/") == "http://jaeger:4318/v1/traces"


def test_a_request_gets_one_span_named_by_its_route():
    app, spans, _ = traced_app()
    with TestClient(app) as client:
        client.get("/things/a")
    names = [span.name for span in spans.get_finished_spans()]
    assert names == ["GET /things/{name}"]


def test_spans_say_which_service_and_version():
    app, spans, _ = traced_app()
    with TestClient(app) as client:
        client.get("/things/a")
    resource = spans.get_finished_spans()[0].resource.attributes
    assert resource["service.name"] == "test-api"
    assert resource["service.version"] == "t1"


def test_probes_are_not_traced():
    app, spans, _ = traced_app()
    with TestClient(app) as client:
        client.get("/livez")
    assert spans.get_finished_spans() == ()


class Recorder(BaseHTTPRequestHandler):
    """A stand-in model that remembers the headers of the last call."""

    seen: dict = {}

    def do_POST(self):
        Recorder.seen = dict(self.headers)
        self.rfile.read(int(self.headers["Content-Length"]))
        body = json.dumps({"predictions": [0.5]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def test_a_call_to_the_model_carries_the_trace():
    _, _, provider = traced_app()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Recorder)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}/v1/models/fraud-detection:predict"

    async def score_inside_a_span():
        model = ModelClient(url, 5.0)
        with provider.get_tracer("test").start_as_current_span("parent") as parent:
            await model.score({"amount": 1.0})
        await model.close()
        return format(parent.get_span_context().trace_id, "032x")

    try:
        trace_id = asyncio.run(score_inside_a_span())
    finally:
        server.shutdown()
        server.server_close()
    header = {key.lower(): value for key, value in Recorder.seen.items()}["traceparent"]
    assert header.split("-")[1] == trace_id


def test_both_apps_trace_when_given_an_endpoint(drift_reference_path):
    inference = create_inference_app(
        InferenceSettings(postgres_user="u", postgres_password="p", otel_exporter_otlp_endpoint=NOWHERE),
        reader=FakeReader(),
        model=FakeModel(),
    )
    drift = create_drift_app(
        DriftSettings(reference_path=str(drift_reference_path), otel_exporter_otlp_endpoint=NOWHERE),
        redis=fakeredis.FakeAsyncRedis(decode_responses=True),
    )
    for app in (inference, drift):
        with TestClient(app) as client:
            assert client.get("/livez").status_code == 200
        assert app._is_instrumented_by_opentelemetry
