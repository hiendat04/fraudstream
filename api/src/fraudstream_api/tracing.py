"""Traces for both APIs, sent to Jaeger over OTLP. Off unless an endpoint is set."""

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# The probes come every few seconds from every pod and would bury the real requests.
UNTRACED = "livez,readyz"


def traces_url(endpoint: str) -> str:
    return f"{endpoint.rstrip('/')}/v1/traces"


def enable_tracing(
    app,
    *,
    service: str,
    version: str,
    endpoint: str | None,
    processor: SpanProcessor | None = None,
) -> TracerProvider | None:
    """Trace every request, and every call the app makes to Redis or over HTTP."""

    if processor is None:
        if not endpoint:
            return None
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=traces_url(endpoint)))

    provider = TracerProvider(
        resource=Resource.create({"service.name": service, "service.version": version})
    )
    provider.add_span_processor(processor)
    # One span per request. The ASGI send and receive steps would add two more each.
    FastAPIInstrumentor.instrument_app(
        app, tracer_provider=provider, excluded_urls=UNTRACED, exclude_spans=["receive", "send"]
    )
    # These patch the libraries for the whole process, so the first app to ask wins.
    for library in (HTTPXClientInstrumentor(), RedisInstrumentor()):
        if not library.is_instrumented_by_opentelemetry:
            library.instrument(tracer_provider=provider)
    return provider
