"""Request metrics for both APIs, served on a port of their own."""

import time

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Around the SLA: p95 at most 500 ms and p99 at most 1 s.
LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)
METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"})
PROBES = frozenset({"/livez", "/readyz"})
UNMATCHED = "unmatched"

REQUESTS = Counter(
    "http_requests_total",
    "Requests answered, by route and status code.",
    ["method", "route", "status"],
)
DURATION = Histogram(
    "http_request_duration_seconds",
    "Time taken to answer one request.",
    ["method", "route"],
    buckets=LATENCY_BUCKETS,
)
IN_PROGRESS = Gauge("http_requests_in_progress", "Requests being answered right now.")


class RequestMetrics:
    """Counts and times every request except the probes. A plain ASGI middleware."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] in PROBES:
            return await self.app(scope, receive, send)

        # What the caller gets if the app raises before it answers.
        status = 500

        async def send_and_note_status(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        method = scope["method"] if scope["method"] in METHODS else "OTHER"
        started = time.perf_counter()
        IN_PROGRESS.inc()
        try:
            await self.app(scope, receive, send_and_note_status)
        finally:
            IN_PROGRESS.dec()
            route = getattr(scope.get("route"), "path", UNMATCHED)
            REQUESTS.labels(method, route, str(status)).inc()
            DURATION.labels(method, route).observe(time.perf_counter() - started)


def serve_metrics(port: int):
    """Serve /metrics on its own port, in a thread. Port 0 picks a free one."""

    server, _thread = start_http_server(port)
    return server
