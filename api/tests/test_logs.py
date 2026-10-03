"""Tests for the JSON log lines and the probe filter."""

import json
import logging
import sys

from opentelemetry.sdk.trace import TracerProvider

from fraudstream_api.logs import JsonFormatter, SkipProbes


def record(name="fraudstream_api.test", message="hello %s", args=("world",), level=logging.INFO):
    return logging.LogRecord(name, level, __file__, 1, message, args, None)


def access(path, status=200):
    return record(
        name="uvicorn.access",
        message='%s - "%s %s HTTP/%s" %d',
        args=("10.20.0.5:51234", "POST", path, "1.1", status),
    )


def test_a_line_is_one_json_object():
    line = json.loads(JsonFormatter().format(record()))
    assert line["message"] == "hello world"
    assert line["level"] == "info"
    assert line["logger"] == "fraudstream_api.test"
    assert line["time"].endswith("+00:00")


def test_outside_a_request_there_is_no_trace():
    line = json.loads(JsonFormatter().format(record()))
    assert "trace_id" not in line
    assert "span_id" not in line


def test_inside_a_request_the_line_carries_its_trace():
    tracer = TracerProvider().get_tracer("test")
    with tracer.start_as_current_span("request") as span:
        line = json.loads(JsonFormatter().format(record()))
    context = span.get_span_context()
    assert line["trace_id"] == format(context.trace_id, "032x")
    assert line["span_id"] == format(context.span_id, "016x")


def test_an_access_line_is_split_into_fields():
    line = json.loads(JsonFormatter().format(access("/v1/predict", 503)))
    assert (line["method"], line["path"], line["status"]) == ("POST", "/v1/predict", 503)
    assert line["client"] == "10.20.0.5:51234"


def test_an_error_keeps_its_traceback():
    try:
        raise ValueError("broken")
    except ValueError:
        failed = logging.LogRecord("x", logging.ERROR, __file__, 1, "failed", (), sys.exc_info())
    line = json.loads(JsonFormatter().format(failed))
    assert "ValueError: broken" in line["error"]


def test_probe_lines_are_dropped():
    assert SkipProbes().filter(access("/livez")) is False
    assert SkipProbes().filter(access("/readyz")) is False


def test_other_lines_are_kept():
    assert SkipProbes().filter(access("/v1/predict")) is True
    assert SkipProbes().filter(record()) is True
