"""JSON log lines that carry their trace, so a line in Kibana leads to its trace in Jaeger."""

import json
import logging
from datetime import UTC, datetime

from opentelemetry import trace

PROBES = ("/livez", "/readyz")


def _access_fields(record: logging.LogRecord) -> dict | None:
    """Uvicorn's access line: client, method, path, HTTP version, status."""

    if record.name != "uvicorn.access" or not isinstance(record.args, tuple) or len(record.args) != 5:
        return None
    client, method, path, _version, status = record.args
    return {"client": client, "method": method, "path": path, "status": int(status)}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        line = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        line.update(_access_fields(record) or {})
        context = trace.get_current_span().get_span_context()
        if context.is_valid:
            line["trace_id"] = format(context.trace_id, "032x")
            line["span_id"] = format(context.span_id, "016x")
        if record.exc_info:
            line["error"] = self.formatException(record.exc_info)
        return json.dumps(line)


class SkipProbes(logging.Filter):
    """Drops the probes' access lines, which come every few seconds from every pod."""

    def filter(self, record: logging.LogRecord) -> bool:
        fields = _access_fields(record)
        return not (fields and str(fields["path"]).startswith(PROBES))
