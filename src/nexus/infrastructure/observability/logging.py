"""JSON logging to stdout. Vercel Functions and Daemons forward stdout/stderr
to the project's runtime logs (and on to log drains), so one JSON object per
line keeps the `extra=` fields used throughout the codebase queryable."""

import json
import logging
import sys
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

request_id_var: ContextVar[str | None] = ContextVar("nexus_request_id", default=None)
# End-to-end correlation ID: taken from the client's X-Correlation-ID (or the
# request ID), stored on the job, carried in the AMQP message and restored by
# the worker, so one ID ties together API, relay and worker log lines.
correlation_id_var: ContextVar[str | None] = ContextVar("nexus_correlation_id", default=None)

_RESERVED = frozenset(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        request_id = request_id_var.get()
        if request_id:
            entry["request_id"] = request_id
        correlation_id = correlation_id_var.get()
        if correlation_id:
            entry["correlation_id"] = correlation_id
        for key, value in vars(record).items():
            if key not in _RESERVED and not key.startswith("_"):
                entry[key] = value
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


_configured = False


def configure_logging(level: str = "INFO") -> None:
    global _configured
    if _configured:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    _configured = True
