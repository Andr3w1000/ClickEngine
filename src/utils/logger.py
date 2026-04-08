"""Logging utilities for ClickEngine notebooks and jobs."""

import json
import logging
import sys
from datetime import datetime, timezone


class _JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        record.msg = record.getMessage()
        record.args = None

        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.msg,
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a logger that writes JSON-formatted records to stdout.

    Args:
        name: Logger name — typically the calling module or notebook name.
        level: Logging level. Defaults to INFO.

    Returns:
        Configured Logger instance.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())

    logging.root.handlers = []
    logging.root.addHandler(handler)
    logging.root.setLevel(level)

    return logging.getLogger(name)
