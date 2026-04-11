"""Logging utilities for ClickEngine notebooks and jobs."""

import json
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession

from src.utils.schemas import LOG_SCHEMA


class _JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def __init__(self, metadata: dict | None = None) -> None:
        super().__init__()
        self._metadata = metadata or {}

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

        ctx = getattr(record, "_ctx", None)
        merged = {**self._metadata, **(ctx or {})}
        if merged:
            payload["metadata"] = merged

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, indent=4, sort_keys=True, default=str)


class DeltaLogHandler(logging.Handler):
    """A logging handler that buffers records and flushes them to a Delta table.

    Records are accumulated in memory and written as a batch when flush()
    is called or when the buffer reaches ``max_buffer_size``.

    The target Delta table must have the following columns:
        timestamp (TIMESTAMP), level (STRING), logger (STRING),
        message (STRING), exception (STRING)
    """

    def __init__(
        self,
        spark: SparkSession,
        table: str,
        level: int = logging.INFO,
        max_buffer_size: int = 100,
        metadata: dict | None = None,
    ) -> None:
        super().__init__(level)
        self._spark = spark
        self._table = table
        self._max_buffer_size = max_buffer_size
        self._metadata = json.dumps(metadata) if metadata else None
        self._buffer: list[Row] = []

    def emit(self, record: logging.LogRecord) -> None:
        exception_text = None
        if record.exc_info:
            exception_text = self.format(record)

        ctx = getattr(record, "_ctx", None)
        if ctx:
            base = json.loads(self._metadata) if self._metadata else {}
            merged = json.dumps({**base, **ctx})
        else:
            merged = self._metadata

        row = Row(
            timestamp=datetime.fromtimestamp(record.created, tz=timezone.utc),
            level=record.levelname,
            logger=record.name,
            message=record.getMessage(),
            exception=exception_text,
            metadata=merged,
        )
        self._buffer.append(row)

        if len(self._buffer) >= self._max_buffer_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return

        rows, self._buffer = self._buffer, []
        df = self._spark.createDataFrame(rows, schema=LOG_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(self._table)


def get_logger(
    name: str,
    level: int = logging.INFO,
    spark: SparkSession | None = None,
    table: str | None = None,
    max_buffer_size: int = 100,
    metadata: dict | None = None,
) -> logging.Logger:
    """Return a logger that writes JSON-formatted records to stdout.

    If ``spark`` and ``table`` are provided, a DeltaLogHandler is also
    attached so every log record is buffered and flushed to the given
    Delta table automatically.

    Args:
        name: Logger name — typically the calling module or notebook name.
        level: Logging level. Defaults to INFO.
        spark: Active SparkSession. Required for Delta logging.
        table: Fully qualified Delta table name (e.g. catalog.schema.logs).
        max_buffer_size: Flush to Delta after this many records.
        metadata: Extra context added to every log record, e.g.
            {"notebook": "load_clicks", "job_id": "123", "run_id": "456"}.

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    if not logger.handlers:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(_JsonFormatter(metadata))
        logger.addHandler(stdout_handler)

    has_delta = any(isinstance(h, DeltaLogHandler) for h in logger.handlers)
    if spark and table and not has_delta:
        logger.addHandler(
            DeltaLogHandler(spark, table, level, max_buffer_size, metadata)
        )

    return logger


def flush_logger(logger: logging.Logger) -> None:
    """Flush all DeltaLogHandlers attached to the logger."""
    for handler in logger.handlers:
        if isinstance(handler, DeltaLogHandler):
            handler.flush()


def append_extra(**kwargs) -> dict:
    """Build an ``extra`` dict for per-message context.

    Usage:
        logger.info("Batch done", extra=append_extra(row_count=500, batch=3))
    """
    return {"_ctx": kwargs}
