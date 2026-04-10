"""Logging utilities for ClickEngine notebooks and jobs."""

import json
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession

from src.utils.schemas import LOG_SCHEMA


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
    ) -> None:
        super().__init__(level)
        self._spark = spark
        self._table = table
        self._max_buffer_size = max_buffer_size
        self._buffer: list[Row] = []

    def emit(self, record: logging.LogRecord) -> None:
        exception_text = None
        if record.exc_info:
            exception_text = self.format(record)

        row = Row(
            timestamp=datetime.fromtimestamp(record.created, tz=timezone.utc),
            level=record.levelname,
            logger=record.name,
            message=record.getMessage(),
            exception=exception_text,
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

    Returns:
        Configured Logger instance.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())

    logging.root.handlers = []
    logging.root.addHandler(handler)
    logging.root.setLevel(level)

    logger = logging.getLogger(name)

    if spark and table:
        delta_handler = DeltaLogHandler(spark, table, level, max_buffer_size)
        logger.addHandler(delta_handler)

    return logger
