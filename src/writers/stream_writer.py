import logging

from pyspark.sql import DataFrame


def write_stream_data(
    df: DataFrame,
    job_config: dict,
    logger: logging.Logger,
) -> None:
    """Write a streaming DataFrame to a Delta table using Structured Streaming.

    Args:
        spark: Active SparkSession.
        df: Streaming DataFrame to write.
        job_config: Dictionary containing write configuration:
            - target_table: Fully qualified table name (catalog.schema.table).
            - checkpoint_location: Path for streaming checkpoint.
            - write_format: Output format (default: "delta").
            - output_mode: Streaming output mode (default: "append").
            - trigger: Trigger interval (default: "availableNow").
            - write_options: Additional writeStream options.

    Returns:
        None
    """
    target_table = job_config.get("target_table")
    checkpoint_location = job_config.get("checkpoint_location")
    write_format = job_config.get("write_format", "delta")
    output_mode = job_config.get("output_mode", "append")
    trigger = job_config.get("trigger", {"availableNow": True})
    write_options = job_config.get("write_options", {})

    logger.info(
        "Writing stream | format=%s | target=%s | mode=%s | checkpoint=%s",
        write_format,
        target_table,
        output_mode,
        checkpoint_location,
    )

    query = (
        df.writeStream
        .format(write_format)
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_location)
        .options(**write_options)
        .trigger(**trigger)
        .toTable(target_table)
    )

    logger.info("Stream write started | target=%s", target_table)

    return query
