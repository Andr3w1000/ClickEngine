import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def read_stream_data(
    spark: SparkSession,
    job_config: dict
) -> DataFrame:
    """Read data from source_location using the given format and options.

    Args:
        spark: Active SparkSession.
        source_location: Path to source data (ADLS path, UC Volume, etc.).
        read_format: Spark read format — e.g. "json", "parquet", "csv", "delta".
        read_options: Spark DataFrameReader options passed directly to .options().

    Returns:
        DataFrame containing the loaded data.

    Raises:
        AnalysisException: If the source path does not exist or the format is invalid.
    """
    logger.info(
        "Reading data | format=%s | source=%s | options=%s",
        job_config.get("read_format", "json"),
        job_config.get("source_location"),
        job_config.get("read_options", {}),
    )

    df = (spark.readStream
          .format(job_config.get("read_format", "json"))
          .options(**job_config.get("read_options", {}))
          .load(job_config.get("source_location")))
    logger.info("Read complete")

    return df
