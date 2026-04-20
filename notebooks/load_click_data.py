# Databricks notebook source
from src.utils.logger import get_logger, flush_logger
from src.readers.stream_reader import read_stream_data
from src.writers.stream_writer import write_stream_data
from src.utils.schemas import CLICK_EVENT_SCHEMA

# COMMAND ----------

logger = get_logger(name = "load_click_data", spark = spark, table = "cda_dev.clickengine.logs")
logger.info("Starting click data load")

# COMMAND ----------

# DBTITLE 1,Cell 3
job_config = {
    "read_format": "cloudFiles",
    "source_location": "/Volumes/cda_dev/clickengine/test_data/click",
    "read_options": {
        "cloudFiles.format": "json",
        "cloudFiles.schemaLocation": "/Volumes/cda_dev/clickengine/schemas/click",
        "multiLine": "true",
    },
    "schema": CLICK_EVENT_SCHEMA,
    "checkpoint_location": "/Volumes/cda_dev/clickengine/checkpoints/load_click/checkpoint",
    "target_table": "cda_dev.clickengine.clicks",
    "mode": "append",
    "trigger": {"availableNow": True},
    "write_format": "delta"
}

# COMMAND ----------

df = read_stream_data(spark, job_config, logger)

write_stream_data(df, job_config, logger)

# COMMAND ----------

flush_logger(logger)
