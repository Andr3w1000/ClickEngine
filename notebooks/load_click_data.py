# Databricks notebook source
from src.utils.logger import get_logger, flush_logger
from src.readers.stream_reader import read_stream_data
from src.writers.stream_writer import write_stream_data

# COMMAND ----------

logger = get_logger(name = "load_click_data", spark = spark, table = "cda_dev.clickengine.logs")
logger.info("Starting click data load")

# COMMAND ----------

# DBTITLE 1,Cell 3
NAMESPACE = "evhns-cda-dev"
EVENTHUB_NAME = "evh-cda-dev"
CONNECTION_STRING= "get_from_secret"

job_config = {
    "read_format": "kafka",
    "read_options": {
                "kafka.bootstrap.servers"  : f"{NAMESPACE}.servicebus.windows.net:9093",
                "kafka.security.protocol"  : "SASL_SSL",
                "kafka.sasl.mechanism"     : "PLAIN",
                "kafka.sasl.jaas.config"   : (
                    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
                    f'username="$ConnectionString" '
                    f'password="{CONNECTION_STRING}";'
                ),
                "subscribe"                : EVENTHUB_NAME,
                "startingOffsets"          : "earliest",
    },
    "checkpoint_location": "/Volumes/cda_dev/clickengine/checkpoints/load_click/checkpoint",
    "target_table": "cda_dev.clickengine.clicks",
    "output_mode": "append",
    "trigger": {"availableNow": True},
    "write_format": "delta"
}

# COMMAND ----------

df = read_stream_data(spark, job_config, logger)

write_stream_data(df, job_config, logger)

# COMMAND ----------

flush_logger(logger)
