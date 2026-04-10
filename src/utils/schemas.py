"""PySpark schemas for ClickEngine event types."""

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

USER_EVENT_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("timestamp", TimestampType(), True),
])

CLICK_EVENT_SCHEMA = StructType([
    StructField("click_id",  StringType(),    False),  # UUID, not nullable
    StructField("user_id",   StringType(),    False),  # UUID, not nullable
    StructField("email",     StringType(),    True),
    StructField("location",  StringType(),    True),
    StructField("page",      StringType(),    True),
    StructField("browser",   StringType(),    True),
    StructField("device",    StringType(),    True),
    StructField("timestamp", TimestampType(), True),
])