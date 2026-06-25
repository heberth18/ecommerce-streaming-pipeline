
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType, TimestampType
)

ORDER_SCHEMA = StructType([
    StructField("event_id",        StringType(),    nullable=False),
    StructField("event_type",      StringType(),    nullable=False),
    StructField("event_timestamp", StringType(),    nullable=False),
    StructField("order_id",        StringType(),    nullable=False),
    StructField("customer_id",     StringType(),    nullable=False),
    StructField("amount",          FloatType(),     nullable=False),
    StructField("currency",        StringType(),    nullable=False),
    StructField("items_count",     IntegerType(),   nullable=False),
])

PAYMENT_SCHEMA = StructType([
    StructField("event_id",        StringType(),    nullable=False),
    StructField("event_type",      StringType(),    nullable=False),
    StructField("event_timestamp", StringType(),    nullable=False),
    StructField("payment_id",      StringType(),    nullable=False),
    StructField("order_id",        StringType(),    nullable=False),
    StructField("amount",          FloatType(),     nullable=False),
    StructField("currency",        StringType(),    nullable=False),
    StructField("gateway",         StringType(),    nullable=False),
    StructField("status",          StringType(),    nullable=False),
    StructField("failure_reason",  StringType(),    nullable=True),
])