
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_json, struct


def detect_invalid_payments(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    invalid_mask = (
        col("event_id").isNull() |
        col("gateway").isNull() |
        col("status").isNull() |
        col("order_id").isNull() |
        col("amount").isNull()
    )

    valid_df = df.filter(~invalid_mask)
    invalid_df = df.filter(invalid_mask)

    return valid_df, invalid_df


def prepare_dlq_message(df: DataFrame, reason: str) -> DataFrame:
    # Attach reason before serializing so it's included in the Kafka message body
    return (
        df
        .withColumn("reason", lit(reason))
        .withColumn("value", to_json(struct("*")))
        .withColumn("key", col("order_id"))
        .select("key", "value")
    )


def write_to_dlq(df: DataFrame, kafka_bootstrap: str, dlq_topic: str) -> None:
    if df.isEmpty():
        return

    (
        df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", dlq_topic)
        .save()
    )