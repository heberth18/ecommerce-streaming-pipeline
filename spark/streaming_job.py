
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, lit,
    window, count, sum as spark_sum, avg, when
)
from schemas import PAYMENT_SCHEMA
from dlq import detect_invalid_payments, prepare_dlq_message, write_to_dlq

KAFKA_BOOTSTRAP = "localhost:9093"
DLQ_TOPIC = "payments-dlq"
LATE_THRESHOLD_SECONDS = 120

GCP_PROJECT = os.environ["GCP_PROJECT"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
BIGQUERY_DATASET = os.environ["BIGQUERY_DATASET"]
GOOGLE_APPLICATION_CREDENTIALS = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

SPARK_HOME = os.path.dirname(os.path.abspath(__file__))
JARS = ",".join([
    f"{SPARK_HOME}/jars/gcs-connector-hadoop3-latest.jar",
    f"{SPARK_HOME}/jars/spark-bigquery-with-dependencies_2.12-0.34.0.jar",
    f"{SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar",
    f"{SPARK_HOME}/jars/kafka-clients-3.4.0.jar",
    f"{SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar",
    f"{SPARK_HOME}/jars/commons-pool2-2.11.1.jar",
])

spark = (
    SparkSession.builder
    .appName("payment-monitoring")
    .config("spark.jars", JARS)
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS)
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("parentProject", GCP_PROJECT)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "payments")
    .option("startingOffsets", "latest")
    .load()
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(from_json(col("json_value"), PAYMENT_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
)

valid_stream, invalid_stream = detect_invalid_payments(parsed_stream)

def send_invalid_to_dlq(batch_df, batch_id):
    dlq_ready = prepare_dlq_message(batch_df, reason="invalid_schema")
    write_to_dlq(dlq_ready, kafka_bootstrap=KAFKA_BOOTSTRAP, dlq_topic=DLQ_TOPIC)

invalid_query = (
    invalid_stream.writeStream
    .foreachBatch(send_invalid_to_dlq)
    .outputMode("append")
    .option("checkpointLocation", "checkpoint/invalid/")
    .start()
)

# Watermark silently drops late events inside the aggregation, so they are
# caught here manually before reaching it, using the same 2-minute threshold.
on_time_events = valid_stream.filter(
    col("event_timestamp") >= (current_timestamp() - lit(LATE_THRESHOLD_SECONDS).cast("interval second"))
)

late_events = valid_stream.filter(
    col("event_timestamp") < (current_timestamp() - lit(LATE_THRESHOLD_SECONDS).cast("interval second"))
)

def send_late_to_dlq(batch_df, batch_id):
    dlq_ready = prepare_dlq_message(batch_df, reason="late_arrival")
    write_to_dlq(dlq_ready, kafka_bootstrap=KAFKA_BOOTSTRAP, dlq_topic=DLQ_TOPIC)

late_query = (
    late_events.writeStream
    .foreachBatch(send_late_to_dlq)
    .outputMode("append")
    .option("checkpointLocation", "checkpoint/late/")
    .start()
)

# Same window/watermark pattern as gold_aggregated, but counting DLQ events
# instead of payment metrics — keeps the DLQ visibility consistent with
# how the rest of the pipeline measures things.
dlq_combined = (
    invalid_stream.withColumn("reason", lit("invalid_schema"))
    .unionByName(late_events.withColumn("reason", lit("late_arrival")), allowMissingColumns=True)
)

dlq_aggregated = (
    dlq_combined
    .withWatermark("event_timestamp", "2 minutes")
    .groupBy(
        window(col("event_timestamp"), "5 minutes"),
        col("reason")
    )
    .count()
    .withColumnRenamed("count", "event_count")
)

def write_dlq_metrics_to_bigquery(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    output_df = (
        batch_df
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .withColumn("_loaded_at", current_timestamp())
        .drop("window")
    )

    (
        output_df.write
        .format("bigquery")
        .option("table", f"{BIGQUERY_DATASET}.dlq_metrics_5min")
        .option("writeMethod", "direct")
        .mode("append")
        .save()
    )

dlq_metrics_query = (
    dlq_aggregated.writeStream
    .foreachBatch(write_dlq_metrics_to_bigquery)
    .outputMode("update")
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", "checkpoint/dlq_metrics/")
    .start()
)

bronze_query = (
    on_time_events.writeStream
    .format("parquet")
    .option("path", f"gs://{GCS_BUCKET}/payments/")
    .option("checkpointLocation", "checkpoint/bronze/")
    .outputMode("append")
    .trigger(processingTime="60 seconds")
    .start()
)

gold_aggregated = (
    on_time_events
    .withWatermark("event_timestamp", "2 minutes")
    .groupBy(
        window(col("event_timestamp"), "5 minutes"),
        col("gateway")
    )
    .agg(
        count("*").alias("total_payments"),
        spark_sum(when(col("status") == "success", 1).otherwise(0)).alias("successful_payments"),
        spark_sum(when(col("status") == "failure", 1).otherwise(0)).alias("failed_payments"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )
    .withColumn("failure_rate", col("failed_payments") / col("total_payments"))
)

def write_to_bigquery(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    output_df = (
        batch_df
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .withColumn("_loaded_at", current_timestamp())
        .drop("window")
    )

    (
        output_df.write
        .format("bigquery")
        .option("table", f"{BIGQUERY_DATASET}.payment_metrics_5min")
        .option("writeMethod", "direct")
        .mode("append")
        .save()
    )

gold_query = (
    gold_aggregated.writeStream
    .foreachBatch(write_to_bigquery)
    .outputMode("update")
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", "checkpoint/gold/")
    .start()
)

spark.streams.awaitAnyTermination()