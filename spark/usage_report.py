#!/usr/bin/env python3
"""
GatewayX usage report — a Spark BATCH job over the `request-logs` Kafka topic.

This is the analytics job the spec calls for: it reads every request-log event the
gateway has produced and computes daily usage reports, top consumers, error rates by
route, and latency percentiles. It complements the always-on Go consumer (which
keeps a live rollup in Postgres, see docs/08); Spark is the tool when you want heavy,
ad-hoc, full-history batch analytics that would be awkward in SQL — percentiles,
large group-bys, joins across long time ranges.

Why BATCH (read once) rather than streaming: reports like "yesterday's top
consumers" run on a schedule (e.g. a nightly cron / Airflow DAG) over a bounded set
of offsets. Batch is simpler and cheaper for that than a long-running stream.

Run (locally, needs Java + the Kafka connector, which --packages fetches):

    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      spark/usage_report.py

Env:
    KAFKA_BROKERS   default localhost:9092
    KAFKA_TOPIC     default request-logs
    OUTPUT_DIR      default ./spark-out   (parquet reports written here)
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
)

# Schema mirrors internal/domain/analytics.go (RequestLog). Keep in sync — the JSON
# field names are the contract between the producer and this job.
REQUEST_LOG_SCHEMA = StructType([
    StructField("time", StringType()),          # RFC3339
    StructField("request_id", StringType()),
    StructField("route_id", StringType()),
    StructField("route_name", StringType()),
    StructField("consumer_id", StringType()),
    StructField("method", StringType()),
    StructField("path", StringType()),
    StructField("status", IntegerType()),
    StructField("upstream", StringType()),
    StructField("duration_ms", LongType()),
    StructField("client_ip", StringType()),
    StructField("bytes_out", LongType()),
])


def enrich(events):
    """Add derived columns (timestamp, date, status_class) and drop route-less rows.
    Kept separate so it can be unit-tested on a hand-built DataFrame."""
    return (
        events
        .withColumn("ts", F.to_timestamp("time"))
        .withColumn("date", F.to_date("ts"))
        .withColumn("status_class", (F.col("status") / 100).cast("int"))
        .filter(F.col("route_name").isNotNull())
    )


def daily_report(events):
    """Daily requests + error rate by route."""
    return (
        events.groupBy("date", "route_name")
        .agg(
            F.count("*").alias("requests"),
            F.sum(F.when(F.col("status_class") == 5, 1).otherwise(0)).alias("errors_5xx"),
            F.sum(F.when(F.col("status_class") == 4, 1).otherwise(0)).alias("client_4xx"),
        )
        .withColumn("error_rate", F.round(F.col("errors_5xx") / F.col("requests"), 4))
        .orderBy("date", F.desc("requests"))
    )


def latency_report(events):
    """Latency percentiles (p50/p95/p99) + avg/max by route."""
    return (
        events.groupBy("route_name")
        .agg(
            F.count("*").alias("requests"),
            F.round(F.avg("duration_ms"), 2).alias("avg_ms"),
            F.expr("percentile_approx(duration_ms, 0.50)").alias("p50_ms"),
            F.expr("percentile_approx(duration_ms, 0.95)").alias("p95_ms"),
            F.expr("percentile_approx(duration_ms, 0.99)").alias("p99_ms"),
            F.max("duration_ms").alias("max_ms"),
        )
        .orderBy(F.desc("requests"))
    )


def top_consumers_report(events, limit=20):
    """Top consumers by request volume (with error count and MB out)."""
    return (
        events.filter(F.col("consumer_id") != "")
        .groupBy("consumer_id")
        .agg(
            F.count("*").alias("requests"),
            F.sum(F.when(F.col("status_class") == 5, 1).otherwise(0)).alias("errors_5xx"),
            F.round(F.sum("bytes_out") / 1e6, 2).alias("mb_out"),
        )
        .orderBy(F.desc("requests"))
        .limit(limit)
    )


def main():
    brokers = os.environ.get("KAFKA_BROKERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "request-logs")
    out_dir = os.environ.get("OUTPUT_DIR", "./spark-out")

    spark = (
        SparkSession.builder
        .appName("gatewayx-usage-report")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- read the whole topic as a batch (earliest -> latest offsets) ---
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # Kafka `value` is bytes; decode JSON into typed columns, then enrich.
    decoded = (
        raw.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json("json", REQUEST_LOG_SCHEMA).alias("e"))
        .select("e.*")
    )
    events = enrich(decoded)
    events.cache()
    total = events.count()
    print(f"read {total} request-log events from topic '{topic}'")

    daily = daily_report(events)
    print("=== daily requests & error rate by route ===")
    daily.show(50, truncate=False)

    latency = latency_report(events)
    print("=== latency by route ===")
    latency.show(50, truncate=False)

    top_consumers = top_consumers_report(events)
    print("=== top consumers ===")
    top_consumers.show(20, truncate=False)

    # --- persist as parquet for downstream BI / archival --------------------
    daily.write.mode("overwrite").parquet(f"{out_dir}/daily_by_route")
    latency.write.mode("overwrite").parquet(f"{out_dir}/latency_by_route")
    top_consumers.write.mode("overwrite").parquet(f"{out_dir}/top_consumers")
    print(f"reports written to {out_dir}/")

    spark.stop()


if __name__ == "__main__":
    main()
