#!/usr/bin/env bash
# Run the GatewayX usage report with Spark, containerised — no local Spark install
# needed. Uses the official apache/spark image and fetches the Kafka connector via
# --packages.
#
# The gateway must have produced some events to the topic first (send traffic with
# KAFKA_ENABLED=true). Point KAFKA_BROKERS at a broker reachable from the container.
#
# Usage:  ./spark/run.sh
set -euo pipefail

KAFKA_BROKERS="${KAFKA_BROKERS:-host.docker.internal:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-request-logs}"

docker run --rm \
  --add-host=host.docker.internal:host-gateway \
  -e KAFKA_BROKERS="${KAFKA_BROKERS}" \
  -e KAFKA_TOPIC="${KAFKA_TOPIC}" \
  -e OUTPUT_DIR="/tmp/spark-out" \
  -v "$(cd "$(dirname "$0")" && pwd)":/job \
  apache/spark:3.5.1 \
  /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /job/usage_report.py
