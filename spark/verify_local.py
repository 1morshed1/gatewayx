#!/usr/bin/env python3
"""
Local verification of the usage-report aggregations — no Kafka, no cluster.

Builds a small DataFrame of sample request-log events (the same shape the gateway
emits) and runs the exact report functions from usage_report.py, asserting the
numbers come out right. This proves the *analytics logic* independently of the Kafka
plumbing, the same way the Go code is unit-tested with miniredis.

Run:  python3 spark/verify_local.py    (needs pyspark + Java)
"""

import sys

from pyspark.sql import SparkSession, Row

from usage_report import enrich, daily_report, latency_report, top_consumers_report


def sample_rows():
    # 5 events on route "echo": 3x200, 1x500, 1x404; two consumers.
    base = "2026-09-25T10:00:00Z"
    return [
        Row(time=base, request_id="1", route_id="r1", route_name="echo", consumer_id="c1",
            method="GET", path="/x", status=200, upstream="u", duration_ms=10, client_ip="ip", bytes_out=100),
        Row(time=base, request_id="2", route_id="r1", route_name="echo", consumer_id="c1",
            method="GET", path="/x", status=200, upstream="u", duration_ms=20, client_ip="ip", bytes_out=100),
        Row(time=base, request_id="3", route_id="r1", route_name="echo", consumer_id="c2",
            method="GET", path="/x", status=200, upstream="u", duration_ms=30, client_ip="ip", bytes_out=100),
        Row(time=base, request_id="4", route_id="r1", route_name="echo", consumer_id="c2",
            method="GET", path="/x", status=500, upstream="u", duration_ms=40, client_ip="ip", bytes_out=100),
        Row(time=base, request_id="5", route_id="r1", route_name="echo", consumer_id="c1",
            method="GET", path="/x", status=404, upstream="u", duration_ms=50, client_ip="ip", bytes_out=100),
    ]


def main():
    spark = SparkSession.builder.appName("verify").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    events = enrich(spark.createDataFrame(sample_rows()))

    daily = daily_report(events).collect()[0]
    assert daily["requests"] == 5, daily["requests"]
    assert daily["errors_5xx"] == 1, daily["errors_5xx"]
    assert daily["client_4xx"] == 1, daily["client_4xx"]
    assert abs(daily["error_rate"] - 0.2) < 1e-9, daily["error_rate"]
    print("daily_report OK:", daily.asDict())

    lat = latency_report(events).collect()[0]
    assert lat["requests"] == 5
    assert abs(lat["avg_ms"] - 30.0) < 1e-9, lat["avg_ms"]  # (10+20+30+40+50)/5
    assert lat["max_ms"] == 50
    print("latency_report OK:", lat.asDict())

    tops = {r["consumer_id"]: r["requests"] for r in top_consumers_report(events).collect()}
    assert tops == {"c1": 3, "c2": 2}, tops
    print("top_consumers_report OK:", tops)

    spark.stop()
    print("\nALL SPARK REPORT ASSERTIONS PASSED")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print("ASSERTION FAILED:", e)
        sys.exit(1)
