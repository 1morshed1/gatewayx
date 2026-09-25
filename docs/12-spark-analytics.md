# 12 — Spark analytics batch job

The plan calls for "a Spark batch job computing daily API usage reports, top
consumers, error rates by route" over the `request-logs` topic. This phase delivers
exactly that.

Files: `spark/usage_report.py` (the job), `spark/verify_local.py` (a no-Kafka
correctness check), `spark/run.sh` (containerised runner), `spark/requirements.txt`.

## Two analytics paths, on purpose

We now have **two** ways to analyse the request-log stream, and that's deliberate —
they answer different questions:

| | Go consumer (doc 08) | Spark job (this doc) |
|---|---|---|
| Style | always-on streaming | scheduled batch |
| Output | live rollup in Postgres | daily reports (parquet + console) |
| Best for | "current" dashboards, admin API | heavy ad-hoc analytics, percentiles, full history |
| Latency | seconds | minutes, on a schedule |

Both read the **same** Kafka topic — which is the whole point of streaming to Kafka
(doc 08): the raw event log is a shared substrate that any number of consumers can
read independently. The Go consumer keeps the operational rollup fresh; Spark does
the heavy lifting (percentiles across millions of rows, big group-bys, joins) that
would be awkward or slow to do in the hot-path consumer.

## Why batch, not streaming

Reports like "yesterday's top consumers" or "last week's error rates" run on a
**schedule** (a nightly cron / Airflow DAG) over a bounded range of Kafka offsets.
For that, a batch read (`startingOffsets=earliest`, `endingOffsets=latest`, process,
exit) is simpler and cheaper than a long-running structured-streaming query. Use
streaming when you need continuous, low-latency output; use batch for periodic
reports. We already have the streaming path (the Go consumer), so Spark is batch.

## What it computes

Reading the topic, decoding each JSON event with a schema that mirrors
`domain.RequestLog`, and enriching with `date` + `status_class`, it produces three
reports:

1. **Daily requests & error rate by route** — `count`, `errors_5xx`, `client_4xx`,
   and `error_rate = errors_5xx / requests`, grouped by `(date, route)`.
2. **Latency by route** — `avg`, `p50`, `p95`, `p99` (via `percentile_approx`), and
   `max`. Percentiles at scale are exactly what Spark is good at and SQL rollups are
   not.
3. **Top consumers** — request count, error count, and MB out per consumer, top 20.

Each is printed and written to parquet under `OUTPUT_DIR` for BI tools / archival.

## Design: separate the logic from the plumbing (so it's testable)

The Kafka read is I/O; the aggregations are the logic worth testing. So the job is
split: `enrich()`, `daily_report()`, `latency_report()`, `top_consumers_report()` are
pure DataFrame→DataFrame functions, and `main()` just wires Kafka in and calls them.

That let us write `verify_local.py`, which builds a small DataFrame of sample events
and asserts the reports come out right — **no Kafka, no cluster** — the same
philosophy as testing the Go limiter with miniredis. It's a real Spark run in
`local[2]` mode:

```text
daily_report OK: {..., 'requests': 5, 'errors_5xx': 1, 'client_4xx': 1, 'error_rate': 0.2}
latency_report OK: {..., 'avg_ms': 30.0, 'p50_ms': 30, 'p95_ms': 50, 'p99_ms': 50, 'max_ms': 50}
top_consumers_report OK: {'c1': 3, 'c2': 2}
ALL SPARK REPORT ASSERTIONS PASSED
```

Run it yourself:

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r spark/requirements.txt   # pyspark (needs Java 17)
python spark/verify_local.py
```

## Running against real Kafka

```bash
# 1. produce events: run the gateway with KAFKA_ENABLED=true and send traffic
# 2. run the report (containerised, no local Spark needed):
./spark/run.sh
# or directly:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark/usage_report.py
```

The `--packages` flag pulls the Spark–Kafka connector at submit time. Point
`KAFKA_BROKERS` at a broker the job can reach (mind Kafka's advertised-listener
config — a broker that advertises an internal hostname isn't reachable from
elsewhere; the same gotcha called out in doc 08).

## The lesson

The valuable architectural idea here isn't Spark specifically — it's that **once
your events are on a durable, replayable log (Kafka), analytics becomes a plug-in
concern.** You can add, remove, or rewrite consumers (a live rollup, a batch report,
a fraud model, a data-lake sink) without ever touching the gateway. The gateway's
only job is to emit the event; everyone downstream decides what to make of it.

Back to the [docs index](README.md).
