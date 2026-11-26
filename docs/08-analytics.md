# 08 — Analytics with Kafka

Every request through the gateway is a data point: who called, which route, what
status, how long it took. Collected, that answers "who are my top consumers?",
"which routes error the most?", "what's my p99 latency by route?". This phase
streams one event per request to Kafka and rolls those events up into Postgres for
reporting.

Files: `internal/analytics/` (producer + consumer), `internal/domain/analytics.go`
(event schema), `migrations/0002_analytics.sql`, `internal/store/analytics.go`,
`cmd/gateway/analytics.go` (consumer subcommand).

## The golden rule: analytics never slows a request

This is the single most important design constraint (ADR-006). Analytics is
valuable but **not on the correctness path** — a request must succeed and return
fast whether or not its analytics event is recorded. Everything below follows from
that rule.

## Why Kafka (and not "just write to Postgres")

We could `INSERT` a row per request straight into Postgres. At even modest traffic
that would (a) add a synchronous DB write to every request's latency, and (b) melt
Postgres under write load. Kafka fixes both:

- **Decoupling.** The gateway (producer) doesn't know or care who consumes the
  stream. Today it's our aggregator; tomorrow you add a Spark job, a ClickHouse
  sink, or a fraud detector — all reading the same topic, no gateway change.
- **Buffering.** Kafka absorbs bursts; consumers process at their own pace.
- **Replayability.** The raw event log is retained, so you can reprocess history
  (recompute a metric you didn't think to collect) by rewinding the consumer.

Raw events live in Kafka (cheap, replayable); Postgres holds only the small,
queryable **aggregate**. Right store for each job, again.

## The producer: fire-and-forget with a bounded buffer

`KafkaEmitter` (implements `proxy.Emitter`) is where "never slow a request" is
enforced:

```go
func (e *KafkaEmitter) Emit(rl domain.RequestLog) {
    select {
    case e.ch <- rl:          // non-blocking enqueue
    default:
        e.dropped.Add(1)      // buffer full -> DROP, don't block
    }
}
```

`Emit` returns instantly. A background worker drains the channel, **batches** up to
100 events, and writes them to Kafka (batching amortises the network cost). Key
properties:

- **Non-blocking:** the request goroutine never waits on Kafka.
- **Bounded:** the channel has a fixed size (10k). If Kafka is slow or down, the
  buffer fills and further events are *dropped and counted* (logged periodically)
  — never queued unboundedly (which would be a memory leak) and never blocking.
- **Availability > completeness:** we would rather lose some analytics than add
  latency or fail a request.

The gateway wraps its `ResponseWriter` in a `recorder` (see `proxy/gateway.go`) to
capture the final status code and byte count, and emits one `RequestLog` per
request in a `defer` — covering every exit path (404, plugin abort, no-upstream,
proxied response, breaker-open).

## The event schema

`domain.RequestLog` is the contract with every downstream consumer, possibly in
other languages, possibly against historical data. So the rule is **additive
only**: add fields, never rename or repurpose. Fields: time, request id, route
id/name, consumer id, method, path, status, upstream, duration, client ip, bytes.

Events are keyed by `route_id` when produced, so all events for a route land on the
same Kafka partition — giving ordering per route and locality for consumers.

## The consumer: aggregate, then upsert

`gateway analytics` runs a separate process (`internal/analytics/consumer.go`). It
consumes in a **consumer group**, so you can run several instances that share the
topic's partitions for horizontal scale, with offsets tracked in Kafka.

It doesn't write one DB row per event — that would defeat the point. Instead it
**aggregates in memory** into `(minute, route, status_class)` buckets and flushes
rollups to Postgres every 5 seconds:

```
raw events ──▶ in-memory map[(minute,route,status_class)] ──every 5s──▶ upsert
```

The upsert (`store.AddAnalytics`) uses `INSERT ... ON CONFLICT DO UPDATE SET
count = count + EXCLUDED.count`, so repeated partial flushes accumulate correctly.
DB write load is proportional to the number of *distinct buckets*, not to raw
request volume — a huge reduction.

`status_class` stores the HTTP family (2/3/4/5) instead of every distinct code, so
error rates are one `FILTER (WHERE status_class = 5)` away without storing
everything. The aggregate table has **no foreign key to routes** — analytics must
outlive a deleted route.

## Reading it back

The admin API exposes `GET /admin/analytics` (last 24h), which runs
`store.AnalyticsSummary`: per-route request count, error count (5xx), error rate,
and average latency, ordered by volume. That's the "top consumers / error rates by
route" report from the spec, straight out of SQL.

## A real gotcha we hit: single-node Kafka & the offsets topic

Verifying this end-to-end surfaced a classic Kafka trap worth remembering. A
consumer group needs the internal `__consumer_offsets` topic, which defaults to
**replication factor 3**. On a single-broker dev cluster that can't be satisfied,
so the broker never brings up the group coordinator and consumers fail with
*"Group Coordinator Not Available"*. The fix is to force the internal topics to
replication factor 1 (see `deploy/docker-compose.yml`):

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
```

Lesson: Kafka's defaults assume a multi-broker cluster; a single-node dev setup
must override the replication factors of the internal topics or consumer groups
silently never start.

## End-to-end

```bash
make infra-up          # includes Kafka
make migrate           # applies 0002_analytics.sql
make run &             # gateway with KAFKA_ENABLED=true
go run ./cmd/gateway analytics &     # the consumer

# send some traffic:
for i in $(seq 1 12); do curl -s -o/dev/null -H 'X-API-Key: demo-key' localhost:8000/echo/x; done

# a few seconds later, read the rollup:
curl -s localhost:8001/admin/analytics
# {"routes":[{"route_name":"echo","requests":12,"errors":0,"error_rate":0,"avg_duration_ms":0.67}], ...}
```

Next: [09 — Observability](09-observability.md).
