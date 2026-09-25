# 09 — Observability

You cannot operate what you cannot see. A gateway on the path of every request is
exactly where you want rich, low-overhead telemetry: it's the one place that
observes *all* traffic. This phase adds Prometheus metrics (and we already have
structured logs from day one).

Files: `internal/observability/`, wired via the `proxy.Metrics` seam.

## The three pillars (and what we use each for)

- **Metrics** — cheap, aggregated numbers over time. "How many? How fast? What
  error rate?" Great for dashboards and alerts. *This phase.*
- **Logs** — structured, per-event detail. "What exactly happened to *this*
  request?" We emit JSON logs (`slog`) from the start.
- **Traces** — a request's path across services. Out of scope here, but the
  `request-id` plugin (doc 03) is the seed: propagate that ID and you can correlate
  logs across services, which is 80% of the value of tracing for far less setup.

## What to measure: the RED method

For any request-serving component, measure **R**ate, **E**rrors, **D**uration:

| Signal | Metric | Answers |
|---|---|---|
| Rate | `gatewayx_requests_total` (counter) | throughput; `rate()` in PromQL |
| Errors | same counter, `status_class="5xx"` | error ratio |
| Duration | `gatewayx_request_duration_seconds` (histogram) | p50/p95/p99 latency |

Plus `gatewayx_rejected_total` for requests stopped before an upstream (auth, rate
limit, breaker) — so you can distinguish "upstream is slow" from "we're rejecting
callers."

Everything is labelled by **route**, the unit an operator reasons about ("the
/payments route is erroring"), and status is bucketed into **classes** (2xx/3xx/
4xx/5xx) rather than raw codes to keep label cardinality low. High-cardinality
labels (raw path, user id, raw status) are the classic way to blow up a Prometheus
server — we avoid them deliberately.

## Why a histogram for latency (not a gauge or average)

An average latency hides everything that matters. "Average 20ms" can mean "everyone
sees 20ms" or "95% see 5ms and 5% see 400ms" — and the second is an incident. A
**histogram** records counts per latency bucket, so Prometheus can compute *any*
quantile after the fact:

```promql
histogram_quantile(0.99, sum(rate(gatewayx_request_duration_seconds_bucket[5m])) by (le, route))
```

The bucket boundaries are chosen for a proxy: fine-grained in the sub-millisecond-
to-10ms range (where gateway overhead lives) and coarse out to 10s (slow upstreams).

## Implementation notes

- **Private registry.** We use our own `prometheus.Registry` rather than the global
  default. It keeps the gateway's metrics self-contained (nothing a library imports
  can sneak metrics in) and makes it testable.
- **Go + process collectors.** Registered for free: goroutine counts, GC pauses,
  memory, open FDs, CPU. On an incident these runtime metrics are often what tell
  you the gateway itself (not an upstream) is the problem.
- **The seam.** The gateway depends on the `proxy.Metrics` interface
  (`ObserveRequest`), with a `NopMetrics` default. The observability package's
  `*Metrics` implements it. Same pattern as every other subsystem — the gateway
  core never imports Prometheus.
- **One record per request.** The gateway's `emit()` (which already runs once per
  request across every exit path via the `recorder`) also calls
  `metrics.ObserveRequest`, so metrics and analytics stay consistent for free.

## Separate port

Metrics are served on their own port (`:9100` by default), not mixed into the
public or admin servers. You scrape `:9100/metrics` from Prometheus and firewall it
to your monitoring network — you never want `/metrics` (which leaks internal detail)
on the public traffic port.

## See it

```bash
make run &
for i in 1 2 3; do curl -s -o/dev/null -H 'X-API-Key: demo-key' localhost:8000/echo/x; done
curl -s -o/dev/null -H 'X-API-Key: bad' localhost:8000/echo/x   # a 401

curl -s localhost:9100/metrics | grep '^gatewayx_requests_total'
# gatewayx_requests_total{route="echo",status_class="2xx"} 3
# gatewayx_requests_total{route="echo",status_class="4xx"} 1
```

Point Prometheus at `:9100` and build a RED dashboard; the data is all there.

Next: [10 — Load testing](10-load-testing.md).
