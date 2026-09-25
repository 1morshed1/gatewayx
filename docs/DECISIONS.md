# Decision log

Architecture Decision Records (ADRs), lightweight. Each entry: the decision, the
context, the alternatives, and why. Newest decisions are appended at the bottom.

The point of this file for a learning project: **the reasoning is the lesson.**
Anyone can copy code; the value is in knowing *why* it's shaped that way.

---

## ADR-001 — Language: Go

**Decision:** Build the gateway in Go.

**Why:** A gateway is 99% network I/O and concurrency. Go's standard library ships
a production-grade reverse proxy (`net/http/httputil.ReverseProxy`), goroutines
make per-request concurrency trivial, and a single static binary is easy to ship
in a container. The alternatives (Rust: steeper; Node: single-threaded event loop
fine but weaker for CPU-bound plugin work; Java: heavier footprint) all work, but
Go hits the sweet spot of performance + simplicity for this class of software.

## ADR-002 — Compiled-in plugins, not `plugin.so`

**Decision:** Plugins implement a Go interface and are registered in a registry at
build time, rather than being loaded from `.so` files at runtime.

**Context:** The plan mentions "load it at runtime". Go *does* have a `plugin`
package, but it's notoriously fragile: exact toolchain/version match required,
Linux/macOS only, no unloading, and easy to crash the host process.

**Why compiled-in:** For a learning project we want to teach the *chain/middleware
pattern* and per-route plugin *configuration*, which is the genuinely interesting
part. Plugins are still "loaded at runtime" in the sense that a route decides
which plugins run and with what config, hot-reloadable from the DB. We get the
architecture lesson without the `.so` foot-guns. See [doc 03](03-plugin-system.md).

## ADR-003 — Rate limiting: sliding-window log via Redis sorted sets

**Decision:** Use the sliding-window-log algorithm implemented with a Redis ZSET.

**Alternatives considered:**
- *Fixed window* (a counter per time bucket): simplest, but allows a "double
  burst" — 2× the limit across a window boundary.
- *Token bucket*: smooth, allows bursts up to bucket size; great for shaping but
  less precise about "N per window".
- *Sliding window log*: store a timestamp per request, count those inside the
  window. Most accurate; costs more memory (one entry per request in the window).

**Why:** Accuracy is the teachable win here, and Redis ZSET operations
(`ZREMRANGEBYSCORE` + `ZCARD` + `ZADD`) make it a clean, atomic implementation.
See [doc 04](04-rate-limiting.md).

## ADR-004 — Circuit-breaker state in Redis

**Decision:** Store breaker state (closed/open/half-open + counters) in Redis,
shared across gateway replicas.

**Why:** If each replica kept its own breaker, one upstream could be "open" on pod
A and "closed" on pod B, so clients get inconsistent behaviour and the failing
upstream keeps getting hammered by half the fleet. Shared state means the whole
fleet agrees. The trade-off is a Redis round-trip on the hot path; we keep it to a
single atomic script. See [doc 05](05-circuit-breaker.md).

## ADR-005 — Config in Postgres with LISTEN/NOTIFY hot reload

**Decision:** Store routes/keys/plugin-config in Postgres; propagate changes to
running gateways via `LISTEN/NOTIFY`, falling back to polling.

**Why:** Config is relational (routes have upstreams have plugins), changes rarely,
and must be correct — that's SQL's home turf. `LISTEN/NOTIFY` gives near-instant,
push-based reload without a restart and without a separate message bus for what is
low-volume control-plane traffic. Polling is offered as a simpler fallback for
environments where a dedicated listener connection is undesirable. See
[doc 07](07-admin-hot-reload.md).

## ADR-006 — Analytics to Kafka, fire-and-forget

**Decision:** Emit one event per proxied request to a Kafka topic, asynchronously,
never blocking the client response on it.

**Why:** Analytics is high volume and not on the correctness path. Kafka decouples
the gateway (producer) from whatever consumes the stream later (Spark batch job,
ClickHouse sink, etc.), and it's replayable. Crucially the producer is async with
a bounded buffer: if Kafka is down, we drop log events rather than add latency
or fail requests. Availability of the proxy > completeness of analytics. See
[doc 08](08-analytics.md).
