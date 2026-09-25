# 00 — Architecture

> Read this first. It gives you the mental model everything else hangs off.

## What is an API gateway, really?

Imagine you have ten backend microservices. Every one of them needs to:

- check that the caller is authenticated,
- reject callers who send too many requests,
- stop hammering a downstream that is already failing,
- record who called what, when, and how long it took.

You could copy that logic into all ten services. Now you have ten copies to keep
in sync, in maybe three different languages. That is where the bug lives.

An **API gateway** pulls all of that cross-cutting behaviour into *one* place that
sits in front of the services. Clients talk to the gateway; the gateway talks to
the services. Because **every request flows through it**, the gateway is the
natural home for policy that should apply to everyone.

This is why gateways are considered critical infrastructure: they are on the hot
path of literally every request. That also makes them a great thing to build if
you want to understand networking, concurrency, and reliability — you can't fake
any of it.

## The request lifecycle

Follow a single request through GatewayX:

```
                        ┌──────────────────────── gateway process ───────────────────────┐
                        │                                                                 │
 client  ──HTTP──▶  [ router ] ──▶ [ plugin chain ] ──▶ [ load balancer ] ──▶ [ proxy ] ──┼──▶ upstream
                        │              │  │  │  │              │                           │
                        │            auth │  │ transform    pick instance                 │
                        │              rate-limit │                                        │
                        │                 circuit-breaker                                  │
                        │                                                                 │
                        │   on the way back: plugins' OnResponse run in reverse order     │
                        └─────────────────────────────────────────────────────────────────┘
                             ▲              ▲                    ▲
                             │              │                    │
                        Postgres         Redis                 Kafka
                     (routes/keys)  (limits/breaker)      (request-logs)
```

1. **Router** — match the incoming request's host + path to a configured `Route`.
   No match → 404. Match → we know the upstream pool and which plugins to run.
2. **Plugin chain (request phase)** — run each plugin's `OnRequest` in priority
   order. Any plugin may *abort* (e.g. auth fails → 401, rate limit hit → 429).
3. **Load balancer** — pick one concrete upstream instance from the route's pool
   using the route's strategy (round-robin / least-conn / consistent-hash).
4. **Proxy** — `httputil.ReverseProxy` forwards the request, wrapped by the
   circuit breaker so a failing upstream trips the breaker.
5. **Plugin chain (response phase)** — run `OnResponse` in *reverse* order so
   plugins wrap symmetrically (like nested function calls).
6. **Analytics** — emit a request-log event to Kafka (fire-and-forget; never
   block the client on it).

## The components and why each one exists

| Component | Job | Backed by | Why that backing store |
|---|---|---|---|
| Config store | Holds routes, API keys, plugin config | **Postgres** | Relational, transactional, supports `LISTEN/NOTIFY` for hot reload |
| Rate limiter | Counts requests per consumer/route | **Redis** | Atomic sorted-set ops make distributed sliding windows easy and fast |
| Circuit breaker | Tracks upstream health, shared across pods | **Redis** | State must be shared so all gateway replicas agree a breaker is open |
| Analytics | Durable stream of every request | **Kafka** | Decouples the hot path from slow analytics; replayable, partitioned |
| Load balancer | Spreads traffic across upstream instances | in-memory | Per-instance is fine; consistent-hash needs no shared state |

### Why these are separate stores

A recurring theme: **match the store to the access pattern.**

- Config changes rarely but must be correct and queryable → SQL.
- Rate-limit counters change on *every request* and must be atomic across
  replicas → Redis.
- Analytics is high-volume, write-once, read-later → a log/stream → Kafka.

Using Postgres for rate limiting would melt it; using Redis for config would lose
you transactions and joins. Right tool, right job.

## Scaling model

The gateway is **stateless** as far as its own process is concerned — all shared
state lives in Redis/Postgres. That means you run N replicas behind an L4 load
balancer and they all behave consistently:

- rate limits are enforced globally (counters in Redis, not per-pod),
- circuit breakers agree (state in Redis),
- config is identical (all read the same Postgres and hot-reload together).

The one deliberately-local piece is load-balancer connection counts for the
least-connections strategy — see [doc 06](06-load-balancing.md) for why that's an
acceptable trade-off.

## What we are NOT building (scope honesty)

To keep the project focused on the gateway concepts, we skip: TLS termination
(assume it's done by an upstream L4 LB / ingress), a web UI (admin is a REST API),
and dynamically-compiled Go plugins via `plugin.so` (we use a compiled-in registry
instead — see [doc 03](03-plugin-system.md) for why that's the saner choice).

Next: [01 — Project scaffolding & infra](01-scaffolding.md).
