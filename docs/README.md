# GatewayX docs — the learning trail

These docs are written to be read **in order**. Each one maps to a build phase and
explains the concepts, the code, and the decisions behind it. If you are learning
how an API gateway works by building one, read straight through.

| # | Doc | What you'll learn |
|---|-----|-------------------|
| 00 | [Architecture](00-architecture.md) | The big picture: what a gateway is, why each component exists, the request lifecycle |
| 01 | [Project scaffolding & infra](01-scaffolding.md) | Go project layout, docker-compose stack, why these backing services |
| 02 | [Config store & reverse proxy](02-reverse-proxy.md) | `httputil.ReverseProxy`, the Route model, Postgres config store |
| 03 | [The plugin system](03-plugin-system.md) | The middleware chain pattern, plugin interface, chain building |
| 04 | [Rate limiting](04-rate-limiting.md) | Sliding-window-log vs fixed-window vs token bucket; Redis sorted sets |
| 05 | [Circuit breaking](05-circuit-breaker.md) | The three-state pattern, failure detection, shared state |
| 06 | [Load balancing](06-load-balancing.md) | Weighted round-robin, least-connections, consistent hashing |
| 07 | [Admin API & hot reload](07-admin-hot-reload.md) | LISTEN/NOTIFY, zero-downtime config changes |
| 08 | [Analytics with Kafka](08-analytics.md) | Fire-and-forget event streaming, the request-log schema |
| 09 | [Observability](09-observability.md) | Prometheus metrics, structured logging, what to measure |
| 10 | [Load testing](10-load-testing.md) | k6 scenarios: throughput, latency overhead, limiter accuracy |

## Decision log

Cross-cutting decisions (why Go, why sliding window, why Redis for breaker state,
etc.) are collected in [`DECISIONS.md`](DECISIONS.md) so you can see the reasoning
without hunting through phase docs.

## How to read the code alongside these docs

Every doc names the packages and files it discusses. The code itself is commented
at a "teach me" density — comments explain *why*, not just *what*. Open the file
next to the doc and read them together.
