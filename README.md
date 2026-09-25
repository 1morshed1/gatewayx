# GatewayX — a programmable API gateway (Mini-Kong)

GatewayX is a from-scratch **API gateway** written in Go. It sits in front of your
backend services and gives every request the cross-cutting behaviour you would
otherwise re-implement in each service: authentication, rate limiting, request
transformation, circuit breaking, load balancing, and analytics — all driven by
a **plugin chain** and configurable at runtime through an **Admin API**.

This is *infrastructure* software: the kind of component every request in a
microservices architecture flows through. It is built as a learning project, so
the code is documented heavily and every design decision is written up in
[`docs/`](docs/).

> New here? Start with [`docs/00-architecture.md`](docs/00-architecture.md), then
> read the numbered docs in order — they follow the build phase by phase.

## What it does

| Capability | How |
|---|---|
| **Reverse proxy** | Go `net/http/httputil.ReverseProxy` with per-route configuration |
| **Plugin chain** | Ordered middleware; each plugin sees the request before and the response after proxying |
| **Authentication** | API-key plugin backed by a Postgres key registry |
| **Rate limiting** | Redis sliding-window-log algorithm, per consumer + per route |
| **Circuit breaking** | Three-state (closed/open/half-open) breaker with shared Redis state |
| **Request transform** | Add/remove/rename headers, rewrite paths |
| **Load balancing** | Weighted round-robin, least-connections, consistent hashing |
| **Hot config reload** | Postgres `LISTEN/NOTIFY` (or polling) — no restart to change routes |
| **Analytics** | Every proxied request streamed to Kafka for downstream aggregation |
| **Observability** | Prometheus metrics + structured logs |

## Architecture at a glance

```
client ─▶ [gateway :8000] ─▶ plugin chain ─▶ load balancer ─▶ upstream pool
                 │                                   │
          admin API :8001                      Redis (rate limit / breaker)
                 │                              Postgres (routes / keys / config)
          metrics :9100                         Kafka (request-logs)
```

Full diagram and the reasoning behind each box: [`docs/00-architecture.md`](docs/00-architecture.md).

## Quick start

Prerequisites: Docker + Docker Compose, and Go 1.24+ (`/usr/local/go/bin` on this
machine — the Makefile puts it on `PATH` for you).

```bash
# 1. Start Postgres, Redis, Kafka, and two demo upstream services
make infra-up

# 2. Apply the database schema
make migrate

# 3. Load a couple of example routes + an API key
make seed

# 4. Run the gateway
make run

# 5. Send a request through it
curl -H "X-API-Key: demo-key" http://localhost:8000/echo/hello
```

## Repository layout

```
cmd/
  gateway/     gateway entrypoint (serve, migrate, seed subcommands)
  echo/        tiny demo upstream service (used by docker-compose)
internal/
  config/      env-based configuration
  domain/      core types (Route, Consumer, plugin contexts)
  store/       Postgres config + analytics store
  proxy/       reverse proxy + gateway core / request lifecycle
  plugin/      plugin interface, registry, chain builder
    plugins/   auth, ratelimit, circuitbreaker, transform
  ratelimit/   Redis sliding-window limiter
  circuitbreaker/ Redis-backed circuit breaker
  loadbalancer/ weighted RR, least-connections, consistent hashing
  admin/       admin REST API
  analytics/   Kafka producer + event schema
  observability/ metrics + logging
migrations/    SQL schema
deploy/        docker-compose, Dockerfiles, k8s manifests
test/k6/       load-test scenarios
docs/          learning write-ups, one per phase / concept
```

## Documentation index

The `docs/` folder is the learning trail. Each file explains *what* was built and
*why*, with the trade-offs considered. See [`docs/README.md`](docs/README.md) for
the running index.

## License

MIT — this is a learning project, use it however helps you learn.
