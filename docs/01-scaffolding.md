# 01 — Project scaffolding & infrastructure

This phase sets up the skeleton: the Go project layout, the local backing-service
stack (Docker Compose), and the developer ergonomics (Makefile). No gateway logic
yet — but getting the foundation right makes every later phase smoother.

## Go project layout

```
cmd/        entrypoints (main packages) — one dir per binary
internal/   private packages — Go forbids importing these from other modules
```

**Why `internal/`?** Anything under `internal/` can only be imported by code
rooted at the same parent. That's the compiler enforcing "this is not a public
API." For an application (as opposed to a library) that's exactly what you want:
freedom to refactor internals without worrying about external consumers.

**Why `cmd/<name>/main.go`?** A repo can produce several binaries (here: the
`gateway` and a demo `echo` upstream). Keeping each `main` in `cmd/<name>` is the
standard Go convention and keeps `main` packages thin — they wire things together
and delegate to `internal/` packages.

We split `internal/` by *domain concept* (proxy, plugin, ratelimit, ...) rather
than by technical layer (handlers, models, services). Each package owns one idea
and its types. This keeps import graphs shallow and makes the code map onto the
architecture doc one-to-one.

## The Docker Compose stack

`deploy/docker-compose.yml` brings up everything the gateway talks to:

- **postgres** — config + analytics store. Alpine image, healthcheck via
  `pg_isready` so dependent steps can wait for it.
- **redis** — rate-limit + breaker state. `--appendonly yes` so restarts don't
  wipe counters during a dev session.
- **kafka** — request-log stream. We use **KRaft mode** (Kafka's built-in Raft
  consensus) so there's *no ZooKeeper* — one fewer container and the modern way to
  run Kafka. `AUTO_CREATE_TOPICS_ENABLE=true` means the `request-logs` topic
  appears on first write; fine for dev, you'd pre-create it in prod.
- **upstream-a / upstream-b** — two instances of our `echo` service so we have
  something real to proxy to and can *see* load balancing (different instance
  names in responses).

### The echo demo service

`cmd/echo` is deliberately tiny but has **test hooks** baked in via query params:

| Param | Effect | Tests |
|---|---|---|
| `?latency=200ms` | sleeps before responding | timeouts, least-connections LB |
| `?status=503` | returns that status | circuit breaker tripping |
| `?fail=0.3` | 30% chance of a 500 | breaker under flaky conditions |

It reports its own `ECHO_NAME` in every response and in the `X-Upstream-Instance`
header. When you hit the gateway repeatedly and watch that name flip between
`upstream-a` and `upstream-b`, you're watching the load balancer work.

## The Makefile

Two things worth calling out:

1. **PATH fix.** The Go toolchain on this machine is at `/usr/local/go/bin`, not on
   the default `PATH`. The Makefile prepends it:
   ```make
   export PATH := /usr/local/go/bin:$(HOME)/go/bin:$(PATH)
   ```
   So `make build` works in a fresh shell without the developer fixing their env.

2. **Self-documenting help.** `make` (no target) prints every `## comment`
   attached to a target. Cheap, and it keeps the command list from rotting.

## Try it

```bash
make infra-up      # start postgres, redis, kafka, upstream-a, upstream-b
docker compose -f deploy/docker-compose.yml ps   # watch them go healthy
curl localhost:9001/hello   # hit upstream-a directly -> {"instance":"upstream-a",...}
curl localhost:9002/hello   # hit upstream-b directly -> {"instance":"upstream-b",...}
```

Once those two curls return different instance names, the foundation works and
we're ready to put a gateway in front of them.

Next: [02 — Config store & reverse proxy](02-reverse-proxy.md).
