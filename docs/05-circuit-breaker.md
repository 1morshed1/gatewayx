# 05 — Circuit breaking

A circuit breaker stops the gateway from hammering an upstream that is already
failing. Without one, when a backend gets sick every request piles on, waits for a
timeout, and consumes a connection — turning one slow service into a cascading
outage across the whole system. The breaker "gives up fast" and periodically checks
whether the backend has recovered.

Files: `internal/circuitbreaker/`.

## The three-state pattern

```
                 failures >= threshold
      ┌────────┐ ───────────────────────▶ ┌────────┐
      │ CLOSED │                           │  OPEN  │
      │ (normal)│ ◀─────────────────────── │(reject)│
      └────────┘   probe succeeds          └────────┘
           ▲                                     │  after OpenTimeout
           │        probe succeeds               ▼
           │                              ┌───────────┐
           └───────────────────────────  │ HALF-OPEN │
                    probe fails ────────▶ │  (1 probe)│  fails ─▶ OPEN
                                          └───────────┘
```

- **CLOSED** — normal. Requests flow. Failures are counted. When failures reach the
  threshold within a rolling window, trip to **OPEN**.
- **OPEN** — reject immediately with `503` and don't even dial the upstream. After
  `OpenTimeout`, move to **HALF-OPEN**.
- **HALF-OPEN** — let a probe request through. Success → **CLOSED** (recovered).
  Failure → back to **OPEN** (still sick).

This is the standard breaker from Nygard's *Release It!* and what libraries like
Hystrix/resilience4j implement.

## Design decision: transport middleware, not a plugin

The plan lists the breaker as a plugin, but we implement it as an
`http.RoundTripper` that wraps the proxy's transport (`circuitbreaker/transport.go`).

Why: a chain plugin runs *before* proxying — it can't see whether the upstream
actually failed. The breaker's entire job is to observe the **real** upstream
outcome (a transport error, or a `5xx` status) and count it. That information only
exists at the transport layer. So the breaker sits exactly where the upstream call
happens:

```go
resp, rtErr := t.base.RoundTrip(req)
success := rtErr == nil && resp.StatusCode < 500
t.mgr.Record(ctx, key, success)
```

Note **4xx does not trip the breaker** — a client sending bad requests is not the
upstream being unhealthy. Only transport errors and `5xx` count. Getting this
distinction right matters: otherwise a burst of 404s would open the breaker and
take down a perfectly healthy service.

When the breaker is open, we return a **synthetic `503` response** (with
`X-Circuit-Broken: true`) rather than a Go error — so `ReverseProxy` serves it
directly and the client gets clean JSON instead of a generic 502.

## Design decision: shared state in Redis

Breaker state lives in Redis, keyed per upstream host (`cb:{host}:*`). This is
ADR-004. With per-replica in-memory breakers, an upstream could be OPEN on one pod
and CLOSED on another — so half your fleet keeps hammering a sick backend, and a
client's experience depends on which pod it hits. Shared state means the **whole
fleet trips and recovers together**.

The cost is a Redis round-trip on the request path. We keep it to two small atomic
Lua scripts (`allow` before, `record` after).

### Why Lua again

Same reason as the rate limiter: the state transitions must be **atomic**. The
`allow` script reads state and performs the OPEN→HALF-OPEN transition in one shot;
the `record` script increments the failure counter and performs the CLOSED→OPEN or
→CLOSED transition atomically. Doing this with separate `GET`/`SET` calls would race
— two requests could both read "4 failures" and both decide not to open, or two
probes could both think they're the single half-open probe.

State layout (three keys per upstream):

| Key | Meaning |
|---|---|
| `cb:{host}:state` | `open` / `half_open` / (absent = `closed`) |
| `cb:{host}:opened_at` | epoch ms when it opened (for the OpenTimeout check) |
| `cb:{host}:fails` | rolling failure count, with a `PEXPIRE` = `Window` so old failures decay |

The `fails` key's TTL is what makes it "N failures **within a window**" rather than
"N failures ever" — isolated, occasional failures expire and never accumulate to a
trip.

## Configuration

Per-host defaults, from env (see `.env.example` / `config`):

| Env | Default | Meaning |
|---|---|---|
| `CB_FAILURE_THRESHOLD` | 5 | failures within the window before opening |
| `CB_OPEN_TIMEOUT` | 30s | how long to stay open before a probe |
| `CB_WINDOW` | 30s | rolling window for counting failures |

## Testing it

Unit tests (`breaker_test.go`, miniredis) cover: opening at threshold, denying while
open, the half-open→closed recovery after the timeout, and success resetting the
counter.

End-to-end (the echo service can fake a `503` with `?status=503`):

```bash
# on a route with threshold 3:
for i in 1 2 3 4 5; do curl -sD- -o/dev/null -H 'X-API-Key: demo-key' \
  "localhost:8000/cb/x?status=503" | grep -iE 'HTTP|circuit-broken'; done
# req1-3: 503 from the upstream (counting failures)
# req4-5: 503 with "X-Circuit-Broken: true" — breaker open, upstream NOT called
# after CB_OPEN_TIMEOUT, a healthy request returns 200 and closes the breaker
```

That transition from "503 from upstream" to "503 X-Circuit-Broken" is the breaker
opening: the gateway stopped forwarding and started failing fast.

Next: [06 — Load balancing](06-load-balancing.md).
