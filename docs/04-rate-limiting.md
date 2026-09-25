# 04 — Rate limiting (and authentication)

Rate limiting protects your backends from being overwhelmed — by a buggy client, a
scraper, or an abusive consumer. This phase builds a **distributed** limiter
(correct across many gateway replicas) plus the **auth** plugin it depends on,
because you can't say "100 requests per *consumer*" until you know who the consumer
is.

Files: `internal/ratelimit/`, `internal/redisx/`,
`internal/plugin/plugins/auth/`, `internal/plugin/plugins/ratelimit/`.

## Authentication first

The auth plugin (`plugins/auth`) reads an API key header (default `X-API-Key`),
looks the consumer up in Postgres, and:

- no key / unknown key → `rc.Abort(401)`,
- valid key → sets `rc.Consumer` and **strips the key header** so the credential
  never reaches the upstream.

Two design points worth copying:

1. **Narrow dependency interface.** The plugin declares its own tiny `Store`
   interface (`GetConsumerByAPIKey`) instead of importing the concrete
   `*store.Store`. The real store satisfies it structurally, and tests can pass a
   fake. This is the Go idiom "accept interfaces, return structs," applied to
   keep the plugin decoupled.
2. **Priority.** Auth runs at `PriorityAuth` (10) — before rate limiting (20) — so
   the consumer is known when the limiter builds its key.

> Production note: storing API keys in plaintext (as we do for demo simplicity)
> is not ideal. Real systems store a hash (e.g. SHA-256) and compare hashes, so a
> DB leak doesn't hand out working keys. The lookup shape is identical; only the
> column contents change.

## Choosing a rate-limit algorithm

| Algorithm | Idea | Weakness | Good for |
|---|---|---|---|
| **Fixed window** | one counter per time bucket | **double burst**: N at end of window + N at start of next = 2N in a moment | crude quotas |
| **Token bucket** | tokens refill at a rate; each request spends one | about *shaping*, allows bursts up to bucket size | smoothing traffic |
| **Sliding window log** | store a timestamp per request; count those still inside the rolling window | memory: one entry per in-window request | accurate "≤ N per rolling window" |

We use **sliding window log** (ADR-003) because accuracy is the teachable property
and Redis sorted sets implement it cleanly.

### The double-burst problem, concretely

Limit = 100/minute, fixed window aligned to the minute. A client sends 100 requests
at 12:00:59 and 100 more at 12:01:00. Both windows see only 100, so both pass — but
in that one real second the backend took **200**. Sliding window log looks at the
*last 60 seconds from now*, so it sees 200 and blocks the excess. That's the whole
point.

## The Redis implementation

A sorted set (ZSET) per key. Score = timestamp (microseconds), member = a unique
id. One request = one member. To check the limit:

```
ZREMRANGEBYSCORE key 0 (now-window)   -- drop entries outside the window
ZCARD key                             -- how many remain?
if count < limit: ZADD key now member; PEXPIRE key window
```

### Why a Lua script, not the pipeline the plan sketched

The plan used a Redis *pipeline*: `ZREMRANGEBYSCORE`, `ZCARD`, `ZADD`, `EXPIRE`.
Pipelining batches network round-trips but **does not make the steps atomic**.
Between `ZCARD` (count) and `ZADD` (record), other requests can run their own
`ZADD`, so several requests all see "count = 49 < 50" and all get admitted — the
limit is breached under concurrency. This is a textbook
time-of-check-to-time-of-use (TOCTOU) race.

A **Lua script** runs atomically on the Redis server: count-then-maybe-add is one
indivisible operation. That is *the* reason `internal/ratelimit/limiter.go` uses
`redis.NewScript(...)`. Correctness under concurrency is the entire job of a rate
limiter, so we don't get this wrong. The script also returns an accurate
`Retry-After` by looking at when the oldest in-window entry will expire.

### Unique members matter

Score alone can't be the member: two requests in the same microsecond would share a
member, and the second `ZADD` would overwrite the first instead of adding a row —
silently undercounting. So the member is `timestamp + random suffix`.

## The rate-limit plugin

`plugins/ratelimit` wraps the limiter:

- **Key** = `ratelimit:consumer:{id}:{route}` when authenticated, else
  `ratelimit:ip:{ip}:{route}`. So limits are **per consumer, per route** — exactly
  the granularity the plan calls for.
- On allow: sets `X-RateLimit-Limit` / `X-RateLimit-Remaining` (via the request
  context's response-header accumulator, since the response isn't built yet).
- On block: sets `Retry-After` and aborts with `429`.

### Fail-open vs fail-closed

If Redis is unreachable, the limiter returns an error. We chose to **fail open** —
allow the request — because for a gateway, a Redis blip making the *entire API*
return 429 is usually worse than briefly not enforcing limits. This is a
threat-model decision: if the limit is a hard security control (not just capacity
protection), you'd fail closed instead. The trade-off is documented right at the
`return nil` in the plugin so nobody "fixes" it by accident.

## Why the state must be shared (distributed correctness)

Counters live in Redis, not in gateway memory. If each of 4 replicas kept its own
counter, a "100/min" limit would actually allow ~400/min (100 per pod). Shared
Redis state means all replicas increment the *same* sorted set, so the global limit
is the real limit. This is the same reasoning as the circuit breaker
([doc 05](05-circuit-breaker.md)).

## Testing it

Unit tests (`limiter_test.go`) use **miniredis** — an in-memory Redis that runs the
Lua script for real — so we test correctness without a container, including
`FastForward` to prove the window actually slides. End-to-end:

```bash
make seed    # sets a small 5-per-10s limit on /echo
make run &
# no/invalid key -> 401
curl -s localhost:8000/echo/x
# 6th request within 10s -> 429 with Retry-After
for i in $(seq 1 7); do curl -s -o /dev/null -w "%{http_code} " -H 'X-API-Key: demo-key' localhost:8000/echo/x; done
# -> 200 200 200 200 200 429 429
```

Next: [05 — Circuit breaking](05-circuit-breaker.md).
