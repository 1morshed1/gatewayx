# 06 — Load balancing

A route usually has more than one upstream instance. The load balancer decides
*which* instance each request goes to. GatewayX implements three strategies,
selectable per route, because no single strategy is best for every workload.

Files: `internal/loadbalancer/`.

## The three strategies

| Strategy | Rule | Best when |
|---|---|---|
| `round_robin` | cycle through instances (weighted) | requests are roughly uniform in cost |
| `least_connections` | send to the instance with fewest in-flight requests | request durations vary a lot |
| `consistent_hash` | same client key → same instance | sticky sessions, cache locality |

The `Balancer` implements `proxy.Balancer` (`Pick` + `Release`) and dispatches on
`route.LBStrategy`. Per-route state (RR cursor, connection counts, the hash ring) is
built lazily and cached, keyed by `route.ID` + `UpdatedAt` — the same version-stamp
trick used by the plugin chain and the breaker, so a config change rebuilds the
state automatically.

## Weighted round-robin — the *smooth* variant

Naive weighted round-robin for weights `{a:5, b:1}` emits `a a a a a b` — five
requests slam `a` back-to-back, then one to `b`. That burst defeats the purpose of
balancing.

We use **nginx's smooth weighted round-robin** instead, which emits
`a a b a a` … — the same 5:1 ratio, but *interleaved*:

```go
for each server i:  cw[i] += weight[i]
total = Σ weight
pick  = argmax(cw)
cw[pick] -= total
```

Each server accumulates "credit" equal to its weight every round; the one with the
most credit is picked and pays back the total. Over time each server is picked in
exact proportion to its weight, but spread out. `TestWeightedRoundRobin` verifies a
3:1 weight yields exactly 300/100 over 400 picks, and a 1:1 split alternates.

## Least-connections

Track in-flight requests per instance: `Pick` increments the chosen instance's
counter, `Release` (called via `defer` in the gateway when the request finishes)
decrements it. Pick the instance with the smallest counter.

Why it helps: with round-robin, one instance can get stuck with several slow
requests while others idle. Least-connections naturally routes around a temporarily
slow instance because its in-flight count stays high. It shines when request costs
are uneven (some cheap, some expensive).

### An honest trade-off: counts are per-replica

The connection counts live in **each gateway replica's memory**, not in Redis.
That means with 4 gateway replicas, each only knows its own view of in-flight
requests, so the global distribution is approximate rather than perfect. We accept
this deliberately: making least-connections globally exact would require a Redis
round-trip (incr/decr) on *every* request's start and end, doubling the hot-path
Redis traffic, for a marginal accuracy gain. Each replica balancing its own share
well is good enough, and it keeps the fast path fast. (Rate limits and breaker
state *are* shared because there correctness genuinely requires it; here it doesn't.)

## Consistent hashing

For sticky routing (a client should keep hitting the same instance — useful for
in-memory sessions or warm caches), we map a request key to an instance via a
**hash ring**.

The naive approach, `instance = hash(key) % N`, has a fatal flaw: change `N` (add or
remove one instance) and **almost every key remaps** to a different instance —
blowing away every warm cache at once. Consistent hashing fixes this: instances are
placed at points around a ring, and a key goes to the next instance clockwise. Add
or remove an instance and only the keys in *that instance's arc* move — about
`1/N` of them.

### Virtual nodes

If each instance had just one ring point, the arcs would be wildly uneven (bad luck
in hashing → one instance owns half the ring). So each instance is placed at **150
virtual nodes** (scaled by weight). Many small arcs per instance average out to an
even distribution. `TestConsistentHashMinimalRemap` proves that removing one of
three instances remaps well under 20% of keys (vs ~66% for `hash % N`).

The key used is the client identity (`X-Forwarded-For`, else remote addr). In a
richer setup you'd hash the consumer ID or a session cookie.

## See round-robin work

With the demo `/echo` route (round-robin over `upstream-a` and `upstream-b`):

```bash
for i in 1 2 3 4 5; do
  curl -s -H 'X-API-Key: demo-key' localhost:8000/echo/x | grep -o '"instance":"[^"]*"'
done
# "instance":"upstream-a"
# "instance":"upstream-b"
# "instance":"upstream-a"
# "instance":"upstream-b"
# "instance":"upstream-a"
```

The instance name flips every request — that's the balancer spreading load. (Change
`lb_strategy` on the route and reload to try the others.)

Next: [07 — Admin API & hot reload](07-admin-hot-reload.md).
