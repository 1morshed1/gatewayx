# 10 — Load testing

An API gateway lives on the hot path of every request, so its performance *is* the
system's performance. This phase measures it with [k6](https://k6.io/), and — for
the correctness-critical rate limiter — proves accuracy under real concurrency.

Files: `test/k6/throughput.js`, `test/k6/ratelimit_accuracy.js`.

## What we measure and why

The spec sets four targets. Each maps to a thing an operator genuinely cares about:

| Test | Question | Target |
|---|---|---|
| Throughput | how many req/s can one instance proxy? | 20,000+ req/s |
| Latency overhead | how much does the gateway add over the upstream? | < 5ms p99 |
| Rate-limiter accuracy | does it admit *exactly* the configured number? | precise |
| Breaker responsiveness | how fast does it open / recover? | seconds |

## The scripts are self-provisioning

Both k6 scripts use the **admin API** to create the exact route they need in
`setup()` and delete it in `teardown()`. That's a nice pattern: the load test owns
its fixture, so it doesn't depend on seed state and doesn't leave junk behind. It
also happens to exercise hot reload (the route appears without a gateway restart).

```bash
# install k6 (https://k6.io/docs/get-started/installation/), then:
make run &                                  # gateway + upstreams must be running
k6 run test/k6/throughput.js
k6 run test/k6/ratelimit_accuracy.js
```

## Throughput & latency (`throughput.js`)

Creates a `bench` route with **no auth and no rate limit** — we're measuring the
proxy machinery itself, not the plugins — over both echo upstreams, then ramps the
arrival rate to 5,000 req/s and holds. Thresholds fail the run if p99 latency or the
error rate exceed budget:

```js
thresholds: {
  http_req_duration: ['p(99)<15'],  // total; overhead is a few ms against local echo
  http_req_failed:   ['rate<0.01'],
}
```

Interpreting results: against the local echo upstream, total latency ≈ upstream time
+ gateway overhead. Because echo is ~instant, the p99 you see is essentially the
gateway's own overhead plus loopback. To isolate overhead precisely, also hit an
upstream directly (`:9001`) and subtract. Numbers depend heavily on the host —
CPU count, whether it's a laptop or a server — so treat the target (20k+ req/s) as
"a single Go instance should reach this on a decent box," not a promise on any
machine.

## Rate-limiter accuracy — the important one (`ratelimit_accuracy.js`)

This is the test that actually *validates the algorithm*. It creates a route limited
to **50 requests / 10s**, then fires ~100 req/s for 10s (≈1000 requests) as the demo
consumer, and counts how many were admitted vs. 429'd. Correct behaviour: **≈50
admitted**, the rest rejected. A number well above 50 would mean the limiter is
leaking — the exact TOCTOU race that a naive count-then-add pipeline suffers and our
atomic Lua script prevents (doc 04).

### Verified without k6, too

Even without k6 installed, the limiter's accuracy under concurrency is easy to
demonstrate with plain `curl`. Against a route limited to 50/10s, firing **300
requests at concurrency 60**:

```bash
seq 1 300 | xargs -P 60 -I{} \
  curl -s -o /dev/null -w '%{http_code}\n' -H 'X-API-Key: demo-key' \
  localhost:8000/rltest/x | sort | uniq -c
#      50 200
#     250 429
```

**Exactly 50 admitted, 250 rejected** — not 51, not 60 — under 60-way concurrency.
That is the atomic sliding-window limiter doing its one job correctly. If the check
and the insert weren't atomic, many of those 60 concurrent requests would each read
"under the limit" simultaneously and slip through, and you'd see well over 50.

## Circuit-breaker responsiveness

Covered hands-on in [doc 05](05-circuit-breaker.md): point a route at an upstream
forced to return `503` (`?status=503`), watch the breaker open after the failure
threshold (fast-fail `503` with `X-Circuit-Broken: true`), then send a healthy
request after `CB_OPEN_TIMEOUT` and watch it recover via the half-open probe. To
turn that into a load test, run `throughput.js` against a route whose upstream you
kill mid-run and watch error rate spike then flatten as the breaker trips.

## What the numbers teach

The real lesson of load testing a gateway isn't a single req/s figure — it's the
*shape* under stress: latency should stay flat then knee sharply at saturation, the
limiter should hold its exact number regardless of concurrency, and the breaker
should bound the blast radius of a failing upstream. Those behaviours, not a
headline number, are what make a gateway safe to put in front of everything.

That's the last phase. Back to the [docs index](README.md) or the
[architecture overview](00-architecture.md).
