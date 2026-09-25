# 02 — Config store & reverse proxy

This phase builds the beating heart: match a request to a route and forward it to
an upstream. By the end, the gateway actually proxies traffic. Everything after
this (plugins, rate limiting, LB strategies) hangs off the seams we create here.

Files: `internal/domain/`, `internal/config/`, `internal/store/`,
`internal/proxy/`, `migrations/`, `cmd/gateway/`.

## The domain model

`internal/domain` holds plain data types with **no I/O and no framework imports**.
That matters: because `domain` depends on nothing, every other package can depend
on `domain` without risking an import cycle. This is the classic "dependencies
point inward" idea.

The key types:

- **`Route`** — a matching rule (`Host` + `PathPrefix`) plus the upstream pool,
  the load-balancing strategy, the plugin list, and a timeout.
- **`Upstream`** — one backend instance (URL, weight, health flag).
- **`Consumer`** — an authenticated caller (rate limits + analytics key off this).
- **`RequestContext` / `ResponseContext`** — what plugins operate on.

### Why `Abort` is a method, not a callback

The plan sketched `Abort` as a `func` field on the context. We made it a **method
over private fields** instead:

```go
func (c *RequestContext) Abort(status int, body any) { ... }
func (c *RequestContext) Aborted() bool { ... }
```

Why: with a callback, the decision to stop is hidden inside a captured closure —
the proxy can't easily ask "did anyone abort?". With a recorded flag, after each
plugin runs the proxy simply checks `rc.Aborted()`. Control flow stays explicit
and inspectable. Also, the *first* abort wins (auth's 401 beats a later 429),
which is trivial to enforce with a flag and impossible to guarantee with fire-and
-forget callbacks.

## Configuration: a tiny env loader

`internal/config` reads everything from environment variables, with an optional
`.env` for local dev. We deliberately **did not** add a config library. Reading
env vars with sensible defaults is ~100 lines, and rolling it ourselves keeps the
entire config surface visible in one file. Real env vars always override `.env`,
so you can tweak a single value on the command line without editing the file.

## The Postgres store

`internal/store` uses **pgx** directly (no ORM). Two reasons:

1. pgx is the fastest, most complete Postgres driver for Go.
2. We need native `LISTEN/NOTIFY` for hot reload (phase 7); ORMs hide that.

### Loading routes: three queries, not one join

`LoadRoutes` issues *three* queries (routes, upstreams, plugins) and stitches them
in memory, rather than one big join. A join of route × upstream × plugin fans out
rows combinatorially and then you de-duplicate anyway. Three small queries over a
small **control-plane** table is simpler and just as fast. (Rule of thumb: joins
are great for filtering; they're awkward for loading independent child collections.)

### The migrator

`Migrate` applies embedded `.sql` files in filename order, tracking applied ones
in `schema_migrations`. It's intentionally minimal — no down-migrations, no
checksums. Each migration runs in its own transaction, so a failure leaves the DB
consistent. The SQL files are embedded via `//go:embed` (see
`migrations/embed.go`) so they ship *inside* the binary — `gateway migrate` works
without the source tree.

## The proxy core

### Router: lock-free on the hot path

`proxy.Router` keeps the route table behind an `atomic.Pointer`. Reads (every
request calls `Match`) take no lock; a hot reload swaps the whole pointer
atomically. On a component that sees every request, lock contention on the read
path would be a real bottleneck, so we design it out.

Routes are pre-sorted by **descending prefix length** so the *most specific*
prefix wins: `/v2/users` beats `/v2` for a request to `/v2/users/42`. The unit
tests in `router_test.go` pin this behaviour down.

### One ReverseProxy, per-request target

Go's `httputil.ReverseProxy` is production-grade — connection pooling, header
handling, streaming bodies. We use **a single instance** for all upstreams rather
than one per request/upstream (which would throw away connection pooling). The
per-request target is stashed in the request `context` and read back in the
`Rewrite` callback:

```go
g.proxy = &httputil.ReverseProxy{
    Rewrite:        g.rewrite,        // point at chosen upstream, strip prefix
    ModifyResponse: g.modifyResponse, // run response-phase plugins
    ErrorHandler:   g.errorHandler,   // upstream failures -> clean 502/504
    Transport:      transport,        // tuned keep-alive pool
}
```

We use the modern `Rewrite` hook (Go 1.20+) instead of the older `Director`
because `Rewrite` gets both the inbound and outbound request and has a built-in
`SetXForwarded()` that safely manages `X-Forwarded-*` headers (Director-based code
was a common source of header-spoofing bugs).

### The interface seams

`proxy/seams.go` defines `Chain`, `ChainFactory`, and `Balancer` as interfaces,
with trivial defaults (`NopChainFactory`, `FirstHealthyBalancer`). The gateway
depends only on these interfaces. That's why later phases can drop in the real
plugin chain and load balancer **without touching `ServeHTTP`**. If you take one
software-design lesson from this project, let it be this: *program to the seam you
know you'll need, and stub it until you build it.*

### The transport matters

`newUpstreamTransport` tunes connection pooling (`MaxIdleConnsPerHost`,
`IdleConnTimeout`). This is not premature optimisation — without keep-alive reuse,
a gateway pays a fresh TCP (and TLS) handshake on *every* proxied request, which
dominates latency at scale. A gateway lives and dies by connection reuse.

## See it work

```bash
make infra-up            # (postgres + upstreams at least)
make migrate             # applies 0001_init.sql
make seed                # demo consumer + /echo route -> upstream-a/b
make run &               # start the gateway
curl localhost:8000/echo/hello        # -> {"instance":"upstream-a","path":"/hello",...}
curl localhost:8001/admin/routes      # read-only route dump
curl -i localhost:8000/nope           # -> 404 {"error":"no route matched"}
```

Note the response `path` is `/hello`, not `/echo/hello` — prefix stripping worked.
Right now every request goes to `upstream-a` because `FirstHealthyBalancer` always
picks the first healthy upstream; real round-robin arrives in
[doc 06](06-load-balancing.md).

Next: [03 — The plugin system](03-plugin-system.md).
