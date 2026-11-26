# 03 — The plugin system

This is the architecturally interesting part of the gateway. A plugin system turns
the gateway from "a hard-coded proxy" into "a programmable platform": behaviour is
composed per route from reusable middleware, configured from the database, and
hot-reloadable.

Files: `internal/plugin/` (core), `internal/plugin/plugins/requestid/` (reference
plugin), `cmd/gateway/plugins.go` (wiring).

## The middleware chain pattern

Every request runs through an ordered list of plugins. Each plugin gets two hooks:

- **`OnRequest`** — before the request is proxied (auth, rate limit, transform).
- **`OnResponse`** — after the upstream replies (add headers, record metrics).

Request-phase runs **low priority → high priority**. Response-phase runs the
**reverse**. That gives symmetric wrapping, exactly like nested function calls:

```
req:  request-id → auth → rate-limit → transform → [PROXY] → transform → rate-limit → auth → request-id  :resp
      (priority 5)  (10)     (20)         (30)                  (30)         (20)       (10)     (5)
```

Any plugin can call `rc.Abort(status, body)` to short-circuit: the chain stops, the
upstream is never called, and the client gets that status. The **first** abort
wins, so auth's 401 takes precedence over a later rate-limit 429. The
`TestChainOrdering` / `TestChainAbortStops` tests lock both behaviours down.

## The Plugin interface

```go
type Plugin interface {
    Name() string
    Priority() int
    OnRequest(rc *domain.RequestContext) error
    OnResponse(rc *domain.ResponseContext) error
}
```

An embeddable `plugin.Base` provides no-op defaults for both hooks, so a plugin
that only cares about one phase implements just that one. The reference plugin
embeds it.

## Big decision: compiled-in plugins, not `.so` files

The spec says "load it at runtime". Go *does* have a `plugin` package that loads
`.so` files, but it is a well-known foot-gun:

- the plugin and host must be built with the **exact** same toolchain version and
  dependency versions, or it panics at load;
- Linux/macOS only, no Windows;
- you can never *unload* a plugin;
- a panic in plugin init takes down the whole gateway.

For a project whose goal is to *teach the architecture*, that machinery is all cost
and no lesson. So plugins here are **Go types registered in a registry at build
time**. Crucially, we keep the two things that actually make a plugin system
interesting:

1. **Per-route selection & config** — a route decides *which* plugins run and with
   *what* settings, read from the database.
2. **Hot reload** — change a route's plugins in the DB and running gateways pick it
   up without a restart (phase 7).

So plugins are still "loaded at runtime" in every sense that matters to an operator
— just without the `.so` fragility. This is written up as ADR-002 in
[`DECISIONS.md`](DECISIONS.md).

## Registry + Factory: how a plugin gets its dependencies

A plugin like `auth` needs the config store; `rate-limit` needs a Redis client.
Rather than a giant shared `Deps` struct that every plugin ignores 90% of, each
plugin exposes a constructor that **captures exactly what it needs** and returns a
`Factory`:

```go
type Factory func(cfg map[string]any) (Plugin, error)

// requestid has no deps:
reg.Register(requestid.Name, requestid.New())
// auth captures the store (phase 4):
reg.Register(auth.Name, auth.New(store))
```

The `Registry` just maps name → Factory. The `Build(name, cfg)` call instantiates
a configured plugin. Unknown name or bad config → error. All registration happens
in one place, `cmd/gateway/plugins.go`, so **adding a plugin to the gateway is a
one-line change** and the rest of the system discovers it by name via route config.

## ChainFactory: build once, cache, rebuild on change

Building a chain (parse config, instantiate plugins, sort by priority) shouldn't
happen on every request. `ChainFactory` caches the built `Chain` per route, keyed
by **route ID + the route's `UpdatedAt` timestamp**:

```go
if c, ok := cache[route.ID]; ok && c.updatedAt.Equal(route.UpdatedAt) {
    return c.chain   // cache hit
}
```

When a hot reload changes a route, its `UpdatedAt` changes, the cache misses, and
the chain is rebuilt with the new config — **no explicit invalidation logic**. This
is a nice trick: use the data's own version stamp as the cache key and correctness
falls out for free.

Chain building is **best-effort**: if one plugin fails to configure, it's logged
and skipped rather than 500-ing every request on the route. A typo in one plugin's
config shouldn't take the whole route down.

## Where circuit breaking and load balancing live (and why not here)

The spec lists the circuit breaker as a plugin. We deliberately **don't** implement
it as a chain plugin:

- The **load balancer** isn't a plugin either — it's the `Balancer` seam, because
  picking an upstream is a routing concern, not request middleware.
- The **circuit breaker** wraps the *actual upstream call* — it needs to observe
  the real transport error / 5xx status to decide failures. That's a
  `http.RoundTripper` wrapper around the proxy transport, not a chain hook that
  runs before proxying. Implementing it as transport middleware is both more
  correct and simpler. (Details in [doc 05](05-circuit-breaker.md).)

Keeping these as their own seams rather than forcing everything into "plugin"
keeps each abstraction honest.

## The reference plugin: `request-id`

`plugins/requestid` is the template to copy. It:

- parses config (`{"header":"X-Request-ID"}`),
- on request: reuses an incoming ID or generates one, stores it in shared
  `Metadata`,
- on response: echoes the ID back to the client.

Try it (after `make seed`, which enables it on `/echo`):

```bash
curl -sD - -o /dev/null localhost:8000/echo/hello | grep -i x-request-id
# X-Request-Id: 11f8525f0353a8b922f85c08806a9791   (generated)

curl -sD - -o /dev/null -H 'X-Request-ID: my-trace-123' localhost:8000/echo/hello | grep -i x-request-id
# X-Request-Id: my-trace-123                        (propagated)
```

Next: [04 — Rate limiting](04-rate-limiting.md).
