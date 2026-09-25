# 11 — The transform plugin

The plan's plugin set includes a **Request Transform Plugin**. This is it. Transform
plugins let you reshape traffic without touching the upstream service — the gateway
becomes the place you adapt requests and responses to whatever a client or backend
needs.

Files: `internal/plugin/plugins/transform/`.

## Why a gateway does transformation

Real reasons you reach for this:

- **Inject context** the upstream needs but the client doesn't send (e.g. an
  internal `X-Tenant` header derived elsewhere).
- **Strip secrets/noise** before the upstream sees them (drop a `Cookie`, remove a
  debug header).
- **Version shimming** — rewrite `/v1/...` to `/v2/...` so an old client keeps
  working against a new backend.
- **Response hygiene** — set `X-Powered-By`, remove a leaky `Server` header, or
  rewrite internal hostnames that leaked into a response body.

Doing this at the gateway means one place, applied uniformly, changeable at runtime
via config — instead of editing every backend.

## What it does

Config has three optional sections (`request`, `response`, `rewrite_path`):

```json
{
  "request":  { "headers": { "set": {"X-Env":"prod"}, "add": {"X-Trace":"1"}, "remove": ["Cookie"] },
                "body_replace": [ {"from":"http://","to":"https://"} ] },
  "response": { "headers": { "set": {"X-Powered-By":"gatewayx"}, "remove": ["Server"] },
                "body_replace": [ {"from":"internal.svc","to":"api.example.com"} ] },
  "rewrite_path": { "from":"/v1", "to":"/v2" }
}
```

Header ops apply in the order **remove → set → add** (so you can remove-then-add
deterministically). `set` overwrites; `add` appends (for multi-value headers).

## Priority: where it sits in the chain

It runs at `PriorityTransform` (30), i.e. **after** auth (10) and rate limit (20).
That ordering is deliberate: you only want to spend effort transforming a request
that has already passed auth and quota. On the response phase the chain runs in
reverse, so transform runs *first* — it decorates the upstream reply before anything
else observes it.

## Two implementation notes worth internalising

### 1. Parse nested config by round-tripping through JSON

Plugin config arrives as a generic `map[string]any` (it came from a JSONB column).
Rather than hand-traverse nested maps (`cfg["request"].(map[string]any)["headers"]…`
— tedious and error-prone), we marshal the map back to JSON and unmarshal into a
typed struct:

```go
b, _ := json.Marshal(raw)
var c config
json.Unmarshal(b, &c)
```

This gives us typed config, validation, and defaults for free. It's a handy trick
any time you have loosely-typed config that has a known shape.

### 2. Body transformation kills streaming — so it's opt-in

Header and path rewrites are cheap: they touch metadata, the body streams through
untouched. **Body** rewriting is different — you can't replace bytes you haven't
seen, so `body_replace` must buffer the whole body into memory, rewrite it, and set
a corrected `Content-Length`. That disables streaming for the route and costs memory
proportional to the body size.

So body replacement is **opt-in per route** and off by default. Two guards:

- Only runs when `body_replace` is configured.
- **Skips compressed bodies** (`Content-Encoding: gzip`, …) — you can't string-
  replace compressed bytes, so we leave them intact rather than corrupt them.

This is a recurring theme in gateway design: **the powerful feature has a cost
(buffering), so you make it explicit and bounded rather than on-by-default.**

## Verify it

The demo `/echo` route enables transform (sets `X-Gateway-Env` on the request and
`X-Powered-By` on the response):

```bash
curl -sD - -H 'X-API-Key: demo-key' localhost:8000/echo/x | grep -i x-powered-by
# X-Powered-By: gatewayx
# ...and the upstream echo shows it received "X-Gateway-Env":["demo"]
```

Body replacement (create a route with a `response.body_replace` rule):

```bash
# a route that rewrites "upstream-a" -> "REDACTED" in the response body:
curl -s localhost:8000/tf/x | grep instance
# "instance":"REDACTED"
```

Unit tests (`transform_test.go`) cover header ops + path rewrite, body replace with
`Content-Length` correction, and the compressed-body skip.

Back to the [docs index](README.md).
