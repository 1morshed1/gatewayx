# 07 — Admin API & hot configuration reload

A gateway's config changes constantly: add a route, rotate an API key, tune a rate
limit. Restarting the gateway for every change is a non-starter — it drops
in-flight connections and every request during the restart. So the gateway must
pick up config changes **while running**. This phase builds the control plane (an
Admin REST API) and the mechanism that pushes changes into the live data plane
(hot reload via Postgres `LISTEN/NOTIFY`).

Files: `internal/admin/`, `internal/store/admin.go`, `internal/store/listen.go`,
migration `0001_init.sql` (the NOTIFY triggers).

## The Admin API

`internal/admin` is a small REST API on its own port (`:8001`), separate from the
public traffic port (`:8000`). Keeping them separate matters: you never want the
admin surface exposed to the internet alongside public traffic — you firewall
`:8001` to your internal network / VPN.

| Method & path | Does |
|---|---|
| `GET /admin/routes` | dump the **live** in-memory route table |
| `POST /admin/routes` | create a route (with upstreams + plugins) |
| `DELETE /admin/routes/{id}` | delete a route |
| `GET /admin/consumers` | list consumers (API keys omitted) |
| `POST /admin/consumers` | create a consumer |
| `DELETE /admin/consumers/{id}` | delete a consumer |
| `POST /admin/reload` | force a reload |
| `GET /healthz` | liveness |

Notes:

- **Live vs stored.** `GET /admin/routes` returns what the gateway is *actually
  serving* (the in-memory table), not a fresh DB read. That's the useful view for
  confirming a change took effect.
- **Atomic writes.** `CreateRoute` inserts the route + its upstreams + its plugins
  in one transaction (`store/admin.go`). You never observe a half-created route.
- **Routing.** Uses Go 1.22's method-and-wildcard mux patterns
  (`DELETE /admin/routes/{id}` + `r.PathValue("id")`) — no third-party router.
- **Auth.** Mutating endpoints go through `requireToken`. If `ADMIN_TOKEN` is set,
  they require a matching `X-Admin-Token` header; if unset, it's open (dev mode).

## Hot reload: the mechanism

The data plane holds its routes in `proxy.Router` behind an `atomic.Pointer`
(doc 02). "Reloading" is just: read all routes from Postgres, build the sorted
table, atomically swap the pointer. In-flight requests finish on the old table;
new requests see the new one. No locks, no dropped connections.

The question is *when* to reload. Two triggers:

### 1. Push via `LISTEN/NOTIFY` (the good one)

Postgres can send asynchronous messages on a named channel. Migration `0001`
installs `AFTER INSERT/UPDATE/DELETE` triggers on `routes`, `upstreams`, and
`route_plugins` that call `pg_notify('route_changes', 'reload')`. Every gateway
replica runs `WatchListen`, which holds a **dedicated** connection doing
`LISTEN route_changes` and blocks on `WaitForNotification`. The moment *anyone*
changes routing config — the admin API, a migration, or even a `psql` by hand —
every replica wakes up within milliseconds and reloads.

```
admin/psql  ──writes──▶  Postgres ──trigger──▶ NOTIFY 'route_changes'
                                                    │  (broadcast)
                        ┌───────────────────────────┼───────────────┐
                        ▼                            ▼               ▼
                   gateway pod 1               gateway pod 2   gateway pod 3
                   reload table                reload table    reload table
```

Why a **dedicated** connection (not the pool): `LISTEN` registration is per
connection, and the connection must stay open and idle waiting for messages. A
pooled connection gets recycled between queries, which would drop the LISTEN. The
watcher also reconnects with backoff if the connection drops (DB restart, network
blip), so hot reload survives transient failures.

Why NOTIFY instead of a message broker (Kafka/Redis pub-sub): this is low-volume
**control-plane** traffic. Adding a broker to distribute "config changed" pings
would be operational weight for no benefit — the database that already holds the
config can announce its own changes for free. (ADR-005.)

### 2. Poll fallback

`WatchPoll` just calls reload on a ticker. It's there for environments where a
dedicated listener connection is a problem — most notably behind **PgBouncer in
transaction pooling mode**, which multiplexes connections and breaks `LISTEN`.
Pick it with `CONFIG_RELOAD_MODE=poll`; the cost is up to `CONFIG_POLL_INTERVAL`
of staleness after a change.

### Immediate local consistency

After an admin mutation, the API also calls `reload()` directly (`reloadNow`), so
the pod that served the write reflects it instantly without waiting for its own
NOTIFY to round-trip. Other replicas get it via NOTIFY. Reloads are idempotent, so
the double reload (direct + NOTIFY) is harmless — you'll see two "route table
reloaded" log lines and that's fine.

## Downstream effect: plugin chains & LB rebuild automatically

Because reload swaps in fresh `Route` values with new `UpdatedAt` timestamps, the
plugin `ChainFactory` and the load balancer both notice the version change and
rebuild their per-route state on next use (docs 03 and 06). So changing a route's
plugins or LB strategy "just works" after a reload — no special invalidation code
anywhere. This is the version-stamp pattern paying off across three subsystems.

## See it work

```bash
make run &     # gateway on :8000, admin on :8001

# /demo doesn't exist yet:
curl -s -o /dev/null -w '%{http_code}\n' localhost:8000/demo/x         # 404

# create it via the admin API — NO restart:
curl -s -XPOST localhost:8001/admin/routes -d '{
  "name":"demo","path_prefix":"/demo","strip_prefix":true,
  "upstreams":[{"url":"http://localhost:9001","weight":1}],
  "plugins":[{"name":"request-id","enabled":true,"config":{}}]}'

# it proxies immediately:
curl -s localhost:8000/demo/hello                                     # {"instance":"upstream-a","path":"/hello",...}

# even a raw psql INSERT triggers a reload on every replica via NOTIFY.
```

Next: [08 — Analytics with Kafka](08-analytics.md).
