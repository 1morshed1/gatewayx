-- 0001_init.sql — core configuration schema for GatewayX.
--
-- Everything the control plane needs: consumers (API callers), routes (matching
-- rules), upstreams (backend instances per route), and per-route plugin config.
-- A NOTIFY trigger lets running gateways hot-reload when this data changes
-- (see docs/07-admin-hot-reload.md).
--
-- Migrations are plain SQL applied in filename order by `cmd/gateway migrate`.
-- They are idempotent (IF NOT EXISTS) so re-running is safe.

CREATE EXTENSION IF NOT EXISTS "pgcrypto"; -- for gen_random_uuid()

-- Consumers: authenticated API callers. Rate limits + analytics key off these.
CREATE TABLE IF NOT EXISTS consumers (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       TEXT NOT NULL,
    api_key    TEXT NOT NULL UNIQUE,
    enabled    BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Routes: match incoming requests and forward to a pool of upstreams.
CREATE TABLE IF NOT EXISTS routes (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT NOT NULL,
    host         TEXT NOT NULL DEFAULT '',        -- '' = match any host
    path_prefix  TEXT NOT NULL,
    strip_prefix BOOLEAN NOT NULL DEFAULT false,
    lb_strategy  TEXT NOT NULL DEFAULT 'round_robin',
    timeout_ms   INTEGER NOT NULL DEFAULT 0,      -- 0 = use gateway default
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Upstreams: concrete backend instances belonging to a route.
CREATE TABLE IF NOT EXISTS upstreams (
    id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    route_id UUID NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    url      TEXT NOT NULL,
    weight   INTEGER NOT NULL DEFAULT 1 CHECK (weight >= 1),
    healthy  BOOLEAN NOT NULL DEFAULT true
);
CREATE INDEX IF NOT EXISTS idx_upstreams_route ON upstreams(route_id);

-- Per-route plugin configuration. `name` selects a registered plugin; `config`
-- carries its plugin-specific settings as JSON.
CREATE TABLE IF NOT EXISTS route_plugins (
    id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    route_id UUID NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    name     TEXT NOT NULL,
    enabled  BOOLEAN NOT NULL DEFAULT true,
    config   JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (route_id, name)
);
CREATE INDEX IF NOT EXISTS idx_route_plugins_route ON route_plugins(route_id);

-- ---------------------------------------------------------------------------
-- Hot-reload signalling.
--
-- Any change to routing config fires NOTIFY on channel 'route_changes'. The
-- gateway LISTENs on this channel and reloads its in-memory route table. We send
-- a generic payload; the gateway reloads the whole table (it is small), which is
-- simpler and less error-prone than diffing individual rows.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION notify_route_changes() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('route_changes', 'reload');
    RETURN NULL; -- AFTER trigger, return value ignored
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_routes_notify ON routes;
CREATE TRIGGER trg_routes_notify
    AFTER INSERT OR UPDATE OR DELETE ON routes
    FOR EACH STATEMENT EXECUTE FUNCTION notify_route_changes();

DROP TRIGGER IF EXISTS trg_upstreams_notify ON upstreams;
CREATE TRIGGER trg_upstreams_notify
    AFTER INSERT OR UPDATE OR DELETE ON upstreams
    FOR EACH STATEMENT EXECUTE FUNCTION notify_route_changes();

DROP TRIGGER IF EXISTS trg_route_plugins_notify ON route_plugins;
CREATE TRIGGER trg_route_plugins_notify
    AFTER INSERT OR UPDATE OR DELETE ON route_plugins
    FOR EACH STATEMENT EXECUTE FUNCTION notify_route_changes();
