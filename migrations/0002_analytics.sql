-- 0002_analytics.sql — aggregated request analytics.
--
-- The analytics consumer reads raw request-log events from Kafka and rolls them up
-- into per-minute, per-route, per-status-class buckets here. We store AGGREGATES,
-- not raw events: raw events live in Kafka (replayable, cheap); Postgres holds the
-- small, queryable summary that powers reports and the admin analytics endpoint.
--
-- status_class is the HTTP status family (2 = 2xx, 3 = 3xx, 4 = 4xx, 5 = 5xx), so
-- we can compute error rates without storing every distinct status code.
--
-- There is deliberately NO foreign key to routes: analytics must survive a route
-- being deleted (you still want the historical numbers).

CREATE TABLE IF NOT EXISTS request_analytics (
    bucket            TIMESTAMPTZ NOT NULL,   -- truncated to the minute
    route_id          UUID        NOT NULL,
    route_name        TEXT        NOT NULL,
    status_class      SMALLINT    NOT NULL,    -- 2,3,4,5
    count             BIGINT      NOT NULL DEFAULT 0,
    total_duration_ms BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket, route_id, status_class)
);

CREATE INDEX IF NOT EXISTS idx_analytics_bucket ON request_analytics(bucket);
