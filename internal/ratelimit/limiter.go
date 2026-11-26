// Package ratelimit implements a distributed sliding-window-log rate limiter on
// top of Redis sorted sets.
//
// Why sliding-window log (see ADR-003):
//   - Fixed window (a counter per bucket) allows a "double burst": N requests at
//     the end of one window + N at the start of the next = 2N in a short span.
//   - Token bucket smooths traffic but is about rate shaping, not "exactly N per
//     window".
//   - Sliding-window log stores a timestamp per request and counts the ones still
//     inside the window. It is the most accurate for "no more than N in any
//     rolling window", at the cost of one sorted-set entry per in-window request.
package ratelimit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/redis/go-redis/v9"
)

// slidingWindowScript performs the whole check-and-record atomically in Redis.
//
// Why a Lua script instead of the pipeline the spec sketched: a pipeline runs
// ZCARD (count) and then ZADD (record) as separate steps. Between them, other
// requests can slip in — a classic time-of-check/time-of-use race that lets the
// limit be exceeded under concurrency. A Lua script runs atomically on the Redis
// server, so count-then-maybe-add is indivisible. This is *the* reason the limiter
// is correct under load.
//
// KEYS[1] = the rate-limit key
// ARGV[1] = now (microseconds)
// ARGV[2] = window (microseconds)
// ARGV[3] = limit
// ARGV[4] = unique member for this request (avoids collisions within one microsecond)
//
// Returns { allowed(1/0), remaining, retryAfterMicros }.
const slidingWindowScript = `
local key    = KEYS[1]
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit  = tonumber(ARGV[3])
local member = ARGV[4]

-- Drop entries older than the window.
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now, member)
    -- Expire the key one window after the last write so idle keys are reclaimed.
    redis.call('PEXPIRE', key, math.ceil(window / 1000))
    return {1, limit - count - 1, 0}
end

-- Blocked: compute when the oldest in-window entry will fall out, so we can send
-- an accurate Retry-After.
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local retry = window
if oldest[2] ~= nil then
    retry = (tonumber(oldest[2]) + window) - now
    if retry < 0 then retry = 0 end
end
return {0, 0, retry}
`

// Result is the outcome of an Allow check.
type Result struct {
	Allowed    bool
	Remaining  int           // requests left in the current window
	RetryAfter time.Duration // how long until the caller may retry (when blocked)
}

// Limiter enforces sliding-window limits using Redis.
type Limiter struct {
	redis  *redis.Client
	script *redis.Script
}

// New creates a limiter backed by the given Redis client. The script is loaded
// lazily by go-redis (EVALSHA with EVAL fallback), so no explicit SCRIPT LOAD.
func New(client *redis.Client) *Limiter {
	return &Limiter{
		redis:  client,
		script: redis.NewScript(slidingWindowScript),
	}
}

// Allow reports whether a request under `key` is within `limit` per `window`.
func (l *Limiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (Result, error) {
	now := time.Now().UnixMicro()
	member := uniqueMember(now)

	res, err := l.script.Run(ctx, l.redis,
		[]string{key},
		now, window.Microseconds(), limit, member,
	).Int64Slice()
	if err != nil {
		return Result{}, err
	}

	out := Result{
		Allowed:   res[0] == 1,
		Remaining: int(res[1]),
	}
	if !out.Allowed {
		out.RetryAfter = time.Duration(res[2]) * time.Microsecond
	}
	return out, nil
}

// uniqueMember makes each request a distinct ZSET member even when two land in the
// same microsecond (score collisions would otherwise silently undercount).
func uniqueMember(now int64) string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return time.Unix(0, now*1000).Format("150405.000000") + "-" + hex.EncodeToString(b)
}
