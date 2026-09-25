// Package ratelimit is the rate-limiting plugin. It wraps the Redis sliding-window
// limiter (internal/ratelimit) and applies per-consumer, per-route limits.
//
// Key design: the limit is keyed by consumer + route, so each API consumer gets
// their own quota on each route ("consumer X may call route Y 100×/min"). If the
// request is unauthenticated (no consumer), it falls back to keying by client IP.
package ratelimit

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/1morshed1/gatewayx/internal/domain"
	"github.com/1morshed1/gatewayx/internal/plugin"
	rl "github.com/1morshed1/gatewayx/internal/ratelimit"
)

// Name is the plugin's config key.
const Name = "rate-limit"

// Plugin enforces a per-consumer/route rate limit.
type Plugin struct {
	plugin.Base
	limiter *rl.Limiter
	limit   int
	window  time.Duration
}

// New returns a Factory capturing the shared limiter (and thus Redis client).
func New(limiter *rl.Limiter) plugin.Factory {
	return func(cfg map[string]any) (plugin.Plugin, error) {
		limit, err := intFromCfg(cfg, "limit")
		if err != nil {
			return nil, err
		}
		if limit <= 0 {
			return nil, fmt.Errorf("rate-limit: limit must be > 0")
		}

		windowStr, _ := cfg["window"].(string)
		if windowStr == "" {
			windowStr = "1m"
		}
		window, err := time.ParseDuration(windowStr)
		if err != nil {
			return nil, fmt.Errorf("rate-limit: invalid window %q: %w", windowStr, err)
		}

		return &Plugin{limiter: limiter, limit: limit, window: window}, nil
	}
}

// Name implements plugin.Plugin.
func (p *Plugin) Name() string { return Name }

// Priority implements plugin.Plugin — after auth so the consumer is known.
func (p *Plugin) Priority() int { return plugin.PriorityRateLimit }

// OnRequest checks the limit and either records the request (allowing it) or
// aborts with 429 + Retry-After.
func (p *Plugin) OnRequest(rc *domain.RequestContext) error {
	key := p.key(rc)

	res, err := p.limiter.Allow(rc.Request.Context(), key, p.limit, p.window)
	if err != nil {
		// Fail-open on a limiter error: a Redis blip shouldn't take down the API.
		// (Fail-closed is the alternative — choose per your threat model. For a
		// gateway, availability usually wins; documented in doc 04.)
		return nil
	}

	// Standard-ish rate-limit headers so clients can self-throttle.
	h := rc.ResponseHeader()
	h.Set("X-RateLimit-Limit", strconv.Itoa(p.limit))
	h.Set("X-RateLimit-Remaining", strconv.Itoa(max(res.Remaining, 0)))

	if !res.Allowed {
		retrySecs := int(res.RetryAfter.Seconds())
		if retrySecs < 1 {
			retrySecs = 1
		}
		h.Set("Retry-After", strconv.Itoa(retrySecs))
		rc.Abort(http.StatusTooManyRequests, map[string]string{
			"error":       "rate limit exceeded",
			"retry_after": strconv.Itoa(retrySecs) + "s",
		})
	}
	return nil
}

// key builds the Redis key: prefer the authenticated consumer, else the client IP.
func (p *Plugin) key(rc *domain.RequestContext) string {
	routeID := rc.Route.ID
	if rc.Consumer != nil {
		return fmt.Sprintf("ratelimit:consumer:%s:%s", rc.Consumer.ID, routeID)
	}
	return fmt.Sprintf("ratelimit:ip:%s:%s", clientIP(rc.Request), routeID)
}

func clientIP(r *http.Request) string {
	// X-Forwarded-For (set upstream) takes precedence, else the remote addr.
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// intFromCfg reads an int from JSON config, tolerating float64 (JSON numbers) and
// string forms.
func intFromCfg(cfg map[string]any, key string) (int, error) {
	switch v := cfg[key].(type) {
	case float64:
		return int(v), nil
	case int:
		return v, nil
	case string:
		return strconv.Atoi(v)
	case nil:
		return 0, fmt.Errorf("rate-limit: missing %q", key)
	default:
		return 0, fmt.Errorf("rate-limit: %q has unexpected type %T", key, v)
	}
}
