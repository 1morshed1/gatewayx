// Package domain holds the core types shared across the gateway: routes,
// upstreams, consumers, and the per-request contexts that plugins operate on.
//
// These types are deliberately free of any I/O or framework concerns. The store
// package loads them from Postgres, the proxy package routes with them, and the
// plugin package reads/mutates the request contexts. Keeping them dependency-free
// avoids import cycles (everything can import domain; domain imports almost
// nothing).
package domain

import "time"

// LBStrategy names a load-balancing algorithm. The string values match what is
// stored in the database so config maps cleanly to behaviour.
type LBStrategy string

const (
	// RoundRobin cycles through upstreams, honouring per-upstream weights.
	RoundRobin LBStrategy = "round_robin"
	// LeastConnections sends each request to the upstream with the fewest
	// in-flight requests. Better when request durations vary a lot.
	LeastConnections LBStrategy = "least_connections"
	// ConsistentHash maps a request key (e.g. consumer ID) to a stable
	// upstream, so the same caller keeps hitting the same instance.
	ConsistentHash LBStrategy = "consistent_hash"
)

// Upstream is one concrete backend instance a route can proxy to.
type Upstream struct {
	ID     string `json:"id"`
	URL    string `json:"url"`    // e.g. http://upstream-a:8080
	Weight int    `json:"weight"` // relative weight for weighted round-robin (>=1)
	// Healthy is updated by health checks / the circuit breaker. Unhealthy
	// upstreams are skipped by the load balancer.
	Healthy bool `json:"healthy"`
}

// Route is a single routing rule: match incoming requests and forward them to a
// pool of upstreams, running the configured plugin chain along the way.
type Route struct {
	ID   string `json:"id"`
	Name string `json:"name"`

	// Matching. A request matches when Host matches (empty = any host) AND the
	// request path starts with PathPrefix. Longest matching PathPrefix wins
	// (see proxy.Router), so "/v2/users" beats "/v2".
	Host       string `json:"host"`
	PathPrefix string `json:"path_prefix"`

	// StripPrefix removes PathPrefix from the path before proxying, so a route
	// on "/echo" forwarding to upstream "/" turns "/echo/hello" into "/hello".
	StripPrefix bool `json:"strip_prefix"`

	Upstreams  []Upstream     `json:"upstreams"`
	LBStrategy LBStrategy     `json:"lb_strategy"`
	Plugins    []PluginConfig `json:"plugins"`

	// Timeout bounds the whole upstream request. Zero means use the gateway
	// default.
	Timeout time.Duration `json:"timeout"`

	UpdatedAt time.Time `json:"updated_at"`
}

// PluginConfig is the per-route configuration for one plugin. Name selects which
// registered plugin to run; Config carries plugin-specific settings (e.g. the
// rate limiter's limit/window). The shape of Config is validated by each plugin.
type PluginConfig struct {
	Name    string         `json:"name"`
	Enabled bool           `json:"enabled"`
	Config  map[string]any `json:"config"`
}

// Consumer is an authenticated API caller. Rate limits and analytics are keyed
// by consumer, so "who" is a first-class concept.
type Consumer struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	APIKey    string    `json:"-"` // never serialised back out
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
}

// MatchesHost reports whether the route applies to the given request host. An
// empty Route.Host is a wildcard that matches any host.
func (r *Route) MatchesHost(host string) bool {
	if r.Host == "" {
		return true
	}
	return r.Host == host
}

// HealthyUpstreams returns only the upstreams currently marked healthy. The load
// balancer operates on this subset so it never picks a known-dead instance.
func (r *Route) HealthyUpstreams() []Upstream {
	out := make([]Upstream, 0, len(r.Upstreams))
	for _, u := range r.Upstreams {
		if u.Healthy {
			out = append(out, u)
		}
	}
	return out
}

// Clone returns a deep-ish copy safe to hand to a request without worrying that
// a concurrent hot-reload mutates it mid-flight. Slices are copied; the maps in
// plugin configs are treated as read-only by convention.
func (r *Route) Clone() *Route {
	cp := *r
	cp.Upstreams = append([]Upstream(nil), r.Upstreams...)
	cp.Plugins = append([]PluginConfig(nil), r.Plugins...)
	return &cp
}
