// Package proxy is the gateway core: it matches requests to routes, runs the
// plugin chain, picks an upstream, and reverse-proxies the request.
//
// The package defines small interfaces (Chain, ChainFactory, Balancer) for the
// pieces that arrive in later phases (plugins, load balancing, circuit breaking).
// This lets the proxy be built and run now with trivial default implementations,
// then upgraded without rewriting the request lifecycle.
package proxy

import (
	"sort"
	"strings"
	"sync/atomic"

	"github.com/1morshed1/gatewayx/internal/domain"
)

// Router matches incoming (host, path) pairs to routes. The route table is held
// behind an atomic.Pointer so hot reload can swap the whole table without locks
// on the read path — matching is on the hot path of every request, so it must be
// lock-free and fast.
type Router struct {
	table atomic.Pointer[[]*domain.Route]
}

// NewRouter creates an empty router.
func NewRouter() *Router {
	r := &Router{}
	empty := []*domain.Route{}
	r.table.Store(&empty)
	return r
}

// Update atomically replaces the route table. Routes are sorted by descending
// path-prefix length so the *most specific* prefix wins: with routes on "/v2"
// and "/v2/users", a request to "/v2/users/1" matches "/v2/users".
func (r *Router) Update(routes []*domain.Route) {
	sorted := make([]*domain.Route, len(routes))
	copy(sorted, routes)
	sort.SliceStable(sorted, func(i, j int) bool {
		return len(sorted[i].PathPrefix) > len(sorted[j].PathPrefix)
	})
	r.table.Store(&sorted)
}

// Match returns the best route for a host+path, or nil if none matches. Because
// the table is pre-sorted by prefix length, the first host+prefix match is the
// most specific one.
func (r *Router) Match(host, path string) *domain.Route {
	table := *r.table.Load()
	for _, route := range table {
		if route.MatchesHost(host) && strings.HasPrefix(path, route.PathPrefix) {
			return route
		}
	}
	return nil
}

// Routes returns a snapshot of the current table (for the admin metrics view).
func (r *Router) Routes() []*domain.Route {
	return *r.table.Load()
}
