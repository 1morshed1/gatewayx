// Package loadbalancer spreads requests across a route's healthy upstream
// instances. It implements proxy.Balancer and supports three strategies chosen
// per route:
//
//	round_robin        smooth weighted round-robin (nginx's algorithm)
//	least_connections  fewest in-flight requests wins
//	consistent_hash    same request key -> same upstream (sticky / cache-friendly)
//
// State (RR cursors, connection counts, the hash ring) is per-route and kept in
// memory. It is rebuilt when a route changes (detected via UpdatedAt), the same
// version-stamp trick the plugin ChainFactory uses.
package loadbalancer

import (
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/1morshed1/gatewayx/internal/domain"
	"github.com/1morshed1/gatewayx/internal/proxy"
)

// Balancer implements proxy.Balancer for all strategies.
type Balancer struct {
	mu     sync.Mutex
	states map[string]*routeState
}

// New creates an empty balancer.
func New() *Balancer {
	return &Balancer{states: make(map[string]*routeState)}
}

// routeState holds the per-route mutable balancing state.
type routeState struct {
	updatedAt time.Time
	strategy  domain.LBStrategy
	ups       []domain.Upstream // healthy snapshot, index-stable

	mu sync.Mutex

	// round-robin: current weights, index-aligned with ups (smooth WRR).
	cw []int

	// least-connections: in-flight count per upstream index.
	conns []int64

	// consistent hash ring.
	ring *hashRing
}

// stateFor returns the per-route state, (re)building it when the route is new or
// changed. Rebuild on version change means health/weight/strategy edits take
// effect after a reload without any explicit invalidation.
func (b *Balancer) stateFor(route *domain.Route) *routeState {
	b.mu.Lock()
	defer b.mu.Unlock()

	if s, ok := b.states[route.ID]; ok && s.updatedAt.Equal(route.UpdatedAt) {
		return s
	}

	healthy := route.HealthyUpstreams()
	s := &routeState{
		updatedAt: route.UpdatedAt,
		strategy:  route.LBStrategy,
		ups:       healthy,
		cw:        make([]int, len(healthy)),
		conns:     make([]int64, len(healthy)),
	}
	if route.LBStrategy == domain.ConsistentHash {
		s.ring = newHashRing(healthy)
	}
	b.states[route.ID] = s
	return s
}

// Pick selects an upstream for the request per the route's strategy.
func (b *Balancer) Pick(route *domain.Route, req *http.Request) (*domain.Upstream, error) {
	s := b.stateFor(route)
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.ups) == 0 {
		return nil, proxy.ErrNoUpstream
	}

	var idx int
	switch s.strategy {
	case domain.LeastConnections:
		idx = s.pickLeastConn()
	case domain.ConsistentHash:
		idx = s.ring.pick(hashKey(req))
	default: // round_robin (and unknown strategies fall back to it)
		idx = s.pickSmoothWRR()
	}

	s.conns[idx]++ // counted for least-connections; harmless for others
	// Return a stable pointer; store the chosen index on nothing — Release recounts
	// by URL match below. We copy to avoid handing out a slice-internal pointer
	// that a later rebuild could reuse.
	up := s.ups[idx]
	return &up, nil
}

// Release records that a request to up has finished, decrementing its in-flight
// count. Matching by URL keeps it correct even if the state was rebuilt mid-flight
// (in which case the decrement simply finds no match and is a no-op).
func (b *Balancer) Release(route *domain.Route, up *domain.Upstream) {
	b.mu.Lock()
	s, ok := b.states[route.ID]
	b.mu.Unlock()
	if !ok || up == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.ups {
		if s.ups[i].URL == up.URL {
			if s.conns[i] > 0 {
				s.conns[i]--
			}
			return
		}
	}
}

// pickSmoothWRR implements nginx's smooth weighted round-robin. Unlike naive WRR
// (which sends w1 requests to server 1, then w2 to server 2, ...), this
// interleaves them: for weights {5,1,1} it yields a,a,b,a,c,a,a rather than
// a,a,a,a,a,b,c. Smoothness matters — bursts to one server defeat the point of
// balancing. Caller holds s.mu.
func (s *routeState) pickSmoothWRR() int {
	total := 0
	best := 0
	for i := range s.ups {
		w := s.ups[i].Weight
		if w < 1 {
			w = 1
		}
		s.cw[i] += w
		total += w
		if s.cw[i] > s.cw[best] {
			best = i
		}
	}
	s.cw[best] -= total
	return best
}

// pickLeastConn returns the index with the fewest in-flight requests. Ties break
// toward the lower index. Caller holds s.mu.
func (s *routeState) pickLeastConn() int {
	best := 0
	for i := range s.conns {
		if s.conns[i] < s.conns[best] {
			best = i
		}
	}
	return best
}

// hashKey chooses the value used for consistent hashing: prefer a stable client
// identity (X-Forwarded-For / consumer would be even better) over the raw remote
// addr, so the same caller lands on the same upstream across connections.
func hashKey(req *http.Request) string {
	if xff := req.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	if host, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
		return host
	}
	return req.RemoteAddr
}
