package proxy

import (
	"net/http"
	"time"

	"github.com/1morshed1/gatewayx/internal/domain"
)

// This file defines the interfaces the gateway depends on for behaviour that is
// implemented in later phases, plus no-op defaults so the gateway runs today.
//
// Depending on interfaces (not concrete types) here is what lets each phase slot
// in without touching the request lifecycle in gateway.go.

// Chain runs the plugin pipeline for one route. Implemented by the plugin package
// (phase 3). A route's chain is built once and reused across requests.
type Chain interface {
	// OnRequest runs request-phase plugins in priority order. A plugin may call
	// rc.Abort(...) to short-circuit; callers check rc.Aborted() afterwards.
	OnRequest(rc *domain.RequestContext)
	// OnResponse runs response-phase plugins in reverse priority order once the
	// upstream has replied. Returning an error turns into a 502 to the client.
	OnResponse(rc *domain.ResponseContext) error
}

// ChainFactory builds (and caches) the Chain for a route. Different routes have
// different plugin sets, so chains are per-route.
type ChainFactory interface {
	For(route *domain.Route) Chain
}

// Balancer selects an upstream instance for a request and is told when the
// request finishes (so strategies like least-connections can count in-flight
// requests). Implemented by the loadbalancer package (phase 5).
type Balancer interface {
	Pick(route *domain.Route, req *http.Request) (*domain.Upstream, error)
	Release(route *domain.Route, up *domain.Upstream)
}

// Emitter receives one analytics event per request. Implemented by the analytics
// package (phase 7). It MUST be non-blocking — the request path never waits on
// analytics.
type Emitter interface {
	Emit(domain.RequestLog)
}

// NopEmitter discards events. Default when analytics is disabled.
type NopEmitter struct{}

// Emit implements Emitter.
func (NopEmitter) Emit(domain.RequestLog) {}

// Metrics records per-request telemetry. Implemented by the observability package.
type Metrics interface {
	ObserveRequest(route string, status int, dur time.Duration)
}

// NopMetrics records nothing. Default when metrics are disabled.
type NopMetrics struct{}

// ObserveRequest implements Metrics.
func (NopMetrics) ObserveRequest(string, int, time.Duration) {}

// --- No-op / trivial defaults ----------------------------------------------

// nopChain runs no plugins. Used until the real plugin chain is wired in.
type nopChain struct{}

func (nopChain) OnRequest(*domain.RequestContext)         {}
func (nopChain) OnResponse(*domain.ResponseContext) error { return nil }

// NopChainFactory returns a chain that does nothing. Handy for tests and for the
// early phases before plugins exist.
type NopChainFactory struct{}

// For implements ChainFactory.
func (NopChainFactory) For(*domain.Route) Chain { return nopChain{} }

// FirstHealthyBalancer is a placeholder balancer: it returns the first healthy
// upstream. The real strategies (round-robin, least-connections, consistent
// hash) replace it in phase 5.
type FirstHealthyBalancer struct{}

// Pick implements Balancer.
func (FirstHealthyBalancer) Pick(route *domain.Route, _ *http.Request) (*domain.Upstream, error) {
	for i := range route.Upstreams {
		if route.Upstreams[i].Healthy {
			return &route.Upstreams[i], nil
		}
	}
	return nil, ErrNoUpstream
}

// Release implements Balancer (no-op for this strategy).
func (FirstHealthyBalancer) Release(*domain.Route, *domain.Upstream) {}
