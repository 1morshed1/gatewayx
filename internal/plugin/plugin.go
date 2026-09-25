// Package plugin defines the gateway's middleware system: the Plugin interface,
// a name->factory registry, and the per-route chain that runs plugins in order.
//
// A "plugin" is a piece of cross-cutting behaviour (auth, rate limiting, request
// transformation, ...) that runs on every request for the routes it is enabled
// on. Plugins are ordered by Priority and can short-circuit a request via
// RequestContext.Abort.
//
// See docs/03-plugin-system.md for the design rationale, including why plugins are
// compiled-in Go interfaces rather than dynamically-loaded .so files.
package plugin

import "github.com/1morshed1/gatewayx/internal/domain"

// Well-known priorities. Lower numbers run first. Spacing leaves room to insert
// new plugins between existing ones without renumbering.
const (
	PriorityAuth      = 10 // authenticate the caller first — everything else needs the consumer
	PriorityRateLimit = 20 // then enforce quota (keyed by the now-known consumer)
	PriorityTransform = 30 // finally reshape the request before it leaves
)

// Plugin is one unit of middleware. Implementations are usually configured per
// route (built by a Factory from JSON config), so a single plugin *type* can run
// with different settings on different routes.
type Plugin interface {
	// Name identifies the plugin (matches the config `name`).
	Name() string

	// Priority controls ordering within the chain; lower runs first on the
	// request phase (and therefore last on the response phase).
	Priority() int

	// OnRequest runs before the request is proxied. Return an error to fail the
	// request (mapped to 500), or call rc.Abort(...) to return a specific status
	// (e.g. 401/429). If rc.Aborted() is set, the chain stops.
	OnRequest(rc *domain.RequestContext) error

	// OnResponse runs after the upstream replies, in reverse priority order.
	OnResponse(rc *domain.ResponseContext) error
}

// Factory builds a configured plugin instance from its per-route JSON config.
// Each plugin package exposes a constructor that captures its own dependencies
// (Redis client, store, ...) and returns a Factory — so the registry never needs
// to know what any given plugin depends on.
type Factory func(cfg map[string]any) (Plugin, error)

// Base is an optional embeddable helper providing no-op OnRequest/OnResponse so a
// plugin that only cares about one phase doesn't have to implement both.
type Base struct{}

// OnRequest is a no-op default.
func (Base) OnRequest(*domain.RequestContext) error { return nil }

// OnResponse is a no-op default.
func (Base) OnResponse(*domain.ResponseContext) error { return nil }
