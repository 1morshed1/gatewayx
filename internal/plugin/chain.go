package plugin

import (
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/1morshed1/gatewayx/internal/domain"
	"github.com/1morshed1/gatewayx/internal/proxy"
)

// Chain is an ordered set of plugin instances for one route. It implements
// proxy.Chain.
type Chain struct {
	plugins []Plugin // sorted by ascending Priority
	log     *slog.Logger
}

// OnRequest runs each plugin's OnRequest in priority order. It stops early if a
// plugin aborts the request or returns an error (which is turned into a 500 via
// Abort so the client gets a clean response and later plugins don't run).
func (c *Chain) OnRequest(rc *domain.RequestContext) {
	for _, p := range c.plugins {
		if err := p.OnRequest(rc); err != nil {
			c.log.Error("plugin OnRequest error", "plugin", p.Name(), "err", err)
			rc.Abort(http.StatusInternalServerError, map[string]string{"error": "internal plugin error"})
		}
		if rc.Aborted() {
			return
		}
	}
}

// OnResponse runs each plugin's OnResponse in *reverse* priority order, so plugins
// wrap the request symmetrically (first-in / last-out), like nested calls. The
// first error is returned and becomes a 502 to the client.
func (c *Chain) OnResponse(rc *domain.ResponseContext) error {
	for i := len(c.plugins) - 1; i >= 0; i-- {
		if err := c.plugins[i].OnResponse(rc); err != nil {
			c.log.Error("plugin OnResponse error", "plugin", c.plugins[i].Name(), "err", err)
			return err
		}
	}
	return nil
}

// ChainFactory builds and caches a Chain per route. It implements
// proxy.ChainFactory.
//
// Chains are cached keyed by route ID + the route's UpdatedAt timestamp: when a
// hot reload changes a route, its UpdatedAt changes, the cache misses, and the
// chain is rebuilt with the new plugin config. No explicit invalidation needed.
type ChainFactory struct {
	registry *Registry
	log      *slog.Logger

	mu    sync.RWMutex
	cache map[string]cachedChain
}

type cachedChain struct {
	updatedAt time.Time
	chain     *Chain
}

// NewChainFactory creates a chain factory backed by the given registry.
func NewChainFactory(registry *Registry, log *slog.Logger) *ChainFactory {
	return &ChainFactory{
		registry: registry,
		log:      log,
		cache:    make(map[string]cachedChain),
	}
}

// For returns the (cached) chain for a route, building it on a cache miss.
func (f *ChainFactory) For(route *domain.Route) proxy.Chain {
	f.mu.RLock()
	if c, ok := f.cache[route.ID]; ok && c.updatedAt.Equal(route.UpdatedAt) {
		f.mu.RUnlock()
		return c.chain
	}
	f.mu.RUnlock()

	chain := f.build(route)

	f.mu.Lock()
	f.cache[route.ID] = cachedChain{updatedAt: route.UpdatedAt, chain: chain}
	f.mu.Unlock()
	return chain
}

// build instantiates and sorts the enabled plugins for a route. It is
// best-effort: a plugin that fails to configure is logged and skipped rather than
// taking down the whole route — a bad config for one plugin shouldn't 500 every
// request on the route.
func (f *ChainFactory) build(route *domain.Route) *Chain {
	var plugins []Plugin
	for _, pc := range route.Plugins {
		if !pc.Enabled {
			continue
		}
		p, err := f.registry.Build(pc.Name, pc.Config)
		if err != nil {
			f.log.Error("skipping plugin", "route", route.Name, "plugin", pc.Name, "err", err)
			continue
		}
		plugins = append(plugins, p)
	}

	sort.SliceStable(plugins, func(i, j int) bool {
		return plugins[i].Priority() < plugins[j].Priority()
	})

	return &Chain{plugins: plugins, log: f.log}
}
