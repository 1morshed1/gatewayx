package main

import (
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/1morshed1/gatewayx/internal/plugin"
	"github.com/1morshed1/gatewayx/internal/plugin/plugins/auth"
	"github.com/1morshed1/gatewayx/internal/plugin/plugins/ratelimit"
	"github.com/1morshed1/gatewayx/internal/plugin/plugins/requestid"
	"github.com/1morshed1/gatewayx/internal/plugin/plugins/transform"
	rl "github.com/1morshed1/gatewayx/internal/ratelimit"
	"github.com/1morshed1/gatewayx/internal/store"
)

// pluginDeps carries the shared dependencies plugins may need. Each plugin's
// constructor takes only the pieces it actually uses.
type pluginDeps struct {
	log   *slog.Logger
	redis *redis.Client
	store *store.Store
}

// buildRegistry is the single place plugins are registered. Adding a plugin to the
// gateway is a one-line change here — the rest of the system discovers it by name
// through route config.
func buildRegistry(deps pluginDeps) *plugin.Registry {
	reg := plugin.NewRegistry()

	limiter := rl.New(deps.redis)

	reg.Register(requestid.Name, requestid.New())
	reg.Register(auth.Name, auth.New(deps.store))
	reg.Register(ratelimit.Name, ratelimit.New(limiter))
	reg.Register(transform.Name, transform.New())

	return reg
}
