package main

import (
	"log/slog"

	"github.com/1morshed1/gatewayx/internal/plugin"
	"github.com/1morshed1/gatewayx/internal/plugin/plugins/requestid"
)

// pluginDeps carries the shared dependencies plugins may need. As later phases
// add plugins (auth needs the store, rate-limit needs Redis), their dependencies
// are added here and passed to the relevant constructor.
type pluginDeps struct {
	log *slog.Logger
	// redis  *redis.Client   (phase 4)
	// store  *store.Store    (phase 4)
}

// buildRegistry is the single place plugins are registered. Adding a plugin to the
// gateway is a one-line change here — the rest of the system discovers it by name
// through route config.
func buildRegistry(deps pluginDeps) *plugin.Registry {
	reg := plugin.NewRegistry()

	reg.Register(requestid.Name, requestid.New())
	// reg.Register(auth.Name, auth.New(deps.store))            (phase 4)
	// reg.Register(ratelimit.Name, ratelimit.New(deps.redis))  (phase 4)
	// reg.Register(transform.Name, transform.New())            (phase 4)

	return reg
}
