package plugin

import "fmt"

// Registry maps a plugin name to its Factory. It is populated once at startup
// (see cmd/gateway wiring) and read-only thereafter, so it needs no locking.
type Registry struct {
	factories map[string]Factory
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]Factory)}
}

// Register adds a plugin factory under a name. Registering the same name twice is
// a programming error and panics — it means two plugins fight over one config key.
func (r *Registry) Register(name string, f Factory) {
	if _, exists := r.factories[name]; exists {
		panic(fmt.Sprintf("plugin %q registered twice", name))
	}
	r.factories[name] = f
}

// Build instantiates a plugin by name with the given config. Returns an error if
// the name is unknown or the factory rejects the config.
func (r *Registry) Build(name string, cfg map[string]any) (Plugin, error) {
	f, ok := r.factories[name]
	if !ok {
		return nil, fmt.Errorf("unknown plugin %q", name)
	}
	p, err := f(cfg)
	if err != nil {
		return nil, fmt.Errorf("configure plugin %q: %w", name, err)
	}
	return p, nil
}

// Names returns the registered plugin names (for the admin/metrics view).
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.factories))
	for n := range r.factories {
		out = append(out, n)
	}
	return out
}
