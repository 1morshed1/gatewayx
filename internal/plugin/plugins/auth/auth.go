// Package auth is the API-key authentication plugin. It identifies the calling
// consumer from an API key header and attaches the Consumer to the request
// context, so downstream plugins (rate limiting, analytics) know *who* is calling.
//
// It runs at PriorityAuth (early) because almost everything else depends on the
// consumer being known.
package auth

import (
	"context"
	"net/http"

	"github.com/1morshed1/gatewayx/internal/domain"
	"github.com/1morshed1/gatewayx/internal/plugin"
)

// Name is the plugin's config key.
const Name = "auth"

// Store is the narrow dependency the auth plugin needs: look up a consumer by API
// key. Defining it here (rather than importing the concrete *store.Store) keeps
// the plugin decoupled and trivially testable with a fake.
type Store interface {
	GetConsumerByAPIKey(ctx context.Context, apiKey string) (*domain.Consumer, error)
}

// Plugin authenticates requests by API key.
type Plugin struct {
	plugin.Base
	store  Store
	header string
}

// New returns a Factory that captures the store dependency.
func New(store Store) plugin.Factory {
	return func(cfg map[string]any) (plugin.Plugin, error) {
		header, _ := cfg["header"].(string)
		if header == "" {
			header = "X-API-Key"
		}
		return &Plugin{store: store, header: header}, nil
	}
}

// Name implements plugin.Plugin.
func (p *Plugin) Name() string { return Name }

// Priority implements plugin.Plugin.
func (p *Plugin) Priority() int { return plugin.PriorityAuth }

// OnRequest validates the API key and attaches the consumer, or aborts with 401.
func (p *Plugin) OnRequest(rc *domain.RequestContext) error {
	key := rc.Request.Header.Get(p.header)
	if key == "" {
		rc.Abort(http.StatusUnauthorized, map[string]string{"error": "missing api key"})
		return nil
	}

	consumer, err := p.store.GetConsumerByAPIKey(rc.Request.Context(), key)
	if err != nil {
		// A store error is our fault, not the caller's: 500, don't leak details.
		return err
	}
	if consumer == nil {
		rc.Abort(http.StatusUnauthorized, map[string]string{"error": "invalid api key"})
		return nil
	}

	rc.Consumer = consumer
	// Strip the credential so it never leaks to the upstream.
	rc.Request.Header.Del(p.header)
	return nil
}
