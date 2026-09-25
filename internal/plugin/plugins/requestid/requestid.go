// Package requestid is a small reference plugin that demonstrates the full plugin
// pattern end-to-end: config parsing, OnRequest mutation, shared metadata, and
// OnResponse mutation.
//
// It ensures every proxied request carries a correlation ID: if the incoming
// request has no ID header, it generates one; either way it echoes the ID back on
// the response so clients (and logs) can correlate a call across services.
//
// Use it as the template when writing your own plugins.
package requestid

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/1morshed1/gatewayx/internal/domain"
	"github.com/1morshed1/gatewayx/internal/plugin"
)

// Name is the plugin's config key.
const Name = "request-id"

const metaKey = "request_id"

// Plugin adds/propagates a correlation ID header.
type Plugin struct {
	plugin.Base // no-op defaults we don't override are inherited
	header      string
}

// New returns the plugin.Factory. This plugin has no external dependencies, so
// the constructor takes none — compare with auth/ratelimit which capture a store
// or Redis client here.
func New() plugin.Factory {
	return func(cfg map[string]any) (plugin.Plugin, error) {
		header, _ := cfg["header"].(string)
		if header == "" {
			header = "X-Request-ID"
		}
		return &Plugin{header: header}, nil
	}
}

// Name implements plugin.Plugin.
func (p *Plugin) Name() string { return Name }

// Priority runs this very early (before auth) so the ID is available to every
// later plugin and appears in all logs for the request.
func (p *Plugin) Priority() int { return 5 }

// OnRequest ensures the header is set, generating an ID when absent, and stashes
// it in shared metadata so other plugins/logs can use it.
func (p *Plugin) OnRequest(rc *domain.RequestContext) error {
	id := rc.Request.Header.Get(p.header)
	if id == "" {
		id = newID()
		rc.Request.Header.Set(p.header, id)
	}
	rc.Set(metaKey, id)
	return nil
}

// OnResponse echoes the correlation ID back to the client.
func (p *Plugin) OnResponse(rc *domain.ResponseContext) error {
	if v, ok := rc.Metadata[metaKey]; ok {
		if id, ok := v.(string); ok {
			rc.Response.Header.Set(p.header, id)
		}
	}
	return nil
}

// newID returns a random 128-bit hex id. crypto/rand avoids a uuid dependency and
// is plenty for a correlation id.
func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
