// Package transform is the request/response transformation plugin. It reshapes
// traffic as it flows through the gateway: add/set/remove headers, rewrite the
// path, and (opt-in) find-and-replace in the body.
//
// This is the "Request Transform Plugin" from the spec. It runs at
// PriorityTransform (after auth + rate limit) so it operates on the final, allowed
// request just before it is proxied; on the response side it runs first (reverse
// order) so it decorates the upstream reply before other plugins see it.
//
// Config shape (all sections optional):
//
//	{
//	  "request": {
//	    "headers": { "set": {"X-Env":"prod"}, "add": {"X-Trace":"1"}, "remove": ["Cookie"] },
//	    "body_replace": [ {"from":"http://","to":"https://"} ]
//	  },
//	  "response": {
//	    "headers": { "set": {"X-Powered-By":"gatewayx"}, "remove": ["Server"] },
//	    "body_replace": [ {"from":"internal.svc","to":"api.example.com"} ]
//	  },
//	  "rewrite_path": { "from":"/v1", "to":"/v2" }   // request path prefix rewrite
//	}
package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/1morshed1/gatewayx/internal/domain"
	"github.com/1morshed1/gatewayx/internal/plugin"
)

// Name is the plugin's config key.
const Name = "transform"

type headerOps struct {
	Set    map[string]string `json:"set"`    // overwrite (or create) the header
	Add    map[string]string `json:"add"`    // append a value (multi-value headers)
	Remove []string          `json:"remove"` // delete the header
}

type replacement struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type sideConfig struct {
	Headers     headerOps     `json:"headers"`
	BodyReplace []replacement `json:"body_replace"`
}

type config struct {
	Request     sideConfig `json:"request"`
	Response    sideConfig `json:"response"`
	RewritePath *struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"rewrite_path"`
}

// Plugin applies configured transformations.
type Plugin struct {
	plugin.Base
	cfg config
}

// New returns a Factory. Config is validated by round-tripping the generic map
// through JSON into the typed config — the simplest way to parse nested plugin
// config without hand-writing map traversal.
func New() plugin.Factory {
	return func(raw map[string]any) (plugin.Plugin, error) {
		b, err := json.Marshal(raw)
		if err != nil {
			return nil, fmt.Errorf("transform: marshal config: %w", err)
		}
		var c config
		if err := json.Unmarshal(b, &c); err != nil {
			return nil, fmt.Errorf("transform: invalid config: %w", err)
		}
		return &Plugin{cfg: c}, nil
	}
}

// Name implements plugin.Plugin.
func (p *Plugin) Name() string { return Name }

// Priority implements plugin.Plugin.
func (p *Plugin) Priority() int { return plugin.PriorityTransform }

// OnRequest applies request-side header ops, path rewrite, and body replacement.
func (p *Plugin) OnRequest(rc *domain.RequestContext) error {
	applyHeaderOps(rc.Request.Header, p.cfg.Request.Headers)

	if p.cfg.RewritePath != nil && p.cfg.RewritePath.From != "" {
		u := rc.Request.URL
		if strings.HasPrefix(u.Path, p.cfg.RewritePath.From) {
			u.Path = p.cfg.RewritePath.To + strings.TrimPrefix(u.Path, p.cfg.RewritePath.From)
			u.RawPath = "" // force re-encode from Path
		}
	}

	if len(p.cfg.Request.BodyReplace) > 0 && rc.Request.Body != nil {
		if err := replaceBody(&bodyTarget{
			get: func() io.ReadCloser { return rc.Request.Body },
			set: func(rc2 io.ReadCloser, n int64) { rc.Request.Body = rc2; rc.Request.ContentLength = n },
			hdr: rc.Request.Header,
		}, p.cfg.Request.BodyReplace); err != nil {
			return fmt.Errorf("transform: request body: %w", err)
		}
	}
	return nil
}

// OnResponse applies response-side header ops and body replacement.
func (p *Plugin) OnResponse(rc *domain.ResponseContext) error {
	applyHeaderOps(rc.Response.Header, p.cfg.Response.Headers)

	if len(p.cfg.Response.BodyReplace) > 0 && rc.Response.Body != nil {
		// Body replacement only works on uncompressed bodies. If the upstream sent
		// a compressed body we can't string-replace it, so skip and leave it intact.
		if enc := rc.Response.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
			return nil
		}
		if err := replaceBody(&bodyTarget{
			get: func() io.ReadCloser { return rc.Response.Body },
			set: func(rc2 io.ReadCloser, n int64) { rc.Response.Body = rc2; rc.Response.ContentLength = n },
			hdr: rc.Response.Header,
		}, p.cfg.Response.BodyReplace); err != nil {
			return fmt.Errorf("transform: response body: %w", err)
		}
	}
	return nil
}

// applyHeaderOps mutates h per the ops (remove, then set, then add).
func applyHeaderOps(h http.Header, ops headerOps) {
	for _, k := range ops.Remove {
		h.Del(k)
	}
	for k, v := range ops.Set {
		h.Set(k, v)
	}
	for k, v := range ops.Add {
		h.Add(k, v)
	}
}

// bodyTarget abstracts request vs response body get/set so replaceBody works for both.
type bodyTarget struct {
	get func() io.ReadCloser
	set func(io.ReadCloser, int64)
	hdr http.Header
}

// replaceBody buffers the body, applies string replacements, and installs the new
// body with a corrected Content-Length.
//
// Caveat (documented deliberately): this buffers the whole body into memory and so
// disables streaming for the route. Body transformation is inherently at odds with
// streaming — you can't rewrite bytes you haven't seen. Enable it only where you
// need it, on non-huge payloads.
func replaceBody(t *bodyTarget, reps []replacement) error {
	body := t.get()
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	s := string(data)
	for _, r := range reps {
		if r.From != "" {
			s = strings.ReplaceAll(s, r.From, r.To)
		}
	}
	nb := []byte(s)
	t.set(io.NopCloser(bytes.NewReader(nb)), int64(len(nb)))
	// Keep Content-Length header consistent with the rewritten body.
	t.hdr.Set("Content-Length", strconv.Itoa(len(nb)))
	return nil
}
