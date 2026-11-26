package domain

import "net/http"

// RequestContext is what a plugin sees on the request phase. It is created once
// per request by the proxy and threaded through the plugin chain, so plugins can
// read the request, learn which route/consumer/upstream applies, share state via
// Metadata, and short-circuit the request via Abort.
//
// Design note: rather than a func field for Abort (as the spec sketched), we use
// methods over private fields. That keeps the abort decision recorded on the
// context itself, so the proxy can inspect it after each plugin runs — no hidden
// control flow through captured closures.
type RequestContext struct {
	// Request is the inbound request. Plugins may mutate headers, etc. before
	// it is proxied. The proxy reads any mutations back out.
	Request *http.Request

	// Route is the matched route (never nil inside the chain).
	Route *Route

	// Consumer is set by the auth plugin once the caller is identified. It is
	// nil for anonymous routes or before auth runs.
	Consumer *Consumer

	// Metadata is scratch space shared between plugins in the same request.
	// e.g. the rate-limit plugin stores remaining quota for a response header.
	Metadata map[string]any

	aborted     bool
	abortStatus int
	abortBody   any

	respHeader http.Header
}

// ResponseHeader returns a lazily-created header map for headers a plugin wants on
// the client response. On a normal (proxied) response these are copied onto the
// upstream response; on an aborted response the gateway writes them directly. This
// is how, e.g., the rate limiter attaches Retry-After to a 429.
func (c *RequestContext) ResponseHeader() http.Header {
	if c.respHeader == nil {
		c.respHeader = make(http.Header)
	}
	return c.respHeader
}

// ResponseHeaders returns the accumulated headers, or nil if none were set.
func (c *RequestContext) ResponseHeaders() http.Header { return c.respHeader }

// NewRequestContext builds a fresh context for a request against a route.
func NewRequestContext(r *http.Request, route *Route) *RequestContext {
	return &RequestContext{
		Request:  r,
		Route:    route,
		Metadata: make(map[string]any, 4),
	}
}

// Abort marks the request as short-circuited. The plugin chain stops, the
// upstream is never called, and the proxy writes status + body to the client.
// The first Abort wins; later calls are ignored so the earliest policy decision
// (e.g. auth 401 before rate-limit 429) is the one returned.
func (c *RequestContext) Abort(status int, body any) {
	if c.aborted {
		return
	}
	c.aborted = true
	c.abortStatus = status
	c.abortBody = body
}

// Aborted reports whether a plugin short-circuited the request.
func (c *RequestContext) Aborted() bool { return c.aborted }

// AbortResult returns the status and body set by Abort. Only meaningful when
// Aborted() is true.
func (c *RequestContext) AbortResult() (int, any) { return c.abortStatus, c.abortBody }

// Set stores a value in the shared metadata map.
func (c *RequestContext) Set(key string, val any) { c.Metadata[key] = val }

// Get reads a value from the shared metadata map.
func (c *RequestContext) Get(key string) (any, bool) {
	v, ok := c.Metadata[key]
	return v, ok
}

// ResponseContext is what a plugin sees on the response phase, after the upstream
// has replied. Plugins run their OnResponse in reverse priority order so they
// wrap the request symmetrically (first in, last out).
type ResponseContext struct {
	Request  *http.Request
	Response *http.Response
	Route    *Route
	Consumer *Consumer
	Metadata map[string]any
}
