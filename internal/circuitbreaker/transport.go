package circuitbreaker

import (
	"io"
	"net/http"
	"strings"
)

// Transport wraps a base http.RoundTripper with circuit-breaking, keyed by the
// upstream host. It is installed as the ReverseProxy's Transport, so the breaker
// observes the *real* upstream call — the actual transport error or 5xx status —
// which is exactly what it needs to judge upstream health.
//
// This is why the breaker is transport middleware and not a chain plugin (see
// doc 03): a chain plugin runs before proxying and can't see the upstream result.
type Transport struct {
	base http.RoundTripper
	mgr  *Manager
}

// NewTransport wraps base with the breaker managed by mgr.
func NewTransport(base http.RoundTripper, mgr *Manager) *Transport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &Transport{base: base, mgr: mgr}
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	key := req.URL.Host
	ctx := req.Context()

	state, allowed, err := t.mgr.Allow(ctx, key)
	if err == nil && !allowed {
		// Fail fast: don't even dial the upstream.
		return openResponse(req, state), nil
	}

	resp, rtErr := t.base.RoundTrip(req)

	// A transport error, or any 5xx, counts as a failure. 4xx are the client's
	// fault, not the upstream's, so they don't trip the breaker.
	success := rtErr == nil && resp.StatusCode < http.StatusInternalServerError
	if _, recErr := t.mgr.Record(ctx, key, success); recErr != nil {
		// Recording is best-effort; never fail the request because Redis hiccuped.
		_ = recErr
	}

	return resp, rtErr
}

// openResponse builds a synthetic 503 so the client gets a clean "breaker open"
// answer rather than a leaked Go error. We return it as a normal response (not an
// error) so ReverseProxy serves it directly instead of routing through its
// ErrorHandler.
func openResponse(req *http.Request, state State) *http.Response {
	body := `{"error":"upstream circuit open","state":"` + string(state) + `"}`
	return &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Status:     "503 Service Unavailable",
		Proto:      req.Proto,
		ProtoMajor: req.ProtoMajor,
		ProtoMinor: req.ProtoMinor,
		Header: http.Header{
			"Content-Type":     []string{"application/json"},
			"X-Circuit-Broken": []string{"true"},
		},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}
