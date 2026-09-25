package proxy

import "errors"

var (
	// ErrNoRoute means no configured route matched the request.
	ErrNoRoute = errors.New("no route matched")
	// ErrNoUpstream means the matched route has no healthy upstream to serve it.
	ErrNoUpstream = errors.New("no healthy upstream")
)
