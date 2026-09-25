package proxy

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/1morshed1/gatewayx/internal/domain"
)

// ctxKey is a private type for request-context keys so we never collide with keys
// set by other packages.
type ctxKey int

const stateKey ctxKey = iota

// requestState carries per-request data from ServeHTTP into the ReverseProxy's
// Rewrite / ModifyResponse / ErrorHandler callbacks (which only receive the
// *http.Request). We stash it in the request context.
type requestState struct {
	rc     *domain.RequestContext
	route  *domain.Route
	target *url.URL
	up     *domain.Upstream
	chain  Chain
	start  time.Time
}

// Gateway is the public-traffic HTTP handler. One instance handles every proxied
// request. It is safe for concurrent use.
type Gateway struct {
	router    *Router
	chains    ChainFactory
	lb        Balancer
	cfg       Options
	log       *slog.Logger
	proxy     *httputil.ReverseProxy
	transport http.RoundTripper
}

// Options configures a Gateway. Kept separate from config.Config so the proxy
// package doesn't import config (keeps dependencies pointing one way).
type Options struct {
	DefaultTimeout time.Duration
}

// New builds a Gateway. Pass real implementations of ChainFactory and Balancer as
// later phases provide them; NopChainFactory{} and FirstHealthyBalancer{} work
// today.
func New(router *Router, chains ChainFactory, lb Balancer, transport http.RoundTripper, opts Options, log *slog.Logger) *Gateway {
	if transport == nil {
		transport = http.DefaultTransport
	}
	g := &Gateway{
		router:    router,
		chains:    chains,
		lb:        lb,
		cfg:       opts,
		log:       log,
		transport: transport,
	}

	// A single ReverseProxy instance serves all upstreams. The per-request
	// target is read from the request context by Rewrite. This avoids allocating
	// a proxy per request.
	g.proxy = &httputil.ReverseProxy{
		Rewrite:        g.rewrite,
		ModifyResponse: g.modifyResponse,
		ErrorHandler:   g.errorHandler,
		Transport:      transport,
	}
	return g
}

// Router exposes the router so callers (hot reload, admin) can update the table.
func (g *Gateway) Router() *Router { return g.router }

// ServeHTTP is the entry point for every public request.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// 1. Route matching.
	route := g.router.Match(hostOnly(r.Host), r.URL.Path)
	if route == nil {
		g.writeJSON(w, http.StatusNotFound, map[string]string{"error": "no route matched"})
		return
	}

	// 2. Build the request context and run request-phase plugins.
	rc := domain.NewRequestContext(r, route)
	chain := g.chains.For(route)
	chain.OnRequest(rc)
	if rc.Aborted() {
		status, body := rc.AbortResult()
		g.writeJSON(w, status, body)
		return
	}

	// 3. Pick an upstream via the load balancer.
	up, err := g.lb.Pick(route, r)
	if err != nil {
		g.writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no healthy upstream"})
		return
	}
	defer g.lb.Release(route, up)

	target, err := url.Parse(up.URL)
	if err != nil {
		g.log.Error("bad upstream url", "url", up.URL, "err", err)
		g.writeJSON(w, http.StatusBadGateway, map[string]string{"error": "bad upstream configuration"})
		return
	}

	// 4. Apply the per-route timeout to the outbound request.
	timeout := g.cfg.DefaultTimeout
	if route.Timeout > 0 {
		timeout = route.Timeout
	}
	ctx := r.Context()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	// 5. Stash per-request state for the proxy callbacks and serve.
	st := &requestState{rc: rc, route: route, target: target, up: up, chain: chain, start: start}
	ctx = context.WithValue(ctx, stateKey, st)
	g.proxy.ServeHTTP(w, r.WithContext(ctx))
}

// rewrite is called by ReverseProxy to build the outbound request. It points the
// request at the chosen upstream and applies path stripping.
func (g *Gateway) rewrite(pr *httputil.ProxyRequest) {
	st, _ := pr.In.Context().Value(stateKey).(*requestState)
	if st == nil {
		return
	}

	pr.SetURL(st.target) // sets scheme+host, and joins paths
	pr.Out.Host = st.target.Host

	// Path stripping: turn "/echo/hello" into "/hello" when the route strips its
	// prefix. SetURL already joined target.Path + inbound path, so we recompute.
	if st.route.StripPrefix && st.route.PathPrefix != "" {
		trimmed := strings.TrimPrefix(st.rc.Request.URL.Path, st.route.PathPrefix)
		if trimmed == "" {
			trimmed = "/"
		}
		if !strings.HasPrefix(trimmed, "/") {
			trimmed = "/" + trimmed
		}
		pr.Out.URL.Path = singleJoin(st.target.Path, trimmed)
		pr.Out.URL.RawPath = ""
	}

	// Preserve the client's forwarded-for chain and identify the gateway.
	pr.SetXForwarded()
	pr.Out.Header.Set("X-Gateway", "gatewayx")
}

// modifyResponse runs response-phase plugins after the upstream replies.
func (g *Gateway) modifyResponse(resp *http.Response) error {
	st, _ := resp.Request.Context().Value(stateKey).(*requestState)
	if st == nil {
		return nil
	}
	respCtx := &domain.ResponseContext{
		Request:  st.rc.Request,
		Response: resp,
		Route:    st.route,
		Consumer: st.rc.Consumer,
		Metadata: st.rc.Metadata,
	}
	return st.chain.OnResponse(respCtx)
}

// errorHandler converts upstream/transport failures into a clean 502/504 rather
// than leaking Go error text to clients.
func (g *Gateway) errorHandler(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusBadGateway
	if r.Context().Err() == context.DeadlineExceeded {
		status = http.StatusGatewayTimeout
	}
	g.log.Warn("upstream error", "path", r.URL.Path, "err", err)
	g.writeJSON(w, status, map[string]string{"error": http.StatusText(status)})
}

// --- helpers ---------------------------------------------------------------

func (g *Gateway) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	if s, ok := body.(string); ok {
		_ = json.NewEncoder(w).Encode(map[string]string{"message": s})
		return
	}
	_ = json.NewEncoder(w).Encode(body)
}

// hostOnly strips any :port from the Host header for host matching.
func hostOnly(host string) string {
	if i := strings.IndexByte(host, ':'); i >= 0 {
		return host[:i]
	}
	return host
}

// singleJoin joins two path segments with exactly one slash between them.
func singleJoin(a, b string) string {
	switch {
	case a == "" || a == "/":
		return b
	case strings.HasSuffix(a, "/") && strings.HasPrefix(b, "/"):
		return a + b[1:]
	case !strings.HasSuffix(a, "/") && !strings.HasPrefix(b, "/"):
		return a + "/" + b
	default:
		return a + b
	}
}
