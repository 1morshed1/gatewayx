package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/1morshed1/gatewayx/internal/circuitbreaker"
	"github.com/1morshed1/gatewayx/internal/config"
	"github.com/1morshed1/gatewayx/internal/plugin"
	"github.com/1morshed1/gatewayx/internal/proxy"
	"github.com/1morshed1/gatewayx/internal/redisx"
	"github.com/1morshed1/gatewayx/internal/store"
)

// runServe wires the dependency graph and runs the servers until the context is
// cancelled (SIGINT/SIGTERM), then shuts down gracefully.
//
// As later phases land, the chain factory and balancer arguments to proxy.New
// get replaced with the real plugin chain and load balancer. The wiring shape
// stays the same — that's the payoff of the interface seams in the proxy package.
func runServe(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	// --- config store ---
	st, err := store.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer st.Close()

	// --- load the route table ---
	routes, err := st.LoadRoutes(ctx)
	if err != nil {
		return err
	}
	router := proxy.NewRouter()
	router.Update(routes)
	log.Info("loaded routes", "count", len(routes))

	// --- redis (rate limit + circuit breaker state) ---
	rdb, err := redisx.New(ctx, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		return err
	}
	defer rdb.Close()

	// --- plugin system ---
	registry := buildRegistry(pluginDeps{log: log, redis: rdb, store: st})
	chains := plugin.NewChainFactory(registry, log)
	log.Info("registered plugins", "plugins", registry.Names())

	// --- circuit breaker (wraps the upstream transport) ---
	cbMgr := circuitbreaker.NewManager(rdb, circuitbreaker.Config{
		FailureThreshold: cfg.CBFailureThreshold,
		OpenTimeout:      cfg.CBOpenTimeout,
		Window:           cfg.CBWindow,
	})
	transport := circuitbreaker.NewTransport(newUpstreamTransport(cfg), cbMgr)

	// --- gateway core ---
	// TODO(phase 5): swap FirstHealthyBalancer for the real load balancer.
	gw := proxy.New(
		router,
		chains,
		proxy.FirstHealthyBalancer{},
		transport,
		proxy.Options{DefaultTimeout: cfg.UpstreamTimeout},
		log,
	)

	// --- HTTP servers ---
	gatewaySrv := &http.Server{
		Addr:              cfg.GatewayAddr,
		Handler:           gw,
		ReadHeaderTimeout: 10 * time.Second,
	}
	adminSrv := &http.Server{
		Addr:              cfg.AdminAddr,
		Handler:           newAdminMux(router),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		log.Info("gateway listening", "addr", cfg.GatewayAddr)
		if err := gatewaySrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		log.Info("admin listening", "addr", cfg.AdminAddr)
		if err := adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// Block until shutdown signal or a server error.
	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-errCh:
		log.Error("server error", "err", err)
	}

	// Graceful shutdown with a bounded deadline.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = gatewaySrv.Shutdown(shutdownCtx)
	_ = adminSrv.Shutdown(shutdownCtx)
	log.Info("stopped")
	return nil
}

// newUpstreamTransport builds the HTTP transport used to reach upstreams. Tuned
// connection pooling matters a lot for a gateway: without keep-alive reuse we'd
// pay a TCP+TLS handshake per proxied request.
func newUpstreamTransport(cfg *config.Config) http.RoundTripper {
	return &http.Transport{
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   200,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: cfg.UpstreamTimeout,
		ForceAttemptHTTP2:     true,
	}
}

// newAdminMux is a placeholder admin server. Phase 6 replaces it with full CRUD.
// For now it exposes health and a read-only route dump so the server is useful
// immediately.
func newAdminMux(router *proxy.Router) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/admin/routes", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, router.Routes())
	})
	return mux
}
