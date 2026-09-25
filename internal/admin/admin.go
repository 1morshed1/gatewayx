// Package admin is the gateway's control-plane REST API: create/list/delete
// routes and consumers, force a config reload, and inspect the live route table.
//
// It writes to the same Postgres the data plane reads, and every mutation triggers
// hot reload (via NOTIFY) so changes take effect without a restart. Mutating
// endpoints are protected by an optional admin token.
package admin

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/1morshed1/gatewayx/internal/domain"
	"github.com/1morshed1/gatewayx/internal/proxy"
	"github.com/1morshed1/gatewayx/internal/store"
)

// ReloadFunc reloads the route table into the running gateway. Admin calls it after
// a mutation for immediate local consistency (NOTIFY handles other replicas).
type ReloadFunc func() error

// API bundles the admin dependencies.
type API struct {
	store  *store.Store
	router *proxy.Router
	reload ReloadFunc
	token  string
	log    *slog.Logger
}

// New builds the admin API. token may be empty (dev mode: no auth on mutations).
func New(st *store.Store, router *proxy.Router, reload ReloadFunc, token string, log *slog.Logger) *API {
	return &API{store: st, router: router, reload: reload, token: token, log: log}
}

// Handler returns the admin HTTP mux. Uses Go 1.22 method+wildcard routing.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", a.health)
	mux.HandleFunc("GET /admin/routes", a.listRoutes)
	mux.HandleFunc("POST /admin/routes", a.requireToken(a.createRoute))
	mux.HandleFunc("DELETE /admin/routes/{id}", a.requireToken(a.deleteRoute))

	mux.HandleFunc("GET /admin/consumers", a.listConsumers)
	mux.HandleFunc("POST /admin/consumers", a.requireToken(a.createConsumer))
	mux.HandleFunc("DELETE /admin/consumers/{id}", a.requireToken(a.deleteConsumer))

	mux.HandleFunc("POST /admin/reload", a.requireToken(a.forceReload))
	mux.HandleFunc("GET /admin/analytics", a.analytics)

	return mux
}

// --- middleware -------------------------------------------------------------

// requireToken guards mutating endpoints. If no token is configured, it's a no-op
// (dev mode). In production you set ADMIN_TOKEN and pass it as X-Admin-Token.
func (a *API) requireToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.token != "" && r.Header.Get("X-Admin-Token") != a.token {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid admin token"})
			return
		}
		next(w, r)
	}
}

// --- handlers ---------------------------------------------------------------

func (a *API) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// listRoutes returns the LIVE in-memory route table (what the gateway is actually
// serving), not a fresh DB read — so you can confirm hot reload took effect.
func (a *API) listRoutes(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.router.Routes())
}

// routeInput is the create-route DTO. Timeout is expressed in milliseconds here
// (JSON-friendly) rather than the domain type's time.Duration.
type routeInput struct {
	Name        string `json:"name"`
	Host        string `json:"host"`
	PathPrefix  string `json:"path_prefix"`
	StripPrefix bool   `json:"strip_prefix"`
	LBStrategy  string `json:"lb_strategy"`
	TimeoutMs   int    `json:"timeout_ms"`
	Upstreams   []struct {
		URL    string `json:"url"`
		Weight int    `json:"weight"`
	} `json:"upstreams"`
	Plugins []struct {
		Name    string         `json:"name"`
		Enabled bool           `json:"enabled"`
		Config  map[string]any `json:"config"`
	} `json:"plugins"`
}

func (a *API) createRoute(w http.ResponseWriter, r *http.Request) {
	var in routeInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json: " + err.Error()})
		return
	}
	if in.Name == "" || in.PathPrefix == "" || len(in.Upstreams) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name, path_prefix and at least one upstream are required"})
		return
	}

	route := &domain.Route{
		Name:        in.Name,
		Host:        in.Host,
		PathPrefix:  in.PathPrefix,
		StripPrefix: in.StripPrefix,
		LBStrategy:  domain.LBStrategy(in.LBStrategy),
		Timeout:     time.Duration(in.TimeoutMs) * time.Millisecond,
	}
	for _, u := range in.Upstreams {
		route.Upstreams = append(route.Upstreams, domain.Upstream{URL: u.URL, Weight: u.Weight, Healthy: true})
	}
	for _, p := range in.Plugins {
		route.Plugins = append(route.Plugins, domain.PluginConfig{Name: p.Name, Enabled: p.Enabled, Config: p.Config})
	}

	id, err := a.store.CreateRoute(r.Context(), route)
	if err != nil {
		a.log.Error("create route", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not create route"})
		return
	}
	a.reloadNow()
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
}

func (a *API) deleteRoute(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ok, err := a.store.DeleteRoute(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not delete route"})
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "route not found"})
		return
	}
	a.reloadNow()
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) listConsumers(w http.ResponseWriter, r *http.Request) {
	consumers, err := a.store.ListConsumers(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not list consumers"})
		return
	}
	writeJSON(w, http.StatusOK, consumers)
}

func (a *API) createConsumer(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name   string `json:"name"`
		APIKey string `json:"api_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Name == "" || in.APIKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name and api_key are required"})
		return
	}
	id, err := a.store.CreateConsumer(r.Context(), in.Name, in.APIKey)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not create consumer"})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
}

func (a *API) deleteConsumer(w http.ResponseWriter, r *http.Request) {
	ok, err := a.store.DeleteConsumer(r.Context(), r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not delete consumer"})
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "consumer not found"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// analytics returns per-route request/error/latency rollups for the last 24h,
// computed from the aggregate table the analytics consumer populates.
func (a *API) analytics(w http.ResponseWriter, r *http.Request) {
	since := time.Now().Add(-24 * time.Hour)
	summary, err := a.store.AnalyticsSummary(r.Context(), since)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not load analytics"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"since": since, "routes": summary})
}

func (a *API) forceReload(w http.ResponseWriter, _ *http.Request) {
	if err := a.reload(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload failed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "reloaded"})
}

// reloadNow reloads and logs any error (best-effort; NOTIFY will also propagate).
func (a *API) reloadNow() {
	if err := a.reload(); err != nil {
		a.log.Error("reload after mutation failed", "err", err)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
