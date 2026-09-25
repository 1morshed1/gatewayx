// Package observability provides Prometheus metrics for the gateway.
//
// What to measure on a gateway comes straight from the "RED method" (Rate, Errors,
// Duration) — the three signals that tell you whether the thing on your request
// path is healthy:
//
//	Rate     requests per second (the requests_total counter, rate()'d in PromQL)
//	Errors   the fraction that are 5xx (requests_total filtered by status_class)
//	Duration how long they take (the request_duration histogram, for p50/p99)
//
// Everything here is labelled by route so you can slice per route — the unit an
// operator actually reasons about.
package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the collectors and their private registry. Using our own registry
// (not the global default) keeps the gateway's metrics self-contained and testable.
type Metrics struct {
	registry *prometheus.Registry

	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	rejected *prometheus.CounterVec
}

// NewMetrics builds and registers the collectors.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		registry: reg,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gatewayx_requests_total",
			Help: "Total proxied requests, by route and HTTP status class (2xx/3xx/4xx/5xx).",
		}, []string{"route", "status_class"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "gatewayx_request_duration_seconds",
			Help: "End-to-end request duration through the gateway, by route.",
			// Buckets tuned for a proxy: sub-ms overhead up to multi-second upstreams.
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"route"}),
		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gatewayx_rejected_total",
			Help: "Requests rejected by the gateway before/without reaching an upstream, by route and reason.",
		}, []string{"route", "reason"}),
	}

	reg.MustRegister(m.requests, m.duration, m.rejected)
	// Also expose Go runtime + process metrics — free, and invaluable in prod.
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	return m
}

// ObserveRequest records one completed request. Implements proxy.Metrics.
func (m *Metrics) ObserveRequest(route string, status int, dur time.Duration) {
	if route == "" {
		route = "unmatched"
	}
	m.requests.WithLabelValues(route, statusClass(status)).Inc()
	m.duration.WithLabelValues(route).Observe(dur.Seconds())
}

// ObserveRejected records a request rejected without reaching an upstream (e.g.
// auth failure, rate limit, breaker open).
func (m *Metrics) ObserveRejected(route, reason string) {
	if route == "" {
		route = "unmatched"
	}
	m.rejected.WithLabelValues(route, reason).Inc()
}

// Handler returns the /metrics HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// statusClass maps a status code to its family label, keeping cardinality low
// (4 values instead of every distinct code).
func statusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}
