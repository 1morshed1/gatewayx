// Command echo is a tiny demo upstream service used to exercise the gateway.
//
// It answers every request with a JSON body describing which instance handled
// the request and what it saw. That is exactly what you need to *see* load
// balancing working (different instance names in the response) and to test the
// circuit breaker (ask it to fail on purpose).
//
// Test hooks (via query params), so you can simulate misbehaving backends:
//
//	?latency=200ms   sleep before responding (test timeouts / least-conn LB)
//	?status=503      respond with this HTTP status (test the circuit breaker)
//	?fail=0.3        respond 500 with 30% probability (test flakiness)
//
// Configured entirely by env vars so the same binary runs as many instances:
//
//	ECHO_NAME  logical name reported in responses (default: "echo")
//	ECHO_PORT  port to listen on (default: "8080")
package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

type echoResponse struct {
	Instance string              `json:"instance"`
	Method   string              `json:"method"`
	Path     string              `json:"path"`
	Query    string              `json:"query"`
	Headers  map[string][]string `json:"headers"`
	Time     string              `json:"time"`
}

func main() {
	name := envOr("ECHO_NAME", "echo")
	port := envOr("ECHO_PORT", "8080")

	mux := http.NewServeMux()

	// Health endpoint the gateway / k8s can probe.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()

		// Simulate latency.
		if d, err := time.ParseDuration(q.Get("latency")); err == nil && d > 0 {
			time.Sleep(d)
		}

		// Simulate random failures.
		if p, err := strconv.ParseFloat(q.Get("fail"), 64); err == nil && p > 0 {
			if rand.Float64() < p {
				http.Error(w, `{"error":"injected failure"}`, http.StatusInternalServerError)
				return
			}
		}

		// Simulate a fixed status code.
		if s, err := strconv.Atoi(q.Get("status")); err == nil && s >= 100 && s < 600 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(s)
			_, _ = w.Write([]byte(`{"instance":"` + name + `","forced_status":` + q.Get("status") + `}`))
			return
		}

		resp := echoResponse{
			Instance: name,
			Method:   r.Method,
			Path:     r.URL.Path,
			Query:    r.URL.RawQuery,
			Headers:  r.Header,
			Time:     time.Now().UTC().Format(time.RFC3339Nano),
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream-Instance", name)
		_ = json.NewEncoder(w).Encode(resp)
	})

	addr := ":" + port
	log.Printf("echo instance %q listening on %s", name, addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
