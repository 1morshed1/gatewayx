// Package config loads gateway configuration from environment variables, with an
// optional .env file for local development.
//
// We roll a tiny loader instead of pulling a dependency: it keeps the config
// surface explicit and readable, and reading env vars is not hard enough to
// justify a library. Every field has a default that targets the docker-compose
// stack, so a bare `make run` works out of the box.
package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the fully-resolved gateway configuration.
type Config struct {
	GatewayAddr string // public proxy listener, e.g. ":8000"
	AdminAddr   string // admin API listener, e.g. ":8001"
	MetricsAddr string // prometheus listener, e.g. ":9100"

	PostgresDSN string

	RedisAddr     string
	RedisPassword string
	RedisDB       int

	KafkaBrokers []string
	KafkaTopic   string
	KafkaEnabled bool

	LogLevel string

	// AdminToken, when set, is required (as X-Admin-Token) on admin mutations.
	AdminToken string

	// ReloadMode is "listen" (Postgres LISTEN/NOTIFY) or "poll".
	ReloadMode   string
	PollInterval time.Duration

	// UpstreamTimeout is the default per-request timeout when a route does not
	// set its own.
	UpstreamTimeout time.Duration

	// Circuit breaker defaults (per upstream host).
	CBFailureThreshold int           // failures within CBWindow before opening
	CBOpenTimeout      time.Duration // how long to stay open before probing
	CBWindow           time.Duration // rolling window over which failures are counted
}

// Load reads configuration from the environment. If a .env file exists in the
// current directory it is loaded first (real environment variables still win, so
// you can override .env on the command line).
func Load() (*Config, error) {
	loadDotEnv(".env")

	cfg := &Config{
		GatewayAddr:     env("GATEWAY_ADDR", ":8000"),
		AdminAddr:       env("ADMIN_ADDR", ":8001"),
		MetricsAddr:     env("METRICS_ADDR", ":9100"),
		PostgresDSN:     env("POSTGRES_DSN", "postgres://gatewayx:gatewayx@localhost:5432/gatewayx?sslmode=disable"),
		RedisAddr:       env("REDIS_ADDR", "localhost:6379"),
		RedisPassword:   env("REDIS_PASSWORD", ""),
		RedisDB:         envInt("REDIS_DB", 0),
		KafkaBrokers:    envList("KAFKA_BROKERS", []string{"localhost:9092"}),
		KafkaTopic:      env("KAFKA_TOPIC", "request-logs"),
		KafkaEnabled:    envBool("KAFKA_ENABLED", true),
		LogLevel:        env("LOG_LEVEL", "info"),
		AdminToken:      env("ADMIN_TOKEN", ""),
		ReloadMode:      env("CONFIG_RELOAD_MODE", "listen"),
		PollInterval:    envDuration("CONFIG_POLL_INTERVAL", 15*time.Second),
		UpstreamTimeout: envDuration("UPSTREAM_TIMEOUT", 30*time.Second),

		CBFailureThreshold: envInt("CB_FAILURE_THRESHOLD", 5),
		CBOpenTimeout:      envDuration("CB_OPEN_TIMEOUT", 30*time.Second),
		CBWindow:           envDuration("CB_WINDOW", 30*time.Second),
	}

	if cfg.ReloadMode != "listen" && cfg.ReloadMode != "poll" {
		return nil, fmt.Errorf("invalid CONFIG_RELOAD_MODE %q (want listen|poll)", cfg.ReloadMode)
	}
	return cfg, nil
}

// --- tiny env helpers -------------------------------------------------------

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envList(key string, def []string) []string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		parts := strings.Split(v, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return parts
	}
	return def
}

// loadDotEnv reads KEY=VALUE lines from path into the process environment,
// without overwriting variables that are already set. Missing file is not an
// error — .env is a convenience, not a requirement.
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.Trim(strings.TrimSpace(val), `"'`)
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, val)
		}
	}
}
