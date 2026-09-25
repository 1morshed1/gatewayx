package store

import (
	"context"
	"fmt"
)

// SeedDemo inserts a minimal, self-contained demo config so a fresh install can
// be exercised immediately:
//
//   - one consumer with API key "demo-key"
//   - one route on /echo that strips the prefix and round-robins across the two
//     docker-compose echo upstreams (localhost:9001 and :9002)
//
// It is idempotent: re-running updates the same rows rather than duplicating.
// Upstream URLs point at localhost ports because the typical demo runs the
// gateway on the host (`make run`) against the compose upstreams.
func (s *Store) SeedDemo(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after successful commit

	// Consumer.
	if _, err := tx.Exec(ctx, `
		INSERT INTO consumers (name, api_key, enabled)
		VALUES ('demo', 'demo-key', true)
		ON CONFLICT (api_key) DO UPDATE SET enabled = true`); err != nil {
		return fmt.Errorf("seed consumer: %w", err)
	}

	// Route (delete + recreate by name for a clean, deterministic demo).
	if _, err := tx.Exec(ctx, `DELETE FROM routes WHERE name = 'echo'`); err != nil {
		return fmt.Errorf("clear route: %w", err)
	}
	var routeID string
	if err := tx.QueryRow(ctx, `
		INSERT INTO routes (name, host, path_prefix, strip_prefix, lb_strategy, timeout_ms)
		VALUES ('echo', '', '/echo', true, 'round_robin', 5000)
		RETURNING id`).Scan(&routeID); err != nil {
		return fmt.Errorf("seed route: %w", err)
	}

	// Upstreams.
	for _, url := range []string{"http://localhost:9001", "http://localhost:9002"} {
		if _, err := tx.Exec(ctx, `
			INSERT INTO upstreams (route_id, url, weight, healthy)
			VALUES ($1, $2, 1, true)`, routeID, url); err != nil {
			return fmt.Errorf("seed upstream %s: %w", url, err)
		}
	}

	// Plugins: request-id (correlation), auth (require the demo API key), and a
	// deliberately small rate limit so you can trip a 429 by hand in the demo.
	plugins := []struct {
		name   string
		config string
	}{
		{"request-id", `{"header":"X-Request-ID"}`},
		{"auth", `{"header":"X-API-Key"}`},
		{"rate-limit", `{"limit":5,"window":"10s"}`},
	}
	for _, pl := range plugins {
		if _, err := tx.Exec(ctx, `
			INSERT INTO route_plugins (route_id, name, enabled, config)
			VALUES ($1, $2, true, $3::jsonb)`, routeID, pl.name, pl.config); err != nil {
			return fmt.Errorf("seed plugin %s: %w", pl.name, err)
		}
	}

	return tx.Commit(ctx)
}
