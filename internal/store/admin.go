package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/1morshed1/gatewayx/internal/domain"
)

// This file holds the control-plane write operations used by the Admin API. Every
// multi-row write runs in a transaction so a route and its upstreams/plugins are
// created (or not) atomically — you never get a half-created route.
//
// The AFTER-statement NOTIFY triggers (see migration 0001) fire on these writes,
// which is what drives hot reload in running gateways.

// CreateRoute inserts a route with its upstreams and plugins atomically and
// returns the new route ID.
func (s *Store) CreateRoute(ctx context.Context, r *domain.Route) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	strategy := r.LBStrategy
	if strategy == "" {
		strategy = domain.RoundRobin
	}
	timeoutMs := int(r.Timeout.Milliseconds())

	var id string
	if err := tx.QueryRow(ctx, `
		INSERT INTO routes (name, host, path_prefix, strip_prefix, lb_strategy, timeout_ms)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id`,
		r.Name, r.Host, r.PathPrefix, r.StripPrefix, string(strategy), timeoutMs,
	).Scan(&id); err != nil {
		return "", fmt.Errorf("insert route: %w", err)
	}

	for _, u := range r.Upstreams {
		weight := u.Weight
		if weight < 1 {
			weight = 1
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO upstreams (route_id, url, weight, healthy)
			VALUES ($1, $2, $3, true)`, id, u.URL, weight); err != nil {
			return "", fmt.Errorf("insert upstream %s: %w", u.URL, err)
		}
	}

	for _, p := range r.Plugins {
		cfg, err := json.Marshal(p.Config)
		if err != nil {
			return "", fmt.Errorf("marshal plugin config: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO route_plugins (route_id, name, enabled, config)
			VALUES ($1, $2, $3, $4::jsonb)`, id, p.Name, p.Enabled, cfg); err != nil {
			return "", fmt.Errorf("insert plugin %s: %w", p.Name, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return id, nil
}

// DeleteRoute removes a route and (via ON DELETE CASCADE) its upstreams/plugins.
// Returns whether a row was deleted.
func (s *Store) DeleteRoute(ctx context.Context, id string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM routes WHERE id = $1`, id)
	if err != nil {
		return false, fmt.Errorf("delete route: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// CreateConsumer inserts a consumer and returns the new ID.
func (s *Store) CreateConsumer(ctx context.Context, name, apiKey string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO consumers (name, api_key, enabled)
		VALUES ($1, $2, true)
		RETURNING id`, name, apiKey).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("insert consumer: %w", err)
	}
	return id, nil
}

// ListConsumers returns all consumers (API keys omitted from the domain type's
// JSON via the `-` tag, so listing them is safe).
func (s *Store) ListConsumers(ctx context.Context) ([]domain.Consumer, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, enabled, created_at FROM consumers ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("query consumers: %w", err)
	}
	defer rows.Close()

	var out []domain.Consumer
	for rows.Next() {
		var c domain.Consumer
		if err := rows.Scan(&c.ID, &c.Name, &c.Enabled, &c.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan consumer: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// DeleteConsumer removes a consumer by ID.
func (s *Store) DeleteConsumer(ctx context.Context, id string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM consumers WHERE id = $1`, id)
	if err != nil {
		return false, fmt.Errorf("delete consumer: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
