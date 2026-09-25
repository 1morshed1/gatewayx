package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/1morshed1/gatewayx/internal/domain"
)

// LoadRoutes reads every route with its upstreams and plugin config, assembling
// full domain.Route values. This is called at startup and on every hot reload.
//
// We issue three queries (routes, upstreams, plugins) and stitch them in memory
// rather than one big join, because a join would fan out rows (route × upstream ×
// plugin) and we'd have to de-duplicate anyway. Three small queries over a small
// config table is simpler and plenty fast — this is control-plane data, not hot
// path.
func (s *Store) LoadRoutes(ctx context.Context) ([]*domain.Route, error) {
	routes, byID, err := s.loadRouteRows(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.attachUpstreams(ctx, byID); err != nil {
		return nil, err
	}
	if err := s.attachPlugins(ctx, byID); err != nil {
		return nil, err
	}
	return routes, nil
}

func (s *Store) loadRouteRows(ctx context.Context) ([]*domain.Route, map[string]*domain.Route, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, host, path_prefix, strip_prefix, lb_strategy, timeout_ms, updated_at
		FROM routes`)
	if err != nil {
		return nil, nil, fmt.Errorf("query routes: %w", err)
	}
	defer rows.Close()

	var routes []*domain.Route
	byID := make(map[string]*domain.Route)
	for rows.Next() {
		var (
			r         domain.Route
			timeoutMs int
		)
		if err := rows.Scan(&r.ID, &r.Name, &r.Host, &r.PathPrefix, &r.StripPrefix,
			&r.LBStrategy, &timeoutMs, &r.UpdatedAt); err != nil {
			return nil, nil, fmt.Errorf("scan route: %w", err)
		}
		r.Timeout = time.Duration(timeoutMs) * time.Millisecond
		routes = append(routes, &r)
		byID[r.ID] = &r
	}
	return routes, byID, rows.Err()
}

func (s *Store) attachUpstreams(ctx context.Context, byID map[string]*domain.Route) error {
	rows, err := s.pool.Query(ctx,
		`SELECT id, route_id, url, weight, healthy FROM upstreams`)
	if err != nil {
		return fmt.Errorf("query upstreams: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			u       domain.Upstream
			routeID string
		)
		if err := rows.Scan(&u.ID, &routeID, &u.URL, &u.Weight, &u.Healthy); err != nil {
			return fmt.Errorf("scan upstream: %w", err)
		}
		if r, ok := byID[routeID]; ok {
			r.Upstreams = append(r.Upstreams, u)
		}
	}
	return rows.Err()
}

func (s *Store) attachPlugins(ctx context.Context, byID map[string]*domain.Route) error {
	rows, err := s.pool.Query(ctx,
		`SELECT route_id, name, enabled, config FROM route_plugins`)
	if err != nil {
		return fmt.Errorf("query route_plugins: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			routeID    string
			pc         domain.PluginConfig
			configJSON []byte
		)
		if err := rows.Scan(&routeID, &pc.Name, &pc.Enabled, &configJSON); err != nil {
			return fmt.Errorf("scan plugin: %w", err)
		}
		if len(configJSON) > 0 {
			if err := json.Unmarshal(configJSON, &pc.Config); err != nil {
				return fmt.Errorf("unmarshal plugin config for %s: %w", pc.Name, err)
			}
		}
		if r, ok := byID[routeID]; ok {
			r.Plugins = append(r.Plugins, pc)
		}
	}
	return rows.Err()
}

// GetConsumerByAPIKey looks up an enabled consumer by API key. Returns nil (no
// error) when there is no match, so callers distinguish "unknown key" from "DB
// error".
func (s *Store) GetConsumerByAPIKey(ctx context.Context, apiKey string) (*domain.Consumer, error) {
	var c domain.Consumer
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, api_key, enabled, created_at
		FROM consumers
		WHERE api_key = $1 AND enabled = true`, apiKey,
	).Scan(&c.ID, &c.Name, &c.APIKey, &c.Enabled, &c.CreatedAt)
	if err != nil {
		if isNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("query consumer: %w", err)
	}
	return &c, nil
}
