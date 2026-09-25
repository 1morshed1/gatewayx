package store

import (
	"context"
	"fmt"
	"time"
)

// AnalyticsBucket is one aggregated row increment produced by the consumer.
type AnalyticsBucket struct {
	Bucket        time.Time
	RouteID       string
	RouteName     string
	StatusClass   int
	Count         int64
	TotalDuration int64
}

// AddAnalytics upserts a batch of aggregate increments in one transaction. Using
// INSERT ... ON CONFLICT DO UPDATE means the consumer can flush partial rollups
// repeatedly and they accumulate correctly (idempotent-per-increment).
func (s *Store) AddAnalytics(ctx context.Context, buckets []AnalyticsBucket) error {
	if len(buckets) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	for _, b := range buckets {
		if _, err := tx.Exec(ctx, `
			INSERT INTO request_analytics (bucket, route_id, route_name, status_class, count, total_duration_ms)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (bucket, route_id, status_class) DO UPDATE
			SET count = request_analytics.count + EXCLUDED.count,
			    total_duration_ms = request_analytics.total_duration_ms + EXCLUDED.total_duration_ms`,
			b.Bucket, b.RouteID, b.RouteName, b.StatusClass, b.Count, b.TotalDuration,
		); err != nil {
			return fmt.Errorf("upsert analytics: %w", err)
		}
	}
	return tx.Commit(ctx)
}

// RouteSummary is a per-route rollup for the admin analytics endpoint.
type RouteSummary struct {
	RouteName     string  `json:"route_name"`
	Requests      int64   `json:"requests"`
	Errors        int64   `json:"errors"` // 5xx
	ErrorRate     float64 `json:"error_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

// AnalyticsSummary returns per-route totals since a given time, ordered by request
// volume. This is the "top consumers / error rates by route" report the plan calls
// for, computed straight from the aggregate table.
func (s *Store) AnalyticsSummary(ctx context.Context, since time.Time) ([]RouteSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT route_name,
		       SUM(count) AS requests,
		       SUM(count) FILTER (WHERE status_class = 5) AS errors,
		       SUM(total_duration_ms) AS total_ms
		FROM request_analytics
		WHERE bucket >= $1
		GROUP BY route_name
		ORDER BY requests DESC`, since)
	if err != nil {
		return nil, fmt.Errorf("query analytics summary: %w", err)
	}
	defer rows.Close()

	var out []RouteSummary
	for rows.Next() {
		var (
			rs      RouteSummary
			errs    *int64
			totalMs int64
		)
		if err := rows.Scan(&rs.RouteName, &rs.Requests, &errs, &totalMs); err != nil {
			return nil, fmt.Errorf("scan summary: %w", err)
		}
		if errs != nil {
			rs.Errors = *errs
		}
		if rs.Requests > 0 {
			rs.ErrorRate = float64(rs.Errors) / float64(rs.Requests)
			rs.AvgDurationMs = float64(totalMs) / float64(rs.Requests)
		}
		out = append(out, rs)
	}
	return out, rows.Err()
}
