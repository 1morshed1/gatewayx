// Package store is the Postgres-backed configuration store. It loads routes,
// upstreams, plugin config, and consumers into the domain types the rest of the
// gateway uses, and it exposes the admin CRUD + change-notification plumbing.
//
// We use pgx (jackc/pgx) directly rather than database/sql + an ORM. pgx is the
// fastest, most feature-complete Postgres driver for Go and gives us native
// access to LISTEN/NOTIFY, which we need for hot config reload.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps a pgx connection pool. A single Store is shared across the gateway.
type Store struct {
	pool *pgxpool.Pool
	dsn  string
}

// New opens a connection pool to Postgres and verifies connectivity. It retries
// briefly so `make run` right after `make infra-up` doesn't fail on a Postgres
// that is still starting.
func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	// Retry the initial ping — Postgres may still be booting.
	var pingErr error
	for i := 0; i < 10; i++ {
		if pingErr = pool.Ping(ctx); pingErr == nil {
			return &Store{pool: pool, dsn: dsn}, nil
		}
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	pool.Close()
	return nil, fmt.Errorf("ping postgres: %w", pingErr)
}

// Pool exposes the underlying pool for packages that need a raw connection
// (e.g. the LISTEN/NOTIFY watcher).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// DSN returns the connection string (used to open a dedicated listener conn).
func (s *Store) DSN() string { return s.dsn }

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }
