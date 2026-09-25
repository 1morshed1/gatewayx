package store

import (
	"context"
	"fmt"
	"io/fs"
	"sort"

	"github.com/1morshed1/gatewayx/migrations"
)

// Migrate applies every embedded .sql migration that has not yet run, in
// filename order, recording applied files in a schema_migrations table.
//
// This is a deliberately minimal migrator: no down-migrations, no checksums. For
// a project this size that is the right amount of machinery — enough to be safe
// and repeatable, not so much that it becomes its own subsystem. Each migration
// is expected to be idempotent (IF NOT EXISTS / CREATE OR REPLACE) as a belt-and
// -braces guard.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			filename   TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && hasSQLSuffix(e.Name()) {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // lexical order => 0001, 0002, ...

	for _, name := range names {
		var exists bool
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)`, name,
		).Scan(&exists); err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if exists {
			continue
		}

		sqlBytes, err := migrations.FS.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}

		// Each migration runs in its own transaction so a failure leaves the DB
		// in a consistent state (either the whole file applied, or none of it).
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations(filename) VALUES ($1)`, name,
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit %s: %w", name, err)
		}
		fmt.Printf("migrated: %s\n", name)
	}
	return nil
}

func hasSQLSuffix(name string) bool {
	return len(name) > 4 && name[len(name)-4:] == ".sql"
}
