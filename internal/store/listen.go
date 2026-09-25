package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// The reload channel used by the NOTIFY triggers in migration 0001.
const reloadChannel = "route_changes"

// WatchListen blocks, listening for Postgres NOTIFY on the route-changes channel,
// calling onChange for each notification. It uses a *dedicated* connection (not the
// pool) because LISTEN state is per-connection and the connection must stay open
// and idle waiting for notifications — you can't do that with a pooled connection
// that gets recycled between queries.
//
// This is push-based hot reload: an admin writes to the DB, the trigger fires
// NOTIFY, every gateway replica listening wakes up and reloads within milliseconds
// — no restart, no polling lag. See docs/07-admin-hot-reload.md.
func (s *Store) WatchListen(ctx context.Context, onChange func()) error {
	for {
		if err := s.listenOnce(ctx, onChange); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Connection dropped (DB restart, network blip). Back off and retry so
			// hot reload survives transient failures.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func (s *Store) listenOnce(ctx context.Context, onChange func()) error {
	conn, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "LISTEN "+reloadChannel); err != nil {
		return err
	}

	for {
		// Blocks until a notification arrives or the context is cancelled.
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		onChange()
	}
}

// WatchPoll is the simpler fallback: it calls onChange on a fixed interval. Use it
// where holding a dedicated listener connection is undesirable (e.g. behind a
// connection pooler like PgBouncer in transaction mode, which breaks LISTEN). The
// trade-off is up to `interval` of staleness after a config change.
func (s *Store) WatchPoll(ctx context.Context, interval time.Duration, onChange func()) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			onChange()
		}
	}
}
