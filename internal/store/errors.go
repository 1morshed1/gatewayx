package store

import (
	"errors"

	"github.com/jackc/pgx/v5"
)

// isNoRows reports whether an error is pgx's "no rows in result set". Isolated in
// one helper so the driver detail doesn't leak across query methods.
func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}
