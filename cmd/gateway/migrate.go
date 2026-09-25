package main

import (
	"context"
	"log/slog"

	"github.com/1morshed1/gatewayx/internal/config"
	"github.com/1morshed1/gatewayx/internal/store"
)

// runMigrate applies pending database migrations and exits.
func runMigrate(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	st, err := store.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer st.Close()

	if err := st.Migrate(ctx); err != nil {
		return err
	}
	log.Info("migrations applied")
	return nil
}
