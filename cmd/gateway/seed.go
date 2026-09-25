package main

import (
	"context"
	"log/slog"

	"github.com/1morshed1/gatewayx/internal/config"
	"github.com/1morshed1/gatewayx/internal/store"
)

// runSeed loads the demo config (see store.SeedDemo) and exits.
func runSeed(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	st, err := store.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer st.Close()

	if err := st.SeedDemo(ctx); err != nil {
		return err
	}
	log.Info("demo config seeded", "api_key", "demo-key", "route", "/echo")
	return nil
}
