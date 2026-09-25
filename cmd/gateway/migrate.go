package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

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

// writeJSON is a shared response helper for the cmd-level HTTP handlers.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
