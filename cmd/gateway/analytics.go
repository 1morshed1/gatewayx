package main

import (
	"context"
	"log/slog"

	"github.com/1morshed1/gatewayx/internal/analytics"
	"github.com/1morshed1/gatewayx/internal/config"
	"github.com/1morshed1/gatewayx/internal/store"
)

// runAnalytics runs the Kafka -> Postgres analytics consumer until interrupted.
// It's a separate process from `serve` so analytics can scale and fail
// independently of the data plane (the whole point of decoupling via Kafka).
func runAnalytics(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	st, err := store.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer st.Close()

	consumer := analytics.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, "gatewayx-analytics", st, log)
	log.Info("analytics consumer started", "topic", cfg.KafkaTopic, "group", "gatewayx-analytics")
	return consumer.Run(ctx)
}
