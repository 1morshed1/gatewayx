// Command gateway is the GatewayX entrypoint. It has three subcommands:
//
//	gateway serve    (default) run the proxy + admin + metrics servers
//	gateway migrate  apply database migrations and exit
//	gateway seed     insert demo routes/consumer for a quick local demo
//
// Keeping migrate/seed in the same binary means ops only ship one artifact, and
// `make migrate` / `make seed` map to obvious subcommands.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/1morshed1/gatewayx/internal/config"
)

func main() {
	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(1)
	}
	log := newLogger(cfg.LogLevel)

	// Root context cancelled on SIGINT/SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var runErr error
	switch cmd {
	case "serve":
		runErr = runServe(ctx, cfg, log)
	case "migrate":
		runErr = runMigrate(ctx, cfg, log)
	case "seed":
		runErr = runSeed(ctx, cfg, log)
	case "analytics":
		runErr = runAnalytics(ctx, cfg, log)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q (want: serve|migrate|seed|analytics)\n", cmd)
		os.Exit(2)
	}

	if runErr != nil {
		log.Error("fatal", "cmd", cmd, "err", runErr)
		os.Exit(1)
	}
}

// newLogger builds a structured JSON logger at the configured level. Structured
// logs are the right default for infrastructure: they're machine-parseable and
// every line carries key/value context.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h)
}
