package main

import (
	"flag"
	"os"

	"git.0x0f.dev/forgery/internal/app"
	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/observability"
)

func main() {
	// 1. Command-line flags.
	configPath := flag.String("config", "forgery.toml", "path to the TOML configuration file")
	flag.Parse()

	// 2. Configuration (exits on error via the slog default logger).
	cfg := config.MustLoad(*configPath)

	// 3. Single structured logger, built once from the loaded config.
	logger := observability.NewLogger(cfg.Global.LogLevel, cfg.Global.LogFormat)

	// 4. Assemble and run; any startup failure exits non-zero.
	if err := app.Run(cfg, logger); err != nil {
		logger.Error("forgery failed", "err", err.Error())
		os.Exit(1)
	}
}
