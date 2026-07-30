// Package observability provides structured logging, metrics, and health checks
// for the forgery daemon.
//
// MVP (Phase 1) implements only slog-based structured logging.
// Prometheus metrics (metrics.go) and health checks (health.go) are Phase 2.
package observability

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// NewLogger creates a configured slog.Logger based on level and format strings.
//
// level: "debug", "info", "warn", "error" (default "info")
// format: "json" or "text" (default "json")
//
// The returned logger writes to os.Stdout.
func NewLogger(level, format string) *slog.Logger {
	return newLogger(os.Stdout, level, format)
}

// newLogger is the internal constructor that accepts an io.Writer for testability.
func newLogger(w io.Writer, level, format string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: lvl,
	}

	var handler slog.Handler
	switch strings.ToLower(format) {
	case "text":
		handler = slog.NewTextHandler(w, opts)
	default:
		handler = slog.NewJSONHandler(w, opts)
	}

	return slog.New(handler)
}
