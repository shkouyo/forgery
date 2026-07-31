// SPDX-License-Identifier: GPL-3.0-or-later

// Copyright (C) 2026 ShinKouyo <i@0x0f.dev>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

// Package observability provides structured logging via slog
// and a simple health-check HTTP endpoint.
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
