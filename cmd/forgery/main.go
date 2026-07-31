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
