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

package observability

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
)

// HealthChecker serves a fixed 200 + {"status":"ok"} health probe response
// on the /healthz and /readyz endpoints.
type HealthChecker struct{}

// NewHealthChecker returns a new HealthChecker.
func NewHealthChecker() *HealthChecker {
	return &HealthChecker{}
}

// Handler returns an http.Handler that serves the health probe on /healthz
// and /readyz only; every other path, including "/", gets a 404.
func (hc *HealthChecker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", hc.probe)
	mux.HandleFunc("/readyz", hc.probe)
	return mux
}

// probe writes the fixed 200 + {"status":"ok"} health probe response.
func (hc *HealthChecker) probe(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// StartHealthServer binds the health HTTP server on the given address and
// serves it in a background goroutine. Binding is synchronous so an occupied
// or invalid address is reported as an error — the caller can fail the
// daemon's startup instead of silently degrading. An empty addr is a no-op
// and returns (nil, nil). The returned *http.Server lets the caller shut it
// down gracefully via Shutdown as part of the daemon's shutdown sequence;
// runtime Serve errors (after a successful bind) are logged via slog and do
// not propagate.
func StartHealthServer(addr string, hc *HealthChecker) (*http.Server, error) {
	if addr == "" {
		return nil, nil
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("health: listen on %s: %w", addr, err)
	}
	srv := &http.Server{Addr: addr, Handler: hc.Handler()}
	go func() {
		// The bind succeeded above, so a Serve failure here is a rare
		// runtime error; log it and keep the daemon running.
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			slog.Error("health HTTP server error", "addr", addr, "err", err)
		}
	}()
	return srv, nil
}
