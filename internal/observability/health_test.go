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
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthChecker_Returns200OK(t *testing.T) {
	hc := NewHealthChecker()
	for _, path := range []string{"/healthz", "/readyz"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		hc.Handler().ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("path %q: expected 200 OK, got %d", path, rec.Code)
		}

		var body map[string]string
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("path %q: failed to decode JSON: %v", path, err)
		}
		if body["status"] != "ok" {
			t.Errorf("path %q: expected status=ok, got %q", path, body["status"])
		}
	}
}

func TestHealthChecker_SetsContentTypeJSON(t *testing.T) {
	hc := NewHealthChecker()
	for _, path := range []string{"/healthz", "/readyz"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		hc.Handler().ServeHTTP(rec, req)

		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("path %q: expected Content-Type=application/json, got %q", path, ct)
		}
	}
}

func TestHealthChecker_RejectsUnknownPaths(t *testing.T) {
	hc := NewHealthChecker()
	for _, path := range []string{"/", "/other", "/healthz/", "/readyz/", "/healthz/extra", "/readyz/extra"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		hc.Handler().ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("path %q: expected 404, got %d", path, rec.Code)
		}
	}
}

func TestStartHealthServer_NoOpOnEmptyAddr(t *testing.T) {
	hc := NewHealthChecker()
	// Should not panic or block, and must return (nil, nil) — no server to
	// shut down later, no bind error.
	srv, err := StartHealthServer("", hc)
	if srv != nil || err != nil {
		t.Fatalf("StartHealthServer with empty addr must return (nil, nil), got (%v, %v)", srv, err)
	}
}

func TestStartHealthServer_ReturnsShutdownableServer(t *testing.T) {
	hc := NewHealthChecker()
	srv, err := StartHealthServer("127.0.0.1:0", hc)
	if err != nil {
		t.Fatalf("StartHealthServer with a real addr: %v", err)
	}
	if srv == nil {
		t.Fatal("StartHealthServer with a real addr must return the server")
	}

	// The server runs in a background goroutine; Shutdown must stop it
	// cleanly as part of the daemon shutdown sequence.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("health server shutdown: %v", err)
	}
}

// TestStartHealthServer_ServesProbes verifies the bound server actually
// answers the health probes end-to-end.
func TestStartHealthServer_ServesProbes(t *testing.T) {
	// Pick a free port, release it, then bind the health server to it.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("test setup: find free port: %v", err)
	}
	addr := probe.Addr().String()
	probe.Close()

	srv, err := StartHealthServer(addr, NewHealthChecker())
	if err != nil {
		t.Fatalf("StartHealthServer: %v", err)
	}
	if srv == nil {
		t.Fatal("StartHealthServer returned nil server")
	}

	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("health probe request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health probe: expected 200, got %d", resp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("health server shutdown: %v", err)
	}
}

// TestStartHealthServer_BindError verifies fail-fast: an occupied address
// returns an error instead of silently degrading.
func TestStartHealthServer_BindError(t *testing.T) {
	// Occupy a port and keep it held.
	occupy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("test setup: occupy port: %v", err)
	}
	defer occupy.Close()

	srv, err := StartHealthServer(occupy.Addr().String(), NewHealthChecker())
	if err == nil {
		t.Fatal("StartHealthServer on an occupied address must return an error")
	}
	if srv != nil {
		t.Fatal("StartHealthServer on an occupied address must return a nil server")
	}
}
