package observability

import (
	"context"
	"encoding/json"
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
	// Should not panic or block, and must return nil (no server to shut
	// down later).
	if srv := StartHealthServer("", hc); srv != nil {
		t.Fatal("StartHealthServer with empty addr must return nil")
	}
}

func TestStartHealthServer_ReturnsShutdownableServer(t *testing.T) {
	hc := NewHealthChecker()
	srv := StartHealthServer("127.0.0.1:0", hc)
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
