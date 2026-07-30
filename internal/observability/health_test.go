package observability

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthChecker_NewDefaults(t *testing.T) {
	hc := NewHealthChecker()

	if !hc.Alive() {
		t.Error("expected Alive()=true by default")
	}
	if hc.Ready() {
		t.Error("expected Ready()=false by default")
	}
}

func TestHealthChecker_SetAlive(t *testing.T) {
	hc := NewHealthChecker()

	hc.SetAlive(false)
	if hc.Alive() {
		t.Error("expected Alive()=false after SetAlive(false)")
	}

	hc.SetAlive(true)
	if !hc.Alive() {
		t.Error("expected Alive()=true after SetAlive(true)")
	}
}

func TestHealthChecker_SetReady(t *testing.T) {
	hc := NewHealthChecker()

	hc.SetReady(true)
	if !hc.Ready() {
		t.Error("expected Ready()=true after SetReady(true)")
	}

	hc.SetReady(false)
	if hc.Ready() {
		t.Error("expected Ready()=false after SetReady(false)")
	}
}

// ── /healthz endpoint ────────────────────────────────────────────────────

func TestHealthz_Returns200WhenAlive(t *testing.T) {
	hc := NewHealthChecker()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	hc.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", body["status"])
	}
}

func TestHealthz_Returns503WhenNotAlive(t *testing.T) {
	hc := NewHealthChecker()
	hc.SetAlive(false)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	hc.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	var body map[string]string
	json.NewDecoder(rec.Body).Decode(&body)
	if body["status"] != "unhealthy" {
		t.Errorf("expected status=unhealthy, got %q", body["status"])
	}
}

// ── /readyz endpoint ─────────────────────────────────────────────────────

func TestReadyz_Returns200WhenReady(t *testing.T) {
	hc := NewHealthChecker()
	hc.SetReady(true)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	hc.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}
	if body["status"] != "ready" {
		t.Errorf("expected status=ready, got %q", body["status"])
	}
}

func TestReadyz_Returns503WhenNotReady(t *testing.T) {
	hc := NewHealthChecker()
	// Default is not ready.

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	hc.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	var body map[string]string
	json.NewDecoder(rec.Body).Decode(&body)
	if body["status"] != "not ready" {
		t.Errorf("expected status='not ready', got %q", body["status"])
	}
}

func TestReadyz_SetReadyChangesResponse(t *testing.T) {
	hc := NewHealthChecker()

	// Initially not ready.
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	hc.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when not ready, got %d", rec.Code)
	}

	// Set ready.
	hc.SetReady(true)

	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	hc.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 after SetReady(true), got %d", rec.Code)
	}
}

// ── Content-Type header ─────────────────────────────────────────────────

func TestHealthz_SetsContentTypeJSON(t *testing.T) {
	hc := NewHealthChecker()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	hc.Handler().ServeHTTP(rec, req)

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type=application/json, got %q", ct)
	}
}

// ── StartHealthServer test ───────────────────────────────────────────────

func TestStartHealthServer_NoOpOnEmptyAddr(t *testing.T) {
	hc := NewHealthChecker()
	// Should not panic or block.
	StartHealthServer("", hc)
}
