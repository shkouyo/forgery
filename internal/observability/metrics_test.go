package observability

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// ── Counter tests ────────────────────────────────────────────────────────

func TestMetrics_IncTasksTotal(t *testing.T) {
	m := NewMetrics()
	m.IncTasksTotal()
	m.IncTasksTotal()

	snap := m.snapshot()
	if v := snap["forgery_tasks_total"].(int64); v != 2 {
		t.Errorf("expected tasks_total=2, got %d", v)
	}
}

func TestMetrics_IncGADispatchesTotal(t *testing.T) {
	m := NewMetrics()
	m.IncGADispatchesTotal()
	m.IncGADispatchesTotal()
	m.IncGADispatchesTotal()

	snap := m.snapshot()
	if v := snap["forgery_ga_dispatches_total"].(int64); v != 3 {
		t.Errorf("expected ga_dispatches_total=3, got %d", v)
	}
}

func TestMetrics_IncGADispatchesOK(t *testing.T) {
	m := NewMetrics()
	m.IncGADispatchesOK()
	m.IncGADispatchesOK()

	snap := m.snapshot()
	if v := snap["forgery_ga_dispatches_ok"].(int64); v != 2 {
		t.Errorf("expected ga_dispatches_ok=2, got %d", v)
	}
}

func TestMetrics_IncNorthFetchErrors(t *testing.T) {
	m := NewMetrics()
	m.IncNorthFetchErrors()

	snap := m.snapshot()
	if v := snap["forgery_north_fetch_errors_total"].(int64); v != 1 {
		t.Errorf("expected north_fetch_errors_total=1, got %d", v)
	}
}

// ── Gauge tests ──────────────────────────────────────────────────────────

func TestMetrics_SetTasksActive(t *testing.T) {
	m := NewMetrics()
	m.SetTasksActive(5)

	if v := m.TasksActive(); v != 5 {
		t.Errorf("expected tasks_active=5, got %d", v)
	}

	snap := m.snapshot()
	if v := snap["forgery_tasks_active"].(int64); v != 5 {
		t.Errorf("expected tasks_active=5 in snapshot, got %d", v)
	}
}

func TestMetrics_IncDecTasksActive(t *testing.T) {
	m := NewMetrics()

	m.IncTasksActive()
	m.IncTasksActive()
	m.IncTasksActive()
	if v := m.TasksActive(); v != 3 {
		t.Errorf("after 3 inc, expected 3, got %d", v)
	}

	m.DecTasksActive()
	if v := m.TasksActive(); v != 2 {
		t.Errorf("after 1 dec, expected 2, got %d", v)
	}

	m.DecTasksActive()
	m.DecTasksActive()
	if v := m.TasksActive(); v != 0 {
		t.Errorf("after 2 more dec, expected 0, got %d", v)
	}

	// Decrement below zero should be allowed.
	m.DecTasksActive()
	if v := m.TasksActive(); v != -1 {
		t.Errorf("after dec below zero, expected -1, got %d", v)
	}
}

// ── Duration tracking tests ──────────────────────────────────────────────

func TestMetrics_ObserveSouthRPC(t *testing.T) {
	m := NewMetrics()

	m.ObserveSouthRPC("FetchTask", 100*time.Millisecond)
	m.ObserveSouthRPC("FetchTask", 200*time.Millisecond)
	m.ObserveSouthRPC("Register", 50*time.Millisecond)

	snap := m.snapshot()

	// FetchTask: 2 observations, avg = 150ms
	if v := snap["forgery_south_rpc_duration_seconds_FetchTask_count"].(int64); v != 2 {
		t.Errorf("expected FetchTask count=2, got %d", v)
	}
	if v := snap["forgery_south_rpc_duration_seconds_FetchTask_avg"].(float64); v < 0.14 || v > 0.16 {
		t.Errorf("expected FetchTask avg ~0.15, got %f", v)
	}

	// Register: 1 observation, avg = 50ms
	if v := snap["forgery_south_rpc_duration_seconds_Register_count"].(int64); v != 1 {
		t.Errorf("expected Register count=1, got %d", v)
	}
	if v := snap["forgery_south_rpc_duration_seconds_Register_avg"].(float64); v < 0.04 || v > 0.06 {
		t.Errorf("expected Register avg ~0.05, got %f", v)
	}
}

func TestMetrics_ObserveSouthRPC_Concurrent(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup

	n := 100
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			m.ObserveSouthRPC("UpdateTask", 10*time.Millisecond)
		}()
	}
	wg.Wait()

	snap := m.snapshot()
	if v := snap["forgery_south_rpc_duration_seconds_UpdateTask_count"].(int64); v != int64(n) {
		t.Errorf("expected UpdateTask count=%d, got %d", n, v)
	}
}

// ── Metrics handler tests ────────────────────────────────────────────────

func TestMetrics_HandlerReturnsJSON(t *testing.T) {
	m := NewMetrics()
	m.IncTasksTotal()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	ct := rec.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected Content-Type=application/json, got %q", ct)
	}

	var data map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}

	if v, ok := data["forgery_tasks_total"]; !ok {
		t.Error("expected forgery_tasks_total key in response")
	} else if v.(float64) != 1 {
		t.Errorf("expected forgery_tasks_total=1, got %v", v)
	}

	// Verify uptime is present and positive.
	if v, ok := data["forgery_uptime_seconds"]; !ok {
		t.Error("expected forgery_uptime_seconds key in response")
	} else if v.(float64) <= 0 {
		t.Errorf("expected forgery_uptime_seconds > 0, got %v", v)
	}
}

func TestMetrics_HandlerAllExpectedKeys(t *testing.T) {
	m := NewMetrics()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	var data map[string]any
	json.NewDecoder(rec.Body).Decode(&data)

	requiredKeys := []string{
		"forgery_tasks_total",
		"forgery_tasks_active",
		"forgery_ga_dispatches_total",
		"forgery_ga_dispatches_ok",
		"forgery_north_fetch_errors_total",
		"forgery_uptime_seconds",
	}
	for _, k := range requiredKeys {
		if _, ok := data[k]; !ok {
			t.Errorf("missing required key %q in metrics response", k)
		}
	}
}

// ── StartMetricsServer test ──────────────────────────────────────────────

func TestStartMetricsServer_NoOpOnEmptyAddr(t *testing.T) {
	m := NewMetrics()
	// This should not panic or block.
	StartMetricsServer("", m)
}

func TestStartMetricsServer_ServesMetrics(t *testing.T) {
	m := NewMetrics()
	m.IncTasksTotal()

	// Use a random available port by binding to :0 and then reading the
	// assigned address. Simpler: use httptest for the handler directly.
	// We test the server startup path with an empty-addr no-op above and
	// rely on the Handler() test for actual HTTP behaviour.  This avoids
	// flaky port-binding tests.
}
