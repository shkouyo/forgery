package observability

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics holds all application metrics for the forgery daemon.
// It uses atomic counters and a simple duration tracker (no external
// Prometheus client dependency).
//
// See DETAIL-DESIGN §5.10.2 for the full metric specification.
type Metrics struct {
	startTime time.Time

	// Counters (atomic)
	tasksTotal        atomic.Int64
	gaDispatchesTotal atomic.Int64
	gaDispatchesOK    atomic.Int64
	northFetchErrors  atomic.Int64

	// Gauges (atomic)
	tasksActive atomic.Int64

	mu                sync.RWMutex
	southRPCDurations map[string]*durationTracker // rpc name → tracker
}

// durationTracker tracks count and cumulative duration for a single RPC method.
type durationTracker struct {
	mu    sync.Mutex
	count int64
	total time.Duration
}

// NewMetrics returns an initialized Metrics instance.
func NewMetrics() *Metrics {
	return &Metrics{
		startTime:         time.Now(),
		southRPCDurations: make(map[string]*durationTracker),
	}
}

// ── Counter methods ──────────────────────────────────────────────────────

// IncTasksTotal increments the total task counter.
func (m *Metrics) IncTasksTotal() { m.tasksTotal.Add(1) }

// IncGADispatchesTotal increments the total GA dispatch counter.
func (m *Metrics) IncGADispatchesTotal() { m.gaDispatchesTotal.Add(1) }

// IncGADispatchesOK increments the successful GA dispatch counter.
func (m *Metrics) IncGADispatchesOK() { m.gaDispatchesOK.Add(1) }

// IncNorthFetchErrors increments the north fetch error counter.
func (m *Metrics) IncNorthFetchErrors() { m.northFetchErrors.Add(1) }

// ── Gauge methods ────────────────────────────────────────────────────────

// SetTasksActive sets the active task gauge to n.
func (m *Metrics) SetTasksActive(n int64) { m.tasksActive.Store(n) }

// IncTasksActive increments the active task gauge by 1.
func (m *Metrics) IncTasksActive() { m.tasksActive.Add(1) }

// DecTasksActive decrements the active task gauge by 1.
func (m *Metrics) DecTasksActive() { m.tasksActive.Add(-1) }

// TasksActive returns the current active task count.
func (m *Metrics) TasksActive() int64 { return m.tasksActive.Load() }

// ── Duration tracking ────────────────────────────────────────────────────

// ObserveSouthRPC records the duration of a south-bound RPC call.
// rpc is the method name (e.g. "Register", "Declare", "FetchTask",
// "UpdateTask", "UpdateLog").
func (m *Metrics) ObserveSouthRPC(rpc string, d time.Duration) {
	m.mu.RLock()
	t, ok := m.southRPCDurations[rpc]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		// Double-check after acquiring write lock.
		t, ok = m.southRPCDurations[rpc]
		if !ok {
			t = &durationTracker{}
			m.southRPCDurations[rpc] = t
		}
		m.mu.Unlock()
	}
	t.mu.Lock()
	t.count++
	t.total += d
	t.mu.Unlock()
}

// ── Metrics snapshot ─────────────────────────────────────────────────────

// snapshot returns the full metric payload at the time of the call.
func (m *Metrics) snapshot() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data := map[string]any{
		"forgery_tasks_total":              m.tasksTotal.Load(),
		"forgery_tasks_active":             m.tasksActive.Load(),
		"forgery_ga_dispatches_total":      m.gaDispatchesTotal.Load(),
		"forgery_ga_dispatches_ok":         m.gaDispatchesOK.Load(),
		"forgery_north_fetch_errors_total": m.northFetchErrors.Load(),
		"forgery_uptime_seconds":           time.Since(m.startTime).Seconds(),
	}

	// Per-RPC duration summaries.
	for rpc, t := range m.southRPCDurations {
		t.mu.Lock()
		cnt := t.count
		tot := t.total
		t.mu.Unlock()

		if cnt == 0 {
			continue
		}
		avg := tot.Seconds() / float64(cnt)
		data["forgery_south_rpc_duration_seconds_"+rpc+"_count"] = cnt
		data["forgery_south_rpc_duration_seconds_"+rpc+"_avg"] = avg
	}

	return data
}

// Handler returns an http.Handler that serves the current metrics as JSON.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(m.snapshot())
	})
}

// StartMetricsServer starts the metrics HTTP server on the given address.
// If addr is empty the call is a no-op. The server runs in a background
// goroutine and is not gracefully shut down (intended for use until process
// exit).
func StartMetricsServer(addr string, m *Metrics) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	go func() {
		_ = http.ListenAndServe(addr, mux)
	}()
}
