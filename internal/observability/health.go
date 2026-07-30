package observability

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

// HealthChecker exposes liveness (/healthz) and readiness (/readyz) probes
// for Kubernetes-style health monitoring.
//
// See DETAIL-DESIGN §5.10.3 for the full specification.
type HealthChecker struct {
	ready atomic.Bool
	alive atomic.Bool
}

// NewHealthChecker returns an initialized HealthChecker.
// Liveness defaults to true; readiness defaults to false.
func NewHealthChecker() *HealthChecker {
	hc := &HealthChecker{}
	hc.alive.Store(true)
	// ready defaults to false — call SetReady(true) after startup.
	return hc
}

// SetReady sets the readiness probe state.
func (hc *HealthChecker) SetReady(v bool) { hc.ready.Store(v) }

// SetAlive sets the liveness probe state.
func (hc *HealthChecker) SetAlive(v bool) { hc.alive.Store(v) }

// Ready returns the current readiness state.
func (hc *HealthChecker) Ready() bool { return hc.ready.Load() }

// Alive returns the current liveness state.
func (hc *HealthChecker) Alive() bool { return hc.alive.Load() }

// Handler returns an http.Handler with both /healthz and /readyz endpoints.
func (hc *HealthChecker) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if hc.alive.Load() {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy"})
		}
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if hc.ready.Load() {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "not ready"})
		}
	})

	return mux
}

// StartHealthServer starts the health HTTP server on the given address.
// If addr is empty the call is a no-op. The server runs in a background
// goroutine and is not gracefully shut down.
func StartHealthServer(addr string, hc *HealthChecker) {
	if addr == "" {
		return
	}
	go func() {
		_ = http.ListenAndServe(addr, hc.Handler())
	}()
}
