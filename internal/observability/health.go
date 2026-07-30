package observability

import (
	"encoding/json"
	"net/http"
)

// HealthChecker serves a fixed 200 + {"status":"ok"} health probe response.
type HealthChecker struct{}

// NewHealthChecker returns a new HealthChecker.
func NewHealthChecker() *HealthChecker {
	return &HealthChecker{}
}

// Handler returns an http.Handler that serves a fixed 200 + {"status":"ok"} response.
func (hc *HealthChecker) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
}

// StartHealthServer starts the health HTTP server on the given address.
// If addr is empty the call is a no-op. The server runs in a background
// goroutine and is not gracefully shut down.
func StartHealthServer(addr string, hc *HealthChecker) {
	startHTTPServer(addr, hc.Handler())
}

// startHTTPServer starts an HTTP server on the given address with the given handler.
// If addr is empty the call is a no-op. The server runs in a background
// goroutine and is not gracefully shut down.
func startHTTPServer(addr string, handler http.Handler) {
	if addr == "" {
		return
	}
	go func() {
		_ = http.ListenAndServe(addr, handler)
	}()
}
