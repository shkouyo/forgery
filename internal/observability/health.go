package observability

import (
	"encoding/json"
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

// StartHealthServer starts the health HTTP server on the given address.
// If addr is empty the call is a no-op and returns nil. The server runs in a
// background goroutine; the returned *http.Server lets the caller shut it
// down gracefully via Shutdown as part of the daemon's shutdown sequence.
func StartHealthServer(addr string, hc *HealthChecker) *http.Server {
	return startHTTPServer(addr, hc.Handler())
}

// startHTTPServer starts an HTTP server on the given address with the given handler.
// If addr is empty the call is a no-op and returns nil. The server runs in a
// background goroutine; the returned *http.Server lets the caller shut it
// down gracefully via Shutdown.
func startHTTPServer(addr string, handler http.Handler) *http.Server {
	if addr == "" {
		return nil
	}
	srv := &http.Server{Addr: addr, Handler: handler}
	go func() {
		_ = srv.ListenAndServe()
	}()
	return srv
}
