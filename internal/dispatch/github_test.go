package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"log/slog"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/store"
)

// testGitHub returns GitHub connection settings suitable for testing.
func testGitHub(apiURL string) GitHub {
	return GitHub{
		APIURL:     apiURL,
		Token:      "test-github-token",
		Repo:       "test-owner/test-repo",
		WorkflowID: "forgery-runner.yml",
		Ref:        "main",
		PublicURL:  "https://forgery.example.com:8443",
	}
}

// testInstances returns two instances with distinct labels and container
// images, for routing verification.
func testInstances() []config.Instance {
	return []config.Instance{
		{
			Name:                  "inst-a",
			ForgejoRunnerLabels:   []string{"ubuntu-latest:docker://node:20", "docker:docker://ghcr.io/test"},
			DefaultContainerImage: "docker://ghcr.io/catthehacker/ubuntu:act-latest",
		},
		{
			Name:                  "inst-b",
			ForgejoRunnerLabels:   []string{"linux:docker://debian:bookworm"},
			DefaultContainerImage: "docker://ghcr.io/catthehacker/debian:bookworm",
		},
	}
}

// testTaskCtx returns a TaskCtx with test values.
func testTaskCtx() *store.TaskCtx {
	return &store.TaskCtx{
		ID:        42,
		Instance:  "inst-a",
		Task:      nil, // dispatch doesn't use Task field
		RegToken:  "abc123def456",
		CreatedAt: time.Now(),
	}
}

// nopLogger returns a logger that discards output.
func nopLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestTrigger_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())

	err := d.Trigger(context.Background(), testTaskCtx(), testInstances()[0])
	if err != nil {
		t.Fatalf("expected nil error for 204, got: %v", err)
	}
}

func TestTrigger_DispatchError(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		respBody   string
	}{
		{"401 Unauthorized", http.StatusUnauthorized, `{"message":"Bad credentials"}`},
		{"404 Not Found", http.StatusNotFound, `{"message":"Not Found"}`},
		{"422 Unprocessable Entity", http.StatusUnprocessableEntity, `{"message":"Invalid inputs"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				w.Write([]byte(tt.respBody))
			}))
			defer srv.Close()

			d := NewDispatcher(testGitHub(srv.URL), nopLogger())

			err := d.Trigger(context.Background(), testTaskCtx(), testInstances()[0])
			if err == nil {
				t.Fatal("expected non-nil error")
			}

			dispErr, ok := err.(*DispatchError)
			if !ok {
				t.Fatalf("expected *DispatchError, got: %T", err)
			}
			if dispErr.StatusCode != tt.statusCode {
				t.Fatalf("expected status %d, got: %d", tt.statusCode, dispErr.StatusCode)
			}
			if dispErr.Body != tt.respBody {
				t.Fatalf("expected body %q, got: %q", tt.respBody, dispErr.Body)
			}
		})
	}
}

func TestTrigger_RequestBodyFormat(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Decode and validate the request body.
		var body dispatchInputs
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("failed to decode request body: %v", err)
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Verify ref.
		if body.Ref != "main" {
			t.Errorf("expected ref 'main', got: %q", body.Ref)
		}

		// Verify all 5 inputs.
		inputs := body.Inputs
		if inputs.ProxyURL != "https://forgery.example.com:8443" {
			t.Errorf("expected proxy_url %q, got: %q", "https://forgery.example.com:8443", inputs.ProxyURL)
		}
		if inputs.RegToken != "abc123def456" {
			t.Errorf("expected reg_token %q, got: %q", "abc123def456", inputs.RegToken)
		}
		expectedLabels := "ubuntu-latest:docker://node:20,docker:docker://ghcr.io/test"
		if inputs.Labels != expectedLabels {
			t.Errorf("expected labels %q, got: %q", expectedLabels, inputs.Labels)
		}
		if inputs.ContainerImage != "docker://ghcr.io/catthehacker/ubuntu:act-latest" {
			t.Errorf("expected container_image %q, got: %q", "docker://ghcr.io/catthehacker/ubuntu:act-latest", inputs.ContainerImage)
		}
		if inputs.TaskID != "42" {
			t.Errorf("expected task_id %q, got: %q", "42", inputs.TaskID)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())

	err := d.Trigger(context.Background(), testTaskCtx(), testInstances()[0])
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

// TestTrigger_PerInstanceInputs verifies the multi-instance routing contract:
// each task's labels and container_image come from its own instance, while
// the global GitHub fields stay fixed.
func TestTrigger_PerInstanceInputs(t *testing.T) {
	var mu sync.Mutex
	labelsByInstance := map[string]string{}
	imagesByInstance := map[string]string{}
	var globalFields []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body dispatchInputs
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("failed to decode request body: %v", err)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		mu.Lock()
		labelsByInstance[body.Inputs.TaskID] = body.Inputs.Labels
		imagesByInstance[body.Inputs.TaskID] = body.Inputs.ContainerImage
		globalFields = append(globalFields, body.Ref+"/"+body.Inputs.ProxyURL)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())
	insts := testInstances()

	// Task 1 belongs to inst-a, task 2 to inst-b.
	taskA := &store.TaskCtx{ID: 1, Instance: "inst-a", RegToken: "token-a", CreatedAt: time.Now()}
	taskB := &store.TaskCtx{ID: 2, Instance: "inst-b", RegToken: "token-b", CreatedAt: time.Now()}

	if err := d.Trigger(context.Background(), taskA, insts[0]); err != nil {
		t.Fatalf("trigger for inst-a: %v", err)
	}
	if err := d.Trigger(context.Background(), taskB, insts[1]); err != nil {
		t.Fatalf("trigger for inst-b: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got := labelsByInstance["1"]; got != "ubuntu-latest:docker://node:20,docker:docker://ghcr.io/test" {
		t.Errorf("inst-a labels = %q, want its own label set", got)
	}
	if got := labelsByInstance["2"]; got != "linux:docker://debian:bookworm" {
		t.Errorf("inst-b labels = %q, want its own label set", got)
	}
	if got := imagesByInstance["1"]; got != "docker://ghcr.io/catthehacker/ubuntu:act-latest" {
		t.Errorf("inst-a container_image = %q, want its own image", got)
	}
	if got := imagesByInstance["2"]; got != "docker://ghcr.io/catthehacker/debian:bookworm" {
		t.Errorf("inst-b container_image = %q, want its own image", got)
	}
	// Global fields are identical for both dispatches.
	if len(globalFields) != 2 || globalFields[0] != globalFields[1] {
		t.Errorf("global fields varied between instances: %v", globalFields)
	}
	if globalFields[0] != "main/https://forgery.example.com:8443" {
		t.Errorf("global ref/proxy_url = %q, want fixed values", globalFields[0])
	}
}

func TestTrigger_AuthorizationHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer test-github-token" {
			t.Errorf("expected Authorization 'Bearer test-github-token', got: %q", auth)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())

	err := d.Trigger(context.Background(), testTaskCtx(), testInstances()[0])
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func TestTrigger_ContentTypeHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ct := r.Header.Get("Content-Type")
		if ct != "application/json" {
			t.Errorf("expected Content-Type 'application/json', got: %q", ct)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())

	err := d.Trigger(context.Background(), testTaskCtx(), testInstances()[0])
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func TestTrigger_UserAgentHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ua := r.Header.Get("User-Agent")
		if ua != "forgery/1.0.0" {
			t.Errorf("expected User-Agent 'forgery/1.0.0', got: %q", ua)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())

	err := d.Trigger(context.Background(), testTaskCtx(), testInstances()[0])
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func TestTrigger_ContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Sleep briefly to ensure the client observes cancellation.
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := d.Trigger(ctx, testTaskCtx(), testInstances()[0])
	if err == nil {
		t.Fatal("expected non-nil error for cancelled context")
	}

	// The error should be context.Canceled (wrapped).
	if !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("expected context cancellation error, got: %v", err)
	}
}

func TestTrigger_PathConstruction(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expectedPath := "/repos/test-owner/test-repo/actions/workflows/forgery-runner.yml/dispatches"
		if r.URL.Path != expectedPath {
			t.Errorf("expected path %q, got: %q", expectedPath, r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())

	err := d.Trigger(context.Background(), testTaskCtx(), testInstances()[0])
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func TestTrigger_HTTPMethod(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST method, got: %q", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())

	err := d.Trigger(context.Background(), testTaskCtx(), testInstances()[0])
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

// failTransport is a RoundTripper that fails the first failN requests
// with a simulated network error, then delegates to next.
type failTransport struct {
	mu       sync.Mutex
	failN    int
	attempts int
	next     http.RoundTripper
}

func (ft *failTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ft.mu.Lock()
	ft.attempts++
	remaining := ft.failN - (ft.attempts - 1)
	ft.mu.Unlock()
	if remaining > 0 {
		return nil, fmt.Errorf("simulated network error: connection refused")
	}
	return ft.next.RoundTrip(req)
}

func TestTrigger_RetryOn5xx(t *testing.T) {
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())

	err := d.Trigger(context.Background(), testTaskCtx(), testInstances()[0])
	if err != nil {
		t.Fatalf("expected nil error after retry on 5xx, got: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts (2 failures + 1 success), got: %d", attempts)
	}
}

func TestTrigger_NoRetryOn4xx(t *testing.T) {
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"message":"Bad credentials"}`))
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())

	err := d.Trigger(context.Background(), testTaskCtx(), testInstances()[0])
	if err == nil {
		t.Fatal("expected non-nil error for 4xx")
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt for 4xx (no retry), got: %d", attempts)
	}
	// Verify it's a DispatchError, not a wrapped retry error.
	var dispErr *DispatchError
	if !errors.As(err, &dispErr) {
		t.Fatalf("expected *DispatchError, got: %T", err)
	}
}

func TestTrigger_MaxRetriesExceeded(t *testing.T) {
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())

	err := d.Trigger(context.Background(), testTaskCtx(), testInstances()[0])
	if err == nil {
		t.Fatal("expected non-nil error after max retries")
	}
	// maxRetries=3 means 4 total attempts (0, 1, 2, 3)
	if attempts != 4 {
		t.Fatalf("expected 4 attempts (maxRetries=3), got: %d", attempts)
	}
	// The error message should mention retries.
	if !strings.Contains(err.Error(), "dispatch failed after 3 retries") {
		t.Fatalf("expected retry exhaustion message, got: %v", err)
	}
}

func TestTrigger_ContextCancellationStopsRetry(t *testing.T) {
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		// Always return 500 to trigger retry loop.
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay — first request completes, then
	// cancellation arrives during backoff sleep.
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := d.Trigger(ctx, testTaskCtx(), testInstances()[0])
	if err == nil {
		t.Fatal("expected non-nil error from context cancellation")
	}
	// Should have completed the first attempt and stopped during backoff.
	if attempts < 1 {
		t.Fatal("expected at least 1 attempt")
	}
	if attempts >= 4 {
		t.Fatalf("expected fewer than 4 attempts (cancelled), got: %d", attempts)
	}
}

func TestTrigger_RetryOnNetworkError(t *testing.T) {
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	d := NewDispatcher(testGitHub(srv.URL), nopLogger())
	// Inject a transport that fails the first request with a network error.
	d.client.Transport = &failTransport{
		failN: 1,
		next:  http.DefaultTransport,
	}

	err := d.Trigger(context.Background(), testTaskCtx(), testInstances()[0])
	if err != nil {
		t.Fatalf("expected nil error after retry on network error, got: %v", err)
	}
	if attempts < 1 {
		t.Fatal("expected at least 1 successful attempt")
	}
}

func TestDispatchError_Error(t *testing.T) {
	e := &DispatchError{StatusCode: 422, Body: "Invalid ref"}
	expected := "dispatch: GitHub API returned 422: Invalid ref"
	if e.Error() != expected {
		t.Errorf("expected %q, got: %q", expected, e.Error())
	}
}

func TestNewDispatcher(t *testing.T) {
	gh := testGitHub("https://api.github.com")
	log := nopLogger()
	d := NewDispatcher(gh, log)

	if d.client == nil {
		t.Fatal("client must not be nil")
	}
	if d.client.Timeout != 30*time.Second {
		t.Fatalf("expected client timeout 30s, got: %v", d.client.Timeout)
	}
	if d.gh != gh {
		t.Fatal("gh field not set correctly")
	}
	if d.log != log {
		t.Fatal("log field not set correctly")
	}
}
