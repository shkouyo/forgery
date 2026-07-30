package north

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
	v1 "code.gitea.io/actions-proto-go/runner/v1"
	"code.gitea.io/actions-proto-go/runner/v1/runnerv1connect"
	"connectrpc.com/connect"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/store"
)

// testLogger returns a no-op logger for tests.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// testConfig returns a minimal config for testing.
func testConfig(url string) *config.Config {
	return &config.Config{
		ForgejoURL:       url,
		MaxParallelTasks: 5,
		PollInterval:     0, // zero means callers should handle
	}
}

// ── TestNewClient ────────────────────────────────────────────────────────────

func TestNewClient(t *testing.T) {
	cfg := testConfig("https://forgejo.example.com")
	cfg.TLSInsecureSkipVerify = true
	s := store.NewMemStore()
	log := testLogger()

	c := New(cfg, s, 3, log)

	if c == nil {
		t.Fatal("New returned nil")
	}
	if c.cfg != cfg {
		t.Error("Client.cfg does not match input config")
	}
	if c.store != s {
		t.Error("Client.store does not match input store")
	}
	if c.log != log {
		t.Error("Client.log does not match input logger")
	}
	if cap(c.sem) != 3 {
		t.Errorf("sem capacity = %d, want 3", cap(c.sem))
	}
	if c.client == nil {
		t.Error("Client.client is nil")
	}
}

// ── TestSemCapacity ──────────────────────────────────────────────────────────

func TestSemCapacity(t *testing.T) {
	tests := []struct {
		name    string
		max     int
		wantCap int
	}{
		{"zero capacity", 0, 0},
		{"one capacity", 1, 1},
		{"default capacity", 5, 5},
		{"large capacity", 100, 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig("https://forgejo.example.com")
			c := New(cfg, store.NewMemStore(), tt.max, testLogger())
			if cap(c.sem) != tt.wantCap {
				t.Errorf("cap(sem) = %d, want %d", cap(c.sem), tt.wantCap)
			}
		})
	}
}

// ── mockHandler ──────────────────────────────────────────────────────────────

// mockHandler is a minimal RunnerServiceHandler implementation for testing.
// It embeds UnimplementedRunnerServiceHandler and overrides specific methods.
type mockHandler struct {
	runnerv1connect.UnimplementedRunnerServiceHandler

	fetchTaskFn  func(context.Context, *connect.Request[v1.FetchTaskRequest]) (*connect.Response[v1.FetchTaskResponse], error)
	updateTaskFn func(context.Context, *connect.Request[v1.UpdateTaskRequest]) (*connect.Response[v1.UpdateTaskResponse], error)
	updateLogFn  func(context.Context, *connect.Request[v1.UpdateLogRequest]) (*connect.Response[v1.UpdateLogResponse], error)
}

func (h *mockHandler) FetchTask(ctx context.Context, req *connect.Request[v1.FetchTaskRequest]) (*connect.Response[v1.FetchTaskResponse], error) {
	if h.fetchTaskFn != nil {
		return h.fetchTaskFn(ctx, req)
	}
	return connect.NewResponse(&v1.FetchTaskResponse{Task: nil}), nil
}

func (h *mockHandler) UpdateTask(ctx context.Context, req *connect.Request[v1.UpdateTaskRequest]) (*connect.Response[v1.UpdateTaskResponse], error) {
	if h.updateTaskFn != nil {
		return h.updateTaskFn(ctx, req)
	}
	return connect.NewResponse(&v1.UpdateTaskResponse{}), nil
}

func (h *mockHandler) UpdateLog(ctx context.Context, req *connect.Request[v1.UpdateLogRequest]) (*connect.Response[v1.UpdateLogResponse], error) {
	if h.updateLogFn != nil {
		return h.updateLogFn(ctx, req)
	}
	return connect.NewResponse(&v1.UpdateLogResponse{}), nil
}

// newMockServer creates an httptest.Server with a connect handler wrapping the
// given mockHandler, and returns the server and a Client connected to it.
//
// The server is configured for HTTP/2 (h2c) so that the gRPC-protocol client
// (used by New) can connect. It also strips the /api/actions prefix that the
// production client appends to match Forgejo's routing.
func newMockServer(t *testing.T, h *mockHandler, maxParallel int) (*httptest.Server, *Client) {
	t.Helper()

	_, muxHandler := runnerv1connect.NewRunnerServiceHandler(h)
	// Strip /api/actions prefix so the handler sees bare procedure paths
	// like /runner.v1.RunnerService/FetchTask.
	stripped := http.StripPrefix("/api/actions", muxHandler)

	// Create an unstarted server so we can configure HTTP/2 (h2c) before
	// starting it. The gRPC protocol requires HTTP/2.
	ts := httptest.NewUnstartedServer(stripped)
	ts.EnableHTTP2 = true
	ts.StartTLS()

	cfg := testConfig(ts.URL)
	cfg.TLSInsecureSkipVerify = true // test server uses self-signed cert
	c := New(cfg, store.NewMemStore(), maxParallel, testLogger())

	return ts, c
}
// ── TestForwardUpdateTask ────────────────────────────────────────────────────

func TestForwardUpdateTask(t *testing.T) {
	var capturedReq *v1.UpdateTaskRequest
	h := &mockHandler{
		updateTaskFn: func(_ context.Context, req *connect.Request[v1.UpdateTaskRequest]) (*connect.Response[v1.UpdateTaskResponse], error) {
			capturedReq = req.Msg
			return connect.NewResponse(&v1.UpdateTaskResponse{}), nil
		},
	}

	ts, c := newMockServer(t, h, 1)
	defer ts.Close()

	req := &v1.UpdateTaskRequest{
		State: &v1.TaskState{
			Id:     42,
			Result: v1.Result_RESULT_UNSPECIFIED,
		},
	}

	resp, err := c.ForwardUpdateTask(context.Background(), req)
	if err != nil {
		t.Fatalf("ForwardUpdateTask returned error: %v", err)
	}
	if resp == nil {
		t.Fatal("ForwardUpdateTask returned nil response")
	}
	if capturedReq == nil {
		t.Fatal("handler did not receive a request")
	}
	if capturedReq.GetState() == nil {
		t.Error("captured request state is nil")
	} else if capturedReq.GetState().GetId() != 42 {
		t.Errorf("captured request task id = %d, want 42", capturedReq.GetState().GetId())
	}
}

// ── TestForwardUpdateLog ─────────────────────────────────────────────────────

func TestForwardUpdateLog(t *testing.T) {
	var capturedReq *v1.UpdateLogRequest
	h := &mockHandler{
		updateLogFn: func(_ context.Context, req *connect.Request[v1.UpdateLogRequest]) (*connect.Response[v1.UpdateLogResponse], error) {
			capturedReq = req.Msg
			return connect.NewResponse(&v1.UpdateLogResponse{}), nil
		},
	}

	ts, c := newMockServer(t, h, 1)
	defer ts.Close()

	req := &v1.UpdateLogRequest{
		TaskId: 42,
		Index:  0,
		NoMore: true,
	}

	resp, err := c.ForwardUpdateLog(context.Background(), req)
	if err != nil {
		t.Fatalf("ForwardUpdateLog returned error: %v", err)
	}
	if resp == nil {
		t.Fatal("ForwardUpdateLog returned nil response")
	}
	if capturedReq == nil {
		t.Fatal("handler did not receive a request")
	}
	if capturedReq.GetTaskId() != 42 {
		t.Errorf("captured request task id = %d, want 42", capturedReq.GetTaskId())
	}
	if !capturedReq.GetNoMore() {
		t.Error("captured request NoMore = false, want true")
	}
}

// ── TestForwardUpdateTaskError ───────────────────────────────────────────────

func TestForwardUpdateTaskError(t *testing.T) {
	h := &mockHandler{
		updateTaskFn: func(_ context.Context, _ *connect.Request[v1.UpdateTaskRequest]) (*connect.Response[v1.UpdateTaskResponse], error) {
			return nil, connect.NewError(connect.CodeInternal, errors.New("test error"))
		},
	}

	ts, c := newMockServer(t, h, 1)
	defer ts.Close()

	req := &v1.UpdateTaskRequest{}
	_, err := c.ForwardUpdateTask(context.Background(), req)
	if err == nil {
		t.Fatal("expected error from ForwardUpdateTask, got nil")
	}
}

// ── TestReleaseSlot ───────────────────────────────────────────────────────────

func TestReleaseSlot(t *testing.T) {
	c := New(testConfig("https://forgejo.example.com"), store.NewMemStore(), 2, testLogger())

	// Fill both slots.
	c.sem <- struct{}{}
	c.sem <- struct{}{}

	// Verify channel is full: a non-blocking send should fail.
	select {
	case c.sem <- struct{}{}:
		t.Error("sem should be full but accepted another send")
	default:
		// Expected: channel is full.
	}

	// Release one slot.
	c.ReleaseSlot()

	// Now we should be able to acquire one slot.
	select {
	case c.sem <- struct{}{}:
		// Good: slot was released and re-acquired.
	default:
		t.Error("expected a free slot after ReleaseSlot, but sem is still full")
	}
}

// ── TestSemBackpressure ───────────────────────────────────────────────────────

func TestSemBackpressure(t *testing.T) {
	c := New(testConfig("https://forgejo.example.com"), store.NewMemStore(), 1, testLogger())

	// Fill the single slot.
	c.sem <- struct{}{}

	// A non-blocking attempt to acquire should fail.
	select {
	case c.sem <- struct{}{}:
		t.Error("sem with capacity 1 (full) should block, but accepted a non-blocking send")
	default:
		// Expected: backpressure in effect.
	}
}

// ── TestPollLoop_ReleasesSemOnError ───────────────────────────────────────────

func TestPollLoop_ReleasesSemOnError(t *testing.T) {
	h := &mockHandler{
		fetchTaskFn: func(_ context.Context, _ *connect.Request[v1.FetchTaskRequest]) (*connect.Response[v1.FetchTaskResponse], error) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("forgejo down"))
		},
	}

	ts, c := newMockServer(t, h, 1)
	defer ts.Close()

	// Prevent tight-looping by setting a long poll interval.
	c.cfg.PollInterval = 10 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskCh := make(chan *store.TaskCtx, 1)

	// Run one iteration: PollLoop acquires sem, calls FetchTask (error),
	// releases sem, then sleeps via wait(). We check the sem during the wait.
	go c.PollLoop(ctx, taskCh)

	// Wait a short time for the loop to run, hit the error, and release the sem.
	time.Sleep(50 * time.Millisecond)

	// After release, the sem should have a free slot.
	select {
	case c.sem <- struct{}{}:
		// Good: slot was released after error.
	default:
		t.Error("expected free slot after FetchTask error, but sem is still full")
	}

	cancel()
}

// ── TestPollLoop_ReleasesSemOnEmpty ────────────────────────────────────────────

func TestPollLoop_ReleasesSemOnEmpty(t *testing.T) {
	h := &mockHandler{
		fetchTaskFn: func(_ context.Context, _ *connect.Request[v1.FetchTaskRequest]) (*connect.Response[v1.FetchTaskResponse], error) {
			return connect.NewResponse(&v1.FetchTaskResponse{Task: nil}), nil
		},
	}

	ts, c := newMockServer(t, h, 1)
	defer ts.Close()

	// Prevent tight-looping by setting a long poll interval.
	c.cfg.PollInterval = 10 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskCh := make(chan *store.TaskCtx, 1)

	go c.PollLoop(ctx, taskCh)

	time.Sleep(50 * time.Millisecond)

	// After empty response, the sem should be released.
	select {
	case c.sem <- struct{}{}:
		// Good: slot was released after empty response.
	default:
		t.Error("expected free slot after empty FetchTask response, but sem is still full")
	}

	cancel()
}

// ── TestPollLoop_HoldsSemOnTask ───────────────────────────────────────────────

func TestPollLoop_HoldsSemOnTask(t *testing.T) {
	h := &mockHandler{
		fetchTaskFn: func(_ context.Context, _ *connect.Request[v1.FetchTaskRequest]) (*connect.Response[v1.FetchTaskResponse], error) {
			return connect.NewResponse(&v1.FetchTaskResponse{
				Task: &v1.Task{Id: 42},
			}), nil
		},
	}

	ts, c := newMockServer(t, h, 1)
	defer ts.Close()

	// Prevent tight-looping after the first task is delivered.
	c.cfg.PollInterval = 10 * time.Second

	ctx, cancel := context.WithCancel(context.Background())

	taskCh := make(chan *store.TaskCtx, 1)

	go func() {
		c.PollLoop(ctx, taskCh)
	}()

	// The poll loop should acquire the sem, fetch a task, and send it.
	select {
	case taskCtx := <-taskCh:
		if taskCtx.ID != 42 {
			t.Errorf("task id = %d, want 42", taskCtx.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task on taskCh")
	}

	// After a successful fetch the sem should STILL be held.
	select {
	case c.sem <- struct{}{}:
		t.Error("sem should be full (slot held for running task), but accepted a send")
	default:
		// Good: slot is held by the running task.
	}

	// Cancel the context so PollLoop stops before we test ReleaseSlot.
	cancel()

	// Now release the slot and verify it frees up.
	c.ReleaseSlot()
	select {
	case c.sem <- struct{}{}:
		// Good.
	default:
		t.Error("expected free slot after ReleaseSlot")
	}
}

// ── TestPollLoop_ContextCancelDuringAcquire ───────────────────────────────────

func TestPollLoop_ContextCancelDuringAcquire(t *testing.T) {
	c := New(testConfig("https://forgejo.example.com"), store.NewMemStore(), 1, testLogger())

	// Fill the sem so acquisition blocks.
	c.sem <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	taskCh := make(chan *store.TaskCtx, 1)

	done := make(chan struct{})
	go func() {
		c.PollLoop(ctx, taskCh)
		close(done)
	}()

	// Cancel the context; PollLoop should exit via ctx.Done() during sem acquisition.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Good: PollLoop exited.
	case <-time.After(2 * time.Second):
		t.Fatal("PollLoop did not exit after context cancellation")
	}
}

// ── TestStartHeartbeat ────────────────────────────────────────────────────────

func TestStartHeartbeat(t *testing.T) {
	callCount := make(chan int, 10) // buffered to avoid blocking the heartbeat goroutine
	var count int

	h := &mockHandler{
		updateTaskFn: func(_ context.Context, req *connect.Request[v1.UpdateTaskRequest]) (*connect.Response[v1.UpdateTaskResponse], error) {
			count++
			// Verify the request carries the right task id and non-terminal result.
			if s := req.Msg.GetState(); s != nil {
				if s.GetId() != 42 {
					t.Errorf("heartbeat UpdateTask task id = %d, want 42", s.GetId())
				}
				if s.GetResult() != v1.Result_RESULT_UNSPECIFIED {
					t.Errorf("heartbeat result = %v, want RESULT_UNSPECIFIED", s.GetResult())
				}
			}
			callCount <- count
			return connect.NewResponse(&v1.UpdateTaskResponse{}), nil
		},
	}

	ts, c := newMockServer(t, h, 1)
	defer ts.Close()

	// Use a short heartbeat interval for the test.
	c.cfg.HeartbeatInterval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskCtx := &store.TaskCtx{ID: 42}

	go c.StartHeartbeat(ctx, taskCtx)

	// Wait for at least 2 heartbeat ticks (50ms * 2 + some margin).
	select {
	case n := <-callCount:
		if n < 1 {
			t.Error("expected at least 1 UpdateTask call")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for first heartbeat tick")
	}

	// Wait for a second tick.
	select {
	case n := <-callCount:
		if n < 2 {
			t.Error("expected at least 2 UpdateTask calls")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for second heartbeat tick")
	}
}

// ── TestStartHeartbeat_StopsOnCancel ──────────────────────────────────────────

func TestStartHeartbeat_StopsOnCancel(t *testing.T) {
	var callCount int

	h := &mockHandler{
		updateTaskFn: func(_ context.Context, _ *connect.Request[v1.UpdateTaskRequest]) (*connect.Response[v1.UpdateTaskResponse], error) {
			callCount++
			return connect.NewResponse(&v1.UpdateTaskResponse{}), nil
		},
	}

	ts, c := newMockServer(t, h, 1)
	defer ts.Close()

	c.cfg.HeartbeatInterval = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())

	taskCtx := &store.TaskCtx{ID: 99}

	done := make(chan struct{})
	go func() {
		c.StartHeartbeat(ctx, taskCtx)
		close(done)
	}()

	// Let at least one tick fire.
	time.Sleep(60 * time.Millisecond)

	// Cancel the context.
	cancel()

	// Wait for StartHeartbeat to exit.
	select {
	case <-done:
		// Good.
	case <-time.After(2 * time.Second):
		t.Fatal("StartHeartbeat did not exit after context cancellation")
	}

	// Record the count at cancellation time.
	callsBeforeCancel := callCount

	// Wait and verify no more calls arrive.
	time.Sleep(100 * time.Millisecond)
	if callCount != callsBeforeCancel {
		t.Errorf("heartbeat continued after cancellation: %d calls before, %d after",
			callsBeforeCancel, callCount)
	}
}
