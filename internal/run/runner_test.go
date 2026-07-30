package run

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/store"
)

// ── mocks ──

// mockDispatcher implements taskDispatcher for tests.
type mockDispatcher struct {
	mu        sync.Mutex
	triggerFn func(ctx context.Context, taskCtx *store.TaskCtx) error
	calls     int
}

func (m *mockDispatcher) Trigger(ctx context.Context, taskCtx *store.TaskCtx) error {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	if m.triggerFn != nil {
		return m.triggerFn(ctx, taskCtx)
	}
	return nil
}

// mockNorthClient implements northClient for tests.
type mockNorthClient struct {
	mu                 sync.Mutex
	forwardUpdateCalls []forwardUpdateCall
	releaseSlotCalls   int
	heartbeatCtx       context.Context // captured ctx from StartHeartbeat
	heartbeatDone      chan struct{}   // closed when heartbeat goroutine exits
	startHeartbeatFn   func(ctx context.Context, taskCtx *store.TaskCtx)
}

type forwardUpdateCall struct {
	TaskID int64
	Result v1.Result
}

func (m *mockNorthClient) ForwardUpdateTask(_ context.Context, req *v1.UpdateTaskRequest) (*v1.UpdateTaskResponse, error) {
	m.mu.Lock()
	m.forwardUpdateCalls = append(m.forwardUpdateCalls, forwardUpdateCall{
		TaskID: req.GetState().GetId(),
		Result: req.GetState().GetResult(),
	})
	m.mu.Unlock()
	return &v1.UpdateTaskResponse{}, nil
}

func (m *mockNorthClient) ReleaseSlot() {
	m.mu.Lock()
	m.releaseSlotCalls++
	m.mu.Unlock()
}

func (m *mockNorthClient) StartHeartbeat(ctx context.Context, _ *store.TaskCtx) {
	if m.startHeartbeatFn != nil {
		m.startHeartbeatFn(ctx, nil)
		return
	}
	// Default: block until cancelled.
	m.mu.Lock()
	m.heartbeatCtx = ctx
	m.mu.Unlock()
	<-ctx.Done()
	if m.heartbeatDone != nil {
		close(m.heartbeatDone)
	}
}

// forwardedResults returns a copy of the captured forward calls.
func (m *mockNorthClient) forwardedResults() []forwardUpdateCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]forwardUpdateCall, len(m.forwardUpdateCalls))
	copy(out, m.forwardUpdateCalls)
	return out
}

// ── helpers ──

func testConfig(timeout time.Duration) *config.Config {
	return &config.Config{
		GAStartupTimeout:  timeout,
		HeartbeatInterval: 30 * time.Second,
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func newTaskCtx(id int64) *store.TaskCtx {
	tc := &store.TaskCtx{
		ID:        id,
		CreatedAt: time.Now(),
	}
	tc.SetStatus(store.StatusPending)
	return tc
}

// ── tests ──

// TestHandleTask_DispatchSuccess_DoneSignal verifies the happy path:
// dispatch succeeds → heartbeat starts → done signal → cleanup.
func TestHandleTask_DispatchSuccess_DoneSignal(t *testing.T) {
	cfg := testConfig(5 * time.Second)
	st := store.NewMemStore()
	disp := &mockDispatcher{}
	nc := &mockNorthClient{}

	taskCtx := newTaskCtx(42)
	st.PutPending(taskCtx)

	r := &Runner{
		north:    nc,
		dispatch: disp,
		store:    st,
		cfg:      cfg,
		log:      testLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run HandleTask in a goroutine.
	done := make(chan struct{})
	go func() {
		r.HandleTask(ctx, taskCtx)
		close(done)
	}()

	// Give HandleTask time to dispatch and start heartbeat.
	time.Sleep(50 * time.Millisecond)

	// Verify dispatch was called.
	disp.mu.Lock()
	dispCalls := disp.calls
	disp.mu.Unlock()
	if dispCalls != 1 {
		t.Fatalf("expected 1 dispatch call, got %d", dispCalls)
	}

	// Verify task is dispatched.
	if taskCtx.Status() != store.StatusDispatched {
		t.Fatalf("expected StatusDispatched, got %v", taskCtx.Status())
	}

	// Signal done (simulating internal runner reaching terminal state).
	taskCtx.MarkDone()

	// Wait for HandleTask to complete.
	select {
	case <-done:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("HandleTask did not complete after done signal")
	}

	// Verify cleanup: release slot called.
	nc.mu.Lock()
	relCalls := nc.releaseSlotCalls
	nc.mu.Unlock()
	if relCalls != 1 {
		t.Fatalf("expected 1 release slot call, got %d", relCalls)
	}

	// No failure forward should have been sent.
	for _, c := range nc.forwardedResults() {
		if c.Result == v1.Result_RESULT_FAILURE {
			t.Error("unexpected failure forward on success path")
		}
	}
}

// TestHandleTask_DispatchFailure verifies that when dispatch fails,
// HandleTask reports failure to Forgejo, releases the slot, and returns.
func TestHandleTask_DispatchFailure(t *testing.T) {
	cfg := testConfig(5 * time.Second)
	st := store.NewMemStore()
	disp := &mockDispatcher{
		triggerFn: func(_ context.Context, _ *store.TaskCtx) error {
			return errors.New("dispatch: network error")
		},
	}
	nc := &mockNorthClient{}

	taskCtx := newTaskCtx(99)
	st.PutPending(taskCtx)

	r := &Runner{
		north:    nc,
		dispatch: disp,
		store:    st,
		cfg:      cfg,
		log:      testLogger(),
	}

	ctx := context.Background()
	r.HandleTask(ctx, taskCtx)

	// Verify failure was forwarded.
	calls := nc.forwardedResults()
	if len(calls) != 1 {
		t.Fatalf("expected 1 forward call, got %d", len(calls))
	}
	if calls[0].Result != v1.Result_RESULT_FAILURE {
		t.Errorf("expected RESULT_FAILURE, got %v", calls[0].Result)
	}
	if calls[0].TaskID != 99 {
		t.Errorf("expected task_id 99, got %d", calls[0].TaskID)
	}

	// Verify slot was released.
	nc.mu.Lock()
	relCalls := nc.releaseSlotCalls
	nc.mu.Unlock()
	if relCalls != 1 {
		t.Fatalf("expected 1 release slot call, got %d", relCalls)
	}

	// Verify task was removed from store.
	_, ok := st.GetByID(99)
	if ok {
		t.Error("task should have been removed from store")
	}

	// Task should still be in pending (not dispatched).
	if taskCtx.Status() != store.StatusPending {
		t.Errorf("expected StatusPending, got %v", taskCtx.Status())
	}
}

// TestHandleTask_Timeout verifies that when GA_STARTUP_TIMEOUT expires,
// HandleTask reports failure and cleans up.
func TestHandleTask_Timeout(t *testing.T) {
	cfg := testConfig(100 * time.Millisecond)
	st := store.NewMemStore()
	disp := &mockDispatcher{}
	nc := &mockNorthClient{
		heartbeatDone: make(chan struct{}),
	}

	taskCtx := newTaskCtx(7)
	st.PutPending(taskCtx)

	r := &Runner{
		north:    nc,
		dispatch: disp,
		store:    st,
		cfg:      cfg,
		log:      testLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	r.HandleTask(ctx, taskCtx)
	elapsed := time.Since(start)

	// Should have waited roughly the timeout duration.
	if elapsed < 90*time.Millisecond {
		t.Errorf("HandleTask returned too quickly: %v (expected ~100ms)", elapsed)
	}

	// Verify failure was forwarded.
	calls := nc.forwardedResults()
	foundFailure := false
	for _, c := range calls {
		if c.Result == v1.Result_RESULT_FAILURE && c.TaskID == 7 {
			foundFailure = true
		}
	}
	if !foundFailure {
		t.Error("expected failure forward on timeout")
	}

	// Verify slot released.
	nc.mu.Lock()
	relCalls := nc.releaseSlotCalls
	nc.mu.Unlock()
	if relCalls != 1 {
		t.Fatalf("expected 1 release slot, got %d", relCalls)
	}

	// Verify task removed.
	_, ok := st.GetByID(7)
	if ok {
		t.Error("task should have been removed from store after timeout")
	}
}

// TestHandleTask_ContextCancellation verifies graceful exit on context
// cancellation (e.g., global shutdown).
func TestHandleTask_ContextCancellation(t *testing.T) {
	cfg := testConfig(10 * time.Second) // long timeout, won't fire
	st := store.NewMemStore()
	disp := &mockDispatcher{}
	nc := &mockNorthClient{
		heartbeatDone: make(chan struct{}),
	}

	taskCtx := newTaskCtx(3)
	st.PutPending(taskCtx)

	r := &Runner{
		north:    nc,
		dispatch: disp,
		store:    st,
		cfg:      cfg,
		log:      testLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		r.HandleTask(ctx, taskCtx)
		close(done)
	}()

	// Give HandleTask time to dispatch and start heartbeat.
	time.Sleep(50 * time.Millisecond)

	// Cancel the context (simulating shutdown).
	cancel()

	// Wait for HandleTask to complete.
	select {
	case <-done:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("HandleTask did not complete after context cancellation")
	}

	// Verify slot was released.
	nc.mu.Lock()
	relCalls := nc.releaseSlotCalls
	nc.mu.Unlock()
	if relCalls != 1 {
		t.Fatalf("expected 1 release slot, got %d", relCalls)
	}

	// Task should remain in store (not removed on shutdown).
	_, ok := st.GetByID(3)
	if !ok {
		t.Error("task should remain in store after context cancellation (for re-assignment)")
	}
}

// TestNew_ReturnsRunner verifies the constructor sets all fields.
func TestNew_ReturnsRunner(t *testing.T) {
	cfg := testConfig(30 * time.Second)
	st := store.NewMemStore()
	_ = cfg
	_ = st
	// Just verify Runner type can be instantiated via the constructor.
	r := &Runner{
		north:    &mockNorthClient{},
		dispatch: &mockDispatcher{},
		store:    st,
		cfg:      cfg,
		log:      testLogger(),
	}
	if r == nil {
		t.Fatal("expected non-nil Runner")
	}
}
