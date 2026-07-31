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
	"git.0x0f.dev/forgery/internal/dispatch"
	"git.0x0f.dev/forgery/internal/north"
	"git.0x0f.dev/forgery/internal/slots"
	"git.0x0f.dev/forgery/internal/store"
)

// ── mocks ──

// mockDispatcher implements taskDispatcher for tests.
type mockDispatcher struct {
	mu        sync.Mutex
	triggerFn func(ctx context.Context, taskCtx *store.TaskCtx, inst config.Instance) error
	calls     int
	insts     []string // instance names seen by Trigger
}

func (m *mockDispatcher) Trigger(ctx context.Context, taskCtx *store.TaskCtx, inst config.Instance) error {
	m.mu.Lock()
	m.calls++
	m.insts = append(m.insts, inst.Name)
	m.mu.Unlock()
	if m.triggerFn != nil {
		return m.triggerFn(ctx, taskCtx, inst)
	}
	return nil
}

// mockSessionRemover implements sessionRemover for tests: it records every
// token passed to Remove.
type mockSessionRemover struct {
	mu      sync.Mutex
	removed []string
}

func (m *mockSessionRemover) Remove(sessionToken string) {
	m.mu.Lock()
	m.removed = append(m.removed, sessionToken)
	m.mu.Unlock()
}

func (m *mockSessionRemover) removedTokens() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.removed...)
}

// mockNorthClient implements north.Client for tests.
type mockNorthClient struct {
	mu                 sync.Mutex
	forwardUpdateCalls []forwardUpdateCall
	heartbeatCtx       context.Context // captured ctx from StartHeartbeat
	heartbeatDone      chan struct{}   // closed when heartbeat goroutine exits
	startHeartbeatFn   func(ctx context.Context, taskCtx *store.TaskCtx)
	heartbeatCalls     int
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

func (m *mockNorthClient) ForwardUpdateLog(_ context.Context, _ *v1.UpdateLogRequest) (*v1.UpdateLogResponse, error) {
	return &v1.UpdateLogResponse{}, nil
}

func (m *mockNorthClient) StartHeartbeat(ctx context.Context, _ *store.TaskCtx) {
	m.mu.Lock()
	m.heartbeatCalls++
	m.mu.Unlock()
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

// heartbeatCount returns how many times StartHeartbeat was invoked.
func (m *mockNorthClient) heartbeatCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heartbeatCalls
}

// forwardedResults returns a copy of the captured forward calls.
func (m *mockNorthClient) forwardedResults() []forwardUpdateCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]forwardUpdateCall, len(m.forwardUpdateCalls))
	copy(out, m.forwardUpdateCalls)
	return out
}

// instanceEntry is one resolver map entry: instance config + client.
type instanceEntry struct {
	inst   config.Instance
	client north.Client
}

// mockResolver implements north.Resolver for tests: a static name → entry map.
type mockResolver struct {
	entries map[string]instanceEntry
}

func newMockResolver(entries map[string]instanceEntry) *mockResolver {
	return &mockResolver{entries: entries}
}

func (r *mockResolver) Resolve(name string) (config.Instance, north.Client, bool) {
	e, ok := r.entries[name]
	if !ok {
		return config.Instance{}, nil, false
	}
	return e.inst, e.client, true
}

// entry builds one resolver map value.
func entry(name string, timeout time.Duration, client north.Client) instanceEntry {
	return instanceEntry{
		inst:   config.Instance{Name: name, GAStartupTimeout: timeout, HeartbeatInterval: 30 * time.Second},
		client: client,
	}
}

// ── helpers ──

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// newTestDispatcher builds a minimal *dispatch.Dispatcher to satisfy the
// Runner constructor signature (it is not exercised in these tests).
func newTestDispatcher() *dispatch.Dispatcher {
	return dispatch.NewDispatcher(dispatch.GitHub{}, testLogger())
}

func newTaskCtx(id int64, instance string) *store.TaskCtx {
	tc := &store.TaskCtx{
		ID:        id,
		Instance:  instance,
		CreatedAt: time.Now(),
	}
	tc.SetStatus(store.StatusPending)
	return tc
}

// newRunner builds a Runner wired to a real slots pool, a single-instance
// resolver, and fresh mocks. The pool is returned for post-conditions.
func newRunner(instances map[string]instanceEntry, disp *mockDispatcher, st store.TaskStore) (*Runner, *slots.Pool) {
	return newRunnerWithSessions(instances, disp, st, &mockSessionRemover{})
}

// newRunnerWithSessions is newRunner with an explicit session remover.
func newRunnerWithSessions(instances map[string]instanceEntry, disp *mockDispatcher, st store.TaskStore, sessions sessionRemover) (*Runner, *slots.Pool) {
	pool := slots.New(1)
	r := &Runner{
		pool:     pool,
		dispatch: disp,
		store:    st,
		resolver: newMockResolver(instances),
		sessions: sessions,
		log:      testLogger(),
	}
	return r, pool
}

// ── tests ──

// TestHandleTask_DispatchSuccess_DoneSignal verifies the happy path:
// dispatch succeeds → heartbeat starts → done signal → cleanup.
func TestHandleTask_DispatchSuccess_DoneSignal(t *testing.T) {
	st := store.NewMemStore()
	disp := &mockDispatcher{}
	nc := &mockNorthClient{}

	taskCtx := newTaskCtx(42, "inst-a")
	st.PutPending(taskCtx)

	instances := map[string]instanceEntry{
		"inst-a": entry("inst-a", 5*time.Second, nc),
	}
	r, pool := newRunner(instances, disp, st)

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

	// Verify dispatch was called with the correct instance.
	disp.mu.Lock()
	dispCalls := disp.calls
	dispInsts := append([]string(nil), disp.insts...)
	disp.mu.Unlock()
	if dispCalls != 1 {
		t.Fatalf("expected 1 dispatch call, got %d", dispCalls)
	}
	if len(dispInsts) != 1 || dispInsts[0] != "inst-a" {
		t.Fatalf("dispatch instance = %v, want [inst-a]", dispInsts)
	}

	// Verify task is dispatched.
	if taskCtx.Status() != store.StatusDispatched {
		t.Fatalf("expected StatusDispatched, got %v", taskCtx.Status())
	}

	// Verify the heartbeat was started on the resolved client.
	if nc.heartbeatCount() != 1 {
		t.Fatalf("expected 1 heartbeat start, got %d", nc.heartbeatCount())
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

	// Verify cleanup: the shared pool slot was released.
	if !slotFree(t, pool) {
		t.Fatal("slot not released after done signal")
	}

	// No failure forward should have been sent.
	for _, c := range nc.forwardedResults() {
		if c.Result == v1.Result_RESULT_FAILURE {
			t.Error("unexpected failure forward on success path")
		}
	}
}

// TestHandleTask_DispatchFailure verifies that when dispatch fails,
// HandleTask reports failure to Forgejo, drops any session, releases the
// slot, and returns.
func TestHandleTask_DispatchFailure(t *testing.T) {
	st := store.NewMemStore()
	disp := &mockDispatcher{
		triggerFn: func(_ context.Context, _ *store.TaskCtx, _ config.Instance) error {
			return errors.New("dispatch: network error")
		},
	}
	nc := &mockNorthClient{}
	sess := &mockSessionRemover{}

	taskCtx := newTaskCtx(99, "inst-a")
	taskCtx.SetSessionToken("sess-99")
	st.PutPending(taskCtx)

	instances := map[string]instanceEntry{
		"inst-a": entry("inst-a", 5*time.Second, nc),
	}
	r, pool := newRunnerWithSessions(instances, disp, st, sess)

	ctx := context.Background()
	r.HandleTask(ctx, taskCtx)

	// Verify failure was forwarded to the resolved client.
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

	// Verify the bound session was removed on the dispatch-failure path.
	if got := sess.removedTokens(); len(got) != 1 || got[0] != "sess-99" {
		t.Errorf("session removes = %v, want [sess-99]", got)
	}

	// Verify slot was released.
	if !slotFree(t, pool) {
		t.Fatal("slot not released after dispatch failure")
	}

	// Verify task was removed from store.
	if _, ok := st.GetByID(99); ok {
		t.Error("task should have been removed from store")
	}

	// Task should still be in pending (not dispatched).
	if taskCtx.Status() != store.StatusPending {
		t.Errorf("expected StatusPending, got %v", taskCtx.Status())
	}
}

// TestHandleTask_Timeout verifies that when GA_STARTUP_TIMEOUT expires,
// HandleTask reports failure and cleans up. The timeout comes from the
// resolved instance, not a global config.
func TestHandleTask_Timeout(t *testing.T) {
	st := store.NewMemStore()
	disp := &mockDispatcher{}
	nc := &mockNorthClient{
		heartbeatDone: make(chan struct{}),
	}

	taskCtx := newTaskCtx(7, "inst-a")
	st.PutPending(taskCtx)

	instances := map[string]instanceEntry{
		"inst-a": entry("inst-a", 100*time.Millisecond, nc),
	}
	r, pool := newRunner(instances, disp, st)

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
	if !slotFree(t, pool) {
		t.Fatal("slot not released after timeout")
	}

	// Verify task removed.
	if _, ok := st.GetByID(7); ok {
		t.Error("task should have been removed from store after timeout")
	}
}

// TestHandleTask_Timeout_DropsSession verifies that the GA_STARTUP_TIMEOUT
// branch removes the runner's session (the runner registered but never
// reported a terminal state), so the session does not leak.
func TestHandleTask_Timeout_DropsSession(t *testing.T) {
	st := store.NewMemStore()
	disp := &mockDispatcher{}
	nc := &mockNorthClient{heartbeatDone: make(chan struct{})}
	sess := &mockSessionRemover{}

	taskCtx := newTaskCtx(7, "inst-a")
	taskCtx.SetSessionToken("sess-7")
	st.PutPending(taskCtx)

	instances := map[string]instanceEntry{
		"inst-a": entry("inst-a", 50*time.Millisecond, nc),
	}
	r, _ := newRunnerWithSessions(instances, disp, st, sess)

	r.HandleTask(context.Background(), taskCtx)

	// The timeout branch must remove the session bound to the task.
	if got := sess.removedTokens(); len(got) != 1 || got[0] != "sess-7" {
		t.Errorf("session removes = %v, want [sess-7]", got)
	}
}

// TestHandleTask_NoSession_RemoveSafe verifies that the failure/timeout paths
// tolerate a task with no session (empty token): Remove must be called with
// the empty string and must not panic.
func TestHandleTask_NoSession_RemoveSafe(t *testing.T) {
	st := store.NewMemStore()
	disp := &mockDispatcher{
		triggerFn: func(_ context.Context, _ *store.TaskCtx, _ config.Instance) error {
			return errors.New("dispatch: boom")
		},
	}
	nc := &mockNorthClient{}
	sess := &mockSessionRemover{}

	taskCtx := newTaskCtx(5, "inst-a") // no session token set
	st.PutPending(taskCtx)

	instances := map[string]instanceEntry{
		"inst-a": entry("inst-a", 5*time.Second, nc),
	}
	r, _ := newRunnerWithSessions(instances, disp, st, sess)

	r.HandleTask(context.Background(), taskCtx)

	// Remove must have been invoked with the empty token (idempotent no-op).
	if got := sess.removedTokens(); len(got) != 1 || got[0] != "" {
		t.Errorf("session removes = %v, want [\"\"]", got)
	}
}

// TestHandleTask_ContextCancellation verifies graceful exit on context
// cancellation (e.g., global shutdown).
func TestHandleTask_ContextCancellation(t *testing.T) {
	st := store.NewMemStore()
	disp := &mockDispatcher{}
	nc := &mockNorthClient{
		heartbeatDone: make(chan struct{}),
	}

	taskCtx := newTaskCtx(3, "inst-a")
	st.PutPending(taskCtx)

	instances := map[string]instanceEntry{
		"inst-a": entry("inst-a", 10*time.Second, nc),
	} // long timeout, won't fire
	r, pool := newRunner(instances, disp, st)

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
	if !slotFree(t, pool) {
		t.Fatal("slot not released after context cancellation")
	}

	// Task should remain in store (not removed on shutdown).
	if _, ok := st.GetByID(3); !ok {
		t.Error("task should remain in store after context cancellation (for re-assignment)")
	}
}

// TestHandleTask_UnknownInstance verifies the defensive path: a task whose
// instance cannot be resolved is dropped (store removed, slot released)
// without touching the dispatcher or any client.
func TestHandleTask_UnknownInstance(t *testing.T) {
	st := store.NewMemStore()
	disp := &mockDispatcher{}
	nc := &mockNorthClient{}

	taskCtx := newTaskCtx(11, "ghost-instance")
	st.PutPending(taskCtx)

	instances := map[string]instanceEntry{
		"inst-a": entry("inst-a", 5*time.Second, nc),
	}
	r, pool := newRunner(instances, disp, st)

	r.HandleTask(context.Background(), taskCtx)

	// Nothing dispatched, nothing forwarded, nothing heartbeated.
	disp.mu.Lock()
	if disp.calls != 0 {
		t.Errorf("expected 0 dispatch calls, got %d", disp.calls)
	}
	disp.mu.Unlock()
	if len(nc.forwardedResults()) != 0 {
		t.Error("expected no forwards for unresolvable instance")
	}
	if nc.heartbeatCount() != 0 {
		t.Error("expected no heartbeat for unresolvable instance")
	}

	// Slot released and task dropped from the store.
	if !slotFree(t, pool) {
		t.Fatal("slot not released for unresolvable instance")
	}
	if _, ok := st.GetByID(11); ok {
		t.Error("task should have been removed from store")
	}
}

// TestHandleTask_UnknownInstance_DropsSession verifies the defensive branch
// also invokes the session remover (idempotent no-op for an empty token).
func TestHandleTask_UnknownInstance_DropsSession(t *testing.T) {
	st := store.NewMemStore()
	disp := &mockDispatcher{}
	nc := &mockNorthClient{}
	sess := &mockSessionRemover{}

	taskCtx := newTaskCtx(11, "ghost-instance")
	taskCtx.SetSessionToken("sess-11")
	st.PutPending(taskCtx)

	instances := map[string]instanceEntry{
		"inst-a": entry("inst-a", 5*time.Second, nc),
	}
	r, _ := newRunnerWithSessions(instances, disp, st, sess)

	r.HandleTask(context.Background(), taskCtx)

	if got := sess.removedTokens(); len(got) != 1 || got[0] != "sess-11" {
		t.Errorf("session removes = %v, want [sess-11]", got)
	}
}

// TestHandleTask_MultiInstanceRouting verifies that heartbeat and failure
// reporting land on the client of the task's own instance, and that the
// GA startup timeout is taken from that instance.
func TestHandleTask_MultiInstanceRouting(t *testing.T) {
	st := store.NewMemStore()
	disp := &mockDispatcher{}
	ncA := &mockNorthClient{heartbeatDone: make(chan struct{})}
	ncB := &mockNorthClient{heartbeatDone: make(chan struct{})}

	instances := map[string]instanceEntry{
		"inst-a": entry("inst-a", 10*time.Second, ncA), // long: must not fire
		"inst-b": entry("inst-b", 100*time.Millisecond, ncB),
	}
	r, pool := newRunner(instances, disp, st)

	// Task owned by inst-b: dispatch must see inst-b, heartbeat must run on
	// ncB, and the timeout (100ms) must come from inst-b's config.
	taskCtx := newTaskCtx(77, "inst-b")
	st.PutPending(taskCtx)

	start := time.Now()
	r.HandleTask(context.Background(), taskCtx)
	elapsed := time.Since(start)

	// The inst-b timeout fired: HandleTask returned in ~100ms and reported
	// failure through ncB, while ncA saw neither heartbeat nor forwards.
	if elapsed < 90*time.Millisecond {
		t.Errorf("HandleTask returned too quickly: %v (expected inst-b timeout ~100ms)", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Errorf("HandleTask took %v — looks like inst-a's 10s timeout was used", elapsed)
	}

	disp.mu.Lock()
	dispInsts := append([]string(nil), disp.insts...)
	disp.mu.Unlock()
	if len(dispInsts) != 1 || dispInsts[0] != "inst-b" {
		t.Errorf("dispatch instances = %v, want [inst-b]", dispInsts)
	}

	if ncA.heartbeatCount() != 0 {
		t.Error("heartbeat started on ncA, want only ncB")
	}
	if ncB.heartbeatCount() != 1 {
		t.Errorf("heartbeat started on ncB %d times, want 1", ncB.heartbeatCount())
	}
	if got := ncB.forwardedResults(); len(got) != 1 || got[0].TaskID != 77 || got[0].Result != v1.Result_RESULT_FAILURE {
		t.Errorf("ncB forwards = %+v, want one failure forward for task 77", got)
	}
	if got := ncA.forwardedResults(); len(got) != 0 {
		t.Errorf("ncA forwards = %+v, want none", got)
	}

	if !slotFree(t, pool) {
		t.Fatal("slot not released after inst-b timeout")
	}
	if _, ok := st.GetByID(77); ok {
		t.Error("task should have been removed from store after inst-b timeout")
	}
}

// slotFree reports whether the pool has a free slot (i.e. the previously
// held slot was released).
func slotFree(t *testing.T, pool *slots.Pool) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	ok := pool.Acquire(ctx) == nil
	if ok {
		pool.Release()
	}
	return ok
}

// TestNew_ReturnsRunner verifies the constructor sets all fields.
func TestNew_ReturnsRunner(t *testing.T) {
	st := store.NewMemStore()
	nc := &mockNorthClient{}
	instances := map[string]instanceEntry{
		"inst-a": entry("inst-a", 30*time.Second, nc),
	}
	pool := slots.New(1)
	resolver := newMockResolver(instances)

	// The constructor takes the concrete *dispatch.Dispatcher; build a real
	// one to satisfy the signature (it is not exercised here).
	dp := newTestDispatcher()
	r := New(pool, dp, st, resolver, &mockSessionRemover{}, testLogger())
	if r == nil {
		t.Fatal("expected non-nil Runner")
	}
	if r.pool != pool {
		t.Error("pool not set")
	}
	if r.store != st {
		t.Error("store not set")
	}
	if r.resolver != resolver {
		t.Error("resolver not set")
	}
	if r.sessions == nil {
		t.Error("sessions not set")
	}
}
