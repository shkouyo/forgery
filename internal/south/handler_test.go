package south

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"
	"connectrpc.com/connect"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/north"
	"git.0x0f.dev/forgery/internal/session"
	"git.0x0f.dev/forgery/internal/store"
)

// ── mockStore ──────────────────────────────────────────────────────────────

// mockStore implements store.TaskStore for testing. It tracks calls and
// allows fine-grained control over behavior.
type mockStore struct {
	mu              sync.RWMutex
	tasks           map[string]*store.TaskCtx // keyed by regToken
	byID            map[int64]*store.TaskCtx  // keyed by task ID
	consumedTokens  map[string]bool           // tracks which tokens were consumed
	removed         map[int64]bool            // tracks which task IDs were removed
	markErr         error                     // error to return from MarkRegTokenConsumed
	getByRegTokenOk bool                      // override: force false from GetByRegToken
}

func newMockStore() *mockStore {
	return &mockStore{
		tasks:           make(map[string]*store.TaskCtx),
		byID:            make(map[int64]*store.TaskCtx),
		consumedTokens:  make(map[string]bool),
		removed:         make(map[int64]bool),
		getByRegTokenOk: true,
	}
}

func (m *mockStore) PutPending(taskCtx *store.TaskCtx) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[taskCtx.RegToken] = taskCtx
	m.byID[taskCtx.ID] = taskCtx
}

func (m *mockStore) GetByRegToken(regToken string) (*store.TaskCtx, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.getByRegTokenOk {
		return nil, false
	}
	tc, ok := m.tasks[regToken]
	return tc, ok
}

func (m *mockStore) MarkRegTokenConsumed(regToken string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.markErr != nil {
		return m.markErr
	}
	if m.consumedTokens[regToken] {
		return store.ErrTokenNotFound
	}
	if _, ok := m.tasks[regToken]; !ok {
		return store.ErrTokenNotFound
	}
	m.consumedTokens[regToken] = true
	delete(m.tasks, regToken)
	return nil
}

func (m *mockStore) GetByID(taskID int64) (*store.TaskCtx, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tc, ok := m.byID[taskID]
	return tc, ok
}

func (m *mockStore) Remove(taskID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removed[taskID] = true
	delete(m.byID, taskID)
}

func (m *mockStore) GC(now time.Time) {}

func (m *mockStore) CountActive() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byID)
}

func (m *mockStore) wasRemoved(taskID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.removed[taskID]
}

// ── mockClient ─────────────────────────────────────────────────────────────

// mockClient implements north.Client for testing. It tracks forwarding calls
// per client so routing assertions can verify the correct instance's client
// received the relay.
type mockClient struct {
	mu               sync.Mutex
	updateTaskCalled int
	updateLogCalled  int
	lastUpdateTask   *v1.UpdateTaskRequest
	lastUpdateLog    *v1.UpdateLogRequest
	updateTaskResp   *v1.UpdateTaskResponse
	updateLogResp    *v1.UpdateLogResponse
	updateTaskErr    error
	updateLogErr     error
}

func newMockClient() *mockClient {
	return &mockClient{
		updateTaskResp: &v1.UpdateTaskResponse{},
		updateLogResp:  &v1.UpdateLogResponse{},
	}
}

func (m *mockClient) ForwardUpdateTask(ctx context.Context, req *v1.UpdateTaskRequest) (*v1.UpdateTaskResponse, error) {
	m.mu.Lock()
	m.updateTaskCalled++
	m.lastUpdateTask = req
	m.mu.Unlock()
	if m.updateTaskErr != nil {
		return nil, m.updateTaskErr
	}
	return m.updateTaskResp, nil
}

func (m *mockClient) ForwardUpdateLog(ctx context.Context, req *v1.UpdateLogRequest) (*v1.UpdateLogResponse, error) {
	m.mu.Lock()
	m.updateLogCalled++
	m.lastUpdateLog = req
	m.mu.Unlock()
	if m.updateLogErr != nil {
		return nil, m.updateLogErr
	}
	return m.updateLogResp, nil
}

func (m *mockClient) StartHeartbeat(ctx context.Context, taskCtx *store.TaskCtx) {
	// Not exercised by the south handler.
	<-ctx.Done()
}

// ── fakeResolver ───────────────────────────────────────────────────────────

// fakeResolver implements north.Resolver with a static map, supporting two
// instances with distinct TTLs for routing tests.
type fakeResolver struct {
	entries map[string]instanceEntry
}

func newFakeResolver(entries map[string]instanceEntry) *fakeResolver {
	return &fakeResolver{entries: entries}
}

func (r *fakeResolver) Resolve(name string) (config.Instance, north.Client, bool) {
	e, ok := r.entries[name]
	if !ok {
		return config.Instance{}, nil, false
	}
	return e.inst, e.client, true
}

// instanceEntry is one resolver map value.
type instanceEntry struct {
	inst   config.Instance
	client north.Client
}

// mkEntry builds a resolver map value.
func mkEntry(name string, client north.Client) instanceEntry {
	return instanceEntry{
		inst:   config.Instance{Name: name},
		client: client,
	}
}

// ── helpers ────────────────────────────────────────────────────────────────

// testResolver returns a resolver with two instances, each with its own
// recording client. Registration-token TTLs are not part of the resolver
// anymore — they travel on the task (see newTaskCtx).
func testResolver() (*fakeResolver, *mockClient, *mockClient) {
	clientA := newMockClient()
	clientB := newMockClient()
	resolver := newFakeResolver(map[string]instanceEntry{
		"inst-a": mkEntry("inst-a", clientA),
		"inst-b": mkEntry("inst-b", clientB),
	})
	return resolver, clientA, clientB
}

func newTestHandler(ms *mockStore, sm *session.Manager, resolver north.Resolver) *Handler {
	return NewHandler(ms, sm, resolver,
		slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
}

// newTaskCtx creates a fresh TaskCtx with a real Forgejo Task payload, a
// registration token, an ID, and an owning instance. RegTokenTTL defaults to
// 15 minutes (the config default); tests probing token expiry override it.
// The task is put into the mock store.
func newTaskCtx(ms *mockStore, id int64, regToken string, instance string) *store.TaskCtx {
	taskCtx := &store.TaskCtx{
		ID:          id,
		Instance:    instance,
		Task:        &v1.Task{Id: id},
		RegToken:    regToken,
		RegTokenTTL: 15 * time.Minute,
		CreatedAt:   time.Now(),
	}
	taskCtx.SetStatus(store.StatusPending)
	ms.PutPending(taskCtx)
	return taskCtx
}

// registerTask performs the Register RPC with the given registration token
// and returns the session token.
func registerTask(t *testing.T, h *Handler, regToken string, taskID int64) string {
	t.Helper()
	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner"})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	return regResp.Msg.GetRunner().GetToken()
}

// setBearer sets the Authorization header on a connect request.
func setBearer[T any](req *connect.Request[T], token string) {
	req.Header().Set("Authorization", "Bearer "+token)
}

// setRunnerToken sets the x-runner-token header on a connect request.
func setRunnerToken[T any](req *connect.Request[T], token string) {
	req.Header().Set("x-runner-token", token)
}

// setRunnerUUID sets the x-runner-uuid header on a connect request.
func setRunnerUUID[T any](req *connect.Request[T], token string) {
	req.Header().Set("x-runner-uuid", token)
}

// discardLog is a logger that discards all output, used in tests for
// sessionTokenFromRequest which requires a logger parameter.
var discardLog = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.Level(999)}))

// ── Tests: sessionTokenFromRequest ──────────────────────────────────────────

func TestSessionTokenFromRequest_Bearer(t *testing.T) {
	req := connect.NewRequest(&v1.DeclareRequest{})
	setBearer(req, "my-secret-token")

	tok, ok := sessionTokenFromRequest(req, discardLog)
	if !ok {
		t.Fatal("expected ok=true for valid bearer")
	}
	if tok != "my-secret-token" {
		t.Fatalf("expected token 'my-secret-token', got %q", tok)
	}
}

func TestSessionTokenFromRequest_RunnerToken(t *testing.T) {
	req := connect.NewRequest(&v1.DeclareRequest{})
	setRunnerToken(req, "runner-session-token")

	tok, ok := sessionTokenFromRequest(req, discardLog)
	if !ok {
		t.Fatal("expected ok=true for x-runner-token header")
	}
	if tok != "runner-session-token" {
		t.Fatalf("expected token 'runner-session-token', got %q", tok)
	}
}

func TestSessionTokenFromRequest_RunnerTokenPreferred(t *testing.T) {
	// x-runner-token should be preferred over Authorization: Bearer
	req := connect.NewRequest(&v1.DeclareRequest{})
	setRunnerToken(req, "runner-token")
	setBearer(req, "bearer-token")

	tok, ok := sessionTokenFromRequest(req, discardLog)
	if !ok {
		t.Fatal("expected ok=true when both headers are present")
	}
	if tok != "runner-token" {
		t.Fatalf("expected x-runner-token to take precedence, got %q", tok)
	}
}

func TestSessionTokenFromRequest_NoHeader(t *testing.T) {
	req := connect.NewRequest(&v1.DeclareRequest{})

	_, ok := sessionTokenFromRequest(req, discardLog)
	if ok {
		t.Fatal("expected ok=false when no headers")
	}
}

func TestSessionTokenFromRequest_NotBearer(t *testing.T) {
	req := connect.NewRequest(&v1.DeclareRequest{})
	req.Header().Set("Authorization", "Basic YWxhZGRpbjpvcGVuc2VzYW1l")

	_, ok := sessionTokenFromRequest(req, discardLog)
	if ok {
		t.Fatal("expected ok=false for non-Bearer scheme")
	}
}

func TestSessionTokenFromRequest_EmptyBearer(t *testing.T) {
	req := connect.NewRequest(&v1.DeclareRequest{})
	req.Header().Set("Authorization", "Bearer ")

	tok, ok := sessionTokenFromRequest(req, discardLog)
	if !ok {
		t.Fatal("expected ok=true for Bearer prefix")
	}
	if tok != "" {
		t.Fatalf("expected empty token, got %q", tok)
	}
}

func TestSessionTokenFromRequest_EmptyRunnerToken(t *testing.T) {
	// Empty x-runner-token should fall through to Authorization header
	req := connect.NewRequest(&v1.DeclareRequest{})
	req.Header().Set("x-runner-token", "")
	setBearer(req, "fallback-token")

	tok, ok := sessionTokenFromRequest(req, discardLog)
	if !ok {
		t.Fatal("expected ok=true when x-runner-token is empty but bearer is present")
	}
	if tok != "fallback-token" {
		t.Fatalf("expected fallback bearer token, got %q", tok)
	}
}

func TestSessionTokenFromRequest_RunnerUUID(t *testing.T) {
	req := connect.NewRequest(&v1.DeclareRequest{})
	setRunnerUUID(req, "runner-uuid-token")

	tok, ok := sessionTokenFromRequest(req, discardLog)
	if !ok {
		t.Fatal("expected ok=true for x-runner-uuid header")
	}
	if tok != "runner-uuid-token" {
		t.Fatalf("expected token 'runner-uuid-token', got %q", tok)
	}
}

func TestSessionTokenFromRequest_RunnerTokenPreferredOverUUID(t *testing.T) {
	// x-runner-token should be preferred over x-runner-uuid
	req := connect.NewRequest(&v1.DeclareRequest{})
	setRunnerToken(req, "runner-token")
	setRunnerUUID(req, "runner-uuid-token")

	tok, ok := sessionTokenFromRequest(req, discardLog)
	if !ok {
		t.Fatal("expected ok=true when both headers are present")
	}
	if tok != "runner-token" {
		t.Fatalf("expected x-runner-token to take precedence over x-runner-uuid, got %q", tok)
	}
}

// ── Tests: Register ────────────────────────────────────────────────────────

func TestRegister_ValidToken(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "valid-reg-token"
	taskCtx := newTaskCtx(ms, 42, regToken, "inst-a")
	taskCtx.CreatedAt = time.Now() // fresh token

	req := connect.NewRequest(&v1.RegisterRequest{
		Token:  regToken,
		Name:   "test-runner",
		Labels: []string{"linux"},
	})

	resp, err := h.Register(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	runner := resp.Msg.GetRunner()
	if runner == nil {
		t.Fatal("expected runner in response")
	}
	if runner.GetToken() == "" {
		t.Fatal("expected non-empty session token")
	}
	if runner.GetId() != taskCtx.ID {
		t.Fatalf("expected runner ID %d (the task ID), got %d", taskCtx.ID, runner.GetId())
	}
	if !runner.GetEphemeral() {
		t.Fatal("expected ephemeral=true")
	}

	// Verify status was updated.
	if taskCtx.Status() != store.StatusRunning {
		t.Fatalf("expected StatusRunning, got %v", taskCtx.Status())
	}

	// Verify session was created and can be looked up.
	sess, ok := sm.Lookup(runner.GetToken())
	if !ok {
		t.Fatal("session not found after Register")
	}
	if sess.RunnerName != "test-runner" {
		t.Fatalf("expected runner name 'test-runner', got %q", sess.RunnerName)
	}
}

// TestRegister_RunnerIDIsTaskID pins the compatibility hack: forgejo-runner
// (one-job) uses the Runner.Id from the Register response as the task ID it
// reports back in UpdateTask's State.Id, so the response must always carry
// the task's own ID.
func TestRegister_RunnerIDIsTaskID(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "hack-pin-token"
	taskCtx := newTaskCtx(ms, 4242, regToken, "inst-a")
	taskCtx.CreatedAt = time.Now() // fresh token

	req := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner"})
	resp, err := h.Register(context.Background(), req)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if got := resp.Msg.GetRunner().GetId(); got != taskCtx.ID {
		t.Fatalf("Register response Runner.Id = %d, want the task ID %d", got, taskCtx.ID)
	}
}

func TestRegister_InvalidToken(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	req := connect.NewRequest(&v1.RegisterRequest{
		Token: "nonexistent",
	})

	_, err := h.Register(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for invalid token")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestRegister_ExpiredToken(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "expired-token"
	taskCtx := newTaskCtx(ms, 42, regToken, "inst-a")
	taskCtx.CreatedAt = time.Now().Add(-20 * time.Minute) // 20 minutes ago

	req := connect.NewRequest(&v1.RegisterRequest{
		Token: regToken,
	})

	_, err := h.Register(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for expired token")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

// TestRegister_TTLByTask verifies the per-task TTL contract: the same token
// age can be expired on one task and still valid on another, depending on
// each task's RegTokenTTL (stamped from the owning instance's reg_token_ttl
// at creation). The TTL travels on the task, not on the instance resolver.
func TestRegister_TTLByTask(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	age := 2 * time.Minute

	// Task with a long TTL (15m): still valid at 2 minutes old.
	taskA := newTaskCtx(ms, 1, "token-a", "inst-a")
	taskA.CreatedAt = time.Now().Add(-age)
	taskA.RegTokenTTL = 15 * time.Minute
	reqA := connect.NewRequest(&v1.RegisterRequest{Token: "token-a", Name: "runner-a"})
	if _, err := h.Register(context.Background(), reqA); err != nil {
		t.Errorf("register with %s-old token under 15m task TTL failed: %v (want success)", age, err)
	}

	// Task with a short TTL (1m): expired at the same age.
	taskB := newTaskCtx(ms, 2, "token-b", "inst-b")
	taskB.CreatedAt = time.Now().Add(-age)
	taskB.RegTokenTTL = 1 * time.Minute
	reqB := connect.NewRequest(&v1.RegisterRequest{Token: "token-b", Name: "runner-b"})
	_, err := h.Register(context.Background(), reqB)
	if err == nil {
		t.Error("register with token older than the task TTL succeeded, want Unauthenticated")
	} else if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("register error = %v, want Unauthenticated", connect.CodeOf(err))
	}
}

func TestRegister_AlreadyConsumed(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "single-use-token"
	newTaskCtx(ms, 42, regToken, "inst-a")

	// First registration should succeed.
	req1 := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "first"})
	_, err := h.Register(context.Background(), req1)
	if err != nil {
		t.Fatalf("first Register should succeed: %v", err)
	}

	// Second registration with same token should fail.
	req2 := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "second"})
	_, err = h.Register(context.Background(), req2)
	if err == nil {
		t.Fatal("expected error for already-consumed token")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

// ── Tests: Declare ─────────────────────────────────────────────────────────

func TestDeclare_ValidSession(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "declare-test-token"
	newTaskCtx(ms, 42, regToken, "inst-a")

	sessionToken := registerTask(t, h, regToken, 42)

	// Now Declare.
	declReq := connect.NewRequest(&v1.DeclareRequest{Version: "1.0.0", Labels: []string{"linux"}})
	setBearer(declReq, sessionToken)

	resp, err := h.Declare(context.Background(), declReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Msg.GetRunner() == nil {
		t.Fatal("expected runner in response")
	}
}

func TestDeclare_InvalidSession(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	req := connect.NewRequest(&v1.DeclareRequest{})
	setBearer(req, "nonexistent-session")

	_, err := h.Declare(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for invalid session")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestDeclare_ValidSessionRunnerToken(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "declare-runner-token-test"
	newTaskCtx(ms, 42, regToken, "inst-a")

	sessionToken := registerTask(t, h, regToken, 42)

	// Declare using x-runner-token header (forgejo-runner v12+ style).
	declReq := connect.NewRequest(&v1.DeclareRequest{Version: "1.0.0", Labels: []string{"linux"}})
	setRunnerToken(declReq, sessionToken)

	resp, err := h.Declare(context.Background(), declReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Msg.GetRunner() == nil {
		t.Fatal("expected runner in response")
	}
}

func TestDeclare_MissingToken(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	req := connect.NewRequest(&v1.DeclareRequest{})

	_, err := h.Declare(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for missing session token")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestDeclare_ValidSessionRunnerUUID(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "declare-uuid-test"
	newTaskCtx(ms, 42, regToken, "inst-a")

	sessionToken := registerTask(t, h, regToken, 42)

	// Declare using x-runner-uuid header (forgejo-runner v13+ style).
	declReq := connect.NewRequest(&v1.DeclareRequest{Version: "1.0.0", Labels: []string{"linux"}})
	setRunnerUUID(declReq, sessionToken)

	resp, err := h.Declare(context.Background(), declReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Msg.GetRunner() == nil {
		t.Fatal("expected runner in response")
	}
}

// ── Tests: FetchTask ───────────────────────────────────────────────────────

func TestFetchTask_ValidSession(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "fetch-test-token"
	taskCtx := newTaskCtx(ms, 42, regToken, "inst-a")
	taskCtx.Task = &v1.Task{Id: 42} // the real Forgejo task

	sessionToken := registerTask(t, h, regToken, 42)

	// FetchTask.
	fetchReq := connect.NewRequest(&v1.FetchTaskRequest{})
	setBearer(fetchReq, sessionToken)

	resp, err := h.FetchTask(context.Background(), fetchReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Msg.GetTask() == nil {
		t.Fatal("expected task in response")
	}
	if resp.Msg.GetTask().GetId() != 42 {
		t.Fatalf("expected task ID 42, got %d", resp.Msg.GetTask().GetId())
	}
}

func TestFetchTask_InvalidSession(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	req := connect.NewRequest(&v1.FetchTaskRequest{})
	setBearer(req, "nonexistent")

	_, err := h.FetchTask(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for invalid session")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestFetchTask_MissingToken(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	req := connect.NewRequest(&v1.FetchTaskRequest{})

	_, err := h.FetchTask(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for missing session token")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

// ── Tests: UpdateTask ──────────────────────────────────────────────────────

func TestUpdateTask_Valid(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, clientA, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "update-test-token"
	newTaskCtx(ms, 42, regToken, "inst-a")

	sessionToken := registerTask(t, h, regToken, 42)

	// UpdateTask with running state.
	updateReq := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{
			Id:     42,
			Result: v1.Result_RESULT_UNSPECIFIED, // non-terminal
		},
	})
	setBearer(updateReq, sessionToken)

	resp, err := h.UpdateTask(context.Background(), updateReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Msg == nil {
		t.Fatal("expected response")
	}
	clientA.mu.Lock()
	calledA := clientA.updateTaskCalled
	clientA.mu.Unlock()
	if calledA != 1 {
		t.Fatalf("expected inst-a client to be called once, got %d", calledA)
	}

	// Session should still exist (non-terminal state).
	if _, ok := sm.Lookup(sessionToken); !ok {
		t.Fatal("session should exist after non-terminal UpdateTask")
	}
}

func TestUpdateTask_TaskIDMismatch(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, clientA, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "mismatch-token"
	newTaskCtx(ms, 42, regToken, "inst-a")

	sessionToken := registerTask(t, h, regToken, 42)

	// UpdateTask with wrong task ID.
	updateReq := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{
			Id: 99, // wrong ID
		},
	})
	setBearer(updateReq, sessionToken)

	_, err := h.UpdateTask(context.Background(), updateReq)
	if err == nil {
		t.Fatal("expected error for task ID mismatch")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
	clientA.mu.Lock()
	calledA := clientA.updateTaskCalled
	clientA.mu.Unlock()
	if calledA != 0 {
		t.Fatal("forwarder should not be called on task ID mismatch")
	}
}

func TestUpdateTask_TerminalCleansUp(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, clientA, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "terminal-token"
	taskCtx := newTaskCtx(ms, 42, regToken, "inst-a")

	sessionToken := registerTask(t, h, regToken, 42)

	// UpdateTask with terminal state (success).
	updateReq := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{
			Id:     42,
			Result: v1.Result_RESULT_SUCCESS,
		},
	})
	setBearer(updateReq, sessionToken)

	resp, err := h.UpdateTask(context.Background(), updateReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Msg == nil {
		t.Fatal("expected response")
	}

	// Verify cleanup: session removed.
	if _, ok := sm.Lookup(sessionToken); ok {
		t.Fatal("session should be removed after terminal UpdateTask")
	}

	// Verify cleanup: store removed.
	if !ms.wasRemoved(42) {
		t.Fatal("task should be removed from store")
	}

	// Verify status set to terminal.
	if taskCtx.Status() != store.StatusTerminal {
		t.Fatalf("expected StatusTerminal, got %v", taskCtx.Status())
	}

	// Forwarder should have been called.
	clientA.mu.Lock()
	calledA := clientA.updateTaskCalled
	clientA.mu.Unlock()
	if calledA != 1 {
		t.Fatalf("expected forwarder to be called, got %d", calledA)
	}
}

func TestUpdateTask_TerminalFailure(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "failure-token"
	taskCtx := newTaskCtx(ms, 42, regToken, "inst-a")

	sessionToken := registerTask(t, h, regToken, 42)

	updateReq := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{
			Id:     42,
			Result: v1.Result_RESULT_FAILURE,
		},
	})
	setBearer(updateReq, sessionToken)

	_, err := h.UpdateTask(context.Background(), updateReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := sm.Lookup(sessionToken); ok {
		t.Fatal("session should be removed for failure result")
	}
	if !ms.wasRemoved(42) {
		t.Fatal("task should be removed from store")
	}
	if taskCtx.Status() != store.StatusTerminal {
		t.Fatalf("expected StatusTerminal, got %v", taskCtx.Status())
	}
}

func TestUpdateTask_TerminalCancelled(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "cancel-token"
	taskCtx := newTaskCtx(ms, 42, regToken, "inst-a")

	sessionToken := registerTask(t, h, regToken, 42)

	updateReq := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{
			Id:     42,
			Result: v1.Result_RESULT_CANCELLED,
		},
	})
	setBearer(updateReq, sessionToken)

	_, err := h.UpdateTask(context.Background(), updateReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := sm.Lookup(sessionToken); ok {
		t.Fatal("session should be removed for cancelled result")
	}
	if !ms.wasRemoved(42) {
		t.Fatal("task should be removed from store")
	}
	if taskCtx.Status() != store.StatusTerminal {
		t.Fatalf("expected StatusTerminal, got %v", taskCtx.Status())
	}
}

func TestUpdateTask_InvalidSession(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	req := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{Id: 1},
	})
	setBearer(req, "nonexistent")

	_, err := h.UpdateTask(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for invalid session")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestUpdateTask_ForwardError(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, clientA, _ := testResolver()
	clientA.updateTaskErr = errors.New("forward failed")
	h := newTestHandler(ms, sm, resolver)

	regToken := "fwd-err-token"
	newTaskCtx(ms, 42, regToken, "inst-a")

	sessionToken := registerTask(t, h, regToken, 42)

	updateReq := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{Id: 42},
	})
	setBearer(updateReq, sessionToken)

	_, err := h.UpdateTask(context.Background(), updateReq)
	if err == nil {
		t.Fatal("expected forward error to propagate")
	}
	if err.Error() != "forward failed" {
		t.Fatalf("expected 'forward failed', got %q", err.Error())
	}
}

// TestUpdateTask_RoutesToOwningInstance verifies the routing matrix: each
// task's UpdateTask is forwarded to the client of its own instance.
func TestUpdateTask_RoutesToOwningInstance(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, clientA, clientB := testResolver()
	h := newTestHandler(ms, sm, resolver)

	// Two tasks, one per instance.
	newTaskCtx(ms, 1, "token-a", "inst-a")
	newTaskCtx(ms, 2, "token-b", "inst-b")
	sessionA := registerTask(t, h, "token-a", 1)
	sessionB := registerTask(t, h, "token-b", 2)

	// UpdateTask on inst-a's task (running state).
	reqA := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{Id: 1, Result: v1.Result_RESULT_UNSPECIFIED},
	})
	setBearer(reqA, sessionA)
	if _, err := h.UpdateTask(context.Background(), reqA); err != nil {
		t.Fatalf("UpdateTask on inst-a task: %v", err)
	}

	// UpdateTask on inst-b's task (running state).
	reqB := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{Id: 2, Result: v1.Result_RESULT_UNSPECIFIED},
	})
	setBearer(reqB, sessionB)
	if _, err := h.UpdateTask(context.Background(), reqB); err != nil {
		t.Fatalf("UpdateTask on inst-b task: %v", err)
	}

	clientA.mu.Lock()
	calledA := clientA.updateTaskCalled
	lastA := clientA.lastUpdateTask
	clientA.mu.Unlock()
	clientB.mu.Lock()
	calledB := clientB.updateTaskCalled
	lastB := clientB.lastUpdateTask
	clientB.mu.Unlock()

	if calledA != 1 || lastA.GetState().GetId() != 1 {
		t.Errorf("inst-a client calls = %d (last task %v), want 1 call for task 1", calledA, lastA)
	}
	if calledB != 1 || lastB.GetState().GetId() != 2 {
		t.Errorf("inst-b client calls = %d (last task %v), want 1 call for task 2", calledB, lastB)
	}
}

// TestUpdateTask_UnknownInstance verifies the defensive path: a session
// bound to an unknown instance fails with CodeInternal and nothing is
// forwarded.
func TestUpdateTask_UnknownInstance(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, clientA, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	// Create a session directly for a task of an unknown instance.
	taskCtx := newTaskCtx(ms, 5, "ghost-token", "ghost-instance")
	sess := sm.CreateWithToken(taskCtx, "ghost-session", "", nil)

	updateReq := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{Id: 5},
	})
	setBearer(updateReq, sess.SessionToken)

	_, err := h.UpdateTask(context.Background(), updateReq)
	if err == nil {
		t.Fatal("expected error for unknown instance")
	}
	if connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("expected Internal, got %v", connect.CodeOf(err))
	}
	clientA.mu.Lock()
	calledA := clientA.updateTaskCalled
	clientA.mu.Unlock()
	if calledA != 0 {
		t.Fatal("no forward should happen for unknown instance")
	}
}

// ── Tests: UpdateLog ───────────────────────────────────────────────────────

func TestUpdateLog_Valid(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, clientA, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "log-test-token"
	newTaskCtx(ms, 42, regToken, "inst-a")

	sessionToken := registerTask(t, h, regToken, 42)

	logReq := connect.NewRequest(&v1.UpdateLogRequest{
		TaskId: 42,
		Index:  0,
		Rows: []*v1.LogRow{
			{Content: "hello world"},
		},
	})
	setBearer(logReq, sessionToken)

	resp, err := h.UpdateLog(context.Background(), logReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Msg == nil {
		t.Fatal("expected response")
	}
	clientA.mu.Lock()
	calledA := clientA.updateLogCalled
	clientA.mu.Unlock()
	if calledA != 1 {
		t.Fatalf("expected forwarder called once, got %d", calledA)
	}
}

func TestUpdateLog_TaskIDMismatch(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, clientA, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "log-mismatch-token"
	newTaskCtx(ms, 42, regToken, "inst-a")

	sessionToken := registerTask(t, h, regToken, 42)

	logReq := connect.NewRequest(&v1.UpdateLogRequest{
		TaskId: 99, // wrong
	})
	setBearer(logReq, sessionToken)

	_, err := h.UpdateLog(context.Background(), logReq)
	if err == nil {
		t.Fatal("expected error for task ID mismatch")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
	clientA.mu.Lock()
	calledA := clientA.updateLogCalled
	clientA.mu.Unlock()
	if calledA != 0 {
		t.Fatal("forwarder should not be called on mismatch")
	}
}

func TestUpdateLog_InvalidSession(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	req := connect.NewRequest(&v1.UpdateLogRequest{TaskId: 1})
	setBearer(req, "nonexistent")

	_, err := h.UpdateLog(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for invalid session")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestUpdateLog_MissingToken(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	req := connect.NewRequest(&v1.UpdateLogRequest{TaskId: 1})

	_, err := h.UpdateLog(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for missing session token")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

func TestUpdateLog_ForwardError(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, clientA, _ := testResolver()
	clientA.updateLogErr = errors.New("log forward failed")
	h := newTestHandler(ms, sm, resolver)

	regToken := "log-fwd-err-token"
	newTaskCtx(ms, 42, regToken, "inst-a")

	sessionToken := registerTask(t, h, regToken, 42)

	logReq := connect.NewRequest(&v1.UpdateLogRequest{TaskId: 42})
	setBearer(logReq, sessionToken)

	_, err := h.UpdateLog(context.Background(), logReq)
	if err == nil {
		t.Fatal("expected forward error to propagate")
	}
	if err.Error() != "log forward failed" {
		t.Fatalf("expected 'log forward failed', got %q", err.Error())
	}
}

// TestUpdateLog_RoutesToOwningInstance verifies the routing matrix for log
// relays: each task's UpdateLog goes to its own instance's client.
func TestUpdateLog_RoutesToOwningInstance(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, clientA, clientB := testResolver()
	h := newTestHandler(ms, sm, resolver)

	newTaskCtx(ms, 1, "token-a", "inst-a")
	newTaskCtx(ms, 2, "token-b", "inst-b")
	sessionA := registerTask(t, h, "token-a", 1)
	sessionB := registerTask(t, h, "token-b", 2)

	reqA := connect.NewRequest(&v1.UpdateLogRequest{TaskId: 1})
	setBearer(reqA, sessionA)
	if _, err := h.UpdateLog(context.Background(), reqA); err != nil {
		t.Fatalf("UpdateLog on inst-a task: %v", err)
	}

	reqB := connect.NewRequest(&v1.UpdateLogRequest{TaskId: 2})
	setBearer(reqB, sessionB)
	if _, err := h.UpdateLog(context.Background(), reqB); err != nil {
		t.Fatalf("UpdateLog on inst-b task: %v", err)
	}

	clientA.mu.Lock()
	calledA := clientA.updateLogCalled
	lastA := clientA.lastUpdateLog
	clientA.mu.Unlock()
	clientB.mu.Lock()
	calledB := clientB.updateLogCalled
	lastB := clientB.lastUpdateLog
	clientB.mu.Unlock()

	if calledA != 1 || lastA.GetTaskId() != 1 {
		t.Errorf("inst-a client log calls = %d (last task %v), want 1 for task 1", calledA, lastA)
	}
	if calledB != 1 || lastB.GetTaskId() != 2 {
		t.Errorf("inst-b client log calls = %d (last task %v), want 1 for task 2", calledB, lastB)
	}
}

// TestUpdateLog_UnknownInstance verifies the defensive path for log relays.
func TestUpdateLog_UnknownInstance(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, clientA, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	taskCtx := newTaskCtx(ms, 5, "ghost-token", "ghost-instance")
	sess := sm.CreateWithToken(taskCtx, "ghost-session", "", nil)

	logReq := connect.NewRequest(&v1.UpdateLogRequest{TaskId: 5})
	setBearer(logReq, sess.SessionToken)

	_, err := h.UpdateLog(context.Background(), logReq)
	if err == nil {
		t.Fatal("expected error for unknown instance")
	}
	if connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("expected Internal, got %v", connect.CodeOf(err))
	}
	clientA.mu.Lock()
	calledA := clientA.updateLogCalled
	clientA.mu.Unlock()
	if calledA != 0 {
		t.Fatal("no log forward should happen for unknown instance")
	}
}

// TestAuthenticate_SessionHit_TouchesSession verifies the F2 fix: every
// authenticated RPC on the session-token path refreshes the session's
// LastActivity, so the GC loop never reaps a session whose runner is
// actively talking to the proxy — even for tasks running far past
// sessionMaxAge.
func TestAuthenticate_SessionHit_TouchesSession(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "touch-session-token"
	newTaskCtx(ms, 42, regToken, "inst-a")
	sessionToken := registerTask(t, h, regToken, 42)

	sess, ok := sm.Lookup(sessionToken)
	if !ok {
		t.Fatal("session not found after Register")
	}

	// Backdate LastActivity past maxAge, then an authenticated RPC must
	// refresh it.
	sess.LastActivity = time.Now().Add(-2 * time.Hour)

	req := connect.NewRequest(&v1.FetchTaskRequest{})
	setBearer(req, sessionToken)
	if _, err := h.FetchTask(context.Background(), req); err != nil {
		t.Fatalf("FetchTask failed: %v", err)
	}

	if time.Since(sess.LastActivity) > time.Minute {
		t.Errorf("FetchTask did not refresh LastActivity: %v ago", time.Since(sess.LastActivity))
	}
	// The refreshed session must survive an Expire pass that would have
	// reaped it before the touch.
	if got := sm.Expire(time.Now(), 30*time.Minute); len(got) != 0 {
		t.Fatalf("touched session expired: %+v", got)
	}
}

// TestAuthenticate_OneJob_TouchesSession verifies the auto-registration path
// also keeps the session fresh: the first RPC creates the session with
// LastActivity initialized, and subsequent RPCs on the same token refresh it.
func TestAuthenticate_OneJob_TouchesSession(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "touch-onejob-token"
	newTaskCtx(ms, 42, regToken, "inst-a")

	// First RPC auto-registers via the reg-token path.
	req := connect.NewRequest(&v1.FetchTaskRequest{})
	setRunnerToken(req, regToken)
	if _, err := h.FetchTask(context.Background(), req); err != nil {
		t.Fatalf("FetchTask failed: %v", err)
	}

	sess, ok := sm.Lookup(regToken)
	if !ok {
		t.Fatal("session not created by one-job auto-registration")
	}
	if sess.LastActivity.IsZero() {
		t.Fatal("auto-registered session has no LastActivity")
	}

	// Backdate, then a second RPC on the same token (now a session lookup
	// hit) must refresh it.
	sess.LastActivity = time.Now().Add(-2 * time.Hour)

	req2 := connect.NewRequest(&v1.FetchTaskRequest{})
	setRunnerToken(req2, regToken)
	if _, err := h.FetchTask(context.Background(), req2); err != nil {
		t.Fatalf("second FetchTask failed: %v", err)
	}

	if time.Since(sess.LastActivity) > time.Minute {
		t.Errorf("second FetchTask did not refresh LastActivity: %v ago", time.Since(sess.LastActivity))
	}
}

// TestAuthenticate_VanishedSession verifies a request for a session that no
// longer exists is rejected with Unauthenticated: authenticate must never
// proceed with a dead session, whether the removal lands before the Lookup
// or in the Lookup→Touch window (where Touch reports false and the request
// is rejected the same way).
func TestAuthenticate_VanishedSession(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	regToken := "vanish-token"
	newTaskCtx(ms, 42, regToken, "inst-a")
	sessionToken := registerTask(t, h, regToken, 42)

	// Remove the session out from under authenticate: Lookup still hits the
	// pointer captured below, but Touch fails.
	sm.Remove(sessionToken)

	req := connect.NewRequest(&v1.FetchTaskRequest{})
	setBearer(req, sessionToken)
	_, err := h.FetchTask(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for a session removed during authentication")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", connect.CodeOf(err))
	}
}

// ── Tests: NewServer ───────────────────────────────────────────────────────

func TestNewServer(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	h := newTestHandler(ms, sm, resolver)

	srv := NewServer(h, ":0")
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	if srv.Addr != ":0" {
		t.Fatalf("expected addr :0, got %q", srv.Addr)
	}
	if srv.Handler == nil {
		t.Fatal("expected non-nil handler")
	}

	// Verify the /api/actions/ prefix path is registered by starting a test
	// server and checking both paths respond (not 404).
	ts := httptest.NewServer(srv.Handler)
	defer ts.Close()

	// The Connect handler at the root path should exist
	resp, err := http.Get(ts.URL + "/runner.v1.RunnerService/FetchTask")
	if err != nil {
		t.Fatalf("root path request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		t.Fatal("root path returned 404")
	}

	// The Connect handler at /api/actions/ prefix should also exist
	resp2, err := http.Get(ts.URL + "/api/actions/runner.v1.RunnerService/FetchTask")
	if err != nil {
		t.Fatalf("/api/actions/ path request failed: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode == http.StatusNotFound {
		t.Fatal("/api/actions/ path returned 404")
	}
}

// ── Tests: NewHandler ──────────────────────────────────────────────────────

func TestNewHandler(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver, _, _ := testResolver()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	h := NewHandler(ms, sm, resolver, log)

	if h == nil {
		t.Fatal("expected non-nil handler")
	}
	if h.store != ms {
		t.Fatal("store not set")
	}
	if h.sessions != sm {
		t.Fatal("sessions not set")
	}
	if h.resolver != resolver {
		t.Fatal("resolver not set")
	}
}
