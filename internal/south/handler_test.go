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
		tasks:          make(map[string]*store.TaskCtx),
		byID:           make(map[int64]*store.TaskCtx),
		consumedTokens: make(map[string]bool),
		removed:        make(map[int64]bool),
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

func (m *mockStore) HasCapacity(max int) bool {
	return m.CountActive() < max
}

func (m *mockStore) wasRemoved(taskID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.removed[taskID]
}

// ── mockForwarder ──────────────────────────────────────────────────────────

// mockForwarder implements northForwarder for testing.
type mockForwarder struct {
	updateTaskCalled int
	updateLogCalled  int
	lastUpdateTask   *v1.UpdateTaskRequest
	lastUpdateLog    *v1.UpdateLogRequest
	updateTaskResp   *v1.UpdateTaskResponse
	updateLogResp    *v1.UpdateLogResponse
	updateTaskErr    error
	updateLogErr     error
}

func newMockForwarder() *mockForwarder {
	return &mockForwarder{
		updateTaskResp: &v1.UpdateTaskResponse{},
		updateLogResp:  &v1.UpdateLogResponse{},
	}
}

func (m *mockForwarder) ForwardUpdateTask(ctx context.Context, req *v1.UpdateTaskRequest) (*v1.UpdateTaskResponse, error) {
	m.updateTaskCalled++
	m.lastUpdateTask = req
	if m.updateTaskErr != nil {
		return nil, m.updateTaskErr
	}
	return m.updateTaskResp, nil
}

func (m *mockForwarder) ForwardUpdateLog(ctx context.Context, req *v1.UpdateLogRequest) (*v1.UpdateLogResponse, error) {
	m.updateLogCalled++
	m.lastUpdateLog = req
	if m.updateLogErr != nil {
		return nil, m.updateLogErr
	}
	return m.updateLogResp, nil
}

// ── helpers ────────────────────────────────────────────────────────────────

func newTestHandler(ms *mockStore, sm *session.Manager, fw *mockForwarder) *Handler {
	return NewHandler(ms, sm, fw, &config.Config{
		RegTokenTTL: 15 * time.Minute,
	}, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
}

// newTaskCtx creates a fresh TaskCtx with a real Forgejo Task payload, a
// registration token, and an ID. The task is put into the mock store.
func newTaskCtx(ms *mockStore, id int64, regToken string) *store.TaskCtx {
	taskCtx := &store.TaskCtx{
		ID:       id,
		Task:     &v1.Task{Id: id},
		RegToken: regToken,
	}
	taskCtx.SetStatus(store.StatusPending)
	ms.PutPending(taskCtx)
	return taskCtx
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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "valid-reg-token"
	taskCtx := newTaskCtx(ms, 42, regToken)
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
	if runner.GetId() != 42 {
		t.Fatalf("expected runner ID 42, got %d", runner.GetId())
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

func TestRegister_InvalidToken(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "expired-token"
	taskCtx := newTaskCtx(ms, 42, regToken)
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

func TestRegister_AlreadyConsumed(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "single-use-token"
	taskCtx := newTaskCtx(ms, 42, regToken)
	taskCtx.CreatedAt = time.Now()

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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "declare-test-token"
	taskCtx := newTaskCtx(ms, 42, regToken)
	taskCtx.CreatedAt = time.Now()

	// Register first to get a session.
	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner", Labels: []string{"linux"}})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	sessionToken := regResp.Msg.GetRunner().GetToken()

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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "declare-runner-token-test"
	taskCtx := newTaskCtx(ms, 42, regToken)
	taskCtx.CreatedAt = time.Now()

	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner", Labels: []string{"linux"}})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	sessionToken := regResp.Msg.GetRunner().GetToken()

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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "declare-uuid-test"
	taskCtx := newTaskCtx(ms, 42, regToken)
	taskCtx.CreatedAt = time.Now()

	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner", Labels: []string{"linux"}})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	sessionToken := regResp.Msg.GetRunner().GetToken()

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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "fetch-test-token"
	taskCtx := newTaskCtx(ms, 42, regToken)
	taskCtx.CreatedAt = time.Now()
	taskCtx.Task = &v1.Task{Id: 42} // the real Forgejo task

	// Register first.
	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner"})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	sessionToken := regResp.Msg.GetRunner().GetToken()

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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "update-test-token"
	taskCtx := newTaskCtx(ms, 42, regToken)
	taskCtx.CreatedAt = time.Now()

	// Register first.
	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner"})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	sessionToken := regResp.Msg.GetRunner().GetToken()

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
	if fw.updateTaskCalled != 1 {
		t.Fatalf("expected forwarder to be called once, got %d", fw.updateTaskCalled)
	}

	// Session should still exist (non-terminal state).
	if _, ok := sm.Lookup(sessionToken); !ok {
		t.Fatal("session should exist after non-terminal UpdateTask")
	}
}

func TestUpdateTask_TaskIDMismatch(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "mismatch-token"
	taskCtx := newTaskCtx(ms, 42, regToken)
	taskCtx.CreatedAt = time.Now()

	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner"})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	sessionToken := regResp.Msg.GetRunner().GetToken()

	// UpdateTask with wrong task ID.
	updateReq := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{
			Id: 99, // wrong ID
		},
	})
	setBearer(updateReq, sessionToken)

	_, err = h.UpdateTask(context.Background(), updateReq)
	if err == nil {
		t.Fatal("expected error for task ID mismatch")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
	// Forwarder should not have been called.
	if fw.updateTaskCalled != 0 {
		t.Fatal("forwarder should not be called on task ID mismatch")
	}
}

func TestUpdateTask_TerminalCleansUp(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "terminal-token"
	taskCtx := newTaskCtx(ms, 42, regToken)
	taskCtx.CreatedAt = time.Now()

	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner"})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	sessionToken := regResp.Msg.GetRunner().GetToken()

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
	if fw.updateTaskCalled != 1 {
		t.Fatalf("expected forwarder to be called, got %d", fw.updateTaskCalled)
	}
}

func TestUpdateTask_TerminalFailure(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "failure-token"
	taskCtx := newTaskCtx(ms, 42, regToken)
	taskCtx.CreatedAt = time.Now()

	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner"})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	sessionToken := regResp.Msg.GetRunner().GetToken()

	updateReq := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{
			Id:     42,
			Result: v1.Result_RESULT_FAILURE,
		},
	})
	setBearer(updateReq, sessionToken)

	_, err = h.UpdateTask(context.Background(), updateReq)
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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "cancel-token"
	taskCtx := newTaskCtx(ms, 42, regToken)
	taskCtx.CreatedAt = time.Now()

	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner"})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	sessionToken := regResp.Msg.GetRunner().GetToken()

	updateReq := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{
			Id:     42,
			Result: v1.Result_RESULT_CANCELLED,
		},
	})
	setBearer(updateReq, sessionToken)

	_, err = h.UpdateTask(context.Background(), updateReq)
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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

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
	fw := newMockForwarder()
	fw.updateTaskErr = errors.New("forward failed")
	h := newTestHandler(ms, sm, fw)

	regToken := "fwd-err-token"
	newTaskCtx(ms, 42, regToken).CreatedAt = time.Now()

	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner"})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	sessionToken := regResp.Msg.GetRunner().GetToken()

	updateReq := connect.NewRequest(&v1.UpdateTaskRequest{
		State: &v1.TaskState{Id: 42},
	})
	setBearer(updateReq, sessionToken)

	_, err = h.UpdateTask(context.Background(), updateReq)
	if err == nil {
		t.Fatal("expected forward error to propagate")
	}
	if err.Error() != "forward failed" {
		t.Fatalf("expected 'forward failed', got %q", err.Error())
	}
}

// ── Tests: UpdateLog ───────────────────────────────────────────────────────

func TestUpdateLog_Valid(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "log-test-token"
	taskCtx := newTaskCtx(ms, 42, regToken)
	taskCtx.CreatedAt = time.Now()

	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner"})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	sessionToken := regResp.Msg.GetRunner().GetToken()

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
	if fw.updateLogCalled != 1 {
		t.Fatalf("expected forwarder called once, got %d", fw.updateLogCalled)
	}
}

func TestUpdateLog_TaskIDMismatch(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

	regToken := "log-mismatch-token"
	taskCtx := newTaskCtx(ms, 42, regToken)
	taskCtx.CreatedAt = time.Now()

	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner"})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	sessionToken := regResp.Msg.GetRunner().GetToken()

	logReq := connect.NewRequest(&v1.UpdateLogRequest{
		TaskId: 99, // wrong
	})
	setBearer(logReq, sessionToken)

	_, err = h.UpdateLog(context.Background(), logReq)
	if err == nil {
		t.Fatal("expected error for task ID mismatch")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
	if fw.updateLogCalled != 0 {
		t.Fatal("forwarder should not be called on mismatch")
	}
}

func TestUpdateLog_InvalidSession(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

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
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

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
	fw := newMockForwarder()
	fw.updateLogErr = errors.New("log forward failed")
	h := newTestHandler(ms, sm, fw)

	regToken := "log-fwd-err-token"
	newTaskCtx(ms, 42, regToken).CreatedAt = time.Now()

	regReq := connect.NewRequest(&v1.RegisterRequest{Token: regToken, Name: "runner"})
	regResp, err := h.Register(context.Background(), regReq)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	sessionToken := regResp.Msg.GetRunner().GetToken()

	logReq := connect.NewRequest(&v1.UpdateLogRequest{TaskId: 42})
	setBearer(logReq, sessionToken)

	_, err = h.UpdateLog(context.Background(), logReq)
	if err == nil {
		t.Fatal("expected forward error to propagate")
	}
	if err.Error() != "log forward failed" {
		t.Fatalf("expected 'log forward failed', got %q", err.Error())
	}
}

// ── Tests: NewServer ───────────────────────────────────────────────────────

func TestNewServer(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	fw := newMockForwarder()
	h := newTestHandler(ms, sm, fw)

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
	fw := newMockForwarder()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	h := NewHandler(ms, sm, fw, &config.Config{RegTokenTTL: 10 * time.Minute}, log)

	if h == nil {
		t.Fatal("expected non-nil handler")
	}
	if h.store != ms {
		t.Fatal("store not set")
	}
	if h.sessions != sm {
		t.Fatal("sessions not set")
	}
	if h.forward != fw {
		t.Fatal("forward not set")
	}
	if h.cfg.RegTokenTTL != 10*time.Minute {
		t.Fatal("config not set correctly")
	}
}
