package north

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"
	"code.gitea.io/actions-proto-go/runner/v1/runnerv1connect"
	"connectrpc.com/connect"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/slots"
	"git.0x0f.dev/forgery/internal/state"
	"git.0x0f.dev/forgery/internal/store"
)

// testLogger returns a no-op logger for tests.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// testInstance returns a minimal instance config for testing.
func testInstance(url string) config.Instance {
	return config.Instance{
		ForgejoURL:         url,
		ForgejoRunnerToken: "reg-token",
		ForgejoRunnerName:  "forgery",
	}
}

// ── fakeIdentities ────────────────────────────────────────────────────────────

// fakeIdentities is an in-memory state.Store for tests, with optional error
// injection for Load and Save.
type fakeIdentities struct {
	mu      sync.Mutex
	data    map[string]state.Identity
	loadErr error
	saveErr error
}

func newFakeIdentities() *fakeIdentities {
	return &fakeIdentities{data: map[string]state.Identity{}}
}

func (f *fakeIdentities) Load(forgejoURL string) (state.Identity, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadErr != nil {
		return state.Identity{}, false, f.loadErr
	}
	id, ok := f.data[forgejoURL]
	return id, ok, nil
}

func (f *fakeIdentities) Save(forgejoURL string, id state.Identity) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	f.data[forgejoURL] = id
	return nil
}

// saved returns the identity currently stored for forgejoURL.
func (f *fakeIdentities) saved(forgejoURL string) (state.Identity, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.data[forgejoURL]
	return id, ok
}

// ── TestNewClient ────────────────────────────────────────────────────────────

func TestNewClient(t *testing.T) {
	inst := testInstance("https://forgejo.example.com")
	inst.TLSInsecureSkipVerify = true
	s := store.NewMemStore()
	log := testLogger()

	c, err := New(inst, s, newFakeIdentities(), log)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	if c == nil {
		t.Fatal("New returned nil")
	}
	if c.inst.ForgejoURL != inst.ForgejoURL {
		t.Error("Client.inst does not match input instance")
	}
	if !c.inst.TLSInsecureSkipVerify {
		t.Error("Client.inst lost TLSInsecureSkipVerify")
	}
	if c.store != s {
		t.Error("Client.store does not match input store")
	}
	// The logger is decorated with the instance name (see
	// TestNew_LoggerCarriesInstance); only non-nil is asserted here.
	if c.log == nil {
		t.Error("Client.log is nil")
	}
	if c.client == nil {
		t.Error("Client.client is nil")
	}
}

// ── TestNew_Errors ───────────────────────────────────────────────────────────

func TestNew_NilIdentityStore(t *testing.T) {
	_, err := New(testInstance("https://forgejo.example.com"), store.NewMemStore(), nil, testLogger())
	if err == nil {
		t.Fatal("New returned nil error, want error for nil identity store")
	}
}

func TestNew_IdentityLoadError(t *testing.T) {
	ids := newFakeIdentities()
	ids.loadErr = errors.New("corrupt state file")

	_, err := New(testInstance("https://forgejo.example.com"), store.NewMemStore(), ids, testLogger())
	if err == nil {
		t.Fatal("New returned nil error, want error from identity load")
	}
	if !errors.Is(err, ids.loadErr) {
		t.Errorf("error %v does not wrap the load error", err)
	}
}

func TestNew_RestoresIdentity(t *testing.T) {
	ids := newFakeIdentities()
	ids.data["https://forgejo.example.com"] = state.Identity{UUID: "uuid-1", Token: "token-1"}

	c, err := New(testInstance("https://forgejo.example.com"), store.NewMemStore(), ids, testLogger())
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if !c.HasIdentity() {
		t.Error("HasIdentity() = false, want true for restored identity")
	}
	if got := c.Identity(); got != (state.Identity{UUID: "uuid-1", Token: "token-1"}) {
		t.Errorf("Identity() = %+v, want restored identity", got)
	}
	// A restored identity counts as generation 1 so concurrent RPCs share
	// one dedupe baseline.
	if gen := c.generation.Load(); gen != 1 {
		t.Errorf("generation = %d, want 1 for restored identity", gen)
	}
}

// TestNew_LoggerCarriesInstance verifies the multi-instance log contract:
// the client's logger is decorated with the "instance" attribute so log
// lines from different Forgejo instances can be told apart.
func TestNew_LoggerCarriesInstance(t *testing.T) {
	inst := testInstance("https://forgejo.example.com")
	inst.Name = "my-forgejo"

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	c, err := New(inst, store.NewMemStore(), newFakeIdentities(), log)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	c.log.Info("probe")
	out := buf.String()
	if !strings.Contains(out, `"instance":"my-forgejo"`) {
		t.Errorf("log line does not carry the instance attribute: %s", out)
	}
	// The decoration must happen exactly once — New is the single owner
	// of the instance attribute, callers pass an undecorated logger.
	if got := strings.Count(out, `"instance":"my-forgejo"`); got != 1 {
		t.Errorf("log line carries the instance attribute %d times, want exactly 1: %s", got, out)
	}
}

// ── TestSemCapacity ──────────────────────────────────────────────────────────

// The backpressure semaphore moved out of the client into the shared
// internal/slots package; its capacity/blocking/ctx-cancellation behavior
// is covered by the slots package tests.

// ── mockHandler ──────────────────────────────────────────────────────────────

// mockHandler is a minimal RunnerServiceHandler implementation for testing.
// It embeds UnimplementedRunnerServiceHandler and overrides specific methods.
type mockHandler struct {
	runnerv1connect.UnimplementedRunnerServiceHandler

	registerFn   func(context.Context, *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error)
	declareFn    func(context.Context, *connect.Request[v1.DeclareRequest]) (*connect.Response[v1.DeclareResponse], error)
	fetchTaskFn  func(context.Context, *connect.Request[v1.FetchTaskRequest]) (*connect.Response[v1.FetchTaskResponse], error)
	updateTaskFn func(context.Context, *connect.Request[v1.UpdateTaskRequest]) (*connect.Response[v1.UpdateTaskResponse], error)
	updateLogFn  func(context.Context, *connect.Request[v1.UpdateLogRequest]) (*connect.Response[v1.UpdateLogResponse], error)
}

func (h *mockHandler) Register(ctx context.Context, req *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
	if h.registerFn != nil {
		return h.registerFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("register not implemented"))
}

func (h *mockHandler) Declare(ctx context.Context, req *connect.Request[v1.DeclareRequest]) (*connect.Response[v1.DeclareResponse], error) {
	if h.declareFn != nil {
		return h.declareFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("declare not implemented"))
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

// startMockServer creates an httptest.Server with a connect handler wrapping
// the given mockHandler. The server is configured for HTTP/2 (h2c) so that
// the gRPC-protocol client (used by New) can connect. It also strips the
// /api/actions prefix that the production client appends to match Forgejo's
// routing. The caller is responsible for closing the server.
func startMockServer(t *testing.T, h *mockHandler) *httptest.Server {
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
	return ts
}

// newMockClient connects a Client to an already-started mock server.
func newMockClient(t *testing.T, ts *httptest.Server, ids state.Store) *client {
	t.Helper()

	inst := testInstance(ts.URL)
	inst.TLSInsecureSkipVerify = true // test server uses self-signed cert
	c, err := New(inst, store.NewMemStore(), ids, testLogger())
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return c
}

// newMockServer combines startMockServer and newMockClient.
func newMockServer(t *testing.T, h *mockHandler, ids state.Store) (*httptest.Server, *client) {
	t.Helper()
	ts := startMockServer(t, h)
	return ts, newMockClient(t, ts, ids)
}

// ── TestStartupFlow ───────────────────────────────────────────────────────────

// TestStartupFlow_SkipsRegisterWhenIdentityExists verifies the startup
// contract: a persisted identity skips Register entirely and goes straight
// to Declare, authenticated with the persisted permanent token.
func TestStartupFlow_SkipsRegisterWhenIdentityExists(t *testing.T) {
	var registerCalls, declareCalls atomic.Int32
	var mu sync.Mutex
	var declareTokens []string

	h := &mockHandler{
		registerFn: func(_ context.Context, _ *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			registerCalls.Add(1)
			return connect.NewResponse(&v1.RegisterResponse{Runner: &v1.Runner{Uuid: "unexpected", Token: "unexpected"}}), nil
		},
		declareFn: func(_ context.Context, req *connect.Request[v1.DeclareRequest]) (*connect.Response[v1.DeclareResponse], error) {
			declareCalls.Add(1)
			mu.Lock()
			declareTokens = append(declareTokens, req.Header().Get("x-runner-token"))
			mu.Unlock()
			return connect.NewResponse(&v1.DeclareResponse{}), nil
		},
	}
	ts := startMockServer(t, h)
	defer ts.Close()

	ids := newFakeIdentities()
	ids.data[ts.URL] = state.Identity{UUID: "uuid-1", Token: "token-1"}
	c := newMockClient(t, ts, ids)

	if !c.HasIdentity() {
		t.Fatal("HasIdentity() = false, want true for restored identity")
	}
	if err := c.Declare(context.Background()); err != nil {
		t.Fatalf("Declare returned error: %v", err)
	}

	if got := registerCalls.Load(); got != 0 {
		t.Errorf("Register called %d times, want 0 (identity restored)", got)
	}
	if got := declareCalls.Load(); got != 1 {
		t.Errorf("Declare called %d times, want 1", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(declareTokens) != 1 || declareTokens[0] != "token-1" {
		t.Errorf("Declare authenticated with %v, want persisted token token-1", declareTokens)
	}
}

// TestStartupFlow_RegistersAndPersists verifies the startup contract for a
// fresh install: no identity → Register → identity persisted → Declare.
func TestStartupFlow_RegistersAndPersists(t *testing.T) {
	var registerCalls atomic.Int32
	var registerToken atomic.Value // string

	h := &mockHandler{
		registerFn: func(_ context.Context, req *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			registerCalls.Add(1)
			registerToken.Store(req.Header().Get("x-runner-token"))
			return connect.NewResponse(&v1.RegisterResponse{Runner: &v1.Runner{Uuid: "uuid-new", Token: "token-new"}}), nil
		},
		declareFn: func(_ context.Context, _ *connect.Request[v1.DeclareRequest]) (*connect.Response[v1.DeclareResponse], error) {
			return connect.NewResponse(&v1.DeclareResponse{}), nil
		},
	}
	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	if c.HasIdentity() {
		t.Fatal("HasIdentity() = true, want false before Register")
	}

	// Startup flow: no identity → Register, then Declare.
	if err := c.Register(context.Background()); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := c.Declare(context.Background()); err != nil {
		t.Fatalf("Declare returned error: %v", err)
	}

	if got := registerCalls.Load(); got != 1 {
		t.Errorf("Register called %d times, want 1", got)
	}
	if !c.HasIdentity() {
		t.Error("HasIdentity() = false, want true after Register")
	}
	if got := registerToken.Load(); got != "reg-token" {
		t.Errorf("Register authenticated with %v, want registration token reg-token", got)
	}
	// The startup registration issues generation 1.
	if gen := c.generation.Load(); gen != 1 {
		t.Errorf("generation = %d, want 1 after startup Register", gen)
	}

	ids := c.identities.(*fakeIdentities)
	saved, ok := ids.saved(ts.URL)
	if !ok {
		t.Fatal("identity was not saved to the store after Register")
	}
	if saved != (state.Identity{UUID: "uuid-new", Token: "token-new"}) {
		t.Errorf("saved identity = %+v, want %+v", saved, state.Identity{UUID: "uuid-new", Token: "token-new"})
	}
}

// ── TestRegister_Errors ───────────────────────────────────────────────────────

func TestRegister_ResponseMissingRunner(t *testing.T) {
	h := &mockHandler{
		registerFn: func(_ context.Context, _ *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			return connect.NewResponse(&v1.RegisterResponse{}), nil
		},
	}
	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	err := c.Register(context.Background())
	if err == nil {
		t.Fatal("Register returned nil error, want error for missing runner in response")
	}
	if c.HasIdentity() {
		t.Error("HasIdentity() = true, want false after failed Register")
	}
	ids := c.identities.(*fakeIdentities)
	if _, ok := ids.saved(ts.URL); ok {
		t.Error("identity was saved despite failed Register")
	}
}

// TestRegister_UnauthenticatedNoFallback verifies Register itself is never
// fed back through the auth-fallback: an auth error from Register is simply
// returned, with no re-registration attempt.
func TestRegister_UnauthenticatedNoFallback(t *testing.T) {
	var registerCalls atomic.Int32
	h := &mockHandler{
		registerFn: func(_ context.Context, _ *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			registerCalls.Add(1)
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("registration token invalid"))
		},
	}
	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	err := c.Register(context.Background())
	if err == nil {
		t.Fatal("Register returned nil error, want Unauthenticated error")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("Register error code = %v, want Unauthenticated", connect.CodeOf(err))
	}
	if got := registerCalls.Load(); got != 1 {
		t.Errorf("Register called %d times, want 1 (no auth-fallback recursion)", got)
	}
	ids := c.identities.(*fakeIdentities)
	if _, ok := ids.saved(ts.URL); ok {
		t.Error("identity was saved despite failed Register")
	}
}

// TestRegister_SaveFailure verifies a persistence failure surfaces as a
// Register error (fail-fast, strict style).
func TestRegister_SaveFailure(t *testing.T) {
	ids := newFakeIdentities()
	ids.saveErr = errors.New("disk full")

	h := &mockHandler{
		registerFn: func(_ context.Context, _ *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			return connect.NewResponse(&v1.RegisterResponse{Runner: &v1.Runner{Uuid: "uuid-new", Token: "token-new"}}), nil
		},
	}
	ts, c := newMockServer(t, h, ids)
	defer ts.Close()

	err := c.Register(context.Background())
	if err == nil {
		t.Fatal("Register returned nil error, want error when identity save fails")
	}
	if !errors.Is(err, ids.saveErr) {
		t.Errorf("Register error %v does not wrap the save error", err)
	}
}

// ── TestAuthFallback ──────────────────────────────────────────────────────────

// newAuthFallbackHandler builds a handler whose declareFn fails with the
// given connect code on its first call and then succeeds. registerFn always
// succeeds with a fresh identity. Returns the handler plus capture fields
// for the register call count and per-call Declare tokens.
func newAuthFallbackHandler(authCode connect.Code) (*mockHandler, *atomic.Int32, *atomic.Int32, *[]string, *sync.Mutex) {
	var registerCalls atomic.Int32
	var declareCalls atomic.Int32
	var tokens []string
	var mu sync.Mutex

	h := &mockHandler{
		registerFn: func(_ context.Context, req *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			registerCalls.Add(1)
			// Register must always be authenticated with the registration
			// token, even when a (revoked) permanent token is held.
			if got := req.Header().Get("x-runner-token"); got != "reg-token" {
				tokens = append(tokens, "WRONG-REG-TOKEN:"+got)
			}
			return connect.NewResponse(&v1.RegisterResponse{Runner: &v1.Runner{Uuid: "uuid-new", Token: "token-new"}}), nil
		},
		declareFn: func(_ context.Context, req *connect.Request[v1.DeclareRequest]) (*connect.Response[v1.DeclareResponse], error) {
			n := declareCalls.Add(1)
			mu.Lock()
			tokens = append(tokens, req.Header().Get("x-runner-token"))
			mu.Unlock()
			if n == 1 {
				return nil, connect.NewError(authCode, errors.New("runner token rejected"))
			}
			return connect.NewResponse(&v1.DeclareResponse{}), nil
		},
	}
	return h, &registerCalls, &declareCalls, &tokens, &mu
}

// TestAuthFallback_DeclareReRegisterRetry verifies: Declare fails with
// Unauthenticated → client re-registers once → saves the new identity →
// retries Declare successfully with the new permanent token.
func TestAuthFallback_DeclareReRegisterRetry(t *testing.T) {
	h, registerCalls, declareCalls, tokens, mu := newAuthFallbackHandler(connect.CodeUnauthenticated)

	ts := startMockServer(t, h)
	defer ts.Close()

	ids := newFakeIdentities()
	ids.data[ts.URL] = state.Identity{UUID: "uuid-1", Token: "token-1"}
	c := newMockClient(t, ts, ids)

	if err := c.Declare(context.Background()); err != nil {
		t.Fatalf("Declare returned error after re-register: %v", err)
	}

	if got := registerCalls.Load(); got != 1 {
		t.Errorf("Register called %d times, want exactly 1 re-registration", got)
	}
	if got := declareCalls.Load(); got != 2 {
		t.Errorf("Declare called %d times, want 2 (original + retry)", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(*tokens) != 2 || (*tokens)[0] != "token-1" || (*tokens)[1] != "token-new" {
		t.Errorf("Declare tokens = %v, want [token-1 token-new]", *tokens)
	}

	saved, ok := ids.saved(ts.URL)
	if !ok {
		t.Fatal("re-registered identity was not persisted")
	}
	if saved != (state.Identity{UUID: "uuid-new", Token: "token-new"}) {
		t.Errorf("saved identity = %+v, want re-registered identity", saved)
	}
	if c.Identity() != saved {
		t.Errorf("in-memory identity = %+v, want %+v", c.Identity(), saved)
	}
}

// TestAuthFallback_RetryStillFails verifies a persistent auth failure is
// returned as an error after exactly one re-registration attempt.
func TestAuthFallback_RetryStillFails(t *testing.T) {
	var registerCalls, declareCalls atomic.Int32
	h := &mockHandler{
		registerFn: func(_ context.Context, _ *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			registerCalls.Add(1)
			return connect.NewResponse(&v1.RegisterResponse{Runner: &v1.Runner{Uuid: "uuid-new", Token: "token-new"}}), nil
		},
		declareFn: func(_ context.Context, _ *connect.Request[v1.DeclareRequest]) (*connect.Response[v1.DeclareResponse], error) {
			declareCalls.Add(1)
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("still rejected"))
		},
	}

	ts := startMockServer(t, h)
	defer ts.Close()

	ids := newFakeIdentities()
	ids.data[ts.URL] = state.Identity{UUID: "uuid-1", Token: "token-1"}
	c := newMockClient(t, ts, ids)

	err := c.Declare(context.Background())
	if err == nil {
		t.Fatal("Declare returned nil error, want error when retry still fails")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("Declare error code = %v, want Unauthenticated", connect.CodeOf(err))
	}
	if got := registerCalls.Load(); got != 1 {
		t.Errorf("Register called %d times, want exactly 1", got)
	}
	if got := declareCalls.Load(); got != 2 {
		t.Errorf("Declare called %d times, want 2", got)
	}
}

// TestAuthFallback_PermissionDenied verifies PermissionDenied triggers the
// same re-register-and-retry path as Unauthenticated.
func TestAuthFallback_PermissionDenied(t *testing.T) {
	h, registerCalls, declareCalls, _, _ := newAuthFallbackHandler(connect.CodePermissionDenied)

	ts := startMockServer(t, h)
	defer ts.Close()

	ids := newFakeIdentities()
	ids.data[ts.URL] = state.Identity{UUID: "uuid-1", Token: "token-1"}
	c := newMockClient(t, ts, ids)

	if err := c.Declare(context.Background()); err != nil {
		t.Fatalf("Declare returned error after re-register: %v", err)
	}
	if got := registerCalls.Load(); got != 1 {
		t.Errorf("Register called %d times, want 1", got)
	}
	if got := declareCalls.Load(); got != 2 {
		t.Errorf("Declare called %d times, want 2", got)
	}
}

// TestAuthFallback_NoReRegisterOnOtherErrors verifies non-auth errors are
// returned as-is without touching Register.
func TestAuthFallback_NoReRegisterOnOtherErrors(t *testing.T) {
	var registerCalls atomic.Int32
	h := &mockHandler{
		registerFn: func(_ context.Context, _ *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			registerCalls.Add(1)
			return connect.NewResponse(&v1.RegisterResponse{Runner: &v1.Runner{Uuid: "u", Token: "t"}}), nil
		},
		declareFn: func(_ context.Context, _ *connect.Request[v1.DeclareRequest]) (*connect.Response[v1.DeclareResponse], error) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("forgejo down"))
		},
	}
	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	err := c.Declare(context.Background())
	if err == nil {
		t.Fatal("Declare returned nil error, want Unavailable error")
	}
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("Declare error code = %v, want Unavailable", connect.CodeOf(err))
	}
	if got := registerCalls.Load(); got != 0 {
		t.Errorf("Register called %d times, want 0 for non-auth error", got)
	}
}

// TestAuthFallback_ForwardUpdateTask verifies the UpdateTask path goes
// through the same re-register-and-retry flow.
func TestAuthFallback_ForwardUpdateTask(t *testing.T) {
	var registerCalls atomic.Int32
	var updateCalls atomic.Int32
	h := &mockHandler{
		registerFn: func(_ context.Context, _ *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			registerCalls.Add(1)
			return connect.NewResponse(&v1.RegisterResponse{Runner: &v1.Runner{Uuid: "uuid-new", Token: "token-new"}}), nil
		},
		updateTaskFn: func(_ context.Context, _ *connect.Request[v1.UpdateTaskRequest]) (*connect.Response[v1.UpdateTaskResponse], error) {
			if updateCalls.Add(1) == 1 {
				return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("token revoked"))
			}
			return connect.NewResponse(&v1.UpdateTaskResponse{}), nil
		},
	}
	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	resp, err := c.ForwardUpdateTask(context.Background(), &v1.UpdateTaskRequest{})
	if err != nil {
		t.Fatalf("ForwardUpdateTask returned error after re-register: %v", err)
	}
	if resp == nil {
		t.Fatal("ForwardUpdateTask returned nil response")
	}
	if got := registerCalls.Load(); got != 1 {
		t.Errorf("Register called %d times, want 1", got)
	}
	if got := updateCalls.Load(); got != 2 {
		t.Errorf("UpdateTask called %d times, want 2", got)
	}
}

// TestAuthFallback_PollLoopFetchTask verifies the FetchTask path in PollLoop
// goes through the same re-register-and-retry flow.
func TestAuthFallback_PollLoopFetchTask(t *testing.T) {
	var registerCalls atomic.Int32
	var fetchCalls atomic.Int32
	h := &mockHandler{
		registerFn: func(_ context.Context, _ *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			registerCalls.Add(1)
			return connect.NewResponse(&v1.RegisterResponse{Runner: &v1.Runner{Uuid: "uuid-new", Token: "token-new"}}), nil
		},
		fetchTaskFn: func(_ context.Context, _ *connect.Request[v1.FetchTaskRequest]) (*connect.Response[v1.FetchTaskResponse], error) {
			if fetchCalls.Add(1) == 1 {
				return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("token revoked"))
			}
			return connect.NewResponse(&v1.FetchTaskResponse{Task: &v1.Task{Id: 7}}), nil
		},
	}
	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	// Prevent tight-looping after the first task is delivered.
	c.inst.PollInterval = 10 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	taskCh := make(chan *store.TaskCtx, 1)
	pool := slots.New(1)
	go c.PollLoop(ctx, pool, taskCh)

	select {
	case taskCtx := <-taskCh:
		if taskCtx.ID != 7 {
			t.Errorf("task id = %d, want 7", taskCtx.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task on taskCh")
	}

	// The task's slot is held; release it so the test doesn't leak.
	pool.Release()

	if got := registerCalls.Load(); got != 1 {
		t.Errorf("Register called %d times, want 1", got)
	}
	if got := fetchCalls.Load(); got != 2 {
		t.Errorf("FetchTask called %d times, want 2", got)
	}
}

// ── TestAuthFallback hardening (generation dedupe, Register backoff) ────────

// expireBackoff forces the Register backoff to look expired, simulating the
// passage of the wait period, while keeping the stored wait value so the
// exponential ramp continues.
func expireBackoff(c *client) {
	c.backoffMu.Lock()
	c.backoffNext = time.Now().Add(-time.Second)
	c.backoffMu.Unlock()
}

// TestAuthFallback_ConcurrentDedupe verifies the generation dedupe: two
// concurrent RPCs that both fail authentication against the old identity
// trigger exactly one Register — the loser of the regMu race skips Register
// (the generation changed under it) and retries with the fresh identity. At
// most one new runner row is created per identity loss.
func TestAuthFallback_ConcurrentDedupe(t *testing.T) {
	var registerCalls atomic.Int32
	var oldMu sync.Mutex
	var oldCalls int
	oldGate := make(chan struct{}) // rendezvous: both old-token RPCs in flight

	h := &mockHandler{
		registerFn: func(_ context.Context, _ *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			registerCalls.Add(1)
			return connect.NewResponse(&v1.RegisterResponse{Runner: &v1.Runner{Uuid: "uuid-new", Token: "token-new"}}), nil
		},
		updateTaskFn: func(_ context.Context, req *connect.Request[v1.UpdateTaskRequest]) (*connect.Response[v1.UpdateTaskResponse], error) {
			// RPCs issued under the revoked identity fail together; RPCs
			// under the fresh identity succeed.
			if req.Header().Get("x-runner-token") != "token-1" {
				return connect.NewResponse(&v1.UpdateTaskResponse{}), nil
			}
			oldMu.Lock()
			oldCalls++
			if oldCalls == 2 {
				close(oldGate)
			}
			oldMu.Unlock()
			<-oldGate
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("token revoked"))
		},
	}

	ts := startMockServer(t, h)
	defer ts.Close()

	ids := newFakeIdentities()
	ids.data[ts.URL] = state.Identity{UUID: "uuid-1", Token: "token-1"}
	c := newMockClient(t, ts, ids)

	const n = 2
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.ForwardUpdateTask(context.Background(), &v1.UpdateTaskRequest{})
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Errorf("ForwardUpdateTask returned error after concurrent re-register: %v", err)
		}
	}
	if got := registerCalls.Load(); got != 1 {
		t.Errorf("Register called %d times, want exactly 1 for two concurrent auth failures", got)
	}
	if c.Identity() != (state.Identity{UUID: "uuid-new", Token: "token-new"}) {
		t.Errorf("identity = %+v, want the re-registered identity", c.Identity())
	}
}

// TestAuthFallback_RegisterBackoff verifies the failure backoff: while
// Register keeps failing, a second auth failure inside the backoff window is
// not followed by another Register — the original auth error is returned
// unchanged — and each consecutive failure doubles the wait (exponential
// ramp).
func TestAuthFallback_RegisterBackoff(t *testing.T) {
	var registerCalls atomic.Int32
	var failRegister atomic.Bool
	failRegister.Store(true)

	h := &mockHandler{
		registerFn: func(_ context.Context, _ *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			registerCalls.Add(1)
			if failRegister.Load() {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("forgejo down"))
			}
			return connect.NewResponse(&v1.RegisterResponse{Runner: &v1.Runner{Uuid: "uuid-new", Token: "token-new"}}), nil
		},
		declareFn: func(_ context.Context, _ *connect.Request[v1.DeclareRequest]) (*connect.Response[v1.DeclareResponse], error) {
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("token revoked"))
		},
	}

	ts := startMockServer(t, h)
	defer ts.Close()

	ids := newFakeIdentities()
	ids.data[ts.URL] = state.Identity{UUID: "uuid-1", Token: "token-1"}
	c := newMockClient(t, ts, ids)

	// First auth failure: Register attempted, fails → backoff armed at base.
	err1 := c.Declare(context.Background())
	if connect.CodeOf(err1) != connect.CodeUnauthenticated {
		t.Fatalf("first Declare error code = %v, want Unauthenticated", connect.CodeOf(err1))
	}
	if got := registerCalls.Load(); got != 1 {
		t.Fatalf("Register called %d times, want 1", got)
	}
	if c.backoffWait != registerBackoffBase {
		t.Errorf("backoff wait = %v, want base %v", c.backoffWait, registerBackoffBase)
	}

	// Second auth failure inside the backoff window: no Register, the
	// original auth error is returned unchanged.
	err2 := c.Declare(context.Background())
	if got := registerCalls.Load(); got != 1 {
		t.Errorf("Register called %d times during backoff, want still 1", got)
	}
	if connect.CodeOf(err2) != connect.CodeUnauthenticated {
		t.Errorf("second Declare error code = %v, want Unauthenticated", connect.CodeOf(err2))
	}
	if err2.Error() != err1.Error() {
		t.Errorf("second Declare error = %q, want the original error %q unchanged", err2, err1)
	}

	// Backoff expires, Register fails again → the wait doubles (ramp).
	expireBackoff(c)
	err3 := c.Declare(context.Background())
	if connect.CodeOf(err3) != connect.CodeUnauthenticated {
		t.Fatalf("third Declare error code = %v, want Unauthenticated", connect.CodeOf(err3))
	}
	if got := registerCalls.Load(); got != 2 {
		t.Errorf("Register called %d times, want 2 after backoff expiry", got)
	}
	if c.backoffWait != 2*registerBackoffBase {
		t.Errorf("backoff wait = %v, want doubled %v", c.backoffWait, 2*registerBackoffBase)
	}
	if c.backoffRemaining() <= 0 {
		t.Error("backoffRemaining() <= 0, want active backoff after failed Register")
	}
}

// TestAuthFallback_BackoffResetOnSuccess verifies a successful Register
// clears the backoff: the next auth failure triggers an immediate Register
// attempt instead of waiting out a stale backoff.
func TestAuthFallback_BackoffResetOnSuccess(t *testing.T) {
	var registerCalls atomic.Int32
	var registerFailures atomic.Int32

	h := &mockHandler{
		registerFn: func(_ context.Context, _ *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			registerCalls.Add(1)
			// Fail exactly once, then succeed.
			if registerFailures.Add(1) == 1 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("forgejo down"))
			}
			return connect.NewResponse(&v1.RegisterResponse{Runner: &v1.Runner{Uuid: "uuid-new", Token: "token-new"}}), nil
		},
		declareFn: func(_ context.Context, _ *connect.Request[v1.DeclareRequest]) (*connect.Response[v1.DeclareResponse], error) {
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("token revoked"))
		},
	}

	ts := startMockServer(t, h)
	defer ts.Close()

	ids := newFakeIdentities()
	ids.data[ts.URL] = state.Identity{UUID: "uuid-1", Token: "token-1"}
	c := newMockClient(t, ts, ids)

	// First auth failure: Register attempted, fails → backoff armed.
	if err := c.Declare(context.Background()); err == nil {
		t.Fatal("first Declare returned nil error, want auth error")
	}
	if c.backoffRemaining() <= 0 {
		t.Fatal("backoff not armed after failed Register")
	}

	// Let the backoff expire; Register succeeds this time and must clear
	// the backoff state.
	expireBackoff(c)
	if err := c.Declare(context.Background()); err == nil {
		t.Fatal("second Declare returned nil error, want auth error (retry still rejected)")
	}
	if got := registerCalls.Load(); got != 2 {
		t.Fatalf("Register called %d times, want 2 after backoff expiry", got)
	}
	c.backoffMu.Lock()
	backoffNext := c.backoffNext
	backoffWait := c.backoffWait
	c.backoffMu.Unlock()
	if !backoffNext.IsZero() || backoffWait != 0 {
		t.Errorf("backoff not reset after successful Register: next=%v wait=%v", backoffNext, backoffWait)
	}

	// Third auth failure: no backoff left, so Register is attempted
	// immediately instead of being gated.
	if err := c.Declare(context.Background()); err == nil {
		t.Fatal("third Declare returned nil error, want auth error")
	}
	if got := registerCalls.Load(); got != 3 {
		t.Errorf("Register called %d times, want 3 (immediate attempt after reset)", got)
	}
}

// TestAuthFallback_RegistrationTokenConfigError verifies the deterministic
// configuration-error path: Register failing with InvalidArgument and a
// message mentioning the registration token is logged at ERROR level with
// remediation hints and jumps straight to the maximum backoff, so subsequent
// auth failures do not re-attempt Register for registerBackoffMax.
func TestAuthFallback_RegistrationTokenConfigError(t *testing.T) {
	var registerCalls atomic.Int32

	h := &mockHandler{
		registerFn: func(_ context.Context, _ *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
			registerCalls.Add(1)
			return nil, connect.NewError(connect.CodeInvalidArgument,
				errors.New("runner registration token not found"))
		},
		declareFn: func(_ context.Context, _ *connect.Request[v1.DeclareRequest]) (*connect.Response[v1.DeclareResponse], error) {
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("token revoked"))
		},
	}

	ts := startMockServer(t, h)
	defer ts.Close()

	ids := newFakeIdentities()
	ids.data[ts.URL] = state.Identity{UUID: "uuid-1", Token: "token-1"}

	// Capture the client's own log output to assert the ERROR log line.
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	inst := testInstance(ts.URL)
	inst.TLSInsecureSkipVerify = true
	c, err := New(inst, store.NewMemStore(), ids, log)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	if err := c.Declare(context.Background()); err == nil {
		t.Fatal("Declare returned nil error, want auth error")
	}
	if got := registerCalls.Load(); got != 1 {
		t.Fatalf("Register called %d times, want 1", got)
	}
	c.backoffMu.Lock()
	backoffWait := c.backoffWait
	backoffNext := c.backoffNext
	c.backoffMu.Unlock()
	if backoffWait != registerBackoffMax {
		t.Errorf("backoff wait = %v, want max %v for config error", backoffWait, registerBackoffMax)
	}
	if time.Until(backoffNext) <= 0 {
		t.Error("backoff not armed for config error")
	}

	// Inside the (maximum) backoff window, no further Register.
	if err := c.Declare(context.Background()); err == nil {
		t.Fatal("second Declare returned nil error, want auth error")
	}
	if got := registerCalls.Load(); got != 1 {
		t.Errorf("Register called %d times during config-error backoff, want still 1", got)
	}

	out := buf.String()
	for _, want := range []string{
		`"level":"ERROR"`,
		"registration token",
		"forgejo_runner_token",
		"Forgejo UI",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q:\n%s", want, out)
		}
	}
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

	ts, c := newMockServer(t, h, newFakeIdentities())
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

	ts, c := newMockServer(t, h, newFakeIdentities())
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

// ── TestForwardUpdateTaskError ────────────────────────────────────────────────

func TestForwardUpdateTaskError(t *testing.T) {
	h := &mockHandler{
		updateTaskFn: func(_ context.Context, _ *connect.Request[v1.UpdateTaskRequest]) (*connect.Response[v1.UpdateTaskResponse], error) {
			return nil, connect.NewError(connect.CodeInternal, errors.New("test error"))
		},
	}

	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	req := &v1.UpdateTaskRequest{}
	_, err := c.ForwardUpdateTask(context.Background(), req)
	if err == nil {
		t.Fatal("expected error from ForwardUpdateTask, got nil")
	}
}

// ── TestPollLoop ───────────────────────────────────────────────────────────────

// acquireSlot checks whether the pool grants a slot within 100ms. The caller
// must Release it when true (the test helper releases immediately).
func slotFreeNow(pool *slots.Pool) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	ok := pool.Acquire(ctx) == nil
	if ok {
		pool.Release()
	}
	return ok
}

// TestPollLoop_ReleasesSlotOnError verifies that a FetchTask error releases
// the acquired slot so the loop can retry.
func TestPollLoop_ReleasesSlotOnError(t *testing.T) {
	h := &mockHandler{
		fetchTaskFn: func(_ context.Context, _ *connect.Request[v1.FetchTaskRequest]) (*connect.Response[v1.FetchTaskResponse], error) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("forgejo down"))
		},
	}
	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	// Prevent tight-looping by setting a long poll interval.
	c.inst.PollInterval = 10 * time.Second

	pool := slots.New(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskCh := make(chan *store.TaskCtx, 1)

	// Run one iteration: PollLoop acquires a slot, calls FetchTask (error),
	// releases the slot, then sleeps via wait(). We check the pool during
	// the wait.
	go c.PollLoop(ctx, pool, taskCh)

	// Wait a short time for the loop to run, hit the error, and release.
	time.Sleep(50 * time.Millisecond)

	if !slotFreeNow(pool) {
		t.Error("expected free slot after FetchTask error, but pool is still full")
	}
}

// TestPollLoop_ReleasesSlotOnEmpty verifies that an empty (nil-task)
// response releases the acquired slot.
func TestPollLoop_ReleasesSlotOnEmpty(t *testing.T) {
	h := &mockHandler{
		fetchTaskFn: func(_ context.Context, _ *connect.Request[v1.FetchTaskRequest]) (*connect.Response[v1.FetchTaskResponse], error) {
			return connect.NewResponse(&v1.FetchTaskResponse{Task: nil}), nil
		},
	}
	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	// Prevent tight-looping by setting a long poll interval.
	c.inst.PollInterval = 10 * time.Second

	pool := slots.New(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskCh := make(chan *store.TaskCtx, 1)

	go c.PollLoop(ctx, pool, taskCh)

	time.Sleep(50 * time.Millisecond)

	if !slotFreeNow(pool) {
		t.Error("expected free slot after empty FetchTask response, but pool is still full")
	}
}

// TestPollLoop_HoldsSlotOnTask verifies that a successfully fetched task
// keeps its slot until the run module releases it, and that the TaskCtx is
// tagged with the owning instance name.
func TestPollLoop_HoldsSlotOnTask(t *testing.T) {
	h := &mockHandler{
		fetchTaskFn: func(_ context.Context, _ *connect.Request[v1.FetchTaskRequest]) (*connect.Response[v1.FetchTaskResponse], error) {
			return connect.NewResponse(&v1.FetchTaskResponse{
				Task: &v1.Task{Id: 42},
			}), nil
		},
	}
	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	// Name the instance so the TaskCtx tagging can be verified.
	c.inst.Name = "forgejo-a"

	// Prevent tight-looping after the first task is delivered.
	c.inst.PollInterval = 10 * time.Second

	pool := slots.New(1)
	ctx, cancel := context.WithCancel(context.Background())

	taskCh := make(chan *store.TaskCtx, 1)

	go func() {
		c.PollLoop(ctx, pool, taskCh)
	}()

	// The poll loop should acquire a slot, fetch a task, and send it.
	var received *store.TaskCtx
	select {
	case received = <-taskCh:
		if received.ID != 42 {
			t.Errorf("task id = %d, want 42", received.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task on taskCh")
	}

	// The TaskCtx must carry the owning instance name (routing key).
	if received.Instance != "forgejo-a" {
		t.Errorf("TaskCtx.Instance = %q, want forgejo-a", received.Instance)
	}

	// After a successful fetch the slot should STILL be held.
	if slotFreeNow(pool) {
		t.Error("slot should be held for the running task, but the pool granted one")
	}

	// Cancel the context so PollLoop stops before we test the release.
	cancel()

	// Release the slot (as the run module would) and verify it frees up.
	pool.Release()
	if !slotFreeNow(pool) {
		t.Error("expected free slot after Release")
	}
}

// TestPollLoop_ReleasesSlotWhenSendBlocked verifies that when the ctx is
// cancelled while PollLoop is blocked sending the task to a full taskCh, the
// acquired slot is released and the loop exits.
func TestPollLoop_ReleasesSlotWhenSendBlocked(t *testing.T) {
	h := &mockHandler{
		fetchTaskFn: func(_ context.Context, _ *connect.Request[v1.FetchTaskRequest]) (*connect.Response[v1.FetchTaskResponse], error) {
			return connect.NewResponse(&v1.FetchTaskResponse{
				Task: &v1.Task{Id: 1},
			}), nil
		},
	}
	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	c.inst.PollInterval = 10 * time.Second

	pool := slots.New(1)
	ctx, cancel := context.WithCancel(context.Background())

	// Unbuffered taskCh: the first task blocks the send until the run
	// layer picks it up. The poll loop then fetches again and blocks on
	// the second send.
	taskCh := make(chan *store.TaskCtx)

	done := make(chan struct{})
	go func() {
		c.PollLoop(ctx, pool, taskCh)
		close(done)
	}()

	// Receive the first task (slot held for it).
	select {
	case <-taskCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first task")
	}

	// Let the loop fetch a second task and block on the send.
	time.Sleep(50 * time.Millisecond)

	// The slot of the second task must be released on cancellation.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("PollLoop did not exit after context cancellation")
	}

	// The first task still holds its slot; the second task's slot was
	// released on the cancel path. Total pool capacity is 1, so the pool
	// must now be full again (first task's slot) — release the first
	// task's slot and verify the pool frees up exactly once.
	if slotFreeNow(pool) {
		t.Error("slot of the first (unfinished) task must still be held")
	}
	pool.Release() // release the first task's slot
	if !slotFreeNow(pool) {
		t.Error("expected free slot after releasing the first task's slot")
	}
}

// TestPollLoop_ContextCancelDuringAcquire verifies PollLoop exits promptly
// when the ctx is cancelled while blocked on pool acquisition.
func TestPollLoop_ContextCancelDuringAcquire(t *testing.T) {
	c, err := New(testInstance("https://forgejo.example.com"), store.NewMemStore(), newFakeIdentities(), testLogger())
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	// Fill the pool so acquisition blocks.
	pool := slots.New(1)
	if err := pool.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	taskCh := make(chan *store.TaskCtx, 1)

	done := make(chan struct{})
	go func() {
		c.PollLoop(ctx, pool, taskCh)
		close(done)
	}()

	// Cancel the context; PollLoop should exit via ctx.Done() during
	// pool acquisition.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Good: PollLoop exited.
	case <-time.After(2 * time.Second):
		t.Fatal("PollLoop did not exit after context cancellation")
	}

	// Hygiene: release the slot this test holds.
	pool.Release()
}

// TestPollLoop_TaskCtxCarriesInstance verifies the poller tags every task
// with its own instance name even when another instance is configured with
// a different name.
func TestPollLoop_TaskCtxCarriesInstance(t *testing.T) {
	h := &mockHandler{
		fetchTaskFn: func(_ context.Context, _ *connect.Request[v1.FetchTaskRequest]) (*connect.Response[v1.FetchTaskResponse], error) {
			return connect.NewResponse(&v1.FetchTaskResponse{
				Task: &v1.Task{Id: 9},
			}), nil
		},
	}
	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	c.inst.Name = "instance-x"
	c.inst.PollInterval = 10 * time.Second

	pool := slots.New(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskCh := make(chan *store.TaskCtx, 1)
	go c.PollLoop(ctx, pool, taskCh)

	select {
	case taskCtx := <-taskCh:
		if taskCtx.Instance != "instance-x" {
			t.Errorf("TaskCtx.Instance = %q, want instance-x", taskCtx.Instance)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task on taskCh")
	}

	// The task's slot is held; release it so the test doesn't leak.
	pool.Release()
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

	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	// Use a short heartbeat interval for the test.
	c.inst.HeartbeatInterval = 50 * time.Millisecond

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

	ts, c := newMockServer(t, h, newFakeIdentities())
	defer ts.Close()

	c.inst.HeartbeatInterval = 20 * time.Millisecond

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

func TestStripContainerMapping(t *testing.T) {
	tests := []struct {
		name  string
		input []string
		want  []string
	}{
		{
			name:  "no colons",
			input: []string{"ubuntu-latest", "debian-bookworm"},
			want:  []string{"ubuntu-latest", "debian-bookworm"},
		},
		{
			name:  "with container mappings",
			input: []string{"ubuntu-latest:docker://node:20-bookworm", "docker:docker://ghcr.io/catthehacker/ubuntu:act-latest"},
			want:  []string{"ubuntu-latest", "docker"},
		},
		{
			name:  "mixed",
			input: []string{"linux", "ubuntu-latest:docker://node:20"},
			want:  []string{"linux", "ubuntu-latest"},
		},
		{
			name:  "empty input",
			input: []string{},
			want:  []string{},
		},
		{
			name:  "nil input",
			input: nil,
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripContainerMapping(tt.input)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("got[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}
