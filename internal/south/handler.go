// Package south implements the southbound server — forgery acting as a Forgejo
// Actions instance for the internal forgejo-runner. It implements the
// RunnerServiceHandler interface (Connect protocol) and authenticates internal
// runners via one-time registration tokens and session bearer tokens.
//
// The south package does NOT import the north package directly. Forwarding of
// UpdateTask/UpdateLog is done through the northForwarder interface, which is
// satisfied by north.Client in the main assembly.
package south

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"
	"code.gitea.io/actions-proto-go/runner/v1/runnerv1connect"
	"connectrpc.com/connect"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/session"
	"git.0x0f.dev/forgery/internal/store"
)

// northForwarder is the interface for forwarding UpdateTask and UpdateLog
// requests to the real Forgejo instance. south does NOT import the north
// package directly — the concrete north.Client is injected by main.
//
// See also: internal/run/runner.go's northClient interface.
// Both interfaces describe overlapping subsets of the same north.Client type.
type northForwarder interface {
	ForwardUpdateTask(ctx context.Context, req *v1.UpdateTaskRequest) (*v1.UpdateTaskResponse, error)
	ForwardUpdateLog(ctx context.Context, req *v1.UpdateLogRequest) (*v1.UpdateLogResponse, error)
}

// Handler implements the Connect RunnerServiceHandler for the southbound
// server. Each method corresponds to an RPC that the internal forgejo-runner
// calls: Register, Declare, FetchTask, UpdateTask, UpdateLog.
type Handler struct {
	runnerv1connect.UnimplementedRunnerServiceHandler

	store    store.TaskStore
	sessions *session.Manager
	forward  northForwarder
	cfg      *config.Config
	log      *slog.Logger
}

// NewHandler creates a new south Handler with the given dependencies.
func NewHandler(s store.TaskStore, sm *session.Manager, fw northForwarder, cfg *config.Config, log *slog.Logger) *Handler {
	return &Handler{
		store:    s,
		sessions: sm,
		forward:  fw,
		cfg:      cfg,
		log:      log,
	}
}

// sessionTokenFromRequest extracts a session token from a Connect request.
// forgejo-runner v12+ sends the token via the x-runner-token header.
// forgejo-runner v13+ may send x-runner-uuid instead.
// Falls back to the standard Authorization: Bearer header.
// Debug logging is emitted through the provided logger.
func sessionTokenFromRequest(req connect.AnyRequest, log *slog.Logger) (string, bool) {
	// Log all relevant headers for debugging
	runnerToken := req.Header().Get("x-runner-token")
	runnerUUID := req.Header().Get("x-runner-uuid")
	authHeader := req.Header().Get("Authorization")
	log.Debug("extracting session token from request",
		"x-runner-token", runnerToken,
		"x-runner-uuid", runnerUUID,
		"Authorization", authHeader,
	)

	// forgejo-runner v12+ sends x-runner-token header
	if runnerToken != "" {
		return runnerToken, true
	}
	// forgejo-runner v13+ may send x-runner-uuid instead
	if runnerUUID != "" {
		return runnerUUID, true
	}
	// Fallback: standard Authorization: Bearer header
	if strings.HasPrefix(authHeader, "Bearer ") {
		return strings.TrimPrefix(authHeader, "Bearer "), true
	}
	return "", false
}

// Register handles the Register RPC. The internal runner presents a one-time
// registration token that forgery generated when the task was dispatched. If
// valid (exists, not expired, not already consumed), a new session is created
// and a session token is returned.
func (h *Handler) Register(ctx context.Context, req *connect.Request[v1.RegisterRequest]) (*connect.Response[v1.RegisterResponse], error) {
	regToken := req.Msg.GetToken()

	// Look up the task context by registration token.
	taskCtx, ok := h.store.GetByRegToken(regToken)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("invalid registration token"))
	}

	if h.isRegTokenExpired(taskCtx) {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("registration token expired"))
	}

	// Atomically consume the token — it can only be used once.
	if err := h.store.MarkRegTokenConsumed(regToken); err != nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("registration token already consumed"))
	}

	// Create a session (generates a session token internally).
	sess := h.sessions.Create(taskCtx, req.Msg.GetName(), req.Msg.GetLabels())

	// Mark the runner as registered (transitions status to Running).
	taskCtx.MarkRunnerRegistered()

	sessionTokenPreview := truncateToken(sess.SessionToken)
	h.log.Info("runner registered",
		"runner_name", req.Msg.GetName(),
		"task_id", taskCtx.ID,
		"token_prefix", sessionTokenPreview,
		"token_len", len(sess.SessionToken),
	)
	return connect.NewResponse(&v1.RegisterResponse{
		Runner: &v1.Runner{
			Id:        taskCtx.ID,
			Uuid:      sess.SessionToken,
			Token:     sess.SessionToken,
			Name:      req.Msg.GetName(),
			Ephemeral: true,
			Labels:    sess.Labels,
		},
	}), nil
}

// authenticate extracts the session token from the request and looks it up.
// If the token is not a session token, it tries to treat it as a registration
// token and auto-creates a session. This supports forgejo-runner v12+ one-job
// which skips the Register RPC and sends the registration token directly as
// x-runner-token in Declare/FetchTask/UpdateTask/UpdateLog.
func (h *Handler) authenticate(req connect.AnyRequest) (*session.Session, error) {
	token, ok := sessionTokenFromRequest(req, h.log)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("missing session token"))
	}

	tokenPreview := truncateToken(token)

	// Try session token first (normal path after explicit Register).
	sess, ok := h.sessions.Lookup(token)
	if ok {
		return sess, nil
	}

	// Not a session token — try registration token (forgejo-runner one-job path).
	taskCtx, ok := h.store.GetByRegToken(token)
	if !ok {
		h.log.Warn("session not found and not a valid registration token",
			"token_prefix", tokenPreview,
			"token_len", len(token),
		)
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("invalid session token"))
	}

	if h.isRegTokenExpired(taskCtx) {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("registration token expired"))
	}

	// Atomically consume the registration token.
	if err := h.store.MarkRegTokenConsumed(token); err != nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("registration token already consumed"))
	}

	// Create a session using the registration token as the session token.
	// This allows subsequent RPCs (FetchTask, UpdateTask, UpdateLog) which
	// send the same token to find the session.
	sess = h.sessions.CreateWithToken(taskCtx, token, "", nil)

	// Mark the runner as registered.
	taskCtx.MarkRunnerRegistered()

	h.log.Info("runner auto-registered (one-job mode)",
		"task_id", taskCtx.ID,
		"token_prefix", tokenPreview,
	)

	return sess, nil
}

// Declare handles the Declare RPC. The internal runner announces its version
// and labels before fetching a task. The session token from Register (or the
// registration token from auto-registration) is validated via the x-runner-token
// header or Authorization header.
func (h *Handler) Declare(ctx context.Context, req *connect.Request[v1.DeclareRequest]) (*connect.Response[v1.DeclareResponse], error) {
	sess, err := h.authenticate(req)
	if err != nil {
		return nil, err
	}

	// Update session metadata from Declare request (runner name and labels
	// are set here when Register was skipped, e.g. one-job mode).
	labels := req.Msg.GetLabels()
	if len(labels) > 0 {
		h.sessions.Update(sess.SessionToken, sess.RunnerName, labels)
		sess.Labels = labels
	}
	// Set a default runner name if none was provided (one-job mode).
	runnerName := sess.RunnerName
	if runnerName == "" {
		runnerName = "one-job-runner"
		h.sessions.Update(sess.SessionToken, runnerName, sess.Labels)
		sess.RunnerName = runnerName
	}

	h.log.Info("runner declared",
		"runner_name", sess.RunnerName,
		"version", req.Msg.GetVersion(),
		"task_id", sess.TaskCtx.ID,
	)

	return connect.NewResponse(&v1.DeclareResponse{
		Runner: &v1.Runner{
			Id:        sess.TaskCtx.ID,
			Uuid:      sess.SessionToken,
			Token:     sess.SessionToken,
			Name:      sess.RunnerName,
			Ephemeral: true,
			Labels:    sess.Labels,
		},
	}), nil
}

// FetchTask handles the FetchTask RPC. The internal runner polls for its task.
// In the one-job model, the task is returned on the first call. Subsequent
// calls return an empty response (defensive — an ephemeral runner should not
// poll again after receiving a task).
func (h *Handler) FetchTask(ctx context.Context, req *connect.Request[v1.FetchTaskRequest]) (*connect.Response[v1.FetchTaskResponse], error) {
	sess, err := h.authenticate(req)
	if err != nil {
		return nil, err
	}

	h.log.Info("task fetched",
		"task_id", sess.TaskCtx.ID,
		"runner_name", sess.RunnerName,
	)

	return connect.NewResponse(&v1.FetchTaskResponse{
		Task: sess.TaskCtx.Task,
	}), nil
}

// UpdateTask handles the UpdateTask RPC. The internal runner reports task
// status (running, success, failure, etc.). The request is transparently
// forwarded to the real Forgejo instance. If the task has reached a terminal
// state, the session and store entry are cleaned up.
func (h *Handler) UpdateTask(ctx context.Context, req *connect.Request[v1.UpdateTaskRequest]) (*connect.Response[v1.UpdateTaskResponse], error) {
	sess, err := h.authenticate(req)
	if err != nil {
		return nil, err
	}

	// Verify the task ID matches the session's task.
	taskID := req.Msg.GetState().GetId()
	if taskID != sess.TaskCtx.ID {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("task_id mismatch: expected %d, got %d", sess.TaskCtx.ID, taskID))
	}

	// Forward to the real Forgejo instance.
	resp, err := h.forward.ForwardUpdateTask(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	// If the result indicates a terminal state, clean up session and store.
	result := req.Msg.GetState().GetResult()
	if result != v1.Result_RESULT_UNSPECIFIED {
		taskCtx := sess.TaskCtx
		taskCtx.SetStatus(store.StatusTerminal)
		taskCtx.MarkDone()
		h.sessions.Remove(sess.SessionToken)
		h.store.Remove(taskCtx.ID)

		h.log.Info("task terminal, cleaned up",
			"task_id", taskCtx.ID,
			"result", result.String(),
		)
	}

	return connect.NewResponse(resp), nil
}

// UpdateLog handles the UpdateLog RPC. The internal runner streams log rows,
// which are transparently forwarded to the real Forgejo instance.
func (h *Handler) UpdateLog(ctx context.Context, req *connect.Request[v1.UpdateLogRequest]) (*connect.Response[v1.UpdateLogResponse], error) {
	sess, err := h.authenticate(req)
	if err != nil {
		return nil, err
	}

	// Verify the task ID matches the session's task.
	if req.Msg.GetTaskId() != sess.TaskCtx.ID {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("task_id mismatch: expected %d, got %d", sess.TaskCtx.ID, req.Msg.GetTaskId()))
	}

	// Forward to the real Forgejo instance.
	resp, err := h.forward.ForwardUpdateLog(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(resp), nil
}

// truncateToken truncates a string to 8 characters for safe logging.
func truncateToken(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

// isRegTokenExpired checks whether the registration token has exceeded its TTL.
func (h *Handler) isRegTokenExpired(tc *store.TaskCtx) bool {
	return time.Since(tc.CreatedAt) > h.cfg.RegTokenTTL
}

// NewServer creates an HTTP server that serves the Connect RunnerServiceHandler
// on the given address. The returned server is configured with the handler's
// mux and is ready to be started with ListenAndServe.
func NewServer(handler *Handler, addr string) *http.Server {
	mux := http.NewServeMux()
	path, h := runnerv1connect.NewRunnerServiceHandler(handler)
	// Register at both paths for compatibility with forgejo-runner
	mux.Handle(path, h)
	mux.Handle("/api/actions/", http.StripPrefix("/api/actions", h))
	return &http.Server{Addr: addr, Handler: mux}
}
