// Package south implements the southbound server — forgery acting as a Forgejo
// Actions instance for the internal forgejo-runner. It implements the
// RunnerServiceHandler interface (Connect protocol) and authenticates internal
// runners via one-time registration tokens and session bearer tokens.
//
// Routing: every registration token belongs to a task owned by exactly one
// Forgejo instance. Registration-token expiry is judged purely from the task
// itself (TaskCtx.RegTokenTTL, stamped by the poller at creation), so south
// depends on the north.Resolver interface only to map the task's instance
// name to its northbound client (UpdateTask/UpdateLog forwarding); it never
// imports the concrete north client type nor internal/app.
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
	"git.0x0f.dev/forgery/internal/north"
	"git.0x0f.dev/forgery/internal/session"
	"git.0x0f.dev/forgery/internal/store"
)

// Handler implements the Connect RunnerServiceHandler for the southbound
// server. Each method corresponds to an RPC that the internal forgejo-runner
// calls: Register, Declare, FetchTask, UpdateTask, UpdateLog.
type Handler struct {
	runnerv1connect.UnimplementedRunnerServiceHandler

	store    store.TaskStore
	sessions *session.Manager
	resolver north.Resolver // instance name → config + northbound client
	log      *slog.Logger
}

// NewHandler creates a new south Handler with the given dependencies.
func NewHandler(s store.TaskStore, sm *session.Manager, resolver north.Resolver, log *slog.Logger) *Handler {
	return &Handler{
		store:    s,
		sessions: sm,
		resolver: resolver,
		log:      log,
	}
}

// resolveInstance looks up the instance configuration and northbound client
// that own the given task. The last return value is false when the task's
// instance name is unknown — a defensive path that returns CodeInternal to
// the caller: the token is valid, but the daemon cannot route it.
func (h *Handler) resolveInstance(taskCtx *store.TaskCtx) (config.Instance, north.Client, bool) {
	inst, forwarder, ok := h.resolver.Resolve(taskCtx.Instance)
	if !ok {
		h.log.Error("task references unknown instance",
			"task_id", taskCtx.ID, "instance", taskCtx.Instance)
	}
	return inst, forwarder, ok
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

	// The token's deadline is per task: the poller stamped
	// taskCtx.RegTokenTTL at creation, so no instance lookup is needed
	// here — the task is the single source of truth.
	if isRegTokenExpired(taskCtx) {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("registration token expired"))
	}

	// Atomically consume the token — it can only be used once.
	if !h.store.MarkRegTokenConsumed(regToken) {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("registration token already consumed"))
	}

	// Create a session (generates a session token internally).
	sess := h.sessions.Create(taskCtx, req.Msg.GetName(), req.Msg.GetLabels())

	// Mark the runner as registered (transitions status to Running).
	taskCtx.MarkRunnerRegistered()

	sessionTokenPreview := truncateToken(sess.SessionToken)
	h.log.Info("runner registered",
		"instance", taskCtx.Instance,
		"runner_name", req.Msg.GetName(),
		"task_id", taskCtx.ID,
		"token_prefix", sessionTokenPreview,
		"token_len", len(sess.SessionToken),
	)
	// Runner.Id deliberately reuses the task ID. This is a compatibility
	// hack: forgejo-runner (one-job) aligns the Runner.Id from the Register
	// response with the State.Id it reports in UpdateTask — the task ID the
	// internal runner reports back must equal the Runner Id it received
	// here. Any other value would make its first UpdateTask fail the south
	// task_id match check.
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
		// Refresh the session's LastActivity so the GC loop's Expire pass
		// never reaps a session whose runner is actively talking to us:
		// every authenticated RPC extends the orphan deadline.
		if !h.sessions.Touch(token) {
			// The session was removed between Lookup and Touch (e.g. the
			// terminal UpdateTask path or the GC loop). Treat the request
			// as unauthenticated.
			h.log.Warn("session vanished during authentication",
				"instance", sess.TaskCtx.Instance,
				"token_prefix", tokenPreview,
				"token_len", len(token),
			)
			return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("invalid session token"))
		}
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

	if isRegTokenExpired(taskCtx) {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("registration token expired"))
	}

	// Atomically consume the registration token.
	if !h.store.MarkRegTokenConsumed(token) {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("registration token already consumed"))
	}

	// Create a session using the registration token as the session token.
	// This allows subsequent RPCs (FetchTask, UpdateTask, UpdateLog) which
	// send the same token to find the session. The freshly created session
	// has LastActivity initialized to now; Touch below keeps the contract
	// that every authenticated RPC refreshes the orphan deadline.
	sess = h.sessions.CreateWithToken(taskCtx, token, "", nil)
	h.sessions.Touch(token)

	// Mark the runner as registered.
	taskCtx.MarkRunnerRegistered()

	h.log.Info("runner auto-registered (one-job mode)",
		"instance", taskCtx.Instance,
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
	// are set here when Register was skipped, e.g. one-job mode). This is
	// the only write path for these fields: the session pointer returned
	// by authenticate is owned by this handler, so the direct field
	// assignment is safe without a manager lock.
	labels := req.Msg.GetLabels()
	if len(labels) > 0 {
		sess.Labels = labels
	}
	// Set a default runner name if none was provided (one-job mode).
	runnerName := sess.RunnerName
	if runnerName == "" {
		runnerName = "one-job-runner"
		sess.RunnerName = runnerName
	}

	h.log.Info("runner declared",
		"instance", sess.TaskCtx.Instance,
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
		"instance", sess.TaskCtx.Instance,
		"task_id", sess.TaskCtx.ID,
		"runner_name", sess.RunnerName,
	)

	return connect.NewResponse(&v1.FetchTaskResponse{
		Task: sess.TaskCtx.Task,
	}), nil
}

// UpdateTask handles the UpdateTask RPC. The internal runner reports task
// status (running, success, failure, etc.). The request is transparently
// forwarded to the real Forgejo instance that owns the task. If the task has
// reached a terminal state, the session and store entry are cleaned up.
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

	// Resolve the owning instance and forward to its Forgejo.
	_, forwarder, ok := h.resolveInstance(sess.TaskCtx)
	if !ok {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("task %d belongs to unknown instance %q", taskID, sess.TaskCtx.Instance))
	}

	resp, err := forwarder.ForwardUpdateTask(ctx, req.Msg)
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
			"instance", taskCtx.Instance,
			"task_id", taskCtx.ID,
			"result", result.String(),
		)
	}

	return connect.NewResponse(resp), nil
}

// UpdateLog handles the UpdateLog RPC. The internal runner streams log rows,
// which are transparently forwarded to the real Forgejo instance that owns
// the task.
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

	// Resolve the owning instance and forward to its Forgejo.
	_, forwarder, ok := h.resolveInstance(sess.TaskCtx)
	if !ok {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("task %d belongs to unknown instance %q", sess.TaskCtx.ID, sess.TaskCtx.Instance))
	}

	resp, err := forwarder.ForwardUpdateLog(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(resp), nil
}

// tokenPreviewLength is how many leading characters of a token are kept
// when logging a truncated preview; full tokens never appear in logs.
const tokenPreviewLength = 8

// truncateToken truncates a string to tokenPreviewLength characters for
// safe logging.
func truncateToken(s string) string {
	if len(s) > tokenPreviewLength {
		return s[:tokenPreviewLength]
	}
	return s
}

// isRegTokenExpired checks whether the task's registration token has
// outlived its TTL. The TTL is read from the task itself — the poller
// stamped RegTokenTTL once at creation from the owning instance's
// reg_token_ttl — making the task the single source of truth shared with
// the store's Pending GC, so the two can never disagree.
func isRegTokenExpired(tc *store.TaskCtx) bool {
	return time.Since(tc.CreatedAt) > tc.RegTokenTTL
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
