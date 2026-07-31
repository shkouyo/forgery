// Package north implements the northbound client — forgery acting as a
// Forgejo Actions runner, connecting to a real Forgejo instance via the
// Connect (gRPC-over-HTTP) protocol. It handles runner registration,
// task polling, and transparent forwarding of task status/log updates.
//
// The package exports two interfaces that decouple the south and run modules
// from the concrete client:
//
//   - Client: the forwarding surface used by south (UpdateTask/UpdateLog
//     relays) and run (heartbeat), implemented by the concrete *client
//     returned from New.
//   - Resolver: maps an instance name to its configuration and Client.
//     The implementation lives in internal/app; south and run depend only
//     on this interface and never import app.
package north

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"
	"code.gitea.io/actions-proto-go/runner/v1/runnerv1connect"
	"connectrpc.com/connect"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/state"
	"git.0x0f.dev/forgery/internal/store"
	"git.0x0f.dev/forgery/internal/version"
)

// defaultHeartbeatInterval is the heartbeat tick used when an instance has
// no heartbeat_interval configured; it mirrors the config package default.
const defaultHeartbeatInterval = 30 * time.Second

// Register failure backoff bounds. After a failed re-registration, further
// Register attempts are gated behind an exponentially growing delay — each
// consecutive failure doubles the wait — so a dead registration token cannot
// hammer Forgejo with one attempt per poll cycle (default 3s) and spam the
// logs. A successful Register resets the backoff. Deterministic configuration
// errors (the registration token was reset in Forgejo or is wrong in the
// config) jump straight to the maximum wait: no retry cadence can fix them
// until an operator intervenes.
const (
	registerBackoffBase = 30 * time.Second
	registerBackoffMax  = 5 * time.Minute
)

// Client is the northbound forwarding surface shared by the south and run
// modules. It is implemented by the concrete *client returned from New.
type Client interface {
	// ForwardUpdateTask relays an UpdateTask request from the internal
	// runner (southbound) to the real Forgejo instance (northbound).
	ForwardUpdateTask(ctx context.Context, req *v1.UpdateTaskRequest) (*v1.UpdateTaskResponse, error)
	// ForwardUpdateLog relays an UpdateLog request from the internal runner
	// to the real Forgejo instance.
	ForwardUpdateLog(ctx context.Context, req *v1.UpdateLogRequest) (*v1.UpdateLogResponse, error)
	// StartHeartbeat keeps the task alive with Forgejo until ctx is
	// cancelled (task completion, GA startup timeout, or shutdown).
	StartHeartbeat(ctx context.Context, taskCtx *store.TaskCtx)
}

// Resolver maps an instance name to its configuration and northbound client.
// The concrete implementation is assembled by internal/app before any
// goroutine starts; south and run use it to route every task to the Forgejo
// instance that owns it.
type Resolver interface {
	// Resolve returns the instance configuration and client registered
	// under name. The second return value is false when no instance with
	// that name exists (defensive path — config validation guarantees
	// every TaskCtx.Instance matches a configured instance).
	Resolve(name string) (config.Instance, Client, bool)
}

// client is the northbound client that connects to a real Forgejo instance.
// It registers as an Actions runner, polls for tasks, and forwards
// status/log updates from the internal runner back to Forgejo.
//
// The runner identity (UUID + permanent token) is held in memory and
// persisted through a state.Store keyed by the instance's forgejo_url, so a
// restart reuses the same runner instead of registering a fresh orphan.
type client struct {
	client      runnerv1connect.RunnerServiceClient
	runnerUUID  string // set from Register response, sent as x-runner-uuid header
	runnerToken string // permanent runner token from Register response, sent as x-runner-token header
	inst        config.Instance
	identities  state.Store
	store       store.TaskStore
	log         *slog.Logger // carries the "instance" attribute for multi-instance logs

	idMu  sync.Mutex // guards runnerUUID/runnerToken
	regMu sync.Mutex // serializes re-registration in retryOnAuth

	// generation counts how many times a runner identity has been issued
	// by Register — startup registration and re-registrations alike; an
	// identity restored from the state file counts as generation 1.
	// retryOnAuth uses it to deduplicate re-registration across
	// concurrent RPCs: an RPC records the generation it was issued under
	// and, on auth failure, re-checks it under regMu. If it changed,
	// another goroutine already re-registered successfully, so this RPC
	// skips Register (which would create yet another runner row) and
	// simply retries with the fresh identity. It is written by Register
	// under regMu (re-registration) or before any goroutine starts
	// (startup) and read concurrently by every RPC, hence the atomic.
	generation atomic.Uint64

	// Backoff state for failed Register attempts (see registerBackoff*
	// constants). backoffMu is a leaf lock: it may be acquired while
	// holding regMu (noteRegisterFailure/resetBackoff run under regMu)
	// but never the other way around, so no lock-order cycle is possible.
	backoffMu   sync.Mutex
	backoffNext time.Time     // zero = no backoff active (last Register succeeded or none attempted)
	backoffWait time.Duration // current backoff value; doubles on each consecutive failure
}

// Compile-time assertion: the concrete client implements the exported
// Client interface.
var _ Client = (*client)(nil)

// New creates a northbound client for a single Forgejo instance.
//
// It constructs a net/http.Client with HTTP/2 support (required for gRPC)
// and wraps it with the Connect-generated RunnerServiceClient configured
// to speak gRPC protocol. An auth interceptor is installed that injects
// x-runner-token, x-runner-version, and x-runner-uuid (once registered)
// into every outgoing request. The base URL has /api/actions appended because
// Forgejo mounts the runner service at that path prefix.
//
// identities is the identity store shared across instances; the persisted
// identity for inst.ForgejoURL is loaded up front so a restart picks up
// where the last run left off. A corrupt state file is a hard error: the
// daemon fails fast instead of silently registering a fresh (orphan) runner.
//
// The logger is decorated with the instance name so multi-instance logs can
// be told apart. Backpressure is NOT part of the client: the shared slots
// pool is passed to PollLoop by the assembler (internal/app).
func New(inst config.Instance, taskStore store.TaskStore, identities state.Store, log *slog.Logger) (*client, error) {
	if identities == nil {
		return nil, fmt.Errorf("north: nil identity store")
	}

	// Decorate the logger with the instance dimension up front so every log
	// line emitted by this client identifies its Forgejo instance.
	log = log.With("instance", inst.Name)

	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	baseTransport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: inst.TLSInsecureSkipVerify, // #nosec G402
	}
	// Enable HTTP/2 over cleartext (h2c) for non-TLS endpoints (tests, dev).
	// Production TLS connections negotiate HTTP/2 via ALPN automatically.
	if baseTransport.Protocols == nil {
		baseTransport.Protocols = new(http.Protocols)
		baseTransport.Protocols.SetHTTP1(true)
		baseTransport.Protocols.SetHTTP2(true)
	}
	baseTransport.Protocols.SetUnencryptedHTTP2(true)

	httpClient := &http.Client{Transport: baseTransport}

	c := &client{
		inst:       inst,
		identities: identities,
		store:      taskStore,
		log:        log,
	}

	// Restore the persisted identity (if any) before any RPC is sent, so
	// startup can skip Register and reuse the same runner on Forgejo.
	if id, ok, err := identities.Load(inst.ForgejoURL); err != nil {
		return nil, fmt.Errorf("north: load identity: %w", err)
	} else if ok {
		c.setIdentity(id.UUID, id.Token)
		// A restored identity is an issued identity: count it as
		// generation 1 so all concurrent RPCs share one baseline.
		c.generation.Store(1)
		c.log.Info("runner identity restored", "forgejo_url", inst.ForgejoURL)
	}

	// Auth interceptor injects runner token and version into every request.
	// Once Register completes, the runner UUID (from the response) is also
	// included. The closure captures c so it always reads the latest values.
	//
	// Register is always authenticated with the registration token, never
	// with the (possibly stale) permanent token — that is the recovery path
	// when the permanent token has been revoked.
	authInterceptor := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return connect.UnaryFunc(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("x-runner-version", version.Version)
			if strings.HasSuffix(req.Spec().Procedure, "/Register") {
				req.Header().Set("x-runner-token", c.inst.ForgejoRunnerToken)
				return next(ctx, req)
			}
			// Use the permanent runner token from Register if available,
			// otherwise fall back to the registration token.
			uuid, token := c.currentIdentity()
			if token != "" {
				req.Header().Set("x-runner-token", token)
			} else {
				req.Header().Set("x-runner-token", c.inst.ForgejoRunnerToken)
			}
			if uuid != "" {
				req.Header().Set("x-runner-uuid", uuid)
			}
			return next(ctx, req)
		})
	})

	// Forgejo mounts the runner gRPC service at /api/actions/.
	runnerURL := inst.ForgejoURL + "/api/actions"
	c.client = runnerv1connect.NewRunnerServiceClient(
		httpClient,
		runnerURL,
		connect.WithGRPC(),
		connect.WithInterceptors(authInterceptor),
	)

	return c, nil
}

// stripContainerMapping removes the container image mapping suffix
// from each label. Forgejo expects bare label names; the full
// mapping is still used for GitHub Actions dispatch.
func stripContainerMapping(labels []string) []string {
	out := make([]string, len(labels))
	for i, l := range labels {
		if idx := strings.IndexByte(l, ':'); idx >= 0 {
			out[i] = l[:idx]
		} else {
			out[i] = l
		}
	}
	return out
}

// Register calls the Forgejo Register RPC with the configured registration
// token, name, labels, and version, then persists the issued identity.
// It must be called once at startup when no persisted identity exists,
// before Declare or FetchTask.
//
// On success the runner UUID and permanent token from the response are
// saved to the identity store and used as the x-runner-uuid / x-runner-token
// headers of all subsequent requests. Register never triggers the
// auth-fallback re-registration — it IS the recovery path.
//
// Every successful Register issues a new runner identity and therefore
// advances the identity generation (see client.generation) and resets the
// failure backoff (see client.backoffNext).
func (c *client) Register(ctx context.Context) error {
	req := connect.NewRequest(&v1.RegisterRequest{
		Token:   c.inst.ForgejoRunnerToken,
		Name:    c.inst.ForgejoRunnerName,
		Labels:  stripContainerMapping(c.inst.ForgejoRunnerLabels),
		Version: version.Version,
	})
	resp, err := c.client.Register(ctx, req)
	if err != nil {
		return err
	}
	runner := resp.Msg.GetRunner()
	if runner == nil {
		return fmt.Errorf("register: response missing runner")
	}
	// Save runner UUID and permanent token for subsequent request headers.
	c.setIdentity(runner.GetUuid(), runner.GetToken())
	// Persist the identity so future starts reuse the same runner.
	if err := c.identities.Save(c.inst.ForgejoURL, state.Identity{UUID: runner.GetUuid(), Token: runner.GetToken()}); err != nil {
		return fmt.Errorf("register: persist identity: %w", err)
	}
	// A new identity generation is now active: concurrent RPCs that saw
	// auth failures against the previous generation skip re-registration
	// and just retry with this identity (see retryOnAuth).
	c.generation.Add(1)
	// Success clears the failure backoff: the next Register, if ever
	// needed, starts from the base delay again.
	c.resetBackoff()
	c.log.Info("runner registered", "runner_uuid", runner.GetUuid())
	return nil
}

// Declare announces the runner's labels and version to Forgejo.
// It must be called after a successful Register and before polling
// for tasks.
func (c *client) Declare(ctx context.Context) error {
	return c.retryOnAuth(ctx, func(ctx context.Context) error {
		_, err := c.client.Declare(ctx, connect.NewRequest(&v1.DeclareRequest{
			Labels:  stripContainerMapping(c.inst.ForgejoRunnerLabels),
			Version: version.Version,
		}))
		return err
	})
}

// ForwardUpdateTask transparently relays an UpdateTask request from the
// internal runner (southbound) to the real Forgejo instance (northbound).
// The request payload is passed through unchanged because task IDs are
// the real Forgejo task IDs.
func (c *client) ForwardUpdateTask(ctx context.Context, req *v1.UpdateTaskRequest) (*v1.UpdateTaskResponse, error) {
	var resp *connect.Response[v1.UpdateTaskResponse]
	err := c.retryOnAuth(ctx, func(ctx context.Context) error {
		var err error
		resp, err = c.client.UpdateTask(ctx, connect.NewRequest(req))
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ForwardUpdateLog transparently relays an UpdateLog request from the
// internal runner to the real Forgejo instance. As with ForwardUpdateTask,
// the payload is passed through unchanged.
func (c *client) ForwardUpdateLog(ctx context.Context, req *v1.UpdateLogRequest) (*v1.UpdateLogResponse, error) {
	var resp *connect.Response[v1.UpdateLogResponse]
	err := c.retryOnAuth(ctx, func(ctx context.Context) error {
		var err error
		resp, err = c.client.UpdateLog(ctx, connect.NewRequest(req))
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// StartHeartbeat sends periodic UpdateTask(state=running) calls to Forgejo
// for the given task until ctx is cancelled. This prevents Forgejo from
// marking the task as stalled during the window between a successful
// workflow_dispatch and the internal runner connecting to pick up the task.
//
// The heartbeat uses inst.HeartbeatInterval as the tick period. Errors are
// logged but do not stop the heartbeat — Forgejo tolerates missed heartbeats
// within a grace period.
//
// Callers (the run module) must cancel ctx when the internal runner
// connects and sends its own UpdateTask, or when the GA startup timeout
// expires.
func (c *client) StartHeartbeat(ctx context.Context, taskCtx *store.TaskCtx) {
	interval := c.inst.HeartbeatInterval
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Build the UpdateTask request once — it never changes across ticks.
	req := &v1.UpdateTaskRequest{
		State: &v1.TaskState{
			Id:     taskCtx.ID,
			Result: v1.Result_RESULT_UNSPECIFIED, // non-terminal = running
		},
	}

	c.log.Debug("heartbeat started", "task_id", taskCtx.ID, "interval", interval)

	for {
		select {
		case <-ctx.Done():
			c.log.Debug("heartbeat stopped", "task_id", taskCtx.ID, "reason", ctx.Err())
			return
		case <-ticker.C:
			if err := c.retryOnAuth(ctx, func(ctx context.Context) error {
				_, err := c.client.UpdateTask(ctx, connect.NewRequest(req))
				return err
			}); err != nil {
				c.log.Warn("heartbeat UpdateTask failed", "task_id", taskCtx.ID, "err", err)
			}
		}
	}
}

// ── identity helpers ──

// HasIdentity reports whether a runner identity is currently held (either
// restored from the identity store or set by a successful Register).
// Callers skip Register at startup when this is true.
func (c *client) HasIdentity() bool {
	uuid, token := c.currentIdentity()
	return uuid != "" && token != ""
}

// Identity returns the currently held runner identity, or the zero value
// when no identity is set.
func (c *client) Identity() state.Identity {
	uuid, token := c.currentIdentity()
	return state.Identity{UUID: uuid, Token: token}
}

// setIdentity records the runner identity for outgoing request headers.
func (c *client) setIdentity(uuid, token string) {
	c.idMu.Lock()
	c.runnerUUID = uuid
	c.runnerToken = token
	c.idMu.Unlock()
}

// currentIdentity returns the runner identity for outgoing request headers.
func (c *client) currentIdentity() (uuid, token string) {
	c.idMu.Lock()
	defer c.idMu.Unlock()
	return c.runnerUUID, c.runnerToken
}

// ── auth-fallback ──

// retryOnAuth runs fn once and, if it fails with an authentication or
// permission error, re-registers the runner once and retries fn a single
// time. This recovers from a revoked or rotated runner token without a
// daemon restart.
//
// The recovery path is hardened against concurrent and repeated failures:
//
//   - Deduplication: the identity generation is recorded when the RPC is
//     issued and re-checked under regMu. If another concurrent RPC already
//     re-registered (generation changed), Register is skipped — it would
//     create a second new runner row for the same token failure — and fn is
//     retried directly with the fresh identity. At most one new runner row
//     is created per identity loss.
//   - Backoff: when Register itself keeps failing, attempts are gated by an
//     exponential backoff (registerBackoffBase → registerBackoffMax). While
//     backing off, fn is not retried — the old identity is rejected, so the
//     retry would fail too — and the original auth error is returned
//     unchanged; callers retry on their own schedule (poll cycle, heartbeat
//     tick). A successful Register resets the backoff.
//   - Deterministic configuration errors: an InvalidArgument Register error
//     whose message mentions the registration token means the token was
//     reset in Forgejo or is wrong in the config. It is logged at ERROR
//     level with remediation hints and jumps straight to the maximum
//     backoff instead of ramping up exponentially.
//
// Register itself never goes through this path — it IS the recovery path.
func (c *client) retryOnAuth(ctx context.Context, fn func(context.Context) error) error {
	// Record the identity generation this RPC was issued under. If it
	// changes while the RPC is in flight, another goroutine has already
	// re-registered, so this call must not Register again.
	gen := c.generation.Load()

	err := fn(ctx)
	if err == nil || !isAuthError(err) {
		return err
	}

	c.regMu.Lock()

	// Concurrent re-registration already produced a fresh identity:
	// skip Register (one new runner row per identity loss) and retry fn
	// with the new identity.
	if c.generation.Load() != gen {
		c.regMu.Unlock()
		c.log.Info("runner identity refreshed by concurrent RPC, retrying", "err", err)
		return fn(ctx)
	}

	// Register is in backoff after a recent failure: do not hammer
	// Forgejo — the old identity would be rejected again anyway. Return
	// the original auth error without retrying fn.
	if wait := c.backoffRemaining(); wait > 0 {
		c.regMu.Unlock()
		c.log.Warn("skipping runner re-registration, Register is backing off",
			"retry_in", wait.Round(time.Second), "err", err)
		return err
	}

	c.log.Warn("RPC rejected by Forgejo, re-registering runner", "err", err)
	regErr := c.Register(ctx)
	if regErr != nil {
		c.noteRegisterFailure(regErr)
		c.regMu.Unlock()
		// The register failure is logged with its backoff schedule;
		// return the original auth error so callers see the RPC-level
		// failure and retry on their own schedule.
		return err
	}
	c.regMu.Unlock()
	c.log.Info("runner re-registered after auth failure")
	return fn(ctx)
}

// isAuthError reports whether err is an Unauthenticated or PermissionDenied
// connect error — the signals Forgejo sends when the runner token is
// rejected. Register itself is exempt from this check because it never goes
// through retryOnAuth.
func isAuthError(err error) bool {
	if err == nil {
		return false
	}
	switch connect.CodeOf(err) {
	case connect.CodeUnauthenticated, connect.CodePermissionDenied:
		return true
	}
	return false
}

// ── Register failure backoff ──

// registrationTokenErrorSubstr is the message fragment Forgejo includes in
// the Register error when the registration token is unknown or has been
// invalidated (e.g. "runner registration token not found" / "...has been
// invalidated, please use the latest one"). Together with the
// InvalidArgument code it identifies a deterministic configuration problem
// rather than a transient failure.
const registrationTokenErrorSubstr = "registration token"

// isRegistrationTokenError reports whether err is the deterministic
// configuration error Forgejo returns for an unknown or invalidated
// registration token. Error code and message are read the same way as
// everywhere else in this package: connect.CodeOf plus the string form of
// the error.
func isRegistrationTokenError(err error) bool {
	return connect.CodeOf(err) == connect.CodeInvalidArgument &&
		strings.Contains(err.Error(), registrationTokenErrorSubstr)
}

// backoffRemaining returns how long Register must stay quiet, or 0 when
// re-registration is allowed. Thread-safe (backoffMu is a leaf lock).
func (c *client) backoffRemaining() time.Duration {
	c.backoffMu.Lock()
	defer c.backoffMu.Unlock()
	if c.backoffNext.IsZero() {
		return 0
	}
	if wait := time.Until(c.backoffNext); wait > 0 {
		return wait
	}
	return 0
}

// noteRegisterFailure records a failed Register attempt and arms the
// backoff. Deterministic configuration errors (invalid registration token)
// jump straight to registerBackoffMax — no retry cadence helps until the
// operator fixes the token — while transient failures double the previous
// wait (registerBackoffBase first, capped at registerBackoffMax). Called
// under regMu; takes backoffMu (leaf).
func (c *client) noteRegisterFailure(regErr error) {
	if isRegistrationTokenError(regErr) {
		c.setBackoff(registerBackoffMax)
		c.log.Error("runner re-registration failed: registration token invalid",
			"err", regErr,
			"hint", "check forgejo_runner_token in the config, or reset the registration token in the Forgejo UI",
			"retry_in", registerBackoffMax.Round(time.Second))
		return
	}
	c.backoffMu.Lock()
	wait := c.backoffWait
	c.backoffMu.Unlock()
	if wait == 0 {
		wait = registerBackoffBase
	} else {
		wait *= 2
		if wait > registerBackoffMax {
			wait = registerBackoffMax
		}
	}
	c.setBackoff(wait)
	c.log.Warn("runner re-registration failed, backing off",
		"err", regErr, "retry_in", wait.Round(time.Second))
}

// setBackoff arms the backoff so the next Register attempt is at least wait
// away, and stores wait as the current backoff value for the exponential
// ramp.
func (c *client) setBackoff(wait time.Duration) {
	c.backoffMu.Lock()
	c.backoffWait = wait
	c.backoffNext = time.Now().Add(wait)
	c.backoffMu.Unlock()
}

// resetBackoff clears the backoff state after a successful Register.
func (c *client) resetBackoff() {
	c.backoffMu.Lock()
	c.backoffWait = 0
	c.backoffNext = time.Time{}
	c.backoffMu.Unlock()
}
