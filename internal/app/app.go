// Package app assembles every subsystem and implements the daemon lifecycle:
// per-instance northbound registration (fail-fast), serving, and the graceful
// shutdown sequence. It is the only package that knows how instances map to
// northbound clients — south and run consume that mapping through the
// north.Resolver interface.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/dispatch"
	"git.0x0f.dev/forgery/internal/north"
	"git.0x0f.dev/forgery/internal/observability"
	"git.0x0f.dev/forgery/internal/run"
	"git.0x0f.dev/forgery/internal/session"
	"git.0x0f.dev/forgery/internal/slots"
	"git.0x0f.dev/forgery/internal/south"
	"git.0x0f.dev/forgery/internal/state"
	"git.0x0f.dev/forgery/internal/store"
)

// gcInterval is how often the periodic garbage-collection pass runs.
// One minute is short enough that orphaned sessions and expired Pending
// tasks are reaped within a minute of reaching their deadline, yet the
// per-tick cost (two map scans over a handful of tasks) is negligible for
// a daemon. A shorter interval would add noise without tightening the
// system's effective memory bound meaningfully.
const gcInterval = 1 * time.Minute

// httpShutdownTimeout is the shared graceful-shutdown budget for the south
// and health HTTP servers (steps 3+4 of the shutdown sequence).
const httpShutdownTimeout = 5 * time.Second

// drainPollInterval is how often the drain phase re-checks the active task
// count while waiting for active tasks to finish during graceful shutdown.
const drainPollInterval = 1 * time.Second

// tokenPreviewLength is how many leading characters of a session token are
// kept when logging a truncated preview; full tokens never appear in logs.
const tokenPreviewLength = 8

// instanceEntry binds one configured Forgejo instance to its northbound
// client. Entries are inserted before any goroutine starts and never
// modified afterwards, so the resolver map needs no locking.
type instanceEntry struct {
	inst   config.Instance
	client north.Client
}

// instanceResolver implements north.Resolver over a static map. It is
// populated once at startup (after config validation has guaranteed unique
// instance names) and read-only afterwards.
type instanceResolver map[string]instanceEntry

// Resolve returns the instance configuration and northbound client for name.
func (r instanceResolver) Resolve(name string) (config.Instance, north.Client, bool) {
	e, ok := r[name]
	if !ok {
		return config.Instance{}, nil, false
	}
	return e.inst, e.client, true
}

// poller is the subset of the northbound client used to start polling. The
// concrete type returned by north.New is unexported; app only needs this
// structural interface.
type poller interface {
	PollLoop(ctx context.Context, pool *slots.Pool, taskCh chan<- *store.TaskCtx)
}

// Run assembles the daemon and blocks until a SIGINT/SIGTERM signal arrives,
// then performs the graceful shutdown sequence and returns nil. Any startup
// failure (config already validated by the caller, identity store errors,
// Register/Declare failures) is returned as a descriptive error — the caller
// decides how to exit.
//
// Shutdown sequence:
//  1. Cancel the daemon context: pollers stop fetching, HandleTask aborts.
//  2. Drain active tasks, polling taskStore.CountActive until zero or until
//     the drain timeout (max of all instances' GAStartupTimeout) elapses.
//  3. Shut down the south HTTP server gracefully (httpShutdownTimeout).
//  4. Shut down the health HTTP server gracefully (same budget).
func Run(cfg *config.Config, log *slog.Logger) error {
	// ── shared stores and backpressure pool ──
	// One MemStore and one identity state file serve all instances: tasks
	// are keyed by the real Forgejo task id, identities by forgejo_url.
	taskStore := store.NewMemStore()
	identities := state.NewFileStore(cfg.Global.StateFile)
	pool := slots.New(cfg.Global.MaxParallelTasks)
	// ── per-instance northbound clients: register/declare, fail-fast ──
	resolver := make(instanceResolver)
	var pollers []poller
	regCtx := context.Background()
	for i := range cfg.Instances {
		inst := cfg.Instances[i]
		instLog := log.With("instance", inst.Name)

		// north.New decorates the logger with the instance attribute
		// itself; pass the undecorated logger to avoid a duplicate.
		c, err := north.New(inst, taskStore, identities, log)
		if err != nil {
			return fmt.Errorf("instance %q: north client init: %w", inst.Name, err)
		}

		// Register only when no persisted identity exists; Declare always
		// confirms connectivity and refreshes labels/version.
		if c.HasIdentity() {
			instLog.Info("runner identity found, skipping registration", "runner_uuid", c.Identity().UUID)
		} else {
			instLog.Info("registering runner with Forgejo")
			if err := c.Register(regCtx); err != nil {
				return fmt.Errorf("instance %q: runner registration failed: %w", inst.Name, err)
			}
		}
		instLog.Info("declaring runner labels and version")
		if err := c.Declare(regCtx); err != nil {
			return fmt.Errorf("instance %q: runner declaration failed: %w", inst.Name, err)
		}

		resolver[inst.Name] = instanceEntry{inst: inst, client: c}
		pollers = append(pollers, c)
	}

	// ── subsystem assembly ──
	dispatcher := dispatch.NewDispatcher(dispatch.GitHub{
		Token:      cfg.Global.GitHubToken,
		Repo:       cfg.Global.GitHubRepo,
		WorkflowID: cfg.Global.GitHubWorkflowID,
		Ref:        cfg.Global.GitHubRef,
		APIURL:     cfg.Global.GitHubAPIURL,
		PublicURL:  cfg.Global.PublicURL,
	}, log)
	sessionMgr := session.NewManager()
	southHandler := south.NewHandler(taskStore, sessionMgr, resolver, log)
	srv := south.NewServer(southHandler, cfg.Global.ListenAddr)
	runner := run.New(pool, dispatcher, taskStore, resolver, sessionMgr, log)

	// ── background services ──
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Periodic GC: reaps expired Pending/old Terminal tasks from the store
	// and expires orphaned runner sessions (see gcLoop and sessionMaxAge).
	// It exits on ctx cancellation and does not block graceful shutdown.
	go gcLoop(ctx, gcInterval, taskStore, sessionMgr, sessionMaxAge(cfg), log)

	// One poller per instance, all acquiring from the same shared pool and
	// feeding the same task channel.
	taskCh := make(chan *store.TaskCtx, cfg.Global.MaxParallelTasks)
	for _, p := range pollers {
		go p.PollLoop(ctx, pool, taskCh)
	}

	// Single dispatcher goroutine: fans each task out to its own HandlerTask.
	go func() {
		for taskCtx := range taskCh {
			go runner.HandleTask(ctx, taskCtx)
		}
	}()

	go func() {
		log.Info("south HTTP server starting", "addr", cfg.Global.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("south HTTP server error", "err", err)
		}
	}()

	healthSrv := observability.StartHealthServer(cfg.Global.HealthAddr, observability.NewHealthChecker())

	// ── wait for shutdown signal ──
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info("received signal, starting graceful shutdown", "signal", sig.String())

	// ── graceful shutdown sequence ──

	// Step 1: stop accepting new work.
	cancel()

	// Step 2: wait for active tasks to drain (with timeout = max of all
	// instances' GAStartupTimeout, per design decision 5).
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), maxGAStartupTimeout(cfg))
	defer shutdownCancel()

	ticker := time.NewTicker(drainPollInterval)
	defer ticker.Stop()

drain:
	for {
		active := taskStore.CountActive()
		if active == 0 {
			log.Info("all tasks drained")
			break
		}
		select {
		case <-ticker.C:
			log.Info("waiting for tasks to drain", "active", active)
		case <-shutdownCtx.Done():
			log.Warn("shutdown timeout, forcing exit", "remaining", active)
			break drain
		}
	}

	// Step 3+4: shut down the south HTTP server and the health server
	// gracefully with a shared httpShutdownTimeout budget.
	httpShutdownCtx, httpCancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	defer httpCancel()
	if err := srv.Shutdown(httpShutdownCtx); err != nil {
		log.Error("south HTTP server shutdown error", "err", err)
	}
	if healthSrv != nil {
		if err := healthSrv.Shutdown(httpShutdownCtx); err != nil {
			log.Error("health server shutdown error", "err", err)
		}
	}

	log.Info("forgery stopped gracefully")
	return nil
}

// maxGAStartupTimeout returns the largest GAStartupTimeout across all
// configured instances; it bounds the task-drain phase of shutdown.
func maxGAStartupTimeout(cfg *config.Config) time.Duration {
	var max time.Duration
	for _, inst := range cfg.Instances {
		if inst.GAStartupTimeout > max {
			max = inst.GAStartupTimeout
		}
	}
	return max
}

// sessionMaxAge returns how old a runner session's last activity may be
// before the GC loop expires it.
//
// Semantics: a session is the runtime credential of a task. In the healthy
// flow the session's lifetime is bounded by the task's own: south removes it
// when the runner reports a terminal UpdateTask, and run.HandleTask removes
// it in the GA_STARTUP_TIMEOUT branch. Expiry is judged against
// LastActivity, which every authenticated RPC refreshes via sessions.Touch:
// an active runner's session is continuously renewed and never reaches
// maxAge, no matter how long the task runs. The ×2 factor absorbs the GC tick
// period and cleanup races (e.g. a one-job auto-registration landing a
// moment after HandleTask already removed the task), guaranteeing Expire
// only ever fires on genuinely orphaned sessions: a runner that registered
// and went silent without a terminal UpdateTask, or a HandleTask that exited
// without cleaning up.
func sessionMaxAge(cfg *config.Config) time.Duration {
	return 2 * maxGAStartupTimeout(cfg)
}

// gcOnce performs one garbage-collection pass: (a) store.GC reaps expired
// Pending tasks and Terminal tasks past their retention; (b) Expire drops
// sessions whose last activity is older than maxAge — i.e. runners that
// registered but went silent — and for each of them signals the task
// terminal (MarkDone releases the backpressure slot if HandleTask is still
// waiting on it) and removes the task from the store. Active sessions never
// reach maxAge because every authenticated RPC refreshes LastActivity.
func gcOnce(now time.Time, st store.TaskStore, sessions *session.Manager, maxAge time.Duration, log *slog.Logger) {
	// (a) Store GC: expired Pending / retained Terminal tasks.
	st.GC(now)

	// (b) Session expiry: orphaned sessions are reaped and their tasks are
	// forced terminal. MarkDone is idempotent (safe against the south
	// terminal path and HandleTask racing us), and Remove is a no-op if the
	// task is already gone.
	expired := sessions.Expire(now, maxAge)
	for _, s := range expired {
		taskCtx := s.TaskCtx
		log.Warn("expiring orphaned runner session",
			"instance", taskCtx.Instance,
			"task_id", taskCtx.ID,
			"session_token_prefix", tokenPrefix(s.SessionToken),
		)
		taskCtx.MarkDone()
		st.Remove(taskCtx.ID)
	}
}

// gcLoop runs gcOnce every interval until ctx is cancelled, then returns.
// It is the background lifecycle safeguard for tasks and sessions that no
// other subsystem will clean up (the store GC was previously only exercised
// by tests, and sessions had no expiry at all).
func gcLoop(ctx context.Context, interval time.Duration, st store.TaskStore, sessions *session.Manager, maxAge time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			gcOnce(now, st, sessions, maxAge, log)
		}
	}
}

// tokenPrefix truncates a session token to tokenPreviewLength characters for
// safe logging; the empty string is returned as-is.
func tokenPrefix(token string) string {
	if len(token) > tokenPreviewLength {
		return token[:tokenPreviewLength]
	}
	return token
}
