// Package run orchestrates the lifecycle of a single task — from dispatch
// through heartbeat to completion or timeout. It wires together the north
// resolver, dispatch module, shared slots pool, and store to implement the
// task arrival flow.
package run

import (
	"context"
	"log/slog"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/dispatch"
	"git.0x0f.dev/forgery/internal/north"
	"git.0x0f.dev/forgery/internal/slots"
	"git.0x0f.dev/forgery/internal/store"
)

// taskDispatcher is the subset of dispatch.Dispatcher methods that the run
// module uses.
type taskDispatcher interface {
	Trigger(ctx context.Context, taskCtx *store.TaskCtx, inst config.Instance) error
}

// sessionRemover is the subset of session.Manager that the run module needs:
// dropping a runner's session when the task fails or times out before
// reaching a terminal state. session does not import run, so wiring the
// concrete *session.Manager here creates no import cycle. The empty string
// is safe to pass — Remove is idempotent.
type sessionRemover interface {
	Remove(sessionToken string)
}

// Runner orchestrates the lifecycle of a single task. It holds references to
// the subsystems needed to dispatch a task to GitHub Actions, maintain a
// heartbeat with Forgejo while waiting for the internal runner to connect,
// and clean up when the task reaches a terminal state or times out.
//
// Every task is routed through the north.Resolver: the TaskCtx.Instance
// field selects both the instance configuration (GA startup timeout) and the
// northbound client (heartbeat, failure reporting) that owns the task.
type Runner struct {
	pool     *slots.Pool // daemon-wide backpressure pool (shared with pollers)
	dispatch taskDispatcher
	store    store.TaskStore
	resolver north.Resolver
	sessions sessionRemover // drops runner sessions on failure/timeout paths
	log      *slog.Logger
}

// New creates a Runner with the required dependencies. The dispatch
// parameter accepts the concrete *dispatch.Dispatcher, which satisfies the
// internal taskDispatcher interface; sessions accepts the concrete
// *session.Manager, which satisfies the internal sessionRemover interface.
func New(pool *slots.Pool, dp *dispatch.Dispatcher, st store.TaskStore, resolver north.Resolver, sessions sessionRemover, log *slog.Logger) *Runner {
	return &Runner{
		pool:     pool,
		dispatch: dp,
		store:    st,
		resolver: resolver,
		sessions: sessions,
		log:      log,
	}
}

// failureUpdateRequest builds an UpdateTaskRequest with RESULT_FAILURE for the given task ID.
func failureUpdateRequest(taskID int64) *v1.UpdateTaskRequest {
	return &v1.UpdateTaskRequest{
		State: &v1.TaskState{
			Id:     taskID,
			Result: v1.Result_RESULT_FAILURE,
		},
	}
}

// HandleTask orchestrates the lifecycle of a single task.
//
// Flow:
//  1. Resolve the owning instance (config + northbound client) by name.
//  2. Dispatch to GitHub Actions via workflow_dispatch API.
//  3. On success: mark dispatched, start heartbeat, wait for completion/timeout.
//  4. On failure: report failure to Forgejo, drop any session, release slot, return.
//  5. On completion (done signal from internal runner): stop heartbeat, release slot.
//  6. On GA_STARTUP_TIMEOUT: report failure, stop heartbeat, drop session, release slot, remove from store.
//  7. On context cancellation: graceful stop of heartbeat, release slot.
func (r *Runner) HandleTask(ctx context.Context, taskCtx *store.TaskCtx) {
	r.log.Info("handling task", "task_id", taskCtx.ID, "instance", taskCtx.Instance)

	// Step 0: Resolve the owning instance. A failure here is a routing
	// inconsistency (config validation guarantees every task's instance
	// exists), so this is a defensive path: there is no client to report
	// to, so log, drop the task, and release the slot.
	inst, client, ok := r.resolver.Resolve(taskCtx.Instance)
	if !ok {
		r.log.Error("task references unknown instance, dropping",
			"task_id", taskCtx.ID, "instance", taskCtx.Instance)
		r.store.Remove(taskCtx.ID)
		// Defensive: no session can exist for an unresolvable instance
		// (Register fails before creating one), but removing is idempotent
		// and keeps the invariant that this exit path leaves no session.
		r.sessions.Remove(taskCtx.SessionToken)
		r.pool.Release()
		return
	}

	// Step 1: Dispatch to GitHub Actions.
	if err := r.dispatch.Trigger(ctx, taskCtx, inst); err != nil {
		r.log.Error("dispatch failed", "task_id", taskCtx.ID, "err", err)

		// Report failure to Forgejo.
		client.ForwardUpdateTask(ctx, failureUpdateRequest(taskCtx.ID))

		// Drop any session bound to this task (normally none yet — the
		// runner cannot register before dispatch succeeds — but a race
		// with one-job auto-registration makes this possible).
		r.sessions.Remove(taskCtx.SessionToken)

		// Release backpressure slot.
		r.pool.Release()

		// Remove from store so it doesn't leak as a pending task.
		r.store.Remove(taskCtx.ID)
		return
	}

	// Step 2: Dispatch succeeded — mark as dispatched.
	taskCtx.MarkDispatched()

	// Step 3: Start heartbeat to keep the task alive with Forgejo while
	// waiting for the internal runner to connect.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go client.StartHeartbeat(hbCtx, taskCtx)

	// Step 4: Wait for completion, timeout, or global shutdown.
	// The timeout is the owning instance's GAStartupTimeout.
	timeout := time.After(inst.GAStartupTimeout)

	select {
	case <-taskCtx.Done():
		// The internal runner reached a terminal state. The south handler
		// has already forwarded the final UpdateTask, cleaned up the session,
		// and removed the task from the store.
		r.log.Info("task completed by internal runner", "task_id", taskCtx.ID)
		hbCancel()
		r.pool.Release()

	case <-timeout:
		// GA_STARTUP_TIMEOUT expired — the internal runner never connected.
		r.log.Warn("task timed out waiting for internal runner", "task_id", taskCtx.ID)

		// Stop the heartbeat.
		hbCancel()

		// Report failure to Forgejo.
		client.ForwardUpdateTask(ctx, failureUpdateRequest(taskCtx.ID))

		// Drop the runner's session if one was created before the timeout
		// (the runner registered but never reported a terminal state).
		// Without this, the session and its Running task would leak:
		// store.GC only reaps Pending/Terminal tasks. The app GC loop's
		// Expire pass is the backstop for any session this misses.
		r.sessions.Remove(taskCtx.SessionToken)

		// Clean up.
		r.pool.Release()
		r.store.Remove(taskCtx.ID)

	case <-ctx.Done():
		// Global shutdown (e.g., SIGTERM). Stop the heartbeat but leave
		// the task in the store — Forgejo will re-assign it when this
		// runner disconnects.
		r.log.Warn("task handling interrupted by shutdown", "task_id", taskCtx.ID)
		hbCancel()
		r.pool.Release()
	}
}
