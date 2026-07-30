// Package run orchestrates the lifecycle of a single task — from dispatch
// through heartbeat to completion or timeout. It wires together the north
// client, dispatch module, and store to implement the task arrival flow
// described in DETAIL-DESIGN §4.2.
package run

import (
	"context"
	"log/slog"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/dispatch"
	"git.0x0f.dev/forgery/internal/north"
	"git.0x0f.dev/forgery/internal/store"
)

// northClient is the subset of north.Client methods that the run module uses.
// Using an interface allows mocking in tests.
//
// See also: internal/south/handler.go's northForwarder interface.
// Both interfaces describe overlapping subsets of the same north.Client type.
type northClient interface {
	ForwardUpdateTask(ctx context.Context, req *v1.UpdateTaskRequest) (*v1.UpdateTaskResponse, error)
	ReleaseSlot()
	StartHeartbeat(ctx context.Context, taskCtx *store.TaskCtx)
}

// taskDispatcher is the subset of dispatch.Dispatcher methods that the run
// module uses.
type taskDispatcher interface {
	Trigger(ctx context.Context, taskCtx *store.TaskCtx) error
}

// Runner orchestrates the lifecycle of a single task. It holds references to
// the subsystems needed to dispatch a task to GitHub Actions, maintain a
// heartbeat with Forgejo while waiting for the internal runner to connect,
// and clean up when the task reaches a terminal state or times out.
type Runner struct {
	north    northClient
	dispatch taskDispatcher
	store    store.TaskStore
	cfg      *config.Config
	log      *slog.Logger
}

// New creates a Runner with the required dependencies. The north and dispatch
// parameters accept concrete types (*north.Client and *dispatch.Dispatcher)
// which satisfy the internal northClient and taskDispatcher interfaces.
func New(cfg *config.Config, nc *north.Client, dp *dispatch.Dispatcher, st store.TaskStore, log *slog.Logger) *Runner {
	return &Runner{
		north:    nc,
		dispatch: dp,
		store:    st,
		cfg:      cfg,
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
//  1. Dispatch to GitHub Actions via workflow_dispatch API.
//  2. On success: mark dispatched, start heartbeat, wait for completion/timeout.
//  3. On failure: report failure to Forgejo, release slot, return.
//  4. On completion (done signal from internal runner): stop heartbeat, release slot.
//  5. On GA_STARTUP_TIMEOUT: report failure, stop heartbeat, release slot, remove from store.
//  6. On context cancellation: graceful stop of heartbeat, release slot.
func (r *Runner) HandleTask(ctx context.Context, taskCtx *store.TaskCtx) {
	r.log.Info("handling task", "task_id", taskCtx.ID)

	// Step 1: Dispatch to GitHub Actions.
	if err := r.dispatch.Trigger(ctx, taskCtx); err != nil {
		r.log.Error("dispatch failed", "task_id", taskCtx.ID, "err", err)

		// Report failure to Forgejo.
		r.north.ForwardUpdateTask(ctx, failureUpdateRequest(taskCtx.ID))

		// Release backpressure slot.
		r.north.ReleaseSlot()

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
	go r.north.StartHeartbeat(hbCtx, taskCtx)

	// Step 4: Wait for completion, timeout, or global shutdown.
	timeout := time.After(r.cfg.GAStartupTimeout)

	select {
	case <-taskCtx.Done():
		// The internal runner reached a terminal state. The south handler
		// has already forwarded the final UpdateTask, cleaned up the session,
		// and removed the task from the store.
		r.log.Info("task completed by internal runner", "task_id", taskCtx.ID)
		hbCancel()
		r.north.ReleaseSlot()

	case <-timeout:
		// GA_STARTUP_TIMEOUT expired — the internal runner never connected.
		r.log.Warn("task timed out waiting for internal runner", "task_id", taskCtx.ID)

		// Stop the heartbeat.
		hbCancel()

		// Report failure to Forgejo.
		r.north.ForwardUpdateTask(ctx, failureUpdateRequest(taskCtx.ID))

		// Clean up.
		r.north.ReleaseSlot()
		r.store.Remove(taskCtx.ID)

	case <-ctx.Done():
		// Global shutdown (e.g., SIGTERM). Stop the heartbeat but leave
		// the task in the store — Forgejo will re-assign it when this
		// runner disconnects.
		r.log.Warn("task handling interrupted by shutdown", "task_id", taskCtx.ID)
		hbCancel()
		r.north.ReleaseSlot()
	}
}
