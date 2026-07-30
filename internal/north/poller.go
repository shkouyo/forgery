package north

import (
	"context"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"
	"connectrpc.com/connect"

	"git.0x0f.dev/forgery/internal/store"
	"git.0x0f.dev/forgery/internal/token"
)

// PollLoop continuously polls the Forgejo instance for new tasks.
// When a task arrives it is wrapped in a TaskCtx, stored, and sent down
// taskCh for the run layer to orchestrate.
//
// Backpressure: before each FetchTask call, PollLoop acquires a slot from
// the capacity semaphore (c.sem). The slot is held until the run module
// completes the task and explicitly releases it via ReleaseSlot. On errors
// or empty (nil-task) responses the slot is immediately released so the
// loop can retry.
//
// The caller is responsible for cancelling ctx to stop the loop.
func (c *Client) PollLoop(ctx context.Context, taskCh chan<- *store.TaskCtx) {
	for {
		// ── Acquire slot BEFORE fetching (backpressure) ──
		// When the semaphore is full, this blocks and prevents
		// additional FetchTask calls — Forgejo will not assign
		// new tasks to a runner that isn't asking for them.
		//
		// TODO(Phase 2): when cfg.PingKeepalive is true, if
		// acquisition blocks for >60s the goroutine should send
		// a Ping RPC to prevent Forgejo from marking the runner
		// offline. For the MVP the ctx cancellation path is
		// sufficient to avoid indefinite stalls.
		select {
		case c.sem <- struct{}{}: // acquire slot
		case <-ctx.Done():
			return
		}

		resp, err := c.client.FetchTask(ctx, connect.NewRequest(&v1.FetchTaskRequest{}))
		if err != nil {
			<-c.sem // release on error
			c.log.Warn("fetch task failed", "err", err)
			if wait(ctx, c.cfg.PollInterval) {
				return
			}
			continue
		}

		if resp.Msg.Task == nil {
			<-c.sem // release on empty
			if wait(ctx, c.cfg.PollInterval) {
				return
			}
			continue
		}

		// Got a task — wrap it in a TaskCtx.
		regToken, tokenErr := token.Generate()
		if tokenErr != nil {
			<-c.sem // release on token error
			c.log.Error("failed to generate registration token", "err", tokenErr)
			if wait(ctx, c.cfg.PollInterval) {
				return
			}
			continue
		}

		taskCtx := &store.TaskCtx{
			ID:        resp.Msg.Task.Id,
			Task:      resp.Msg.Task,
			RegToken:  regToken,
			CreatedAt: time.Now(),
		}
		taskCtx.SetStatus(store.StatusPending)

		c.store.PutPending(taskCtx)

		// ── Send task to run layer ──
		// Slot is NOT released here — the run module will call
		// ReleaseSlot when the task reaches a terminal state.
		select {
		case taskCh <- taskCtx:
		case <-ctx.Done():
			<-c.sem // release — task was never dispatched
			return
		}
	}
}

// wait sleeps for d or returns true if ctx is cancelled during the wait.
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return true
	case <-timer.C:
		return false
	}
}
