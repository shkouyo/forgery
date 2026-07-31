// SPDX-License-Identifier: GPL-3.0-or-later

// Copyright (C) 2026 ShinKouyo <i@0x0f.dev>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package north

import (
	"context"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"
	"connectrpc.com/connect"

	"git.0x0f.dev/forgery/internal/slots"
	"git.0x0f.dev/forgery/internal/store"
	"git.0x0f.dev/forgery/internal/token"
)

// PollLoop continuously polls the Forgejo instance for new tasks.
// When a task arrives it is wrapped in a TaskCtx (tagged with the owning
// instance name), stored, and sent down taskCh for the run layer to
// orchestrate.
//
// Backpressure: before each FetchTask call, PollLoop acquires a slot from
// the shared pool passed by the assembler. The pool is a daemon-wide budget
// shared by every instance's poller, so max_parallel_tasks caps the total
// number of in-flight tasks across all Forgejo connections. The slot is
// held until the run module completes the task and explicitly releases it
// via pool.Release. On errors or empty (nil-task) responses the slot is
// immediately released so the loop can retry.
//
// The caller is responsible for cancelling ctx to stop the loop.
func (c *client) PollLoop(ctx context.Context, pool *slots.Pool, taskCh chan<- *store.TaskCtx) {
	for {
		// ── Acquire slot BEFORE fetching (backpressure) ──
		// When the pool is exhausted, this blocks and prevents
		// additional FetchTask calls — Forgejo will not assign
		// new tasks to a runner that isn't asking for them.
		//
		// NOTE: a keepalive Ping RPC was planned for long acquisition
		// blocks (>60s) but is not yet implemented. Only ctx cancellation
		// prevents indefinite stalls.
		if err := pool.Acquire(ctx); err != nil {
			return
		}

		var resp *connect.Response[v1.FetchTaskResponse]
		err := c.retryOnAuth(ctx, func(ctx context.Context) error {
			var err error
			resp, err = c.client.FetchTask(ctx, connect.NewRequest(&v1.FetchTaskRequest{}))
			return err
		})
		if err != nil {
			pool.Release() // release on error
			c.log.Warn("fetch task failed", "err", err)
			if wait(ctx, c.inst.PollInterval) {
				return
			}
			continue
		}

		if resp.Msg.Task == nil {
			pool.Release() // release on empty
			if wait(ctx, c.inst.PollInterval) {
				return
			}
			continue
		}

		// Got a task — wrap it in a TaskCtx. RegTokenTTL is stamped once,
		// at creation, from the owning instance's reg_token_ttl: the task
		// is the single source of truth shared by south's token-expiry
		// check and the store's Pending GC.
		regToken, tokenErr := token.Generate()
		if tokenErr != nil {
			pool.Release() // release on token error
			c.log.Error("failed to generate registration token", "err", tokenErr)
			if wait(ctx, c.inst.PollInterval) {
				return
			}
			continue
		}

		taskCtx := &store.TaskCtx{
			ID:          resp.Msg.Task.Id,
			Instance:    c.inst.Name,
			Task:        resp.Msg.Task,
			RegToken:    regToken,
			RegTokenTTL: c.inst.RegTokenTTL,
			CreatedAt:   time.Now(),
		}
		taskCtx.SetStatus(store.StatusPending)

		c.store.PutPending(taskCtx)

		c.log.Info("task received from Forgejo", "task_id", resp.Msg.Task.Id)

		// ── Send task to run layer ──
		// Slot is NOT released here — the run module will call
		// pool.Release when the task reaches a terminal state.
		select {
		case taskCh <- taskCtx:
		case <-ctx.Done():
			pool.Release() // release — task was never dispatched
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
