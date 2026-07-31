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

// Package store provides in-memory task context storage with concurrency-safe
// access for the forgery proxy. It defines the task lifecycle states, the
// per-task context that carries the Forgejo Task payload through the proxy
// pipeline, and the TaskStore interface that all storage backends implement.
package store

import (
	"sync"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"
)

// TaskStatus represents a discrete stage in the forgery task lifecycle.
type TaskStatus int

const (
	// StatusPending — initial state, workflow_dispatch not yet sent.
	StatusPending TaskStatus = iota

	// StatusDispatched — workflow_dispatch sent, waiting for runner.
	StatusDispatched

	// StatusRunning — internal runner connected, task executing.
	StatusRunning

	// StatusTerminal — task completed (success, failure, or cancelled).
	StatusTerminal
)

// TaskCtx carries the full context of a single Forgejo task through the
// forgery proxy pipeline. Immutable fields are set at creation and never
// change. Mutable fields are protected by the embedded read-write mutex.
type TaskCtx struct {
	// Immutable fields — set once at creation, never modified.
	ID          int64
	Instance    string        // Name of the owning Forgejo instance (routing key)
	Task        *v1.Task      // Complete Forgejo Task (contains secrets, vars, needs)
	RegToken    string        // One-time registration token for the internal runner
	RegTokenTTL time.Duration // Registration-token lifetime (instance reg_token_ttl)
	CreatedAt   time.Time

	// Mutable fields — protected by mu.
	mu           sync.RWMutex
	status       TaskStatus
	sessionToken string        // Session token assigned after successful Register
	done         chan struct{} // closed when task reaches terminal state
	registered   chan struct{} // closed when the internal runner completes Register
}

// Status returns the current task status under a read lock.
func (t *TaskCtx) Status() TaskStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status
}

// SetStatus updates the task status under a write lock.
func (t *TaskCtx) SetStatus(s TaskStatus) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = s
}

// SetSessionToken records the session token assigned during Register under a
// write lock.
func (t *TaskCtx) SetSessionToken(token string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sessionToken = token
}

// SessionToken returns the session token assigned during Register under a
// read lock. It is the read counterpart of SetSessionToken: the field is
// unexported so every access goes through one of the two locked accessors.
func (t *TaskCtx) SessionToken() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.sessionToken
}

// MarkDispatched transitions the task to StatusDispatched under a write lock.
func (t *TaskCtx) MarkDispatched() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = StatusDispatched
}

// MarkRunnerRegistered transitions the task to StatusRunning and closes the
// registered channel exactly once under a write lock. Safe to call multiple
// times (idempotent).
func (t *TaskCtx) MarkRunnerRegistered() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = StatusRunning
	closeSignal(t.ensureRegistered())
}

// ensureDone lazily initialises and returns the done channel.
// The caller must hold t.mu.
func (t *TaskCtx) ensureDone() chan struct{} {
	if t.done == nil {
		t.done = make(chan struct{})
	}
	return t.done
}

// MarkDone closes the done channel exactly once to signal that the task has
// reached a terminal state. Safe to call multiple times (idempotent).
func (t *TaskCtx) MarkDone() {
	t.mu.Lock()
	defer t.mu.Unlock()
	closeSignal(t.ensureDone())
}

// Done returns a channel that is closed when the task reaches a terminal state.
// Callers can select on this channel to be notified of task completion.
func (t *TaskCtx) Done() <-chan struct{} {
	t.mu.Lock()
	d := t.ensureDone()
	t.mu.Unlock()
	return d
}

// ensureRegistered lazily initialises and returns the registered channel.
// The caller must hold t.mu.
func (t *TaskCtx) ensureRegistered() chan struct{} {
	if t.registered == nil {
		t.registered = make(chan struct{})
	}
	return t.registered
}

// Registered returns a channel that is closed when the internal runner has
// completed registration for this task. HandleTask selects on it to learn
// when the GAStartupTimeout no longer applies. Callers can select on this
// channel to be notified of runner registration.
func (t *TaskCtx) Registered() <-chan struct{} {
	t.mu.Lock()
	ch := t.ensureRegistered()
	t.mu.Unlock()
	return ch
}

// closeSignal closes ch if it is not already closed. The caller must hold
// t.mu; it makes MarkDone and MarkRunnerRegistered idempotent under
// concurrent callers.
func closeSignal(ch chan struct{}) {
	select {
	case <-ch:
		// already closed
	default:
		close(ch)
	}
}

// TaskStore is the abstract interface for task context storage. All methods
// are concurrency-safe. Implementations must respect the lock hierarchy:
// the store's own mutex is the outer lock and TaskCtx.mu is the inner lock
// — never acquire TaskCtx.mu while holding the store lock.
type TaskStore interface {
	// PutPending stores a newly-pulled task and indexes it by its
	// registration token. The task is expected to be in StatusPending.
	PutPending(taskCtx *TaskCtx)

	// GetByRegToken looks up a task by its one-time registration token.
	// The second return value is false when no task matches the token.
	GetByRegToken(regToken string) (*TaskCtx, bool)

	// MarkRegTokenConsumed atomically removes the registration token from
	// the index so it can never be used again: the token is one-time, and
	// a concurrent consumer either wins it or loses it. It returns true
	// when the token existed and was consumed, false when it was never
	// stored or had already been consumed.
	MarkRegTokenConsumed(regToken string) bool

	// Remove deletes a task and all of its registration-token index entries.
	Remove(taskID int64)

	// GC removes Pending tasks whose registration token has outlived its
	// per-task TTL (now.Sub(CreatedAt) > RegTokenTTL). Terminal tasks are
	// not retained: every terminal path removes them eagerly, so GC never
	// keeps a retention branch for them. now is the caller's current
	// time, typically time.Now().
	GC(now time.Time)

	// CountActive returns the number of tasks that are not in
	// StatusTerminal.
	CountActive() int
}
