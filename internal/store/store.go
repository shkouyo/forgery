// Package store provides in-memory task context storage with concurrency-safe
// access for the forgery proxy. It defines the task lifecycle states, the
// per-task context that carries the Forgejo Task payload through the proxy
// pipeline, and the TaskStore interface that all storage backends implement.
package store

import (
	"errors"
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

// ErrTokenNotFound is returned by MarkRegTokenConsumed when the supplied
// registration token does not exist in the store (never stored or already
// consumed).
var ErrTokenNotFound = errors.New("store: registration token not found")

// TaskCtx carries the full context of a single Forgejo task through the
// forgery proxy pipeline. Immutable fields are set at creation and never
// change. Mutable fields are protected by the embedded read-write mutex.
type TaskCtx struct {
	// Immutable fields — set once at creation, never modified.
	ID        int64
	Task      *v1.Task // Complete Forgejo Task (contains secrets, vars, needs)
	RegToken  string   // One-time registration token for the internal runner
	CreatedAt time.Time

	// Mutable fields — protected by mu.
	mu                 sync.RWMutex
	status             TaskStatus
	SessionToken       string    // Session token assigned after successful Register
	DispatchedAt       time.Time // When workflow_dispatch succeeded
	RunnerRegisteredAt time.Time // When the internal runner completed Register
	done               chan struct{} // closed when task reaches terminal state
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
	t.SessionToken = token
}

// MarkDispatched transitions the task to StatusDispatched and records the
// dispatch timestamp under a write lock.
func (t *TaskCtx) MarkDispatched() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = StatusDispatched
	t.DispatchedAt = time.Now()
}

// MarkRunnerRegistered transitions the task to StatusRunning and records the
// runner registration timestamp under a write lock.
func (t *TaskCtx) MarkRunnerRegistered() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = StatusRunning
	t.RunnerRegisteredAt = time.Now()
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
	t.ensureDone()
	select {
	case <-t.done:
		// already closed
	default:
		close(t.done)
	}
}

// Done returns a channel that is closed when the task reaches a terminal state.
// Callers can select on this channel to be notified of task completion.
func (t *TaskCtx) Done() <-chan struct{} {
	t.mu.Lock()
	d := t.ensureDone()
	t.mu.Unlock()
	return d
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
	// the index so it cannot be used again. Returns ErrTokenNotFound if
	// the token was never stored or was already consumed.
	MarkRegTokenConsumed(regToken string) error

	// GetByID looks up a task by its real Forgejo task id.
	GetByID(taskID int64) (*TaskCtx, bool)

	// Remove deletes a task and all of its registration-token index entries.
	Remove(taskID int64)

	// GC removes expired Pending tasks and Terminal tasks that are older
	// than the configured retention period. now is the caller's current
	// time, typically time.Now().
	GC(now time.Time)

	// CountActive returns the number of tasks that are not in
	// StatusTerminal.
	CountActive() int

	// HasCapacity returns true when the number of active tasks is strictly
	// less than max.
	HasCapacity(max int) bool
}
