package store

import (
	"sync"
	"time"
)

// Default time constants used by GC and MemStore defaults.
const (
	// DefaultPendingTTL is how long a task may remain in StatusPending before GC
	// considers it expired. Aligns with the default REG_TOKEN_TTL of 15 min.
	DefaultPendingTTL = 15 * time.Minute

	// DefaultTerminalRetention is how long completed (StatusTerminal) tasks are
	// kept before GC removes them.
	DefaultTerminalRetention = 24 * time.Hour
)

// MemStoreOption is a functional option for configuring MemStore behavior.
type MemStoreOption func(*MemStore)

// WithPendingTTL sets the TTL for pending tasks before GC considers them expired.
func WithPendingTTL(d time.Duration) MemStoreOption {
	return func(m *MemStore) {
		m.pendingTTL = d
	}
}

// WithTerminalRetention sets how long completed tasks are retained before GC
// removes them.
func WithTerminalRetention(d time.Duration) MemStoreOption {
	return func(m *MemStore) {
		m.terminalRetention = d
	}
}

// MemStore is the default in-memory implementation of TaskStore. It stores
// tasks in a map keyed by the real Forgejo task id and maintains a secondary
// index from registration token to task id for O(1) token lookups.
//
// All exported methods are concurrency-safe. The lock hierarchy is:
//
//	MemStore.mu  (outer — acquired first)
//	TaskCtx.mu   (inner — acquired second, never while holding MemStore.mu)
type MemStore struct {
	mu                sync.RWMutex
	tasks             map[int64]*TaskCtx // key = real Forgejo task_id
	byReg             map[string]int64   // regToken → task_id index
	pendingTTL        time.Duration
	terminalRetention time.Duration
}

// NewMemStore returns an initialized, empty MemStore ready for use.
// Default TTLs are DefaultPendingTTL (15 min) and DefaultTerminalRetention (24 h).
// Use WithPendingTTL and WithTerminalRetention to customize.
func NewMemStore(opts ...MemStoreOption) *MemStore {
	m := &MemStore{
		tasks:             make(map[int64]*TaskCtx),
		byReg:             make(map[string]int64),
		pendingTTL:        DefaultPendingTTL,
		terminalRetention: DefaultTerminalRetention,
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

// PutPending stores a task and indexes it by its registration token.
func (m *MemStore) PutPending(taskCtx *TaskCtx) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[taskCtx.ID] = taskCtx
	m.byReg[taskCtx.RegToken] = taskCtx.ID
}

// GetByRegToken performs a two-step lookup: regToken → taskID → TaskCtx.
// Returns false when the token is not found or the task has been removed.
func (m *MemStore) GetByRegToken(regToken string) (*TaskCtx, bool) {
	m.mu.RLock()
	id, ok := m.byReg[regToken]
	if !ok {
		m.mu.RUnlock()
		return nil, false
	}
	taskCtx, ok := m.tasks[id]
	m.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return taskCtx, true
}

// MarkRegTokenConsumed atomically removes the registration token from the
// index so it can never be used again. Returns ErrTokenNotFound when the
// token was never stored or has already been consumed.
func (m *MemStore) MarkRegTokenConsumed(regToken string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.byReg[regToken]; !ok {
		return ErrTokenNotFound
	}
	delete(m.byReg, regToken)
	return nil
}

// GetByID looks up a task by its real Forgejo task id.
func (m *MemStore) GetByID(taskID int64) (*TaskCtx, bool) {
	m.mu.RLock()
	taskCtx, ok := m.tasks[taskID]
	m.mu.RUnlock()
	return taskCtx, ok
}

// Remove deletes the task and cleans up every registration-token index entry
// that pointed to this task id.
func (m *MemStore) Remove(taskID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeLocked(taskID)
}

// removeLocked removes a task and its byReg entries. Caller must hold m.mu.
func (m *MemStore) removeLocked(taskID int64) {
	delete(m.tasks, taskID)
	for regToken, id := range m.byReg {
		if id == taskID {
			delete(m.byReg, regToken)
		}
	}
}

// GC removes expired Pending tasks (older than the configured pendingTTL) and
// Terminal tasks that are older than the configured terminalRetention.
// It respects the lock hierarchy by collecting candidates under the store lock,
// inspecting task status outside the lock, and re-acquiring for deletion.
func (m *MemStore) GC(now time.Time) {
	// Collect task pointers and ids under read lock.
	m.mu.RLock()
	taskPtrs := make([]*TaskCtx, 0, len(m.tasks))
	taskIDs := make([]int64, 0, len(m.tasks))
	for id, t := range m.tasks {
		taskPtrs = append(taskPtrs, t)
		taskIDs = append(taskIDs, id)
	}
	m.mu.RUnlock()

	// Inspect each task outside the store lock (TaskCtx.Status acquires its
	// own lock). Accumulate ids to remove.
	var toRemove []int64
	for i, t := range taskPtrs {
		st := t.Status()
		switch {
		case st == StatusPending && now.Sub(t.CreatedAt) > m.pendingTTL:
			toRemove = append(toRemove, taskIDs[i])
		case st == StatusTerminal && now.Sub(t.CreatedAt) > m.terminalRetention:
			toRemove = append(toRemove, taskIDs[i])
		}
	}

	if len(toRemove) == 0 {
		return
	}

	// Remove collected ids under write lock.
	m.mu.Lock()
	for _, id := range toRemove {
		m.removeLocked(id)
	}
	m.mu.Unlock()
}

// CountActive returns the number of tasks whose status is not StatusTerminal.
// It respects the lock hierarchy by collecting task pointers under the store
// read lock and calling Status() (which locks TaskCtx.mu) outside it.
func (m *MemStore) CountActive() int {
	m.mu.RLock()
	taskPtrs := make([]*TaskCtx, 0, len(m.tasks))
	for _, t := range m.tasks {
		taskPtrs = append(taskPtrs, t)
	}
	m.mu.RUnlock()

	count := 0
	for _, t := range taskPtrs {
		if t.Status() != StatusTerminal {
			count++
		}
	}
	return count
}

// HasCapacity returns true when the number of active tasks is strictly less
// than max.
func (m *MemStore) HasCapacity(max int) bool {
	return m.CountActive() < max
}
