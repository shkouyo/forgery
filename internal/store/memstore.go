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

package store

import (
	"sync"
	"time"
)

// MemStore is the default in-memory implementation of TaskStore. It stores
// tasks in a map keyed by the real Forgejo task id and maintains a secondary
// index from registration token to task id for O(1) token lookups.
//
// All exported methods are concurrency-safe. The lock hierarchy is:
//
//	MemStore.mu  (outer — acquired first)
//	TaskCtx.mu   (inner — acquired second, never while holding MemStore.mu)
type MemStore struct {
	mu    sync.RWMutex
	tasks map[int64]*TaskCtx // key = real Forgejo task_id
	byReg map[string]int64   // regToken → task_id index
}

// NewMemStore returns an initialized, empty MemStore ready for use. Expiry
// policies live on the tasks themselves (TaskCtx.RegTokenTTL), so the store
// carries no configuration.
func NewMemStore() *MemStore {
	return &MemStore{
		tasks: make(map[int64]*TaskCtx),
		byReg: make(map[string]int64),
	}
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
// index so it can never be used again. It returns true when the token
// existed and was consumed, false when the token was never stored or has
// already been consumed.
func (m *MemStore) MarkRegTokenConsumed(regToken string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.byReg[regToken]; !ok {
		return false
	}
	delete(m.byReg, regToken)
	return true
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

// GC removes Pending tasks whose registration token has outlived its TTL.
// The deadline is per task — now.Sub(t.CreatedAt) > t.RegTokenTTL — so tasks
// of instances with different reg_token_ttl settings expire independently,
// and the Pending branch stays the single reclaimer for tasks stuck in
// Pending (e.g. a shutdown race that interrupted dispatch before
// MarkDispatched ran: nothing else ever touches them again).
//
// Terminal tasks are deliberately not retained: every terminal path (south's
// terminal UpdateTask, run's failure/timeout branches, gcOnce's session
// expiry) removes them eagerly, so GC would never have anything to reap.
//
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
		if t.Status() == StatusPending && now.Sub(t.CreatedAt) > t.RegTokenTTL {
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
