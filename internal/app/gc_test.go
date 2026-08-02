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

package app

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/north"
	"git.0x0f.dev/forgery/internal/session"
	"git.0x0f.dev/forgery/internal/store"
)

// ── helpers ──

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// newBufferLogger returns a text logger writing into a buffer, for asserting
// log content (e.g. the orphan-failure WARN) in GC tests.
func newBufferLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

// gcClient implements north.Client for GC tests: it records every
// ForwardUpdateTask request and can be configured to fail the report.
type gcClient struct {
	mu         sync.Mutex
	forwarded  []*v1.UpdateTaskRequest
	forwardErr error
}

func (c *gcClient) ForwardUpdateTask(_ context.Context, req *v1.UpdateTaskRequest) (*v1.UpdateTaskResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forwarded = append(c.forwarded, req)
	if c.forwardErr != nil {
		return nil, c.forwardErr
	}
	return &v1.UpdateTaskResponse{}, nil
}

func (c *gcClient) ForwardUpdateLog(_ context.Context, _ *v1.UpdateLogRequest) (*v1.UpdateLogResponse, error) {
	return &v1.UpdateLogResponse{}, nil
}

func (c *gcClient) StartHeartbeat(ctx context.Context, _ *store.TaskCtx) {
	<-ctx.Done()
}

func (c *gcClient) calls() []*v1.UpdateTaskRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*v1.UpdateTaskRequest(nil), c.forwarded...)
}

// gcResolver is a static instance-name → client map implementing
// north.Resolver for GC tests. An empty map makes every Resolve fail.
type gcResolver map[string]north.Client

func (r gcResolver) Resolve(name string) (config.Instance, north.Client, bool) {
	c, ok := r[name]
	if !ok {
		return config.Instance{}, nil, false
	}
	return config.Instance{Name: name}, c, true
}

func (r gcResolver) OnlyInstance() (string, bool) {
	if len(r) != 1 {
		return "", false
	}
	for name := range r {
		return name, true
	}
	return "", false
}

// newGCTask builds a TaskCtx in the given status with a controlled age.
// RegTokenTTL defaults to 15 minutes (the config default) so Pending tasks
// age out at the production boundary; tests that probe the TTL override it.
func newGCTask(id int64, instance, regToken string, age time.Duration, status store.TaskStatus) *store.TaskCtx {
	tc := &store.TaskCtx{
		ID:          id,
		Instance:    instance,
		RegToken:    regToken,
		RegTokenTTL: 15 * time.Minute,
		CreatedAt:   time.Now().Add(-age),
	}
	tc.SetStatus(status)
	return tc
}

// newOrphanedSession registers a session for a task and backdates its last
// activity past maxAge (an orphan is a runner that registered and then went
// silent; Expire judges age by LastActivity, refreshed by Touch).
func newOrphanedSession(m *session.Manager, taskCtx *store.TaskCtx, age time.Duration) *session.Session {
	s := m.CreateWithToken(taskCtx, "sess-"+string(rune('a'+taskCtx.ID)), "runner", nil)
	s.LastActivity = time.Now().Add(-age)
	return s
}

func cfgWithTimeouts(instances ...time.Duration) *config.Config {
	cfg := &config.Config{}
	for i, t := range instances {
		cfg.Instances = append(cfg.Instances, config.Instance{
			Name:             string(rune('a' + i)),
			GAStartupTimeout: t,
		})
	}
	return cfg
}

// ── gcOnce ──

// TestGCOnce_ExpiredPendingCleared verifies the store GC pass reaps an
// expired Pending task (the wiring that production previously lacked).
func TestGCOnce_ExpiredPendingCleared(t *testing.T) {
	st := store.NewMemStore()
	m := session.NewManager()

	expired := newGCTask(1, "inst-a", "reg-1", 16*time.Minute, store.StatusPending)
	fresh := newGCTask(2, "inst-a", "reg-2", time.Minute, store.StatusPending)
	st.PutPending(expired)
	st.PutPending(fresh)

	gcOnce(context.Background(), time.Now(), st, m, gcResolver{}, time.Hour, testLogger())

	if _, ok := st.GetByRegToken("reg-1"); ok {
		t.Error("expired Pending task should have been removed by gcOnce")
	}
	if _, ok := st.GetByRegToken("reg-2"); !ok {
		t.Error("fresh Pending task must survive gcOnce")
	}
}

// TestGCOnce_ExpiredSession_TerminatesAndRemovesTask verifies the session
// expiry pass: the session is deleted, the task is removed from the store,
// and MarkDone closes the task's Done channel (which releases the
// backpressure slot if HandleTask is still waiting on it).
func TestGCOnce_ExpiredSession_TerminatesAndRemovesTask(t *testing.T) {
	st := store.NewMemStore()
	m := session.NewManager()

	taskCtx := newGCTask(7, "inst-a", "reg-7", time.Hour, store.StatusRunning)
	st.PutPending(taskCtx)
	s := newOrphanedSession(m, taskCtx, 2*time.Hour)

	gcOnce(context.Background(), time.Now(), st, m, gcResolver{}, time.Hour, testLogger())

	// Session deleted.
	if _, ok := m.Lookup(s.SessionToken); ok {
		t.Error("expired session should have been removed")
	}
	// Task removed from store.
	if st.CountActive() != 0 {
		t.Error("task of expired session should have been removed")
	}
	// Task forced terminal: Done channel closed.
	select {
	case <-taskCtx.Done():
		// OK
	default:
		t.Error("task Done channel should be closed after session expiry")
	}
}

// TestGCOnce_ExpiredSession_AlreadyRemovedTask verifies the race where the
// task was already removed (e.g. HandleTask timed out) while the session is
// still registered: gcOnce must not panic and must clean the session.
func TestGCOnce_ExpiredSession_AlreadyRemovedTask(t *testing.T) {
	st := store.NewMemStore()
	m := session.NewManager()

	taskCtx := newGCTask(8, "inst-a", "reg-8", time.Hour, store.StatusRunning)
	s := newOrphanedSession(m, taskCtx, 2*time.Hour)
	st.Remove(taskCtx.ID) // task gone, session orphaned

	gcOnce(context.Background(), time.Now(), st, m, gcResolver{}, time.Hour, testLogger())

	if _, ok := m.Lookup(s.SessionToken); ok {
		t.Error("orphaned session should have been removed")
	}
	// Done must still be closed so a still-waiting HandleTask releases its slot.
	select {
	case <-taskCtx.Done():
		// OK
	default:
		t.Error("task Done channel should be closed after session expiry")
	}
}

// TestGCOnce_KeepsFreshSessionAndTask verifies the pass is a no-op for
// healthy state.
func TestGCOnce_KeepsFreshSessionAndTask(t *testing.T) {
	st := store.NewMemStore()
	m := session.NewManager()

	taskCtx := newGCTask(9, "inst-a", "reg-9", time.Minute, store.StatusRunning)
	st.PutPending(taskCtx)
	s := m.CreateWithToken(taskCtx, "sess-fresh", "runner", nil) // fresh: now

	gcOnce(context.Background(), time.Now(), st, m, gcResolver{}, time.Hour, testLogger())

	if _, ok := m.Lookup(s.SessionToken); !ok {
		t.Error("fresh session must survive gcOnce")
	}
	if st.CountActive() != 1 {
		t.Error("fresh running task must survive gcOnce")
	}
}

// TestGCOnce_KeepsLongRunningTask verifies the F2 guarantee at the pass
// level: a task that has been running longer than maxAge survives gcOnce as
// long as its session shows recent activity (the runner keeps calling RPCs,
// each of which refreshes LastActivity via Touch).
func TestGCOnce_KeepsLongRunningTask(t *testing.T) {
	st := store.NewMemStore()
	m := session.NewManager()

	// Task dispatched hours ago, still Running — well past sessionMaxAge.
	taskCtx := newGCTask(15, "inst-a", "reg-15", 3*time.Hour, store.StatusRunning)
	st.PutPending(taskCtx)
	s := m.CreateWithToken(taskCtx, "sess-active", "runner", nil)
	s.LastActivity = time.Now().Add(-time.Second) // but the runner is active

	gcOnce(context.Background(), time.Now(), st, m, gcResolver{}, time.Hour, testLogger())

	if _, ok := m.Lookup(s.SessionToken); !ok {
		t.Error("active session of a long-running task was expired")
	}
	if st.CountActive() != 1 {
		t.Error("long-running task was removed by gcOnce")
	}
	select {
	case <-taskCtx.Done():
		t.Error("long-running task was marked done by gcOnce")
	default:
	}
}

// TestGCOnce_ExpiredSession_ReportsFailureToForgejo verifies the F8 fix: an
// expired orphan session triggers exactly one failure report to the owning
// instance's client, carrying the task id and RESULT_FAILURE, followed by the
// usual local cleanup.
func TestGCOnce_ExpiredSession_ReportsFailureToForgejo(t *testing.T) {
	st := store.NewMemStore()
	m := session.NewManager()
	client := &gcClient{}
	resolver := gcResolver{"inst-a": client}

	taskCtx := newGCTask(7, "inst-a", "reg-7", time.Hour, store.StatusRunning)
	st.PutPending(taskCtx)
	s := newOrphanedSession(m, taskCtx, 2*time.Hour)

	gcOnce(context.Background(), time.Now(), st, m, resolver, time.Hour, testLogger())

	// Exactly one failure report, for this task, with RESULT_FAILURE.
	calls := client.calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 failure report, got %d", len(calls))
	}
	if got := calls[0].GetState().GetId(); got != 7 {
		t.Errorf("reported task id = %d, want 7", got)
	}
	if got := calls[0].GetState().GetResult(); got != v1.Result_RESULT_FAILURE {
		t.Errorf("reported result = %v, want RESULT_FAILURE", got)
	}

	// Local cleanup still happened.
	if _, ok := m.Lookup(s.SessionToken); ok {
		t.Error("expired session should have been removed")
	}
	if st.CountActive() != 0 {
		t.Error("task of expired session should have been removed")
	}
	select {
	case <-taskCtx.Done():
		// OK
	default:
		t.Error("task Done channel should be closed after session expiry")
	}
}

// TestGCOnce_ExpiredSession_ReportFailureStillCleansUp verifies the
// best-effort contract: when the failure report itself fails (e.g. Forgejo
// rejects the update because the runner identity was re-registered), the
// error is logged with the instance and task id, but MarkDone and Remove
// still run — the report is never retried and never blocks cleanup.
func TestGCOnce_ExpiredSession_ReportFailureStillCleansUp(t *testing.T) {
	st := store.NewMemStore()
	m := session.NewManager()
	client := &gcClient{forwardErr: errors.New("invalid runner for task")}
	logger, buf := newBufferLogger()

	taskCtx := newGCTask(7, "inst-a", "reg-7", time.Hour, store.StatusRunning)
	st.PutPending(taskCtx)
	s := newOrphanedSession(m, taskCtx, 2*time.Hour)

	gcOnce(context.Background(), time.Now(), st, m, gcResolver{"inst-a": client}, time.Hour, logger)

	// Cleanup is unconditional: session gone, task gone, Done closed.
	if _, ok := m.Lookup(s.SessionToken); ok {
		t.Error("expired session should have been removed despite failed report")
	}
	if st.CountActive() != 0 {
		t.Error("task should have been removed despite failed report")
	}
	select {
	case <-taskCtx.Done():
		// OK
	default:
		t.Error("task Done channel should be closed despite failed report")
	}
	// The failure is logged with instance and task id for the operator.
	out := buf.String()
	if !strings.Contains(out, "failed to report orphaned task failure") {
		t.Errorf("expected WARN about the failed report, got: %s", out)
	}
	if !strings.Contains(out, "task_id=7") {
		t.Errorf("expected task id in the report-failure log, got: %s", out)
	}
}

// TestGCOnce_ExpiredSession_UnknownInstanceStillCleansUp verifies the
// defensive path: when the task's instance cannot be resolved (no client to
// report to), gcOnce logs the instance and task id and still performs the
// local cleanup.
func TestGCOnce_ExpiredSession_UnknownInstanceStillCleansUp(t *testing.T) {
	st := store.NewMemStore()
	m := session.NewManager()
	logger, buf := newBufferLogger()

	taskCtx := newGCTask(7, "ghost-instance", "reg-7", time.Hour, store.StatusRunning)
	st.PutPending(taskCtx)
	s := newOrphanedSession(m, taskCtx, 2*time.Hour)

	// Empty resolver: Resolve("ghost-instance") fails.
	gcOnce(context.Background(), time.Now(), st, m, gcResolver{}, time.Hour, logger)

	if _, ok := m.Lookup(s.SessionToken); ok {
		t.Error("expired session should have been removed")
	}
	if st.CountActive() != 0 {
		t.Error("task should have been removed despite unresolvable instance")
	}
	select {
	case <-taskCtx.Done():
		// OK
	default:
		t.Error("task Done channel should be closed despite unresolvable instance")
	}
	out := buf.String()
	if !strings.Contains(out, "instance not found") {
		t.Errorf("expected WARN about the unresolvable instance, got: %s", out)
	}
	if !strings.Contains(out, "task_id=7") {
		t.Errorf("expected task id in the unresolved-instance log, got: %s", out)
	}
}

// TestGCOnce_Empty verifies the pass is safe on empty state.
func TestGCOnce_Empty(t *testing.T) {
	gcOnce(context.Background(), time.Now(), store.NewMemStore(), session.NewManager(), gcResolver{}, time.Hour, testLogger())
}

// ── sessionMaxAge ──

func TestSessionMaxAge_TwiceLargestTimeout(t *testing.T) {
	cfg := cfgWithTimeouts(10*time.Minute, 15*time.Minute, 5*time.Minute)
	if got := sessionMaxAge(cfg); got != 30*time.Minute {
		t.Fatalf("sessionMaxAge = %v, want 30m", got)
	}
}

func TestSessionMaxAge_SingleInstance(t *testing.T) {
	cfg := cfgWithTimeouts(7 * time.Minute)
	if got := sessionMaxAge(cfg); got != 14*time.Minute {
		t.Fatalf("sessionMaxAge = %v, want 14m", got)
	}
}

// ── gcLoop ──

// TestGCLoop_TicksAndCleans verifies the interval parameter takes effect:
// with a short interval the loop reaps an expired session within a few
// ticks, and it exits promptly on context cancellation.
func TestGCLoop_TicksAndCleans(t *testing.T) {
	st := store.NewMemStore()
	m := session.NewManager()

	taskCtx := newGCTask(21, "inst-a", "reg-21", time.Hour, store.StatusRunning)
	st.PutPending(taskCtx)
	newOrphanedSession(m, taskCtx, 2*time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		gcLoop(ctx, 10*time.Millisecond, st, m, gcResolver{}, time.Hour, testLogger())
		close(done)
	}()

	// The session must be reaped shortly after the first tick.
	deadline := time.After(2 * time.Second)
	for {
		if _, ok := m.Lookup("sess-" + string(rune('a'+21))); !ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("gcLoop did not reap the expired session within 2s")
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Cancel: the loop must exit and not block.
	cancel()
	select {
	case <-done:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("gcLoop did not exit after context cancellation")
	}
}

// TestGCLoop_AlreadyCancelled verifies the loop returns immediately when the
// context is already cancelled (does not block graceful shutdown).
func TestGCLoop_AlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		gcLoop(ctx, time.Millisecond, store.NewMemStore(), session.NewManager(), gcResolver{}, time.Hour, testLogger())
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("gcLoop blocked on an already-cancelled context")
	}
}

// TestTokenPrefix verifies the safe-logging truncation helper.
func TestTokenPrefix(t *testing.T) {
	if got := tokenPrefix("0123456789abcdef"); got != "01234567" {
		t.Errorf("tokenPrefix(long) = %q, want 8 chars", got)
	}
	if got := tokenPrefix("abc"); got != "abc" {
		t.Errorf("tokenPrefix(short) = %q, want unchanged", got)
	}
	if got := tokenPrefix(""); got != "" {
		t.Errorf("tokenPrefix(empty) = %q, want empty", got)
	}
}
