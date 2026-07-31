package app

import (
	"strings"
	"testing"
	"time"

	"git.0x0f.dev/forgery/internal/store"
)

// TestDrainTasks_EmptyStoreCompletes verifies the happy path: no active
// tasks means drain returns nil immediately.
func TestDrainTasks_EmptyStoreCompletes(t *testing.T) {
	if err := drainTasks(store.NewMemStore(), 2*time.Second, 10*time.Millisecond, testLogger()); err != nil {
		t.Fatalf("drainTasks on an empty store: %v", err)
	}
}

// TestDrainTasks_WaitsForCompletion verifies the poll loop: drain returns
// nil once the last active task disappears before the timeout.
func TestDrainTasks_WaitsForCompletion(t *testing.T) {
	st := store.NewMemStore()
	st.PutPending(newGCTask(1, "inst-a", "reg-1", 0, store.StatusRunning))
	go func() {
		time.Sleep(50 * time.Millisecond)
		st.Remove(1)
	}()

	start := time.Now()
	if err := drainTasks(st, 2*time.Second, 10*time.Millisecond, testLogger()); err != nil {
		t.Fatalf("drainTasks: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("drainTasks took %v — looks like the timeout fired instead of draining", elapsed)
	}
}

// TestDrainTasks_TimeoutReturnsError verifies the forced-exit condition:
// tasks still active after the timeout yield an error so the caller (main)
// exits with a non-zero code instead of reporting a clean shutdown.
func TestDrainTasks_TimeoutReturnsError(t *testing.T) {
	st := store.NewMemStore()
	st.PutPending(newGCTask(1, "inst-a", "reg-1", 0, store.StatusDispatched))

	err := drainTasks(st, 50*time.Millisecond, 10*time.Millisecond, testLogger())
	if err == nil {
		t.Fatal("drainTasks must return an error when the timeout elapses with active tasks")
	}
	if !strings.Contains(err.Error(), "shutdown timeout") {
		t.Errorf("error should mention the shutdown timeout, got %q", err)
	}
}
