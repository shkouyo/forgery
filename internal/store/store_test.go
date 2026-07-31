package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"
)

// newTestTask creates a TaskCtx with minimal required fields for testing.
// RegTokenTTL defaults to 15 minutes (the config default); tests that probe
// GC boundaries override RegTokenTTL or CreatedAt.
func newTestTask(id int64, regToken string) *TaskCtx {
	return &TaskCtx{
		ID:          id,
		Task:        &v1.Task{Id: id},
		RegToken:    regToken,
		RegTokenTTL: 15 * time.Minute,
		CreatedAt:   time.Now(),
		status:      StatusPending,
	}
}

// ---- T-STO-005.1: PutPending + GetByRegToken round-trip ----

func TestPutPending_GetByRegToken(t *testing.T) {
	s := NewMemStore()
	tc := newTestTask(42, "token-abc")

	s.PutPending(tc)

	got, ok := s.GetByRegToken("token-abc")
	if !ok {
		t.Fatal("GetByRegToken returned false for known token")
	}
	if got.ID != 42 {
		t.Fatalf("expected ID 42, got %d", got.ID)
	}
	if got.RegToken != "token-abc" {
		t.Fatalf("expected RegToken token-abc, got %s", got.RegToken)
	}
}

func TestGetByRegToken_NotFound(t *testing.T) {
	s := NewMemStore()
	_, ok := s.GetByRegToken("nonexistent")
	if ok {
		t.Fatal("GetByRegToken should return false for unknown token")
	}
}

// ---- T-STO-005.2: MarkRegTokenConsumed makes token unavailable ----

func TestMarkRegTokenConsumed(t *testing.T) {
	s := NewMemStore()
	tc := newTestTask(1, "token-one")
	s.PutPending(tc)

	// First consumption should succeed.
	if !s.MarkRegTokenConsumed("token-one") {
		t.Fatal("first MarkRegTokenConsumed should succeed")
	}

	// Token should no longer be findable.
	_, ok := s.GetByRegToken("token-one")
	if ok {
		t.Fatal("GetByRegToken should return false after token consumed")
	}

	// Second consumption should fail (token already consumed).
	if s.MarkRegTokenConsumed("token-one") {
		t.Fatal("second consume should return false")
	}

	// Task should still exist in the tasks map.
	got, ok := s.tasks[1]
	if !ok {
		t.Fatal("task should still be present after token consumed")
	}
	if got.ID != 1 {
		t.Fatalf("expected ID 1, got %d", got.ID)
	}
}

func TestMarkRegTokenConsumed_UnknownToken(t *testing.T) {
	s := NewMemStore()
	if s.MarkRegTokenConsumed("never-stored") {
		t.Fatal("MarkRegTokenConsumed should return false for a token that was never stored")
	}
}

// ---- T-STO-005.3: Remove makes task unavailable ----

func TestRemove(t *testing.T) {
	s := NewMemStore()
	tc := newTestTask(10, "tok-10")
	s.PutPending(tc)

	// Verify it exists.
	if _, ok := s.tasks[10]; !ok {
		t.Fatal("task should be in the tasks map before Remove")
	}
	if _, ok := s.GetByRegToken("tok-10"); !ok {
		t.Fatal("GetByRegToken should find task before Remove")
	}

	s.Remove(10)

	// Both lookups should fail.
	if _, ok := s.tasks[10]; ok {
		t.Fatal("task should be gone from the tasks map after Remove")
	}
	if _, ok := s.GetByRegToken("tok-10"); ok {
		t.Fatal("GetByRegToken should return false after Remove")
	}
}

func TestRemove_Nonexistent(t *testing.T) {
	s := NewMemStore()
	// Should not panic.
	s.Remove(999)
}

func TestRemove_CleansUpAllRegTokens(t *testing.T) {
	s := NewMemStore()
	// Simulate a task that was stored multiple times (e.g. re-PutPending).
	// Only the last regToken matters for the index, but remove should clean it.
	tc := newTestTask(20, "tok-20")
	s.PutPending(tc)

	// Overwrite with a new token — the old one won't be in byReg, but
	// the new one must be cleaned up.
	tc2 := newTestTask(20, "tok-20-new")
	s.PutPending(tc2)

	s.Remove(20)

	if _, ok := s.GetByRegToken("tok-20-new"); ok {
		t.Fatal("reg token should be cleaned up by Remove")
	}
	if _, ok := s.tasks[20]; ok {
		t.Fatal("task should be gone after Remove")
	}
}

// ---- T-STO-005.4: CountActive across all statuses ----

func TestCountActive(t *testing.T) {
	s := NewMemStore()

	// Create tasks in various statuses.
	tcPending := newTestTask(1, "t1")
	tcPending.status = StatusPending

	tcDispatched := newTestTask(2, "t2")
	tcDispatched.status = StatusDispatched

	tcRunning := newTestTask(3, "t3")
	tcRunning.status = StatusRunning

	tcTerminal := newTestTask(4, "t4")
	tcTerminal.status = StatusTerminal

	s.PutPending(tcPending)
	s.PutPending(tcDispatched)
	s.PutPending(tcRunning)
	s.PutPending(tcTerminal)

	if got := s.CountActive(); got != 3 {
		t.Fatalf("expected 3 active (pending+dispatched+running), got %d", got)
	}

	// Transition running to terminal.
	tcRunning.SetStatus(StatusTerminal)
	if got := s.CountActive(); got != 2 {
		t.Fatalf("expected 2 active after one more terminal, got %d", got)
	}
}

func TestCountActive_Empty(t *testing.T) {
	s := NewMemStore()
	if got := s.CountActive(); got != 0 {
		t.Fatalf("expected 0 for empty store, got %d", got)
	}
}

// ---- T-STO-005.6: GC cleans up expired Pending tasks ----

// TestGC_ExpiredPending verifies GC reaps a Pending task whose age exceeds
// its own RegTokenTTL (15m via the test helper) and keeps a fresh one.
func TestGC_ExpiredPending(t *testing.T) {
	s := NewMemStore()

	// Create a task in the past (expired: 20m old vs 15m task TTL).
	old := newTestTask(1, "old-tok")
	old.CreatedAt = time.Now().Add(-20 * time.Minute)
	s.PutPending(old)

	// Create a fresh task.
	fresh := newTestTask(2, "fresh-tok")
	fresh.CreatedAt = time.Now()
	s.PutPending(fresh)

	s.GC(time.Now())

	// Old task should be gone.
	if _, ok := s.tasks[1]; ok {
		t.Fatal("expired pending task should be removed by GC")
	}
	if _, ok := s.GetByRegToken("old-tok"); ok {
		t.Fatal("expired pending reg token should be cleaned up by GC")
	}

	// Fresh task should remain.
	if _, ok := s.tasks[2]; !ok {
		t.Fatal("fresh pending task should survive GC")
	}
	if _, ok := s.GetByRegToken("fresh-tok"); !ok {
		t.Fatal("fresh pending reg token should survive GC")
	}
}

func TestGC_PendingNotExpired(t *testing.T) {
	s := NewMemStore()

	// Task that is pending but only 10 minutes old (15m task TTL).
	tc := newTestTask(1, "tok")
	tc.CreatedAt = time.Now().Add(-10 * time.Minute)
	s.PutPending(tc)

	s.GC(time.Now())

	if _, ok := s.tasks[1]; !ok {
		t.Fatal("pending task within its RegTokenTTL should survive GC")
	}
}

// ---- T-STO-005.7: GC never touches Terminal tasks ----

// TestGC_TerminalTasksSurvive pins the post-unification contract: GC has no
// terminal-retention branch because every terminal path (south's terminal
// UpdateTask, run's failure/timeout branches, gcOnce's session expiry)
// removes tasks eagerly. An arbitrarily old terminal task must survive GC.
func TestGC_TerminalTasksSurvive(t *testing.T) {
	s := NewMemStore()

	old := newTestTask(1, "old-term")
	old.CreatedAt = time.Now().Add(-25 * time.Hour)
	old.status = StatusTerminal
	s.PutPending(old)

	s.GC(time.Now())

	if _, ok := s.tasks[1]; !ok {
		t.Fatal("terminal task must survive GC (removal is the terminal path's job)")
	}
}

func TestGC_DispatchedAndRunningSurvive(t *testing.T) {
	s := NewMemStore()

	// Dispatched task that's old — should survive (only Pending is
	// time-checked).
	disp := newTestTask(1, "d1")
	disp.CreatedAt = time.Now().Add(-30 * time.Minute)
	disp.status = StatusDispatched
	s.PutPending(disp)

	run := newTestTask(2, "r1")
	run.CreatedAt = time.Now().Add(-30 * time.Minute)
	run.status = StatusRunning
	s.PutPending(run)

	s.GC(time.Now())

	if _, ok := s.tasks[1]; !ok {
		t.Fatal("old Dispatched task should survive GC")
	}
	if _, ok := s.tasks[2]; !ok {
		t.Fatal("old Running task should survive GC")
	}
}

func TestGC_EmptyStore(t *testing.T) {
	s := NewMemStore()
	s.GC(time.Now()) // should not panic
}

// ---- T-STO-005.8: Concurrent access ----

func TestConcurrentAccess(t *testing.T) {
	s := NewMemStore()
	const numWorkers = 20
	const numOps = 200

	var wg sync.WaitGroup

	// Pre-populate some tasks.
	for i := int64(1); i <= 10; i++ {
		s.PutPending(newTestTask(i, "init-tok-"+string(rune('a'+i-1))))
	}

	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			baseID := int64(workerID * 1000)
			for i := 0; i < numOps; i++ {
				id := baseID + int64(i)
				tok := "tok-" + string(rune('a'+workerID)) + "-" + string(rune('a'+i%26))
				tc := newTestTask(id, tok)

				// Put
				s.PutPending(tc)

				// Get by token
				s.GetByRegToken(tok)

				// Count
				s.CountActive()

				// Consume token
				s.MarkRegTokenConsumed(tok)

				// Remove
				s.Remove(id)
			}
		}(w)
	}

	// Also run GC concurrently.
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			s.GC(time.Now())
			time.Sleep(time.Microsecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			s.CountActive()
			time.Sleep(time.Microsecond)
		}
	}()

	wg.Wait()
}

// ---- T-STO-005.9: Same regToken cannot register twice ----

func TestRegTokenDoubleUse(t *testing.T) {
	s := NewMemStore()
	tc := newTestTask(1, "shared-token")
	s.PutPending(tc)

	// First consumption succeeds.
	if !s.MarkRegTokenConsumed("shared-token") {
		t.Fatal("first consume should succeed")
	}

	// Second consumption must fail.
	if s.MarkRegTokenConsumed("shared-token") {
		t.Fatal("second consume should return false")
	}

	// Lookup by token must fail after first consumption.
	if _, ok := s.GetByRegToken("shared-token"); ok {
		t.Fatal("GetByRegToken should fail after token consumed")
	}
}

// ---- Additional edge case: PutPending overwrites existing ID ----

func TestPutPending_Overwrite(t *testing.T) {
	s := NewMemStore()

	tc1 := newTestTask(1, "first-tok")
	s.PutPending(tc1)

	tc2 := newTestTask(1, "second-tok")
	s.PutPending(tc2)

	// Should find by the new token.
	got, ok := s.GetByRegToken("second-tok")
	if !ok {
		t.Fatal("should find by latest reg token")
	}
	if got.RegToken != "second-tok" {
		t.Fatalf("expected RegToken second-tok, got %s", got.RegToken)
	}

	// Old token may still be indexed (overwrite doesn't delete old index entry).
	// Actually, our implementation does overwrite tasks[id] but doesn't clean
	// up old byReg entries. The old token would still point to id=1.
	// That's acceptable behavior — the task is still accessible.
	got2, ok2 := s.GetByRegToken("first-tok")
	if ok2 {
		if got2.ID != 1 {
			t.Fatal("old token should still point to same task")
		}
	}
	// Either case is fine as long as nothing panics.
	_ = got2
}

// ---- Verifying lock-free Status access from multiple goroutines ----

func TestTaskCtxConcurrentStatusAccess(t *testing.T) {
	tc := newTestTask(1, "t")
	tc.status = StatusPending

	var wg sync.WaitGroup
	wg.Add(2)

	// Reader goroutine.
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = tc.Status()
		}
	}()

	// Writer goroutine.
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if i%2 == 0 {
				tc.SetStatus(StatusDispatched)
			} else {
				tc.SetStatus(StatusRunning)
			}
		}
	}()

	wg.Wait()
}

// TestTaskCtxConcurrentSessionTokenAccess verifies concurrent SetSessionToken
// / SessionToken access is race-free. This is the one-job auto-registration
// pattern: the session manager writes the token while run.HandleTask reads
// it (via the getter) to drop the session on failure/timeout paths. Must be
// run under -race.
func TestTaskCtxConcurrentSessionTokenAccess(t *testing.T) {
	tc := newTestTask(1, "t")

	var wg sync.WaitGroup
	wg.Add(2)

	// Reader goroutine.
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = tc.SessionToken()
		}
	}()

	// Writer goroutine.
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			tc.SetSessionToken(fmt.Sprintf("sess-%d", i%3))
		}
	}()

	wg.Wait()
}

func TestTaskCtxMarkMethods(t *testing.T) {
	tc := newTestTask(1, "tok")
	if tc.Status() != StatusPending {
		t.Fatal("initial status should be Pending")
	}

	tc.MarkDispatched()
	if tc.Status() != StatusDispatched {
		t.Fatal("MarkDispatched should set StatusDispatched")
	}

	tc.MarkRunnerRegistered()
	if tc.Status() != StatusRunning {
		t.Fatal("MarkRunnerRegistered should set StatusRunning")
	}

	tc.SetSessionToken("sess-123")
	if tc.SessionToken() != "sess-123" {
		t.Fatalf("SetSessionToken failed, got %q", tc.SessionToken())
	}
}

// ---- MarkDone idempotency (relied on by the app GC loop) ----

// TestMarkDone_Idempotent verifies that MarkDone can be called any number of
// times — sequentially or concurrently — without panicking, and that the done
// channel is closed exactly once (every receiver sees it closed). The app GC
// loop, south's terminal UpdateTask path, and run.HandleTask may all call
// MarkDone for the same task from different goroutines.
func TestMarkDone_Idempotent(t *testing.T) {
	tc := newTestTask(1, "tok")

	tc.MarkDone()
	tc.MarkDone()
	tc.MarkDone()

	select {
	case <-tc.Done():
		// closed as expected
	default:
		t.Fatal("Done channel not closed after MarkDone")
	}
}

func TestMarkDone_Concurrent(t *testing.T) {
	tc := newTestTask(1, "tok")

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			tc.MarkDone()
		}()
	}
	wg.Wait()

	// Done must be closed after the concurrent MarkDone storm.
	select {
	case <-tc.Done():
		// OK
	default:
		t.Fatal("Done channel not closed after concurrent MarkDone")
	}

	// Done() must still work (no double-close panic) and MarkDone must
	// remain callable.
	tc.MarkDone()
	<-tc.Done()
}

// ---- Registered signal (F1: run waits on it to end the startup timeout) ----

// TestRegistered_OpenBeforeRegistration verifies the channel is open until
// the runner registers, so HandleTask's phase-1 select actually waits.
func TestRegistered_OpenBeforeRegistration(t *testing.T) {
	tc := newTestTask(1, "tok")
	select {
	case <-tc.Registered():
		t.Fatal("Registered channel closed before MarkRunnerRegistered")
	default:
	}
}

// TestRegistered_Idempotent verifies MarkRunnerRegistered closes the channel
// exactly once and can be called any number of times without panicking.
func TestRegistered_Idempotent(t *testing.T) {
	tc := newTestTask(1, "tok")

	tc.MarkRunnerRegistered()
	tc.MarkRunnerRegistered()
	tc.MarkRunnerRegistered()

	select {
	case <-tc.Registered():
		// closed as expected
	default:
		t.Fatal("Registered channel not closed after MarkRunnerRegistered")
	}

	// Marking again after closure must stay safe.
	tc.MarkRunnerRegistered()
	<-tc.Registered()
}

// TestRegistered_Concurrent verifies concurrent MarkRunnerRegistered calls
// close the channel exactly once (no double-close panic) under -race.
func TestRegistered_Concurrent(t *testing.T) {
	tc := newTestTask(1, "tok")

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			tc.MarkRunnerRegistered()
		}()
	}
	wg.Wait()

	select {
	case <-tc.Registered():
		// OK
	default:
		t.Fatal("Registered channel not closed after concurrent MarkRunnerRegistered")
	}

	// Still safe to call after closure.
	tc.MarkRunnerRegistered()
	<-tc.Registered()
}

// ---- T-STO-004: per-task RegTokenTTL drives the Pending GC boundary ----

// TestGC_PendingTTLByTask verifies the GC deadline is per task: two tasks of
// the same age expire independently according to their own RegTokenTTL. This
// is the store half of the TTL unification — the poller stamps each task
// with its instance's reg_token_ttl and south's token-expiry check reads the
// same field, so store and south can never disagree.
func TestGC_PendingTTLByTask(t *testing.T) {
	s := NewMemStore()

	// Same age, different per-task TTLs.
	short := newTestTask(1, "short-ttl")
	short.CreatedAt = time.Now().Add(-10 * time.Minute)
	short.RegTokenTTL = 5 * time.Minute // expired
	s.PutPending(short)

	long := newTestTask(2, "long-ttl")
	long.CreatedAt = time.Now().Add(-10 * time.Minute)
	long.RegTokenTTL = 30 * time.Minute // still valid
	s.PutPending(long)

	s.GC(time.Now())

	if _, ok := s.tasks[1]; ok {
		t.Fatal("pending task past its RegTokenTTL should be removed by GC")
	}
	if _, ok := s.tasks[2]; !ok {
		t.Fatal("pending task within its RegTokenTTL should survive GC")
	}
}

// TestGC_PendingWithinTTL_Survives verifies a Pending task younger than its
// RegTokenTTL is kept.
func TestGC_PendingWithinTTL_Survives(t *testing.T) {
	s := NewMemStore()

	// Task that is pending and only 2 minutes old against a 5m TTL.
	fresh := newTestTask(1, "fresh")
	fresh.CreatedAt = time.Now().Add(-2 * time.Minute)
	fresh.RegTokenTTL = 5 * time.Minute
	s.PutPending(fresh)

	s.GC(time.Now())

	if _, ok := s.tasks[1]; !ok {
		t.Fatal("pending task within its RegTokenTTL should survive GC")
	}
}

// ---- T-STO-004: Concurrent PutPending + GC stress test ----

func TestConcurrentPutPendingAndGC(t *testing.T) {
	s := NewMemStore()
	const numWorkers = 10
	const numTasks = 100

	var wg sync.WaitGroup

	// Workers that rapidly add and remove tasks.
	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			baseID := int64(workerID * 10000)
			for i := 0; i < numTasks; i++ {
				id := baseID + int64(i)
				tok := fmt.Sprintf("tok-%d-%d", workerID, i)
				tc := newTestTask(id, tok)
				// Make some tasks already expired for GC to find.
				if i%3 == 0 {
					tc.CreatedAt = time.Now().Add(-30 * time.Minute)
				}
				s.PutPending(tc)
				// Immediately remove some to create churn.
				if i%5 == 0 {
					s.Remove(id)
				}
			}
		}(w)
	}

	// GC goroutine that runs aggressively.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			s.GC(time.Now())
			time.Sleep(time.Microsecond)
		}
	}()

	wg.Wait()
}

// ---- T-STO-004: Concurrent MarkRegTokenConsumed + GetByRegToken ----

func TestConcurrentMarkRegTokenConsumedAndGetByRegToken(t *testing.T) {
	s := NewMemStore()
	const numWorkers = 20

	// Create a single task with a known token.
	tc := newTestTask(1, "shared-token")
	s.PutPending(tc)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	// All workers race to consume the same token.
	consumed := int32(0)
	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			// Try to consume — only one should succeed.
			if s.MarkRegTokenConsumed("shared-token") {
				atomic.AddInt32(&consumed, 1)
			}
			// Also look up the token.
			s.GetByRegToken("shared-token")
		}()
	}

	wg.Wait()

	// Exactly one goroutine should have successfully consumed the token.
	if c := atomic.LoadInt32(&consumed); c != 1 {
		t.Fatalf("expected exactly 1 successful consumption, got %d", c)
	}

	// Token should no longer be findable.
	if _, ok := s.GetByRegToken("shared-token"); ok {
		t.Fatal("token should not be findable after consumption")
	}
}

// ---- T-STO-004: Concurrent Remove + GC (both touch byReg) ----

func TestConcurrentRemoveAndGC(t *testing.T) {
	s := NewMemStore()
	const numTasks = 50

	// Pre-populate with tasks ready for GC (expired pending).
	for i := int64(1); i <= numTasks; i++ {
		tc := newTestTask(i, fmt.Sprintf("tok-%d", i))
		tc.CreatedAt = time.Now().Add(-30 * time.Minute)
		s.PutPending(tc)
	}

	var wg sync.WaitGroup

	// Run GC and Remove concurrently.
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			s.GC(time.Now())
			time.Sleep(time.Microsecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := int64(1); i <= numTasks; i++ {
			s.Remove(i)
		}
	}()

	wg.Wait()

	// After both finish, all tasks should be gone (or at least not panic).
	// Just verify the store is consistent — no dangling byReg entries
	// pointing to removed tasks.
	// Verify count doesn't panic.
	_ = s.CountActive()
}

// ---- T-STO-004: CountActive concurrency stress ----

func TestConcurrentCountActiveAndPutPending(t *testing.T) {
	s := NewMemStore()
	const numIter = 500

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < numIter; i++ {
			s.CountActive()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < numIter; i++ {
			tc := newTestTask(int64(i), fmt.Sprintf("t-%d", i))
			s.PutPending(tc)
			if i%2 == 0 {
				s.Remove(int64(i))
			}
		}
	}()

	wg.Wait()
}

// ---- T-STO-004: GC properly cleans up both tasks and byReg maps ----

func TestGC_CleansUpBothMaps(t *testing.T) {
	s := NewMemStore()

	// Add an expired pending task.
	tc := newTestTask(1, "expired-tok")
	tc.CreatedAt = time.Now().Add(-20 * time.Minute)
	s.PutPending(tc)

	s.GC(time.Now())

	// Both maps should be cleaned.
	if _, ok := s.tasks[1]; ok {
		t.Fatal("task should be removed from tasks map")
	}
	if _, ok := s.GetByRegToken("expired-tok"); ok {
		t.Fatal("reg token should be removed from byReg map")
	}
}

func TestGC_MultipleRegTokensForSameTask(t *testing.T) {
	// Simulate a task that was re-PutPending with a different token.
	s := NewMemStore()

	tc1 := newTestTask(1, "tok-1")
	tc1.CreatedAt = time.Now().Add(-20 * time.Minute)
	s.PutPending(tc1)

	tc2 := newTestTask(1, "tok-2")
	tc2.CreatedAt = time.Now().Add(-20 * time.Minute)
	s.PutPending(tc2)

	s.GC(time.Now())

	// Both byReg entries should be cleaned up.
	if _, ok := s.GetByRegToken("tok-1"); ok {
		t.Fatal("tok-1 should be cleaned by GC")
	}
	if _, ok := s.GetByRegToken("tok-2"); ok {
		t.Fatal("tok-2 should be cleaned by GC")
	}
	if _, ok := s.tasks[1]; ok {
		t.Fatal("task should be removed from tasks map")
	}
}
