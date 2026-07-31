package session

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"git.0x0f.dev/forgery/internal/store"
)

// newTaskCtx creates a minimal TaskCtx suitable for session tests.
// The session module only accesses ID, RegToken, CreatedAt, and
// calls SetSessionToken / SessionToken.
func newTaskCtx(id int64, regToken string) *store.TaskCtx {
	return &store.TaskCtx{
		ID:        id,
		Task:      nil, // not needed for session tests
		RegToken:  regToken,
		CreatedAt: time.Now(),
	}
}

func TestCreate_Lookup_ReturnsCorrectSession(t *testing.T) {
	m := NewManager()
	taskCtx := newTaskCtx(1, "reg-token-1")

	session := m.Create(taskCtx, "runner-1", []string{"label-a", "label-b"})

	// Verify returned session is populated correctly.
	if session.SessionToken == "" {
		t.Fatal("expected non-empty SessionToken")
	}
	if len(session.SessionToken) != 32 {
		t.Fatalf("expected 32-char hex token, got %d chars: %q",
			len(session.SessionToken), session.SessionToken)
	}
	if session.TaskCtx != taskCtx {
		t.Fatal("session.TaskCtx does not match")
	}
	if session.RunnerName != "runner-1" {
		t.Fatalf("expected RunnerName 'runner-1', got %q", session.RunnerName)
	}
	if len(session.Labels) != 2 || session.Labels[0] != "label-a" || session.Labels[1] != "label-b" {
		t.Fatalf("unexpected Labels: %v", session.Labels)
	}

	// Lookup must return the same session.
	got, ok := m.Lookup(session.SessionToken)
	if !ok {
		t.Fatal("Lookup returned false for existing session")
	}
	if got != session {
		t.Fatal("Lookup returned different pointer")
	}
}

func TestLookup_NonexistentToken_ReturnsFalse(t *testing.T) {
	m := NewManager()

	_, ok := m.Lookup("nonexistent-token")
	if ok {
		t.Fatal("expected false for nonexistent token")
	}
}

func TestRemove_ThenLookup_ReturnsFalse(t *testing.T) {
	m := NewManager()
	taskCtx := newTaskCtx(2, "reg-token-2")

	session := m.Create(taskCtx, "runner-2", nil)

	// Verify it exists before remove.
	_, ok := m.Lookup(session.SessionToken)
	if !ok {
		t.Fatal("expected session to exist before Remove")
	}

	m.Remove(session.SessionToken)

	// Must be gone after remove.
	_, ok = m.Lookup(session.SessionToken)
	if ok {
		t.Fatal("expected session gone after Remove")
	}
}

func TestRemove_Idempotent(t *testing.T) {
	m := NewManager()
	taskCtx := newTaskCtx(3, "reg-token-3")

	session := m.Create(taskCtx, "runner-3", nil)

	// First remove — should succeed.
	m.Remove(session.SessionToken)

	// Second remove — must not panic.
	m.Remove(session.SessionToken)
	m.Remove(session.SessionToken)

	// Remove on never-created token — must not panic.
	m.Remove("never-existed")
}

func TestTaskCtx_SetSessionToken_CalledDuringCreate(t *testing.T) {
	m := NewManager()
	taskCtx := newTaskCtx(4, "reg-token-4")

	// Before Create, SessionToken should be empty.
	if taskCtx.SessionToken != "" {
		t.Fatal("expected empty SessionToken before Create")
	}

	session := m.Create(taskCtx, "runner-4", nil)

	// After Create, TaskCtx.SessionToken must match the session token.
	if taskCtx.SessionToken != session.SessionToken {
		t.Fatalf("TaskCtx.SessionToken = %q, want %q",
			taskCtx.SessionToken, session.SessionToken)
	}
}

func TestDifferentSessions_Isolated(t *testing.T) {
	m := NewManager()

	taskCtx1 := newTaskCtx(10, "reg-token-10")
	taskCtx2 := newTaskCtx(20, "reg-token-20")

	s1 := m.Create(taskCtx1, "runner-10", []string{"a"})
	s2 := m.Create(taskCtx2, "runner-20", []string{"b"})

	// Tokens must differ.
	if s1.SessionToken == s2.SessionToken {
		t.Fatal("different sessions must have different tokens")
	}

	// Each lookup returns the correct session.
	got1, ok1 := m.Lookup(s1.SessionToken)
	if !ok1 || got1 != s1 {
		t.Fatal("Lookup s1 failed or returned wrong session")
	}

	got2, ok2 := m.Lookup(s2.SessionToken)
	if !ok2 || got2 != s2 {
		t.Fatal("Lookup s2 failed or returned wrong session")
	}

	// Removing one session must not affect the other.
	m.Remove(s1.SessionToken)

	_, ok := m.Lookup(s1.SessionToken)
	if ok {
		t.Fatal("s1 should be gone after Remove")
	}

	got2After, ok := m.Lookup(s2.SessionToken)
	if !ok || got2After != s2 {
		t.Fatal("s2 should still exist after removing s1")
	}
}

func TestConcurrent_CreateLookupRemove(t *testing.T) {
	// This test is designed to be run with -race to detect data races.
	m := NewManager()
	n := 200

	var wg sync.WaitGroup

	// Start workers that each create, lookup, and remove their own session.
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			taskCtx := newTaskCtx(int64(idx+1), "reg-token-conc-"+string(rune('0'+idx%10)))
			session := m.Create(taskCtx, "runner-conc", []string{"label"})

			// Lookup must return the session.
			got, ok := m.Lookup(session.SessionToken)
			if !ok {
				t.Errorf("concurrent Lookup failed for worker %d", idx)
				return
			}
			if got.SessionToken != session.SessionToken {
				t.Errorf("concurrent Lookup returned wrong session for worker %d", idx)
				return
			}

			// Remove must succeed.
			m.Remove(session.SessionToken)

			// Must be gone after remove.
			if _, ok := m.Lookup(session.SessionToken); ok {
				t.Errorf("concurrent session still exists after Remove for worker %d", idx)
			}
		}(i)
	}

	wg.Wait()
}

func TestRemove_OnlyTargetSession(t *testing.T) {
	// Verify that Remove only deletes the specific session, not others
	// that happen to share labels/names.
	m := NewManager()

	taskCtx1 := newTaskCtx(100, "reg-100")
	taskCtx2 := newTaskCtx(200, "reg-200")

	s1 := m.Create(taskCtx1, "same-name", []string{"x", "y"})
	s2 := m.Create(taskCtx2, "same-name", []string{"x", "y"})

	// Different tokens even though metadata matches.
	if s1.SessionToken == s2.SessionToken {
		t.Fatal("tokens must differ")
	}

	m.Remove(s1.SessionToken)

	// s1 gone.
	if _, ok := m.Lookup(s1.SessionToken); ok {
		t.Fatal("s1 should be removed")
	}
	// s2 still present.
	if _, ok := m.Lookup(s2.SessionToken); !ok {
		t.Fatal("s2 should still exist")
	}
}

func TestNewManager_InitializesMap(t *testing.T) {
	m := NewManager()
	if m.sessions == nil {
		t.Fatal("NewManager must initialize the sessions map")
	}
	// Must be usable immediately — Lookup on empty manager must not panic.
	_, ok := m.Lookup("anything")
	if ok {
		t.Fatal("expected false on empty manager")
	}
}

// ── Expire ──

// backdate sets a session's CreatedAt to force it to a known age.
// CreatedAt is immutable in production; tests may set it directly before
// any concurrent access begins.
func backdate(s *Session, age time.Duration) {
	s.CreatedAt = time.Now().Add(-age)
}

func TestExpire_RemovesExpired_KeepsFresh(t *testing.T) {
	m := NewManager()

	expired := m.Create(newTaskCtx(1, "reg-1"), "runner-a", nil)
	fresh := m.Create(newTaskCtx(2, "reg-2"), "runner-b", nil)
	backdate(expired, 2*time.Hour)

	got := m.Expire(time.Now(), time.Hour)

	if len(got) != 1 {
		t.Fatalf("expected 1 expired session, got %d", len(got))
	}
	if got[0] != expired {
		t.Fatal("Expire returned the wrong session")
	}

	// Expired session must be deleted from the manager.
	if _, ok := m.Lookup(expired.SessionToken); ok {
		t.Error("expired session still present after Expire")
	}
	// Fresh session must survive.
	if _, ok := m.Lookup(fresh.SessionToken); !ok {
		t.Error("fresh session removed by Expire")
	}
}

func TestExpire_Boundary_ExactlyMaxAgeNotExpired(t *testing.T) {
	m := NewManager()
	s := m.Create(newTaskCtx(1, "reg-1"), "runner", nil)

	// Exactly maxAge old: strict > comparison must keep it alive.
	const maxAge = time.Hour
	now := time.Now()
	s.CreatedAt = now.Add(-maxAge)

	got := m.Expire(now, maxAge)
	if len(got) != 0 {
		t.Fatalf("session exactly maxAge old must not expire, got %d", len(got))
	}
	if _, ok := m.Lookup(s.SessionToken); !ok {
		t.Fatal("session exactly maxAge old was removed")
	}

	// One nanosecond past maxAge: must expire.
	got = m.Expire(now.Add(time.Nanosecond), maxAge)
	if len(got) != 1 || got[0] != s {
		t.Fatalf("session past maxAge must expire, got %d sessions", len(got))
	}
}

func TestExpire_ReturnsAllExpiredSessions(t *testing.T) {
	m := NewManager()

	var want []*Session
	for i := int64(1); i <= 3; i++ {
		s := m.Create(newTaskCtx(i, fmt.Sprintf("reg-%d", i)), "runner", nil)
		backdate(s, 2*time.Hour)
		want = append(want, s)
	}
	keep := m.Create(newTaskCtx(9, "reg-9"), "runner", nil) // fresh

	got := m.Expire(time.Now(), time.Hour)

	if len(got) != len(want) {
		t.Fatalf("expected %d expired sessions, got %d", len(want), len(got))
	}
	// Order is unspecified — compare by session token.
	gotTokens := make(map[string]bool, len(got))
	for _, s := range got {
		gotTokens[s.SessionToken] = true
	}
	for _, s := range want {
		if !gotTokens[s.SessionToken] {
			t.Errorf("expected session %q in Expire result", s.SessionToken)
		}
	}

	// Expired sessions must be gone, the fresh one must remain.
	for _, s := range want {
		if _, ok := m.Lookup(s.SessionToken); ok {
			t.Errorf("expired session %q still present", s.SessionToken)
		}
	}
	if _, ok := m.Lookup(keep.SessionToken); !ok {
		t.Error("fresh session removed by Expire")
	}
}

func TestExpire_EmptyAndAllFresh(t *testing.T) {
	m := NewManager()

	// Empty manager: no panic, empty result.
	if got := m.Expire(time.Now(), time.Hour); len(got) != 0 {
		t.Fatalf("expected no expired sessions on empty manager, got %d", len(got))
	}

	// Fresh sessions: nothing expired.
	m.Create(newTaskCtx(1, "reg-1"), "runner", nil)
	m.Create(newTaskCtx(2, "reg-2"), "runner", nil)
	if got := m.Expire(time.Now(), time.Hour); len(got) != 0 {
		t.Fatalf("expected no expired sessions, got %d", len(got))
	}
}

func TestExpire_ZeroMaxAge(t *testing.T) {
	m := NewManager()
	s := m.Create(newTaskCtx(1, "reg-1"), "runner", nil)

	// maxAge = 0 expires any session created strictly before now.
	now := time.Now()
	s.CreatedAt = now.Add(-time.Millisecond)

	got := m.Expire(now, 0)
	if len(got) != 1 || got[0] != s {
		t.Fatalf("expected the aged session to expire with maxAge 0, got %d", len(got))
	}
}

func TestExpire_ConcurrentWithCreateAndLookup(t *testing.T) {
	// Run with -race: Expire must not race with Create/Lookup/Remove.
	m := NewManager()

	const n = 100
	var wg sync.WaitGroup

	// Seed sessions that will age past the expiry window.
	for i := 0; i < 50; i++ {
		s := m.Create(newTaskCtx(int64(i+1), "reg-seed"), "runner", nil)
		backdate(s, 2*time.Hour)
	}

	// Workers interleave Expire with Create/Lookup/Remove.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < n; j++ {
				s := m.Create(newTaskCtx(int64(1000+j), "reg-new"), "runner", nil)
				_, _ = m.Lookup(s.SessionToken)
				m.Remove(s.SessionToken)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < n; j++ {
			_ = m.Expire(time.Now(), time.Hour)
		}
	}()

	wg.Wait()

	// Only the seeded sessions (all expired) may be gone; the fresh ones
	// created by the workers were removed by their own Remove. The manager
	// must still be internally consistent (no panics, no leftovers of the
	// expired seeds).
	for i := 0; i < 50; i++ {
		// Can't assert presence (workers removed their own), but Lookup
		// must not panic and Expire must not have returned nil entries.
		for _, s := range m.Expire(time.Now(), 0) {
			if s == nil {
				t.Error("Expire returned a nil session")
			}
		}
	}
}
