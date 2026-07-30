package session

import (
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
