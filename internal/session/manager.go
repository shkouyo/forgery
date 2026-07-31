// Package session manages the lifecycle of internal runner sessions in the
// forgery proxy. An internal forgejo-runner registers with a one-time token,
// receives a session token, and all subsequent RPCs (Declare, FetchTask,
// UpdateTask, UpdateLog) are authenticated via that session token.
//
// The Manager provides concurrency-safe Create, Lookup, Touch, Remove,
// and Expire operations backed by a map protected with sync.RWMutex.
package session

import (
	"encoding/hex"
	"sync"
	"time"

	"git.0x0f.dev/forgery/internal/store"
	"git.0x0f.dev/forgery/internal/token"
)

// sessionTokenBytes is the length in bytes of the randomly generated
// session token, encoded as 32 hex characters.
const sessionTokenBytes = 16

// Session represents an authenticated internal runner session. It binds a
// session token to the task context that the runner is authorized to access,
// along with metadata declared by the runner during registration.
//
// LastActivity anchors the Expire deadline that reaps orphaned sessions (see
// Manager.Expire): it is initialized at creation and refreshed by
// Manager.Touch on every authenticated RPC, so an active runner's session is
// continuously renewed while a silent one ages toward expiry.
//
// LastActivity is not guarded by the manager mutex once the session is
// handed out; Touch refreshes it under the manager lock, and Expire reads it
// under the same lock, so the two never race.
type Session struct {
	SessionToken string
	TaskCtx      *store.TaskCtx
	RunnerName   string
	Labels       []string
	LastActivity time.Time // refreshed by Touch on every authenticated RPC
}

// Manager is a concurrency-safe registry of active sessions, keyed by session
// token. A session is created when an internal runner successfully registers
// with a valid one-time registration token, and is removed when the task
// reaches a terminal state or when Expire reaps it as orphaned.
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session // key = sessionToken
}

// NewManager returns an initialized Manager with an empty session map.
func NewManager() *Manager {
	return &Manager{
		sessions: make(map[string]*Session),
	}
}

// Create generates a new session for the given task context. It generates a
// cryptographically random 16-byte session token (32 hex characters), stores
// the session in the manager, links the session token back to the TaskCtx via
// SetSessionToken, and returns the session.
//
// The runnerName and labels are metadata declared by the internal runner during
// Register.
func (m *Manager) Create(taskCtx *store.TaskCtx, runnerName string, labels []string) *Session {
	raw, err := token.GenerateBytes(sessionTokenBytes)
	if err != nil {
		// crypto/rand.Read failures are effectively impossible on a
		// working system; panic to surface a fatal runtime error.
		panic("session: failed to generate token: " + err.Error())
	}

	return m.CreateWithToken(taskCtx, hex.EncodeToString(raw), runnerName, labels)
}

// CreateWithToken creates a session with an explicit session token (instead of
// generating a random one). This is used when auto-registering a runner that
// skips the Register RPC (e.g., forgejo-runner one-job) — the registration
// token is reused as the session token so subsequent RPCs with the same token
// are recognized.
func (m *Manager) CreateWithToken(taskCtx *store.TaskCtx, sessionToken string, runnerName string, labels []string) *Session {
	session := &Session{
		SessionToken: sessionToken,
		TaskCtx:      taskCtx,
		RunnerName:   runnerName,
		Labels:       labels,
		LastActivity: time.Now(),
	}

	m.mu.Lock()
	m.sessions[sessionToken] = session
	m.mu.Unlock()

	taskCtx.SetSessionToken(sessionToken)

	return session
}

// Lookup returns the session associated with the given token. The second return
// value is false when no session matches the token.
func (m *Manager) Lookup(sessionToken string) (*Session, bool) {
	m.mu.RLock()
	session, ok := m.sessions[sessionToken]
	m.mu.RUnlock()
	return session, ok
}

// Touch refreshes the LastActivity timestamp of the session identified by
// sessionToken, extending the deadline after which Expire reaps it. It
// returns false when no session matches the token. The south handler calls
// Touch on every authenticated RPC, so a runner that keeps talking to the
// proxy never has its session expired, while a runner that registered and
// then went silent is still reaped once LastActivity ages past maxAge.
func (m *Manager) Touch(sessionToken string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionToken]
	if !ok {
		return false
	}
	s.LastActivity = time.Now()
	return true
}

// Remove deletes the session identified by sessionToken from the manager. It is
// idempotent: calling Remove with a nonexistent or already-removed token is a
// no-op and never panics.
func (m *Manager) Remove(sessionToken string) {
	m.mu.Lock()
	delete(m.sessions, sessionToken)
	m.mu.Unlock()
}

// Expire deletes and returns every session whose last activity is older than
// maxAge, i.e. every session with now.Sub(LastActivity) > maxAge. A session
// whose last activity is exactly maxAge old is not expired. The order of the
// returned slice is unspecified.
//
// A session is the runtime credential of a task: its normal lifecycle is
// bounded by the task itself and terminated explicitly — south removes the
// session in the terminal UpdateTask path, and run removes it in the failure
// and GA_STARTUP_TIMEOUT branches. Expire is the fallback for orphaned
// sessions: a runner that registered and then died without ever reporting a
// terminal state (or a HandleTask that exited without cleaning up) would
// otherwise leak the session and its Running task forever. Because every
// authenticated RPC refreshes LastActivity via Touch, Expire only ever fires
// on sessions whose runner has been silent for longer than maxAge — active
// long-running tasks are never reaped. Expire is idempotent in effect —
// sessions already removed are simply not returned.
func (m *Manager) Expire(now time.Time, maxAge time.Duration) []*Session {
	m.mu.Lock()
	defer m.mu.Unlock()

	var expired []*Session
	for token, s := range m.sessions {
		if now.Sub(s.LastActivity) > maxAge {
			expired = append(expired, s)
			delete(m.sessions, token)
		}
	}
	return expired
}
