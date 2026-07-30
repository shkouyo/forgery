// Package session manages the lifecycle of internal runner sessions in the
// forgery proxy. An internal forgejo-runner registers with a one-time token,
// receives a session token, and all subsequent RPCs (Declare, FetchTask,
// UpdateTask, UpdateLog) are authenticated via that session token.
//
// The Manager provides concurrency-safe Create, Lookup, and Remove operations
// backed by a map protected with sync.RWMutex.
package session

import (
	"encoding/hex"
	"sync"

	"git.0x0f.dev/forgery/internal/store"
	"git.0x0f.dev/forgery/internal/token"
)

// Session represents an authenticated internal runner session. It binds a
// session token to the task context that the runner is authorized to access,
// along with metadata declared by the runner during registration.
type Session struct {
	SessionToken string
	TaskCtx      *store.TaskCtx
	RunnerName   string
	Labels       []string
}

// Manager is a concurrency-safe registry of active sessions, keyed by session
// token. A session is created when an internal runner successfully registers
// with a valid one-time registration token, and is removed when the task
// reaches a terminal state.
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
	raw, err := token.GenerateBytes(16)
	if err != nil {
		// crypto/rand.Read failures are effectively impossible on a
		// working system; panic to surface a fatal runtime error.
		panic("session: failed to generate token: " + err.Error())
	}

	sessionToken := hex.EncodeToString(raw)

	session := &Session{
		SessionToken: sessionToken,
		TaskCtx:      taskCtx,
		RunnerName:   runnerName,
		Labels:       labels,
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

// Remove deletes the session identified by sessionToken from the manager. It is
// idempotent: calling Remove with a nonexistent or already-removed token is a
// no-op and never panics.
func (m *Manager) Remove(sessionToken string) {
	m.mu.Lock()
	delete(m.sessions, sessionToken)
	m.mu.Unlock()
}
