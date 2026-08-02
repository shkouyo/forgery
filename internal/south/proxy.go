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

package south

import (
	"crypto/tls"
	"encoding/base64"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/north"
	"git.0x0f.dev/forgery/internal/session"
	"git.0x0f.dev/forgery/internal/store"
)

// ── git path matching ─────────────────────────────────────────────────────

// gitEndpoints are the git smart-HTTP (and dumb-HTTP) endpoints that may
// follow the /{owner}/{repo} prefix of a Forgejo repository URL. Requests
// for any other suffix (web UI pages, issues, etc.) are not proxied.
var gitEndpoints = []string{
	"/info/refs",
	"/git-upload-pack",
	"/git-receive-pack",
	"/git-upload-archive",
	"/HEAD",
}

// matchGitPath parses a Forgejo git HTTP path of the form
// /{owner}/{repo}[.git]/{endpoint} (the .git clone suffix is optional and
// stripped) and reports whether it is a git request Forgery should proxy.
// Endpoints are the smart-HTTP RPCs (info/refs, git-upload-pack,
// git-receive-pack, git-upload-archive) plus the dumb-HTTP /HEAD file and
// /objects/ tree. It returns the owner and repo names on a match.
func matchGitPath(p string) (owner, repo string, ok bool) {
	rest := strings.TrimPrefix(p, "/")
	owner, rest, ok = strings.Cut(rest, "/")
	if !ok || !validRepoName(owner) {
		return "", "", false
	}
	repo, tail, ok := strings.Cut(rest, "/")
	if !ok {
		return "", "", false
	}
	repo = strings.TrimSuffix(repo, ".git")
	if !validRepoName(repo) {
		return "", "", false
	}
	// The Cut above removed the leading slash of the endpoint; restore it
	// so matchGitEndpoint compares against its canonical /-prefixed forms.
	if !matchGitEndpoint("/" + tail) {
		return "", "", false
	}
	return owner, repo, true
}

// matchGitEndpoint reports whether rest is one of the git endpoints that
// follow the /{owner}/{repo} prefix.
func matchGitEndpoint(rest string) bool {
	for _, e := range gitEndpoints {
		if rest == e {
			return true
		}
	}
	return strings.HasPrefix(rest, "/objects/")
}

// validRepoName reports whether s is a plausible Forgejo owner/repo path
// segment: non-empty, no path traversal, and only the characters Forgejo
// permits in repository names (letters, digits, '-', '_', '.').
func validRepoName(s string) bool {
	if s == "" || s == "." || s == ".." {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}

// ── token extraction ──────────────────────────────────────────────────────

// proxyToken extracts the Forgejo task token from an Authorization header.
// Git clients (actions/checkout) authenticate with HTTP basic auth
// ("Basic base64(x-access-token:<token>)"), while Forgejo API and artifact
// requests use a bearer-style header ("Bearer <token>" or "token <token>").
// The scheme is matched case-insensitively — git and octokit emit lowercase
// forms ("basic", "token") in the wild. Some git clients put the token in
// the username field ("<token>:" or "<token>:x-oauth-basic"); the password,
// when present, takes precedence.
func proxyToken(auth string) (string, bool) {
	auth = strings.TrimSpace(auth)
	scheme, rest, _ := strings.Cut(auth, " ")
	rest = strings.TrimSpace(rest)
	switch strings.ToLower(scheme) {
	case "basic":
		raw, err := base64.StdEncoding.DecodeString(rest)
		if err != nil {
			return "", false
		}
		user, pass, ok := strings.Cut(string(raw), ":")
		if !ok || user == "" {
			return "", false
		}
		// The password is the token in the common forms
		// ("x-access-token:<token>", "<user>:<token>"), except for the
		// GitHub-style sentinel "<token>:x-oauth-basic" where the token
		// sits in the username field.
		if pass != "" && pass != "x-oauth-basic" {
			return pass, true
		}
		return user, true
	case "bearer":
		if rest != "" {
			return rest, true
		}
	case "token":
		if rest != "" {
			return rest, true
		}
	}
	return "", false
}

// ── token → instance registry ─────────────────────────────────────────────

// proxyEntryTTL is the wall-clock lifetime of a token→instance routing
// entry. It is only a memory backstop: an entry is also invalidated as soon
// as its task reaches a terminal state or its runner session is removed, so
// a long-running task is never cut off no matter how long it runs.
const proxyEntryTTL = 24 * time.Hour

// tokenRegistry maps Forgejo task tokens — the tokens the internal runner
// presents in the Authorization header of git/API/artifact requests — to the
// instance that owns the task. Entries are created when the runner fetches
// a task and lazily evicted when they expire, the task terminates, or the
// runner's session disappears.
type tokenRegistry struct {
	mu       sync.Mutex
	entries  map[string]*tokenEntry
	sessions *session.Manager // liveness signal: an entry dies with its session
}

// tokenEntry is one token→instance mapping.
type tokenEntry struct {
	instance     string
	task         *store.TaskCtx
	sessionToken string
	expireAt     time.Time
}

func newTokenRegistry(sessions *session.Manager) *tokenRegistry {
	return &tokenRegistry{
		entries:  make(map[string]*tokenEntry),
		sessions: sessions,
	}
}

// putTaskTokens registers every runtime token the internal runner may use
// for this task's git/API requests, routing all of them to the task's
// owning instance. Idempotent: re-fetching the same task overwrites the
// entries with a fresh expiry.
func (r *tokenRegistry) putTaskTokens(tc *store.TaskCtx, sessionToken string, now time.Time) {
	expireAt := now.Add(proxyEntryTTL)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, tok := range runtimeTaskTokens(tc.Task) {
		r.entries[tok] = &tokenEntry{
			instance:     tc.Instance,
			task:         tc,
			sessionToken: sessionToken,
			expireAt:     expireAt,
		}
	}
}

// lookup returns the instance owning the given task token. Entries whose
// wall-clock lifetime ran out, whose task reached a terminal state, or
// whose runner session is gone are evicted lazily and reported as unknown.
func (r *tokenRegistry) lookup(token string, now time.Time) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[token]
	if !ok || !now.Before(e.expireAt) || e.task.Status() == store.StatusTerminal {
		delete(r.entries, token)
		return "", false
	}
	if _, ok := r.sessions.Lookup(e.sessionToken); !ok {
		delete(r.entries, token)
		return "", false
	}
	return e.instance, true
}

// count returns the number of registered entries (test helper).
func (r *tokenRegistry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// runtimeTaskTokens returns the tokens the internal runner may present in
// the Authorization header of git/API requests for task, deduplicated, in
// precedence order: the task context token (context["token"], used by
// actions/checkout and the artifact API), the Gitea runtime token
// (context["gitea_runtime_token"], used by newer artifact uploads), and the
// GITEA_TOKEN / GITHUB_TOKEN secret overrides forgejo-runner applies when
// the context token is empty.
func runtimeTaskTokens(task *v1.Task) []string {
	var out []string
	seen := make(map[string]bool)
	add := func(s string) {
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		out = append(out, s)
	}
	if task != nil {
		if ctx := task.GetContext(); ctx != nil {
			add(ctx.Fields["token"].GetStringValue())
			add(ctx.Fields["gitea_runtime_token"].GetStringValue())
		}
		add(task.GetSecrets()["GITEA_TOKEN"])
		add(task.GetSecrets()["GITHUB_TOKEN"])
	}
	return out
}

// ── reverse proxy ─────────────────────────────────────────────────────────

// proxyHandler reverse-proxies the HTTP paths the internal runner needs
// from the Forgejo instance that owns the task: git smart/dumb HTTP for
// actions/checkout, the /api/v1/repos/… default-branch query, and the
// /api/actions_pipeline/… artifact endpoints. The owning instance is
// resolved from the task token in the request's Authorization header and
// the header is passed through unchanged, so the upstream Forgejo validates
// the token itself.
type proxyHandler struct {
	registry *tokenRegistry
	resolver north.Resolver
	log      *slog.Logger

	mu         sync.Mutex
	transports map[string]*http.Transport // per-instance, keyed by instance name
}

// newProxyHandler builds the reverse-proxy handler for the south server.
func (h *Handler) newProxyHandler() http.Handler {
	return &proxyHandler{
		registry:   h.registry,
		resolver:   h.resolver,
		log:        h.log,
		transports: make(map[string]*http.Transport),
	}
}

// isProxiedPath reports whether the request path belongs to one of the
// three path classes the south proxy serves: git paths and the two Forgejo
// API prefixes.
func isProxiedPath(path string) bool {
	if _, _, ok := matchGitPath(path); ok {
		return true
	}
	return strings.HasPrefix(path, "/api/v1/repos/") ||
		strings.HasPrefix(path, "/api/actions_pipeline/")
}

func (p *proxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !isProxiedPath(r.URL.Path) {
		http.NotFound(w, r)
		return
	}

	token, ok := proxyToken(r.Header.Get("Authorization"))
	if !ok {
		p.log.Warn("proxy: request without a recognizable task token",
			"method", r.Method, "path", r.URL.Path)
		writeProxyError(w, http.StatusUnauthorized, "missing task token")
		return
	}

	instance, ok := p.registry.lookup(token, time.Now())
	if !ok {
		// Tasks whose context carries no token field (the runner falls back
		// to a secret) never reach the registry; with exactly one configured
		// instance there is no ambiguity, so route there directly.
		instance, ok = p.resolver.OnlyInstance()
	}
	if !ok {
		p.log.Warn("proxy: unknown task token",
			"method", r.Method, "path", r.URL.Path)
		writeProxyError(w, http.StatusUnauthorized, "unknown task token")
		return
	}

	inst, _, ok := p.resolver.Resolve(instance)
	if !ok {
		p.log.Error("proxy: task token routes to unknown instance",
			"instance", instance, "method", r.Method, "path", r.URL.Path)
		writeProxyError(w, http.StatusBadGateway, "unknown instance")
		return
	}
	target, err := url.Parse(inst.ForgejoURL)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
		p.log.Error("proxy: invalid instance forgejo_url",
			"instance", instance, "forgejo_url", inst.ForgejoURL)
		writeProxyError(w, http.StatusBadGateway, "invalid instance URL")
		return
	}

	plog := p.log.With("instance", instance)
	plog.Debug("proxy: forwarding request",
		"method", r.Method, "path", r.URL.Path, "query", r.URL.RawQuery, "target", inst.ForgejoURL)

	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			// Same path, same query string: SetURL keeps both (the target
			// has no base path) and blanks Out.Host, so the upstream sees
			// Host = the Forgejo host. The Authorization header is copied
			// unchanged from the inbound request.
			pr.SetURL(target)
			// Reproduce the Director-path default forwarding headers.
			pr.SetXForwarded()
		},
		Transport:     p.transport(inst),
		FlushInterval: -1, // stream git packfiles and artifact bodies immediately
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			plog.Error("proxy: upstream request failed",
				"path", r.URL.Path, "err", err)
			writeProxyError(w, http.StatusBadGateway, "bad gateway")
		},
	}
	rp.ServeHTTP(w, r)
}

// transport returns the HTTP transport for an instance, cloning the default
// transport once per instance and applying the instance's TLS settings
// (tls_insecure_skip_verify), mirroring how the northbound client builds
// its transport.
func (p *proxyHandler) transport(inst config.Instance) http.RoundTripper {
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.transports[inst.Name]; ok {
		return t
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = &tls.Config{InsecureSkipVerify: inst.TLSInsecureSkipVerify} // #nosec G402
	p.transports[inst.Name] = t
	return t
}

// writeProxyError writes a JSON error response.
func writeProxyError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":` + strconv.Quote(msg) + "}\n"))
}
