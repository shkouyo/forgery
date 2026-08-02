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
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/session"
	"git.0x0f.dev/forgery/internal/store"
)

// ── matchGitPath ──────────────────────────────────────────────────────────

func TestMatchGitPath(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		wantOwner string
		wantRepo  string
		wantMatch bool
	}{
		{name: "smart http info/refs", path: "/owner/repo/info/refs", wantOwner: "owner", wantRepo: "repo", wantMatch: true},
		{name: "smart http with .git suffix", path: "/owner/repo.git/info/refs", wantOwner: "owner", wantRepo: "repo", wantMatch: true},
		{name: "upload pack", path: "/owner/repo/git-upload-pack", wantOwner: "owner", wantRepo: "repo", wantMatch: true},
		{name: "upload pack with .git suffix", path: "/owner/repo.git/git-upload-pack", wantOwner: "owner", wantRepo: "repo", wantMatch: true},
		{name: "receive pack", path: "/owner/repo/git-receive-pack", wantOwner: "owner", wantRepo: "repo", wantMatch: true},
		{name: "upload archive", path: "/owner/repo/git-upload-archive", wantOwner: "owner", wantRepo: "repo", wantMatch: true},
		{name: "dumb http HEAD", path: "/owner/repo/HEAD", wantOwner: "owner", wantRepo: "repo", wantMatch: true},
		{name: "dumb http objects", path: "/owner/repo/objects/ab/0123456789abcdef0123456789abcdef01234567", wantOwner: "owner", wantRepo: "repo", wantMatch: true},
		{name: "objects with .git suffix", path: "/owner/repo.git/objects/info/packs", wantOwner: "owner", wantRepo: "repo", wantMatch: true},
		{name: "names with dashes dots underscores", path: "/some-owner/repo.name_2.git/info/refs", wantOwner: "some-owner", wantRepo: "repo.name_2", wantMatch: true},
		{name: "bare repo path", path: "/owner/repo", wantMatch: false},
		{name: "bare repo with .git", path: "/owner/repo.git", wantMatch: false},
		{name: "trailing slash", path: "/owner/repo/", wantMatch: false},
		{name: "web page", path: "/owner/repo/issues", wantMatch: false},
		{name: "web page settings", path: "/owner/repo/settings", wantMatch: false},
		{name: "connect rpc", path: "/runner.v1.RunnerService/FetchTask", wantMatch: false},
		{name: "connect rpc ping", path: "/ping.v1.PingService/Ping", wantMatch: false},
		{name: "forged api path", path: "/api/foo", wantMatch: false},
		{name: "v1 api repos", path: "/api/v1/repos/owner/repo", wantMatch: false},
		{name: "single segment", path: "/owner", wantMatch: false},
		{name: "single segment trailing slash", path: "/owner/", wantMatch: false},
		{name: "empty first segment", path: "//owner/repo/info/refs", wantMatch: false},
		{name: "path traversal repo", path: "/owner/../etc/info/refs", wantMatch: false},
		{name: "dotgit only", path: "/owner/.git/info/refs", wantMatch: false},
		{name: "invalid chars", path: "/own+er/repo/info/refs", wantMatch: false},
		{name: "invalid chars repo", path: "/owner/re%40po/info/refs", wantMatch: false},
		{name: "uppercase owner", path: "/Owner/Repo/info/refs", wantOwner: "Owner", wantRepo: "Repo", wantMatch: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, repo, ok := matchGitPath(tt.path)
			if ok != tt.wantMatch {
				t.Fatalf("matchGitPath(%q) = %v, want %v", tt.path, ok, tt.wantMatch)
			}
			if !ok {
				return
			}
			if owner != tt.wantOwner || repo != tt.wantRepo {
				t.Fatalf("matchGitPath(%q) = (%q, %q), want (%q, %q)",
					tt.path, owner, repo, tt.wantOwner, tt.wantRepo)
			}
		})
	}
}

// ── proxyToken ────────────────────────────────────────────────────────────

func TestProxyToken(t *testing.T) {
	basic := func(userPass string) string {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(userPass))
	}
	tests := []struct {
		name      string
		auth      string
		wantToken string
		wantOK    bool
	}{
		{name: "checkout basic", auth: basic("x-access-token:task-tok-1"), wantToken: "task-tok-1", wantOK: true},
		{name: "lowercase basic scheme", auth: "basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:task-tok-1")), wantToken: "task-tok-1", wantOK: true},
		{name: "mixed case basic scheme", auth: "bAsIc " + base64.StdEncoding.EncodeToString([]byte("x-access-token:task-tok-1")), wantToken: "task-tok-1", wantOK: true},
		{name: "github style basic", auth: basic("task-tok-1:x-oauth-basic"), wantToken: "task-tok-1", wantOK: true},
		{name: "bare user token", auth: basic("task-tok-1:"), wantToken: "task-tok-1", wantOK: true},
		{name: "bearer", auth: "Bearer task-tok-1", wantToken: "task-tok-1", wantOK: true},
		{name: "mixed case bearer scheme", auth: "BeArEr task-tok-1", wantToken: "task-tok-1", wantOK: true},
		{name: "lowercase bearer scheme", auth: "bearer task-tok-1", wantToken: "task-tok-1", wantOK: true},
		{name: "token scheme (octokit lowercase)", auth: "token task-tok-1", wantToken: "task-tok-1", wantOK: true},
		{name: "mixed case token scheme", auth: "ToKeN task-tok-1", wantToken: "task-tok-1", wantOK: true},
		{name: "empty header", auth: "", wantOK: false},
		{name: "garbage scheme", auth: "Digest foo", wantOK: false},
		{name: "lowercase garbage scheme", auth: "digest foo", wantOK: false},
		{name: "malformed base64", auth: "Basic not-base64!!", wantOK: false},
		{name: "empty password and user", auth: basic(":"), wantOK: false},
		{name: "empty bearer", auth: "Bearer  ", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tok, ok := proxyToken(tt.auth)
			if ok != tt.wantOK || (ok && tok != tt.wantToken) {
				t.Fatalf("proxyToken(%q) = (%q, %v), want (%q, %v)",
					tt.auth, tok, ok, tt.wantToken, tt.wantOK)
			}
		})
	}
}

// ── runtimeTaskTokens ─────────────────────────────────────────────────────

func TestRuntimeTaskTokens(t *testing.T) {
	mk := func(ctx map[string]any, secrets map[string]string) *v1.Task {
		task := &v1.Task{Secrets: secrets}
		if ctx != nil {
			s, err := structpb.NewStruct(ctx)
			if err != nil {
				t.Fatalf("structpb.NewStruct: %v", err)
			}
			task.Context = s
		}
		return task
	}

	t.Run("context token only", func(t *testing.T) {
		got := runtimeTaskTokens(mk(map[string]any{"token": "tok-a"}, nil))
		if len(got) != 1 || got[0] != "tok-a" {
			t.Fatalf("got %v, want [tok-a]", got)
		}
	})
	t.Run("context token and runtime token", func(t *testing.T) {
		got := runtimeTaskTokens(mk(map[string]any{
			"token": "tok-a", "gitea_runtime_token": "tok-rt",
		}, nil))
		want := []string{"tok-a", "tok-rt"}
		if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
	t.Run("secret fallbacks", func(t *testing.T) {
		got := runtimeTaskTokens(mk(nil, map[string]string{
			"GITEA_TOKEN": "tok-secret", "GITHUB_TOKEN": "tok-gh",
		}))
		want := []string{"tok-secret", "tok-gh"}
		if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
	t.Run("dedupe and empty", func(t *testing.T) {
		got := runtimeTaskTokens(mk(map[string]any{
			"token": "", "gitea_runtime_token": "same",
		}, map[string]string{"GITEA_TOKEN": "same", "GITHUB_TOKEN": ""}))
		if len(got) != 1 || got[0] != "same" {
			t.Fatalf("got %v, want [same]", got)
		}
	})
	t.Run("nil task", func(t *testing.T) {
		if got := runtimeTaskTokens(nil); len(got) != 0 {
			t.Fatalf("got %v, want empty", got)
		}
	})
}

// ── integration fixtures ──────────────────────────────────────────────────

// recordedRequest captures the parts of a proxied request the fake Forgejo
// cares about: method, path, query string, Authorization header, and body.
type recordedRequest struct {
	method string
	path   string
	query  string
	auth   string
	body   string
}

// fakeForgejo is a minimal Forgejo stand-in for proxy tests. It records
// every request and answers with a distinctive body that echoes the path
// and query string, so tests can assert path/query passthrough, response
// passthrough, and per-instance routing in one place.
type fakeForgejo struct {
	mu       sync.Mutex
	requests []recordedRequest
	server   *httptest.Server
}

func newFakeForgejo(t *testing.T) *fakeForgejo {
	t.Helper()
	f := &fakeForgejo{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.requests = append(f.requests, recordedRequest{
			method: r.Method,
			path:   r.URL.Path,
			query:  r.URL.RawQuery,
			auth:   r.Header.Get("Authorization"),
			body:   string(body),
		})
		f.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "forgejo:%s?%s", r.URL.Path, r.URL.RawQuery)
	}))
	t.Cleanup(f.server.Close)
	return f
}

// last returns the most recent recorded request.
func (f *fakeForgejo) last() recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[len(f.requests)-1]
}

func (f *fakeForgejo) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

// proxyFixture wires a full south server (the real mux from NewServer) with
// a resolver over the given fake Forgejos and a task owned by taskInstance.
// The task's context carries taskToken; the runner session is created and
// FetchTask is invoked, so the token registry is seeded exactly through the
// production path. An empty taskToken produces a task without a context
// token field (registry stays empty — the single-instance fallback case).
type proxyFixture struct {
	srv          *httptest.Server
	h            *Handler
	sm           *session.Manager
	taskCtx      *store.TaskCtx
	sessionToken string
	registry     *tokenRegistry
}

func newProxyFixture(t *testing.T, forgejos map[string]*fakeForgejo, taskInstance, taskToken string) *proxyFixture {
	t.Helper()
	ms := newMockStore()
	sm := session.NewManager()
	entries := make(map[string]instanceEntry, len(forgejos))
	for name, f := range forgejos {
		entries[name] = instanceEntry{
			inst:   config.Instance{Name: name, ForgejoURL: f.server.URL},
			client: newMockClient(),
		}
	}
	h := NewHandler(ms, sm, newFakeResolver(entries), slog.New(slog.DiscardHandler))

	sessionToken := "sess-" + taskToken
	taskCtx := &store.TaskCtx{
		ID:          77,
		Instance:    taskInstance,
		Task:        &v1.Task{Id: 77},
		RegToken:    "reg-" + taskToken,
		RegTokenTTL: 15 * time.Minute,
		CreatedAt:   time.Now(),
	}
	if taskToken != "" {
		taskCtx.Task.Context = &structpb.Struct{Fields: map[string]*structpb.Value{
			"token": structpb.NewStringValue(taskToken),
		}}
	}
	ms.PutPending(taskCtx)
	sm.CreateWithToken(taskCtx, sessionToken, "", nil)

	// Fetch the task so the token registry is populated (production path).
	req := connect.NewRequest(&v1.FetchTaskRequest{})
	setBearer(req, sessionToken)
	if _, err := h.FetchTask(context.Background(), req); err != nil {
		t.Fatalf("FetchTask failed: %v", err)
	}

	srv := httptest.NewServer(NewServer(h, ":0").Handler)
	t.Cleanup(srv.Close)
	return &proxyFixture{srv: srv, h: h, sm: sm, taskCtx: taskCtx, sessionToken: sessionToken, registry: h.registry}
}

// do performs an HTTP request against the south server.
func (f *proxyFixture) do(t *testing.T, method, path, query, auth, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, f.srv.URL+path+"?"+query, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request %s %s failed: %v", method, path, err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// readBody drains and returns the response body.
func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

// basicAuth builds a Basic Authorization header for x-access-token:token,
// the exact form actions/checkout uses for git requests.
func basicAuth(token string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+token))
}

// ── proxy integration tests ───────────────────────────────────────────────

// TestProxyGitAndAPIPassthrough exercises all three proxied path classes
// against one instance: git smart HTTP (info/refs with query string,
// git-upload-pack with a request body), the /api/v1/repos/… default-branch
// query, and the /api/actions_pipeline/… artifact upload. Path, query,
// Authorization, and request body must all arrive unchanged at the
// upstream, and the upstream response must come back unchanged.
func TestProxyGitAndAPIPassthrough(t *testing.T) {
	forgejo := newFakeForgejo(t)
	f := newProxyFixture(t, map[string]*fakeForgejo{"inst-a": forgejo}, "inst-a", "tok-a")
	auth := basicAuth("tok-a")

	tests := []struct {
		name   string
		method string
		path   string
		query  string
		body   string
		auth   string
	}{
		{name: "info/refs with service query", method: http.MethodGet,
			path: "/someowner/somerepo.git/info/refs", query: "service=git-upload-pack", auth: auth},
		{name: "git-upload-pack post", method: http.MethodPost,
			path: "/someowner/somerepo/git-upload-pack", body: "want 0123456789abcdef0123456789abcdef01234567\n", auth: auth},
		{name: "dumb http HEAD", method: http.MethodGet,
			path: "/someowner/somerepo/HEAD", auth: auth},
		{name: "default branch query", method: http.MethodGet,
			path: "/api/v1/repos/someowner/somerepo", query: "page=1", auth: "Bearer tok-a"},
		{name: "default branch query lowercase bearer", method: http.MethodGet,
			path: "/api/v1/repos/someowner/somerepo", query: "page=1", auth: "bearer tok-a"},
		{name: "default branch query mixed case bearer", method: http.MethodGet,
			path: "/api/v1/repos/someowner/somerepo", query: "page=1", auth: "BeArEr tok-a"},
		{name: "default branch query token scheme", method: http.MethodGet,
			path: "/api/v1/repos/someowner/somerepo", query: "page=1", auth: "token tok-a"},
		{name: "git lowercase basic scheme", method: http.MethodGet,
			path: "/someowner/somerepo.git/info/refs", query: "service=git-upload-pack", auth: "basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:tok-a"))},
		{name: "artifact upload", method: http.MethodPost,
			path: "/api/actions_pipeline/upload", query: "run_id=9", body: "artifact-bytes", auth: "Bearer tok-a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := f.do(t, tt.method, tt.path, tt.query, tt.auth, tt.body)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body: %q)", resp.StatusCode, readBody(t, resp))
			}
			// Response passthrough: the fake echoes its view of the request.
			wantBody := "forgejo:" + tt.path + "?" + tt.query
			if got := readBody(t, resp); got != wantBody {
				t.Fatalf("response body = %q, want %q", got, wantBody)
			}
			rec := forgejo.last()
			if rec.method != tt.method || rec.path != tt.path || rec.query != tt.query {
				t.Fatalf("upstream saw (%s %s?%s), want (%s %s?%s)",
					rec.method, rec.path, rec.query, tt.method, tt.path, tt.query)
			}
			if rec.auth != tt.auth {
				t.Fatalf("upstream Authorization = %q, want %q", rec.auth, tt.auth)
			}
			if rec.body != tt.body {
				t.Fatalf("upstream body = %q, want %q", rec.body, tt.body)
			}
		})
	}
	if forgejo.count() != len(tests) {
		t.Fatalf("upstream saw %d requests, want %d", forgejo.count(), len(tests))
	}
}

// TestProxyMultiInstanceRouting verifies the token→instance routing matrix:
// each task token routes to its own instance's Forgejo.
func TestProxyMultiInstanceRouting(t *testing.T) {
	forgejoA := newFakeForgejo(t)
	forgejoB := newFakeForgejo(t)
	f := newProxyFixture(t, map[string]*fakeForgejo{
		"inst-a": forgejoA,
		"inst-b": forgejoB,
	}, "inst-a", "tok-a")

	// A second task owned by inst-b with its own token.
	ms := newMockStore()
	taskB := &store.TaskCtx{
		ID:       78,
		Instance: "inst-b",
		Task: &v1.Task{Id: 78, Context: &structpb.Struct{Fields: map[string]*structpb.Value{
			"token": structpb.NewStringValue("tok-b"),
		}}},
		RegToken:    "reg-tok-b",
		RegTokenTTL: 15 * time.Minute,
		CreatedAt:   time.Now(),
	}
	ms.PutPending(taskB)
	f.sm.CreateWithToken(taskB, "sess-tok-b", "", nil)
	req := connect.NewRequest(&v1.FetchTaskRequest{})
	setBearer(req, "sess-tok-b")
	if _, err := f.h.FetchTask(context.Background(), req); err != nil {
		t.Fatalf("FetchTask for task B failed: %v", err)
	}

	// tok-a → inst-a's Forgejo.
	resp := f.do(t, http.MethodGet, "/owner/repo/info/refs", "service=git-upload-pack", basicAuth("tok-a"), "")
	if got := readBody(t, resp); !strings.HasPrefix(got, "forgejo:") || forgejoA.count() != 1 || forgejoB.count() != 0 {
		t.Fatalf("tok-a: body %q, forgejoA=%d forgejoB=%d — want routed to A only", got, forgejoA.count(), forgejoB.count())
	}

	// tok-b → inst-b's Forgejo.
	resp = f.do(t, http.MethodGet, "/owner/repo/info/refs", "service=git-upload-pack", basicAuth("tok-b"), "")
	if got := readBody(t, resp); !strings.HasPrefix(got, "forgejo:") || forgejoB.count() != 1 || forgejoA.count() != 1 {
		t.Fatalf("tok-b: body %q, forgejoA=%d forgejoB=%d — want routed to B only", got, forgejoA.count(), forgejoB.count())
	}
}

// TestProxyUnauthorized covers the 401 paths: a missing Authorization
// header and an unknown token (multi-instance, so no fallback).
func TestProxyUnauthorized(t *testing.T) {
	forgejoA := newFakeForgejo(t)
	forgejoB := newFakeForgejo(t)
	f := newProxyFixture(t, map[string]*fakeForgejo{
		"inst-a": forgejoA,
		"inst-b": forgejoB,
	}, "inst-a", "tok-a")

	tests := []struct {
		name      string
		auth      string
		wantError string
	}{
		{name: "no authorization header", wantError: "missing task token"},
		{name: "unrecognizable scheme", auth: "Digest abc", wantError: "missing task token"},
		{name: "lowercase unrecognizable scheme", auth: "digest abc", wantError: "missing task token"},
		{name: "unknown token", auth: basicAuth("not-a-task-token"), wantError: "unknown task token"},
		{name: "unknown token lowercase basic", auth: "basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:not-a-task-token")), wantError: "unknown task token"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := f.do(t, http.MethodGet, "/owner/repo/info/refs", "service=git-upload-pack", tt.auth, "")
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.StatusCode)
			}
			if got := readBody(t, resp); !strings.Contains(got, tt.wantError) {
				t.Fatalf("body = %q, want it to contain %q", got, tt.wantError)
			}
			if ctype := resp.Header.Get("Content-Type"); ctype != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", ctype)
			}
		})
	}
	if forgejoA.count()+forgejoB.count() != 0 {
		t.Fatal("no request may reach an upstream for 401 responses")
	}
}

// TestProxySingleInstanceFallback covers the special case: a task whose
// context carries no token field never seeds the registry, but with exactly
// one configured instance the request is routed to it anyway.
func TestProxySingleInstanceFallback(t *testing.T) {
	forgejo := newFakeForgejo(t)
	// Empty task token → the task context has no "token" field, so the
	// registry stays empty and every proxied request must fall back.
	f := newProxyFixture(t, map[string]*fakeForgejo{"only-inst": forgejo}, "only-inst", "")

	// Uppercase basic, the canonical checkout form.
	resp := f.do(t, http.MethodGet, "/owner/repo/info/refs", "service=git-upload-pack", basicAuth("any-token"), "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %q)", resp.StatusCode, readBody(t, resp))
	}
	if forgejo.count() != 1 {
		t.Fatalf("upstream saw %d requests, want 1", forgejo.count())
	}
	rec := forgejo.last()
	if rec.path != "/owner/repo/info/refs" || rec.query != "service=git-upload-pack" {
		t.Fatalf("upstream saw %s?%s, want /owner/repo/info/refs?service=git-upload-pack", rec.path, rec.query)
	}
	// The Authorization header must pass through even on the fallback path.
	if rec.auth != basicAuth("any-token") {
		t.Fatalf("upstream Authorization = %q, want %q", rec.auth, basicAuth("any-token"))
	}

	// Lowercase schemes (git and octokit spellings) must fall back too.
	lowerBasic := "basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:any-token"))
	resp = f.do(t, http.MethodGet, "/owner/repo/info/refs", "service=git-upload-pack", lowerBasic, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("lowercase basic: status = %d, want 200 (body: %q)", resp.StatusCode, readBody(t, resp))
	}
	if rec := forgejo.last(); rec.auth != lowerBasic {
		t.Fatalf("lowercase basic: upstream Authorization = %q, want %q", rec.auth, lowerBasic)
	}

	resp = f.do(t, http.MethodGet, "/api/v1/repos/owner/repo", "page=1", "token any-token", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("lowercase token: status = %d, want 200 (body: %q)", resp.StatusCode, readBody(t, resp))
	}
	if rec := forgejo.last(); rec.auth != "token any-token" {
		t.Fatalf("lowercase token: upstream Authorization = %q, want %q", rec.auth, "token any-token")
	}

	resp = f.do(t, http.MethodGet, "/api/v1/repos/owner/repo", "page=1", "BeArEr any-token", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mixed-case bearer: status = %d, want 200 (body: %q)", resp.StatusCode, readBody(t, resp))
	}
	if rec := forgejo.last(); rec.auth != "BeArEr any-token" {
		t.Fatalf("mixed-case bearer: upstream Authorization = %q, want %q", rec.auth, "BeArEr any-token")
	}

	if forgejo.count() != 4 {
		t.Fatalf("upstream saw %d requests, want 4", forgejo.count())
	}
}

// TestProxyNonGitPath404 verifies the catch-all mount does not swallow paths
// that are not git requests: web pages and the Connect RPC paths keep their
// previous behavior (404 from the mux's fallthrough).
func TestProxyNonGitPath404(t *testing.T) {
	forgejo := newFakeForgejo(t)
	f := newProxyFixture(t, map[string]*fakeForgejo{"inst-a": forgejo}, "inst-a", "tok-a")

	for _, path := range []string{
		"/owner/repo/issues",
		"/owner/repo",
		"/api/foo",
		"/",
	} {
		resp := f.do(t, http.MethodGet, path, "", basicAuth("tok-a"), "")
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404", path, resp.StatusCode)
		}
	}
	// The Connect RPC paths are not proxied either: they stay on the
	// RunnerService mount (more specific than the catch-all), so the proxy
	// upstream must never see them regardless of the RPC response.
	resp := f.do(t, http.MethodGet, "/runner.v1.RunnerService/FetchTask", "", basicAuth("tok-a"), "")
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
		t.Fatalf("RunnerService path: status = %d, want a connect-handler response (not the proxy's)", resp.StatusCode)
	}
	if forgejo.count() != 0 {
		t.Fatal("no non-git path may reach an upstream")
	}
}

// TestProxyBadGateway covers the unreachable-upstream path: the owning
// instance's server is closed, so the proxy must answer 502 with a log.
func TestProxyBadGateway(t *testing.T) {
	forgejo := newFakeForgejo(t)
	f := newProxyFixture(t, map[string]*fakeForgejo{"inst-a": forgejo}, "inst-a", "tok-a")
	forgejo.server.Close() // make the upstream unreachable

	resp := f.do(t, http.MethodGet, "/owner/repo/info/refs", "service=git-upload-pack", basicAuth("tok-a"), "")
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (body: %q)", resp.StatusCode, readBody(t, resp))
	}
	if got := readBody(t, resp); !strings.Contains(got, "bad gateway") {
		t.Fatalf("body = %q, want it to contain \"bad gateway\"", got)
	}
}

// ── registry lifecycle ────────────────────────────────────────────────────

// TestProxyFetchTaskRegistersToken pins the FetchTask token-extraction path:
// the task context token (and secret fallbacks) must land in the registry.
func TestProxyFetchTaskRegistersToken(t *testing.T) {
	newHandler := func() *Handler {
		return NewHandler(newMockStore(), session.NewManager(), newFakeResolver(map[string]instanceEntry{
			"inst-a": mkEntry("inst-a", newMockClient()),
		}), slog.New(slog.DiscardHandler))
	}
	fetch := func(t *testing.T, h *Handler, sessToken string) {
		t.Helper()
		req := connect.NewRequest(&v1.FetchTaskRequest{})
		setBearer(req, sessToken)
		if _, err := h.FetchTask(context.Background(), req); err != nil {
			t.Fatalf("FetchTask failed: %v", err)
		}
	}

	t.Run("context token", func(t *testing.T) {
		h := newHandler()
		tc := &store.TaskCtx{
			ID: 1, Instance: "inst-a",
			Task: &v1.Task{Id: 1, Context: &structpb.Struct{Fields: map[string]*structpb.Value{
				"token": structpb.NewStringValue("ctx-tok"),
			}}},
			RegTokenTTL: 15 * time.Minute, CreatedAt: time.Now(),
		}
		h.store.(*mockStore).PutPending(tc)
		h.sessions.CreateWithToken(tc, "sess-1", "", nil)
		fetch(t, h, "sess-1")
		if h.registry.count() != 1 {
			t.Fatalf("registry count = %d, want 1", h.registry.count())
		}
		if inst, ok := h.registry.lookup("ctx-tok", time.Now()); !ok || inst != "inst-a" {
			t.Fatalf("lookup(ctx-tok) = (%q, %v), want (inst-a, true)", inst, ok)
		}
	})

	t.Run("no token field", func(t *testing.T) {
		h := newHandler()
		tc := &store.TaskCtx{
			ID: 2, Instance: "inst-a",
			Task:        &v1.Task{Id: 2},
			RegTokenTTL: 15 * time.Minute, CreatedAt: time.Now(),
		}
		h.store.(*mockStore).PutPending(tc)
		h.sessions.CreateWithToken(tc, "sess-2", "", nil)
		fetch(t, h, "sess-2")
		if h.registry.count() != 0 {
			t.Fatalf("registry count = %d, want 0", h.registry.count())
		}
	})

	t.Run("secret fallback", func(t *testing.T) {
		h := newHandler()
		tc := &store.TaskCtx{
			ID: 3, Instance: "inst-a",
			Task: &v1.Task{Id: 3, Secrets: map[string]string{
				"GITEA_TOKEN": "secret-tok",
			}},
			RegTokenTTL: 15 * time.Minute, CreatedAt: time.Now(),
		}
		h.store.(*mockStore).PutPending(tc)
		h.sessions.CreateWithToken(tc, "sess-3", "", nil)
		fetch(t, h, "sess-3")
		if inst, ok := h.registry.lookup("secret-tok", time.Now()); !ok || inst != "inst-a" {
			t.Fatalf("lookup(secret-tok) = (%q, %v), want (inst-a, true)", inst, ok)
		}
	})
}

// TestProxyRegistryLifecycle verifies the lazy-eviction signals: wall-clock
// expiry, terminal task status, and session removal all invalidate entries.
func TestProxyRegistryLifecycle(t *testing.T) {
	sm := session.NewManager()
	reg := newTokenRegistry(sm)
	tc := &store.TaskCtx{ID: 1, Instance: "inst-a", Task: &v1.Task{Id: 1}}
	tc.Task.Context = &structpb.Struct{Fields: map[string]*structpb.Value{
		"token": structpb.NewStringValue("tok-life"),
	}}
	sm.CreateWithToken(tc, "sess-life", "", nil)

	now := time.Now()
	reg.putTaskTokens(tc, "sess-life", now)
	if inst, ok := reg.lookup("tok-life", now); !ok || inst != "inst-a" {
		t.Fatalf("fresh entry: lookup = (%q, %v), want (inst-a, true)", inst, ok)
	}

	t.Run("session removal invalidates", func(t *testing.T) {
		sm.Remove("sess-life")
		if _, ok := reg.lookup("tok-life", time.Now()); ok {
			t.Fatal("entry must be invalid after its session is removed")
		}
		if reg.count() != 0 {
			t.Fatalf("count = %d, want 0 after lazy eviction", reg.count())
		}
	})

	t.Run("terminal task invalidates", func(t *testing.T) {
		sm.CreateWithToken(tc, "sess-life", "", nil) // restore the session
		reg.putTaskTokens(tc, "sess-life", time.Now())
		tc.SetStatus(store.StatusTerminal)
		if _, ok := reg.lookup("tok-life", time.Now()); ok {
			t.Fatal("entry must be invalid after the task reaches a terminal state")
		}
	})

	t.Run("wall-clock expiry invalidates", func(t *testing.T) {
		sm.CreateWithToken(tc, "sess-life", "", nil) // restore the session
		start := time.Now()
		reg.putTaskTokens(tc, "sess-life", start)
		if _, ok := reg.lookup("tok-life", start.Add(proxyEntryTTL)); ok {
			t.Fatal("entry must be invalid past its wall-clock lifetime")
		}
		if reg.count() != 0 {
			t.Fatalf("count = %d, want 0 after lazy eviction", reg.count())
		}
	})
}
