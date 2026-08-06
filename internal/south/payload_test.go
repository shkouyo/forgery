// SPDX-License-Identifier: GPL-3.0-or-later
//
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
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/session"
	"git.0x0f.dev/forgery/internal/store"
)

// ── isCheckoutUses ──────────────────────────────────────────────────────────

func TestIsCheckoutUses(t *testing.T) {
	tests := []struct {
		name string
		uses string
		want bool
	}{
		// Matches: the plain, bare-host, and scheme+host spellings.
		{name: "plain v4", uses: "actions/checkout@v4", want: true},
		{name: "plain no ref", uses: "actions/checkout", want: true},
		{name: "bare host prefix", uses: "code.forgejo.org/actions/checkout@v6", want: true},
		{name: "https host prefix", uses: "https://github.com/actions/checkout@v4", want: true},
		{name: "http host prefix", uses: "http://example.com/actions/checkout@v1", want: true},
		{name: "action subpath", uses: "actions/checkout/subdir@v4", want: true},
		{name: "surrounding whitespace", uses: "  actions/checkout@v4  ", want: true},
		// Exclusions: docker images and local actions.
		{name: "docker image", uses: "docker://alpine:3.20", want: false},
		{name: "docker image with actions path", uses: "docker://ghcr.io/actions/checkout:latest", want: false},
		{name: "local action", uses: "./local-action", want: false},
		{name: "parent local action", uses: "../shared/checkout@v4", want: false},
		// Other remote actions and garbage.
		{name: "other action", uses: "actions/setup-node@v4", want: false},
		{name: "setup-node bare host", uses: "code.forgejo.org/actions/setup-node@v4", want: false},
		{name: "single segment", uses: "checkout@v4", want: false},
		{name: "empty", uses: "", want: false},
		{name: "path only", uses: "some/org/checkout@v4", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isCheckoutUses(tt.uses); got != tt.want {
				t.Fatalf("isCheckoutUses(%q) = %v, want %v", tt.uses, got, tt.want)
			}
		})
	}
}

// ── injectCheckoutServerURL ─────────────────────────────────────────────────

// samplePayload is a Forgejo-style workflow payload (single job, on: kept,
// needs erased, 4-space indentation — the shape jobparser produces).
const samplePayload = `name: Build and deploy
"on": push
jobs:
    job9:
        name: job9
        runs-on: ubuntu-latest
        steps:
            - uses: actions/checkout@v4
            - name: build
              run: ./deploy --build ${{ needs.job1.outputs.output1 }}
            - uses: code.forgejo.org/actions/checkout@v6
              with:
                ref: main
              env:
                TOKEN: ${{ secrets.BUILD_TOKEN }}
            - uses: docker://alpine:3.20
`

func TestInjectCheckoutServerURL(t *testing.T) {
	const url = "https://forgejo.own.example.com"

	t.Run("adds with block and preserves formatting", func(t *testing.T) {
		out, err := injectCheckoutServerURL([]byte(samplePayload), url)
		if err != nil {
			t.Fatalf("injectCheckoutServerURL: %v", err)
		}
		s := string(out)

		// Both checkout steps (plain and bare-host spellings) get the input.
		if strings.Count(s, "github-server-url: "+url) != 2 {
			t.Fatalf("want 2 injected github-server-url entries, got:\n%s", s)
		}
		// The existing with block gains the key; other keys survive.
		if !strings.Contains(s, "ref: main") || !strings.Contains(s, "TOKEN: ${{ secrets.BUILD_TOKEN }}") {
			t.Fatalf("existing step keys were lost:\n%s", s)
		}
		// The docker step stays untouched.
		if strings.Contains(s, "docker://alpine:3.20\n              with:") {
			t.Fatalf("docker step must not gain a with block:\n%s", s)
		}
		// Formatting: quoted "on" key, ${{ }} scalars, key order preserved.
		if !strings.Contains(s, "\"on\": push") {
			t.Fatalf("quoted on key not preserved:\n%s", s)
		}
		if !strings.Contains(s, "run: ./deploy --build ${{ needs.job1.outputs.output1 }}") {
			t.Fatalf("${{ }} scalar not preserved:\n%s", s)
		}
		if strings.Index(s, "name:") > strings.Index(s, "\"on\":") || strings.Index(s, "\"on\":") > strings.Index(s, "jobs:") {
			t.Fatalf("top-level key order changed:\n%s", s)
		}
		// 4-space indentation per level (steps content at 12).
		if !strings.Contains(s, "\n            - uses: actions/checkout@v4\n              with:\n                github-server-url: "+url) {
			t.Fatalf("unexpected indentation around injected with block:\n%s", s)
		}
	})

	t.Run("env without with block gains with", func(t *testing.T) {
		in := strings.Replace(samplePayload,
			"            - uses: actions/checkout@v4\n",
			"            - uses: actions/checkout@v4\n              env:\n                FOO: bar\n", 1)
		out, err := injectCheckoutServerURL([]byte(in), url)
		if err != nil {
			t.Fatalf("injectCheckoutServerURL: %v", err)
		}
		s := string(out)
		if !strings.Contains(s, "FOO: bar") || !strings.Contains(s, "github-server-url: "+url) {
			t.Fatalf("env or injected input missing:\n%s", s)
		}
		// with must come after env (appended at the end of the step mapping).
		if strings.Index(s, "FOO: bar") > strings.Index(s, "github-server-url: ") {
			t.Fatalf("with block must be appended after env:\n%s", s)
		}
	})

	t.Run("overwrites existing github-server-url", func(t *testing.T) {
		in := strings.Replace(samplePayload,
			"              with:\n                ref: main",
			"              with:\n                github-server-url: https://old.example.com\n                ref: main", 1)
		out, err := injectCheckoutServerURL([]byte(in), url)
		if err != nil {
			t.Fatalf("injectCheckoutServerURL: %v", err)
		}
		s := string(out)
		if strings.Contains(s, "https://old.example.com") {
			t.Fatalf("stale github-server-url survived:\n%s", s)
		}
		if strings.Count(s, "github-server-url: "+url) != 2 {
			t.Fatalf("want 2 injected entries, got:\n%s", s)
		}
		// The key keeps its position inside with (before ref).
		if strings.Index(s, "github-server-url: "+url) > strings.Index(s, "ref: main") {
			t.Fatalf("github-server-url must keep its original position:\n%s", s)
		}
	})

	t.Run("no checkout steps returns payload unchanged", func(t *testing.T) {
		in := []byte(`name: t
"on": push
jobs:
    j:
        steps:
            - uses: actions/setup-node@v4
            - uses: docker://alpine:3.20
            - uses: ./local-action
`)
		out, err := injectCheckoutServerURL(in, url)
		if err != nil {
			t.Fatalf("injectCheckoutServerURL: %v", err)
		}
		if !bytes.Equal(out, in) {
			t.Fatalf("payload changed without checkout steps:\n%s", out)
		}
	})

	t.Run("no jobs key returns payload unchanged", func(t *testing.T) {
		in := []byte("name: lone\non: push\n")
		out, err := injectCheckoutServerURL(in, url)
		if err != nil {
			t.Fatalf("injectCheckoutServerURL: %v", err)
		}
		if !bytes.Equal(out, in) {
			t.Fatalf("payload changed without jobs key:\n%s", out)
		}
	})

	t.Run("invalid yaml fails and returns original", func(t *testing.T) {
		in := []byte("jobs: [unclosed")
		out, err := injectCheckoutServerURL(in, url)
		if err == nil {
			t.Fatal("want error for invalid yaml")
		}
		if !bytes.Equal(out, in) {
			t.Fatal("original payload must be returned on parse failure")
		}
	})

	t.Run("non-mapping root fails and returns original", func(t *testing.T) {
		in := []byte("- just\n- a\n- list\n")
		out, err := injectCheckoutServerURL(in, url)
		if err == nil {
			t.Fatal("want error for non-mapping root")
		}
		if !bytes.Equal(out, in) {
			t.Fatal("original payload must be returned on structural failure")
		}
	})

	t.Run("malformed with block leaves step untouched", func(t *testing.T) {
		in := strings.Replace(samplePayload,
			"- uses: code.forgejo.org/actions/checkout@v6\n              with:\n                ref: main",
			"- uses: code.forgejo.org/actions/checkout@v6\n              with: just-a-string", 1)
		out, err := injectCheckoutServerURL([]byte(in), url)
		if err != nil {
			t.Fatalf("injectCheckoutServerURL: %v", err)
		}
		s := string(out)
		if strings.Count(s, "github-server-url: "+url) != 1 {
			t.Fatalf("want only the well-formed step injected:\n%s", s)
		}
		if !strings.Contains(s, "with: just-a-string") {
			t.Fatalf("malformed with block was rewritten:\n%s", s)
		}
	})
}

// ── normalizeServerURLs ─────────────────────────────────────────────────────

// serverURLPayload is a workflow payload exercising the server-URL expression
// family: the github context (server_url / api_url / repository_url), the
// env expressions (GITHUB_SERVER_URL / GITHUB_API_URL / FORGEJO_SERVER_URL),
// a docker-registry login (the public_url leak this fix targets), and a
// checkout step so injection and normalization can be tested together.
const serverURLPayload = `name: push image
"on": push
jobs:
    job9:
        runs-on: ubuntu-latest
        steps:
            - uses: actions/checkout@v4
            - uses: docker/login-action@v3
              with:
                registry: ${{ github.server_url }}
            - name: api
              run: curl -sS ${{ github.api_url }}/repos/${{ github.repository_url }}
            - name: envs
              run: echo ${{ env.GITHUB_SERVER_URL }} ${{ env.GITHUB_API_URL }} ${{ env.FORGEJO_SERVER_URL }}
`

func TestNormalizeServerURLs(t *testing.T) {
	const url = "https://forgejo.own.example.com"
	const repo = "octocat/hello-world"

	// The six keys act derives from --url, and the literal each becomes.
	replacements := map[string]string{
		"github.server_url":      url,
		"github.api_url":         url + "/api/v1",
		"github.repository_url":  url + "/" + repo,
		"env.GITHUB_SERVER_URL":  url,
		"env.GITHUB_API_URL":     url + "/api/v1",
		"env.FORGEJO_SERVER_URL": url,
	}

	t.Run("expression table across whitespace variants", func(t *testing.T) {
		variants := []struct {
			name string
			expr string
		}{
			{"no space", `${{%s}}`},
			{"single space", `${{ %s }}`},
			{"multi space", `${{  %s  }}`},
		}
		for key, want := range replacements {
			for _, v := range variants {
				name := strings.ReplaceAll(key, ".", "-") + " / " + v.name
				t.Run(name, func(t *testing.T) {
					expr := fmt.Sprintf(v.expr, key)
					in := []byte("name: t\njobs:\n    j:\n        steps:\n            - run: echo " + expr + "\n")
					out, err := normalizeServerURLs(in, url, repo)
					if err != nil {
						t.Fatalf("normalizeServerURLs: %v", err)
					}
					s := string(out)
					if !strings.Contains(s, "echo "+want) {
						t.Fatalf("want %q replaced by %q, got:\n%s", expr, want, s)
					}
					if strings.Contains(s, "${{") {
						t.Fatalf("expression survived normalization:\n%s", s)
					}
				})
			}
		}
	})

	t.Run("quoted scalars are rewritten in style", func(t *testing.T) {
		in := []byte(`name: t
jobs:
    j:
        steps:
            - run: "${{ github.server_url }}"
            - run: '${{ github.server_url }}'
`)
		out, err := normalizeServerURLs(in, url, repo)
		if err != nil {
			t.Fatalf("normalizeServerURLs: %v", err)
		}
		s := string(out)
		if !strings.Contains(s, "\""+url+"\"") {
			t.Fatalf("double-quoted scalar not rewritten:\n%s", s)
		}
		if !strings.Contains(s, "'"+url+"'") {
			t.Fatalf("single-quoted scalar not rewritten:\n%s", s)
		}
	})

	t.Run("docker login registry input is rewritten", func(t *testing.T) {
		in := []byte(`name: t
jobs:
    j:
        steps:
            - uses: docker/login-action@v3
              with:
                registry: ${{ github.server_url }}
                username: ${{ github.actor }}
`)
		out, err := normalizeServerURLs(in, url, repo)
		if err != nil {
			t.Fatalf("normalizeServerURLs: %v", err)
		}
		s := string(out)
		if !strings.Contains(s, "registry: "+url) {
			t.Fatalf("registry input not rewritten:\n%s", s)
		}
		if !strings.Contains(s, "username: ${{ github.actor }}") {
			t.Fatalf("unrelated with input was touched:\n%s", s)
		}
	})

	t.Run("run script interpolation keeps surrounding text", func(t *testing.T) {
		in := []byte(`name: t
jobs:
    j:
        steps:
            - run: docker push ${{ github.server_url }}/team/image:latest
`)
		out, err := normalizeServerURLs(in, url, repo)
		if err != nil {
			t.Fatalf("normalizeServerURLs: %v", err)
		}
		s := string(out)
		if !strings.Contains(s, "docker push "+url+"/team/image:latest") {
			t.Fatalf("interpolated expression not rewritten in place:\n%s", s)
		}
	})

	t.Run("unrelated expressions are left alone", func(t *testing.T) {
		in := []byte(`name: t
jobs:
    j:
        steps:
            - run: echo ${{ github.repository }} ${{ github.sha }} ${{ github.token }}
            - run: echo ${{ secrets.BUILD_TOKEN }} ${{ needs.job1.outputs.output1 }}
            - uses: actions/checkout@v4
              with:
                ref: ${{ github.ref }}
`)
		out, err := normalizeServerURLs(in, url, repo)
		if err != nil {
			t.Fatalf("normalizeServerURLs: %v", err)
		}
		s := string(out)
		for _, keep := range []string{
			"${{ github.repository }}",
			"${{ github.sha }}",
			"${{ github.token }}",
			"${{ secrets.BUILD_TOKEN }}",
			"${{ needs.job1.outputs.output1 }}",
			"ref: ${{ github.ref }}",
		} {
			if !strings.Contains(s, keep) {
				t.Fatalf("unrelated expression %q was touched:\n%s", keep, s)
			}
		}
	})

	t.Run("comments containing the expression are untouched", func(t *testing.T) {
		in := []byte(`name: t
jobs:
    j:
        # legacy: registry was ${{ github.server_url }}
        steps:
            - run: echo ${{ github.server_url }}
`)
		out, err := normalizeServerURLs(in, url, repo)
		if err != nil {
			t.Fatalf("normalizeServerURLs: %v", err)
		}
		s := string(out)
		if !strings.Contains(s, "# legacy: registry was ${{ github.server_url }}") {
			t.Fatalf("comment was rewritten:\n%s", s)
		}
		if !strings.Contains(s, "echo "+url) {
			t.Fatalf("scalar expression not rewritten:\n%s", s)
		}
	})

	t.Run("repository_url with repository", func(t *testing.T) {
		in := []byte(`name: t
jobs:
    j:
        steps:
            - run: echo ${{ github.repository_url }}
`)
		out, err := normalizeServerURLs(in, url, repo)
		if err != nil {
			t.Fatalf("normalizeServerURLs: %v", err)
		}
		if !bytes.Contains(out, []byte("echo "+url+"/"+repo)) {
			t.Fatalf("repository_url not rewritten to url/repo:\n%s", out)
		}
	})

	t.Run("repository_url skipped without repository", func(t *testing.T) {
		in := []byte(`name: t
jobs:
    j:
        steps:
            - run: echo ${{ github.repository_url }} ${{ github.server_url }}
`)
		out, err := normalizeServerURLs(in, url, "")
		if err != nil {
			t.Fatalf("normalizeServerURLs: %v", err)
		}
		s := string(out)
		if !strings.Contains(s, "echo ${{ github.repository_url }} "+url) {
			t.Fatalf("repository_url must survive without a repository, got:\n%s", s)
		}
	})

	t.Run("no expressions returns payload byte-identical", func(t *testing.T) {
		in := []byte(`name: t
jobs:
    j:
        steps:
            - run: echo hi
`)
		out, err := normalizeServerURLs(in, url, repo)
		if err != nil {
			t.Fatalf("normalizeServerURLs: %v", err)
		}
		if !bytes.Equal(out, in) {
			t.Fatalf("payload without expressions must be returned unchanged:\n%s", out)
		}
	})

	t.Run("invalid yaml fails and returns original", func(t *testing.T) {
		in := []byte("jobs: [unclosed")
		out, err := normalizeServerURLs(in, url, repo)
		if err == nil {
			t.Fatal("want error for invalid yaml")
		}
		if !bytes.Equal(out, in) {
			t.Fatal("original payload must be returned on parse failure")
		}
	})

	t.Run("idempotent across repeated normalizations", func(t *testing.T) {
		first, err := normalizeServerURLs([]byte(serverURLPayload), url, repo)
		if err != nil {
			t.Fatalf("normalizeServerURLs: %v", err)
		}
		second, err := normalizeServerURLs(first, url, repo)
		if err != nil {
			t.Fatalf("normalizeServerURLs: %v", err)
		}
		if !bytes.Equal(first, second) {
			t.Fatal("repeated normalization must produce identical bytes")
		}
	})
}

// ── rewriteWorkflowPayload ──────────────────────────────────────────────────

func TestRewriteWorkflowPayload(t *testing.T) {
	const url = "https://forgejo.own.example.com"
	original := []byte(samplePayload)
	task := &v1.Task{Id: 1, WorkflowPayload: original}

	rewritten := rewriteWorkflowPayload(task, url, "")
	if rewritten == task {
		t.Fatal("rewrite must return a copy, not the store task")
	}
	if !bytes.Contains(rewritten.WorkflowPayload, []byte("github-server-url: "+url)) {
		t.Fatalf("rewritten payload lacks the input:\n%s", rewritten.WorkflowPayload)
	}
	// The store task is untouched.
	if !bytes.Equal(task.WorkflowPayload, original) {
		t.Fatal("store task payload was mutated")
	}

	t.Run("idempotent across repeated rewrites", func(t *testing.T) {
		first := rewriteWorkflowPayload(task, url, "").WorkflowPayload
		second := rewriteWorkflowPayload(task, url, "").WorkflowPayload
		if !bytes.Equal(first, second) {
			t.Fatal("repeated rewrites must produce identical bytes")
		}
	})

	t.Run("normalizes expressions alongside checkout injection", func(t *testing.T) {
		orig := []byte(serverURLPayload)
		got := rewriteWorkflowPayload(&v1.Task{Id: 2, WorkflowPayload: orig}, url, "octocat/hello-world")
		if got.WorkflowPayload == nil {
			t.Fatal("rewrite returned nil payload")
		}
		s := string(got.WorkflowPayload)
		for _, want := range []string{
			"github-server-url: " + url,
			"registry: " + url,
			"curl -sS " + url + "/api/v1/repos/" + url + "/octocat/hello-world",
			"echo " + url + " " + url + "/api/v1 " + url,
		} {
			if !strings.Contains(s, want) {
				t.Fatalf("rewritten payload lacks %q:\n%s", want, s)
			}
		}
		if strings.Contains(s, "${{") {
			t.Fatalf("server-URL expressions survived rewrite:\n%s", s)
		}
		// The store task is untouched.
		if !bytes.Equal(orig, []byte(serverURLPayload)) {
			t.Fatal("store task payload was mutated")
		}
		// Rewriting the already-rewritten payload changes nothing.
		again := rewriteWorkflowPayload(&v1.Task{Id: 2, WorkflowPayload: got.WorkflowPayload}, url, "octocat/hello-world")
		if !bytes.Equal(again.WorkflowPayload, got.WorkflowPayload) {
			t.Fatal("rewriting an already-rewritten payload must be a no-op")
		}
	})

	t.Run("passes through when nothing matches", func(t *testing.T) {
		plain := &v1.Task{Id: 3, WorkflowPayload: []byte("name: t\njobs:\n  j:\n    steps:\n      - uses: actions/setup-node@v4\n")}
		if got := rewriteWorkflowPayload(plain, url, ""); got != plain {
			t.Fatal("no-match rewrite must return the original task")
		}
	})

	t.Run("passes through on empty payload or url", func(t *testing.T) {
		if got := rewriteWorkflowPayload(&v1.Task{Id: 4}, url, ""); got.WorkflowPayload != nil {
			t.Fatal("nil payload must pass through")
		}
		if got := rewriteWorkflowPayload(&v1.Task{Id: 5, WorkflowPayload: original}, "", ""); !bytes.Equal(got.WorkflowPayload, original) {
			t.Fatal("empty url must pass through")
		}
		if got := rewriteWorkflowPayload(nil, url, ""); got != nil {
			t.Fatal("nil task must pass through")
		}
	})

	t.Run("passes through on parse failure", func(t *testing.T) {
		broken := &v1.Task{Id: 6, WorkflowPayload: []byte("jobs: [unclosed")}
		if got := rewriteWorkflowPayload(broken, url, ""); got != broken {
			t.Fatal("unparseable payload must pass through unchanged")
		}
	})
}

// ── FetchTask integration ───────────────────────────────────────────────────

// TestFetchTaskRewritesCheckoutPayload wires a full handler: a store task
// with a real workflow payload owned by an instance with a ForgejoURL, and
// asserts FetchTask returns the rewritten payload (checkout input injected,
// server-URL expressions normalized) while the store's task stays
// byte-identical, and that a second FetchTask is byte-identical too.
func TestFetchTaskRewritesCheckoutPayload(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver := newFakeResolver(map[string]instanceEntry{
		"inst-a": {
			inst:   config.Instance{Name: "inst-a", ForgejoURL: "https://forgejo.a.example.com"},
			client: newMockClient(),
		},
	})
	h := NewHandler(ms, sm, resolver, slog.New(slog.DiscardHandler))

	taskCtx := &store.TaskCtx{
		ID:       99,
		Instance: "inst-a",
		Task: &v1.Task{
			Id:              99,
			WorkflowPayload: []byte(serverURLPayload),
			Context: &structpb.Struct{Fields: map[string]*structpb.Value{
				"repository": structpb.NewStringValue("octocat/hello-world"),
			}},
		},
		RegToken:    "reg-payload",
		RegTokenTTL: 15 * time.Minute,
		CreatedAt:   time.Now(),
	}
	ms.PutPending(taskCtx)
	sm.CreateWithToken(taskCtx, "sess-payload", "", nil)

	fetch := func(t *testing.T) *v1.Task {
		t.Helper()
		req := connect.NewRequest(&v1.FetchTaskRequest{})
		setBearer(req, "sess-payload")
		resp, err := h.FetchTask(context.Background(), req)
		if err != nil {
			t.Fatalf("FetchTask failed: %v", err)
		}
		return resp.Msg.GetTask()
	}

	first := fetch(t)
	s := string(first.WorkflowPayload)
	for _, want := range []string{
		"github-server-url: https://forgejo.a.example.com",
		"registry: https://forgejo.a.example.com",
		"curl -sS https://forgejo.a.example.com/api/v1/repos/https://forgejo.a.example.com/octocat/hello-world",
		"echo https://forgejo.a.example.com https://forgejo.a.example.com/api/v1 https://forgejo.a.example.com",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("fetched payload lacks %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "${{") {
		t.Fatalf("server-URL expressions survived FetchTask:\n%s", s)
	}
	if !bytes.Equal(taskCtx.Task.WorkflowPayload, []byte(serverURLPayload)) {
		t.Fatal("store task payload was mutated by FetchTask")
	}

	second := fetch(t)
	if !bytes.Equal(first.WorkflowPayload, second.WorkflowPayload) {
		t.Fatal("repeated FetchTask must return byte-identical payloads")
	}
	if !bytes.Equal(taskCtx.Task.WorkflowPayload, []byte(serverURLPayload)) {
		t.Fatal("store task payload was mutated by a second FetchTask")
	}
}

// TestFetchTaskSkipsRepositoryURLWithoutContext pins the repository_url
// fallback: a task whose context carries no "repository" field keeps the
// expression while every other server-URL expression is still normalized.
func TestFetchTaskSkipsRepositoryURLWithoutContext(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	resolver := newFakeResolver(map[string]instanceEntry{
		"inst-b": {
			inst:   config.Instance{Name: "inst-b", ForgejoURL: "https://forgejo.b.example.com"},
			client: newMockClient(),
		},
	})
	h := NewHandler(ms, sm, resolver, slog.New(slog.DiscardHandler))

	taskCtx := &store.TaskCtx{
		ID:          101,
		Instance:    "inst-b",
		Task:        &v1.Task{Id: 101, WorkflowPayload: []byte("name: t\njobs:\n    j:\n        steps:\n            - run: echo ${{ github.repository_url }} ${{ github.server_url }}\n")},
		RegToken:    "reg-norepo",
		RegTokenTTL: 15 * time.Minute,
		CreatedAt:   time.Now(),
	}
	ms.PutPending(taskCtx)
	sm.CreateWithToken(taskCtx, "sess-norepo", "", nil)

	req := connect.NewRequest(&v1.FetchTaskRequest{})
	setBearer(req, "sess-norepo")
	resp, err := h.FetchTask(context.Background(), req)
	if err != nil {
		t.Fatalf("FetchTask failed: %v", err)
	}
	s := string(resp.Msg.GetTask().GetWorkflowPayload())
	if !strings.Contains(s, "echo ${{ github.repository_url }} https://forgejo.b.example.com") {
		t.Fatalf("repository_url must survive without a repository context:\n%s", s)
	}
	if !bytes.Equal(taskCtx.Task.WorkflowPayload, []byte("name: t\njobs:\n    j:\n        steps:\n            - run: echo ${{ github.repository_url }} ${{ github.server_url }}\n")) {
		t.Fatal("store task payload was mutated by FetchTask")
	}
}

// TestFetchTaskPassesThroughWithoutInstance pins the defensive path: a task
// whose instance cannot be resolved gets its stored payload unchanged.
func TestFetchTaskPassesThroughWithoutInstance(t *testing.T) {
	ms := newMockStore()
	sm := session.NewManager()
	h := NewHandler(ms, sm, newFakeResolver(nil), slog.New(slog.DiscardHandler))

	taskCtx := &store.TaskCtx{
		ID:          100,
		Instance:    "ghost",
		Task:        &v1.Task{Id: 100, WorkflowPayload: []byte(samplePayload)},
		RegToken:    "reg-ghost",
		RegTokenTTL: 15 * time.Minute,
		CreatedAt:   time.Now(),
	}
	ms.PutPending(taskCtx)
	sm.CreateWithToken(taskCtx, "sess-ghost", "", nil)

	req := connect.NewRequest(&v1.FetchTaskRequest{})
	setBearer(req, "sess-ghost")
	resp, err := h.FetchTask(context.Background(), req)
	if err != nil {
		t.Fatalf("FetchTask failed: %v", err)
	}
	got := resp.Msg.GetTask().GetWorkflowPayload()
	if !bytes.Equal(got, []byte(samplePayload)) {
		t.Fatalf("unresolvable instance must pass the payload through unchanged:\n%s", got)
	}
}
