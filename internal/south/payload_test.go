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
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"
	"connectrpc.com/connect"

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

// ── rewriteWorkflowPayload ──────────────────────────────────────────────────

func TestRewriteWorkflowPayload(t *testing.T) {
	const url = "https://forgejo.own.example.com"
	original := []byte(samplePayload)
	task := &v1.Task{Id: 1, WorkflowPayload: original}

	rewritten := rewriteWorkflowPayload(task, url)
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
		first := rewriteWorkflowPayload(task, url).WorkflowPayload
		second := rewriteWorkflowPayload(task, url).WorkflowPayload
		if !bytes.Equal(first, second) {
			t.Fatal("repeated rewrites must produce identical bytes")
		}
	})

	t.Run("passes through when nothing matches", func(t *testing.T) {
		plain := &v1.Task{Id: 2, WorkflowPayload: []byte("name: t\njobs:\n  j:\n    steps:\n      - uses: actions/setup-node@v4\n")}
		if got := rewriteWorkflowPayload(plain, url); got != plain {
			t.Fatal("no-match rewrite must return the original task")
		}
	})

	t.Run("passes through on empty payload or url", func(t *testing.T) {
		if got := rewriteWorkflowPayload(&v1.Task{Id: 3}, url); got.WorkflowPayload != nil {
			t.Fatal("nil payload must pass through")
		}
		if got := rewriteWorkflowPayload(&v1.Task{Id: 4, WorkflowPayload: original}, ""); !bytes.Equal(got.WorkflowPayload, original) {
			t.Fatal("empty url must pass through")
		}
		if got := rewriteWorkflowPayload(nil, url); got != nil {
			t.Fatal("nil task must pass through")
		}
	})

	t.Run("passes through on parse failure", func(t *testing.T) {
		broken := &v1.Task{Id: 5, WorkflowPayload: []byte("jobs: [unclosed")}
		if got := rewriteWorkflowPayload(broken, url); got != broken {
			t.Fatal("unparseable payload must pass through unchanged")
		}
	})
}

// ── FetchTask integration ───────────────────────────────────────────────────

// TestFetchTaskRewritesCheckoutPayload wires a full handler: a store task
// with a real workflow payload owned by an instance with a ForgejoURL, and
// asserts FetchTask returns the rewritten payload while the store's task
// stays byte-identical, and that a second FetchTask is byte-identical too.
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
		ID:          99,
		Instance:    "inst-a",
		Task:        &v1.Task{Id: 99, WorkflowPayload: []byte(samplePayload)},
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
	if !bytes.Contains(first.WorkflowPayload, []byte("github-server-url: https://forgejo.a.example.com")) {
		t.Fatalf("fetched payload lacks the injected input:\n%s", first.WorkflowPayload)
	}
	if !bytes.Equal(taskCtx.Task.WorkflowPayload, []byte(samplePayload)) {
		t.Fatal("store task payload was mutated by FetchTask")
	}

	second := fetch(t)
	if !bytes.Equal(first.WorkflowPayload, second.WorkflowPayload) {
		t.Fatal("repeated FetchTask must return byte-identical payloads")
	}
	if !bytes.Equal(taskCtx.Task.WorkflowPayload, []byte(samplePayload)) {
		t.Fatal("store task payload was mutated by a second FetchTask")
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
