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
	"fmt"
	"regexp"
	"strings"

	v1 "code.gitea.io/actions-proto-go/runner/v1"
	"go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/proto"
)

// isCheckoutUses reports whether a step's uses value is an actions/checkout
// remote action, following the shape rule of forgejo-runner's vendored act
// (act/runner/step_action_remote.go: newRemoteAction + remoteAction.IsCheckout):
// an optional http(s)://host/ prefix and an @ref suffix are ignored, then the
// org/repo[/path] must be actions/checkout. The bare-host spelling
// code.forgejo.org/actions/checkout@v6 (the Forgejo default action instance
// written out) matches too, as does https://github.com/actions/checkout@v4.
// Docker images (docker://…) and local actions (./…) are never matched.
func isCheckoutUses(uses string) bool {
	u := strings.TrimSpace(uses)
	// act classifies docker:// and ./ steps as non-remote actions and never
	// runs the remote-action checkout check on them (model.Step.Type).
	if strings.HasPrefix(u, "docker://") || strings.HasPrefix(u, "./") || strings.HasPrefix(u, "../") {
		return false
	}
	// Strip an optional scheme+host prefix (act's newRemoteAction).
	for _, scheme := range []string{"https://", "http://"} {
		if after, ok := strings.CutPrefix(u, scheme); ok {
			_, rest, ok := strings.Cut(after, "/")
			if !ok {
				return false
			}
			u = rest
			break
		}
	}
	// Strip the @ref suffix (act's parseAction requires it; matching does not).
	if i := strings.IndexByte(u, '@'); i >= 0 {
		u = u[:i]
	}
	segs := strings.Split(u, "/")
	if len(segs) < 2 {
		return false
	}
	// Either the plain org/repo[/path] form or the bare host/org/repo form
	// (code.forgejo.org/actions/checkout) ends in actions/checkout.
	if segs[0] == "actions" && segs[1] == "checkout" {
		return true
	}
	last := len(segs) - 1
	return segs[last-1] == "actions" && segs[last] == "checkout"
}

// injectCheckoutServerURL parses payload (a Forgejo workflow payload — the
// single-job YAML Forgejo stores in the task) into a yaml.Node tree, sets
// with.github-server-url to forgejoURL on every checkout step (see
// isCheckoutUses), and re-encodes the tree in place, preserving key order,
// scalar styles, and 4-space indentation. An existing with block gets the key
// added or overwritten; a missing one is appended to the step. On any parse
// or encode failure — or when no checkout step matches — the original payload
// is returned unchanged, so the runner-side validation can never break.
func injectCheckoutServerURL(payload []byte, forgejoURL string) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(payload, &doc); err != nil {
		return payload, fmt.Errorf("workflow payload is not valid YAML: %w", err)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return payload, fmt.Errorf("workflow payload is not a YAML document")
	}
	top := doc.Content[0]
	if top.Kind != yaml.MappingNode {
		return payload, fmt.Errorf("workflow payload root is not a mapping")
	}

	injected := false
	jobs := mappingValue(top, "jobs")
	if jobs != nil && jobs.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(jobs.Content); i += 2 {
			job := jobs.Content[i+1]
			if job.Kind != yaml.MappingNode {
				continue
			}
			steps := mappingValue(job, "steps")
			if steps == nil || steps.Kind != yaml.SequenceNode {
				continue
			}
			for _, step := range steps.Content {
				if step.Kind != yaml.MappingNode {
					continue
				}
				uses := mappingValue(step, "uses")
				if uses == nil || uses.Kind != yaml.ScalarNode || !isCheckoutUses(uses.Value) {
					continue
				}
				if setWithInput(step, "github-server-url", forgejoURL) {
					injected = true
				}
			}
		}
	}
	if !injected {
		return payload, nil
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(4) // match the Forgejo generator's output style
	if err := enc.Encode(&doc); err != nil {
		return payload, fmt.Errorf("re-encoding workflow payload: %w", err)
	}
	if err := enc.Close(); err != nil {
		return payload, fmt.Errorf("closing workflow payload encoder: %w", err)
	}
	return buf.Bytes(), nil
}

// serverURLExprRe matches a ${{ }} expression whose entire content is one of
// the server-URL derived keys act exposes: the github context fields
// (server_url / api_url; repository_url is not a field of the vendored act's
// GithubContext, so it evaluates to empty and rewriting to a literal is even
// more correct) and the env keys withGithubEnv writes (GITHUB_SERVER_URL /
// GITHUB_API_URL / FORGEJO_SERVER_URL — FORGEJO_* is set alongside every
// GITHUB_* by withGithubEnv's prefix loop). Whitespace inside the braces is
// ignored: ${{github.server_url}}, ${{ github.server_url }} and
// ${{  github.server_url  }} all match. Anything else — ${{ github.repository
// }}, ${{ secrets.X }}, larger format() expressions — is left alone.
var serverURLExprRe = regexp.MustCompile(
	`\$\{\{\s*(github\.server_url|github\.api_url|github\.repository_url|env\.GITHUB_SERVER_URL|env\.GITHUB_API_URL|env\.FORGEJO_SERVER_URL)\s*\}\}`)

// normalizeServerURLs replaces every server-URL derived ${{ }} expression in
// the workflow payload with the forgejoURL literal, so the job behaves as if
// it were natively connected to the owning instance: github.server_url and
// env.GITHUB_SERVER_URL / env.FORGEJO_SERVER_URL become forgejoURL,
// github.api_url and env.GITHUB_API_URL become forgejoURL + "/api/v1", and
// github.repository_url becomes forgejoURL + "/" + repository (repository is
// the task context's "owner/repo" — when it is empty the repository_url
// expression is left untouched). Only scalar value nodes are rewritten —
// mapping keys and comments are never touched — and any other expression is
// preserved verbatim. On parse/encode failure or when nothing matched, the
// original payload bytes are returned (unchanged, not re-encoded), so the
// runner-side validation can never break.
func normalizeServerURLs(payload []byte, forgejoURL, repository string) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(payload, &doc); err != nil {
		return payload, fmt.Errorf("workflow payload is not valid YAML: %w", err)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return payload, fmt.Errorf("workflow payload is not a YAML document")
	}
	top := doc.Content[0]
	if top.Kind != yaml.MappingNode {
		return payload, fmt.Errorf("workflow payload root is not a mapping")
	}

	replacement := func(expr string) string {
		key := serverURLExprRe.FindStringSubmatch(expr)[1]
		switch key {
		case "github.server_url", "env.GITHUB_SERVER_URL", "env.FORGEJO_SERVER_URL":
			return forgejoURL
		case "github.api_url", "env.GITHUB_API_URL":
			return forgejoURL + "/api/v1"
		case "github.repository_url":
			if repository == "" {
				return expr // no owner/repo in the task context — leave as is
			}
			return forgejoURL + "/" + repository
		}
		return expr
	}
	changed := false
	rewriteScalarValues(top, func(v string) string {
		if !strings.Contains(v, "${{") {
			return v
		}
		out := serverURLExprRe.ReplaceAllStringFunc(v, replacement)
		if out != v {
			changed = true
		}
		return out
	})
	if !changed {
		return payload, nil
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(4) // match the Forgejo generator's output style
	if err := enc.Encode(&doc); err != nil {
		return payload, fmt.Errorf("re-encoding workflow payload: %w", err)
	}
	if err := enc.Close(); err != nil {
		return payload, fmt.Errorf("closing workflow payload encoder: %w", err)
	}
	return buf.Bytes(), nil
}

// rewriteScalarValues walks the yaml node tree and passes the decoded string
// of every scalar VALUE node through fn, replacing the node with the result.
// Mapping keys are skipped (their even offsets inside a mapping's Content are
// never visited) and comments are stored separately by yaml.v3, so neither
// key names nor comment text can be touched.
func rewriteScalarValues(n *yaml.Node, fn func(string) string) {
	switch n.Kind {
	case yaml.MappingNode:
		// Content holds key,value pairs — only values get rewritten.
		for i := 1; i < len(n.Content); i += 2 {
			rewriteScalarValues(n.Content[i], fn)
		}
	case yaml.ScalarNode:
		n.Value = fn(n.Value)
	default: // DocumentNode, SequenceNode, AliasNode (empty Content)
		for _, c := range n.Content {
			rewriteScalarValues(c, fn)
		}
	}
}

// mappingValue returns the value node for key in the mapping node m, or nil
// when the key is absent or m is not a mapping.
func mappingValue(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Kind == yaml.ScalarNode && m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

// setWithInput ensures the step mapping carries with.<input> == value: it
// replaces an existing input value in place (keeping key position), appends a
// new key to an existing with block, or appends a fresh with block when the
// step has none. It reports whether the node tree changed.
func setWithInput(step *yaml.Node, input, value string) bool {
	str := func(s string) *yaml.Node {
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: s}
	}
	with := mappingValue(step, "with")
	if with == nil {
		step.Content = append(step.Content,
			str("with"),
			&yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{
				str(input), str(value),
			}})
		return true
	}
	if with.Kind != yaml.MappingNode {
		// Malformed with block — leave the step untouched; the payload is
		// invalid workflow syntax anyway.
		return false
	}
	for i := 0; i+1 < len(with.Content); i += 2 {
		if with.Content[i].Kind == yaml.ScalarNode && with.Content[i].Value == input {
			if with.Content[i+1].Value == value {
				return false // already set — nothing to do
			}
			with.Content[i+1] = str(value)
			return true
		}
	}
	with.Content = append(with.Content, str(input), str(value))
	return true
}

// rewriteWorkflowPayload returns a copy of task whose workflow payload is
// adapted to the owning Forgejo instance: checkout steps are pointed at
// forgejoURL via the with.github-server-url input (so actions/checkout clones
// straight from that instance instead of through Forgery's proxy), and every
// server-URL derived ${{ }} expression is normalized to the forgejoURL
// literal (see normalizeServerURLs), so docker registry pushes, API calls,
// and clone URLs behave as if the job were natively connected to Forgejo.
// repository is the task context's "owner/repo" used for the repository_url
// replacement; it may be empty. The store's task is never mutated: the
// payload bytes are the only difference from the stored task. On any
// parse/match/encode failure — or when there is nothing to rewrite — the
// original task pointer is returned unchanged, keeping FetchTask's
// pass-through behavior intact. Rewriting is always based on the stored
// payload, so repeated FetchTask calls cannot accumulate pollution
// (idempotent by construction).
func rewriteWorkflowPayload(task *v1.Task, forgejoURL, repository string) *v1.Task {
	if task == nil || len(task.WorkflowPayload) == 0 || forgejoURL == "" {
		return task
	}
	rewritten, err := injectCheckoutServerURL(task.WorkflowPayload, forgejoURL)
	if err != nil {
		return task
	}
	rewritten, err = normalizeServerURLs(rewritten, forgejoURL, repository)
	if err != nil {
		return task
	}
	if bytes.Equal(rewritten, task.WorkflowPayload) {
		return task
	}
	clone := proto.Clone(task).(*v1.Task) // deep copy: the store task is never mutated
	clone.WorkflowPayload = rewritten
	return clone
}
