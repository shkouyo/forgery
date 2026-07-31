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

package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// statePath returns a state file path inside a fresh temp dir.
func statePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "forgery-state.json")
}

// readRaw reads the state file bytes without parsing.
func readRaw(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read state file: %v", err)
	}
	return data
}

// ── TestLoad_MissingFile ───────────────────────────────────────────────────────

func TestLoad_MissingFile(t *testing.T) {
	f := NewFileStore(statePath(t))

	id, ok, err := f.Load("https://forgejo.example.com")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if ok {
		t.Error("ok = true, want false for missing file")
	}
	if id != (Identity{}) {
		t.Errorf("id = %+v, want zero value", id)
	}
}

// ── TestSave_ThenLoad ──────────────────────────────────────────────────────────

func TestSave_ThenLoad(t *testing.T) {
	f := NewFileStore(statePath(t))
	id := Identity{UUID: "uuid-1", Token: "token-1"}

	if err := f.Save("https://forgejo.example.com", id); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	got, ok, err := f.Load("https://forgejo.example.com")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !ok {
		t.Fatal("ok = false, want true after Save")
	}
	if got != id {
		t.Errorf("id = %+v, want %+v", got, id)
	}
}

// ── TestSave_MultipleURLs ──────────────────────────────────────────────────────

func TestSave_MultipleURLs(t *testing.T) {
	f := NewFileStore(statePath(t))
	a := Identity{UUID: "uuid-a", Token: "token-a"}
	b := Identity{UUID: "uuid-b", Token: "token-b"}

	if err := f.Save("https://a.example.com", a); err != nil {
		t.Fatalf("Save a returned error: %v", err)
	}
	if err := f.Save("https://b.example.com", b); err != nil {
		t.Fatalf("Save b returned error: %v", err)
	}

	gotA, okA, err := f.Load("https://a.example.com")
	if err != nil {
		t.Fatalf("Load a returned error: %v", err)
	}
	if !okA || gotA != a {
		t.Errorf("a = %+v, ok = %v; want %+v", gotA, okA, a)
	}
	gotB, okB, err := f.Load("https://b.example.com")
	if err != nil {
		t.Fatalf("Load b returned error: %v", err)
	}
	if !okB || gotB != b {
		t.Errorf("b = %+v, ok = %v; want %+v", gotB, okB, b)
	}
}

// ── TestSave_OverwritePreservesOthers ──────────────────────────────────────────

func TestSave_OverwritePreservesOthers(t *testing.T) {
	f := NewFileStore(statePath(t))
	a := Identity{UUID: "uuid-a", Token: "token-a"}
	b := Identity{UUID: "uuid-b", Token: "token-b"}
	a2 := Identity{UUID: "uuid-a2", Token: "token-a2"}

	if err := f.Save("https://a.example.com", a); err != nil {
		t.Fatalf("Save a returned error: %v", err)
	}
	if err := f.Save("https://b.example.com", b); err != nil {
		t.Fatalf("Save b returned error: %v", err)
	}
	if err := f.Save("https://a.example.com", a2); err != nil {
		t.Fatalf("Save a2 returned error: %v", err)
	}

	gotA, okA, err := f.Load("https://a.example.com")
	if err != nil {
		t.Fatalf("Load a returned error: %v", err)
	}
	if !okA || gotA != a2 {
		t.Errorf("a = %+v, ok = %v; want overwritten to %+v", gotA, okA, a2)
	}
	gotB, okB, err := f.Load("https://b.example.com")
	if err != nil {
		t.Fatalf("Load b returned error: %v", err)
	}
	if !okB || gotB != b {
		t.Errorf("b = %+v, ok = %v; want preserved %+v", gotB, okB, b)
	}
}

// ── TestLoad_MissingKey ────────────────────────────────────────────────────────

func TestLoad_MissingKey(t *testing.T) {
	f := NewFileStore(statePath(t))
	if err := f.Save("https://a.example.com", Identity{UUID: "u", Token: "t"}); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	// Same file, different key: no identity, no error.
	id, ok, err := f.Load("https://b.example.com")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if ok {
		t.Error("ok = true, want false for unstored URL")
	}
	if id != (Identity{}) {
		t.Errorf("id = %+v, want zero value", id)
	}
}

// ── TestLoad_CorruptFile ───────────────────────────────────────────────────────

func TestLoad_CorruptFile(t *testing.T) {
	for _, content := range []string{
		"not json at all",
		`{"version":1,`,
		`{"version":1,"identities":{"https://a":"wrong-shape"}}`,
		``, // empty file is corrupt too: strict parsing
	} {
		t.Run(strings.ReplaceAll(content, " ", "_"), func(t *testing.T) {
			path := statePath(t)
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatalf("write corrupt file: %v", err)
			}
			f := NewFileStore(path)

			_, _, err := f.Load("https://a.example.com")
			if err == nil {
				t.Fatal("Load returned nil error, want error for corrupt file")
			}
			if !strings.Contains(err.Error(), "state:") {
				t.Errorf("error %q lacks state: context", err)
			}
		})
	}
}

// ── TestLoad_WrongVersion ──────────────────────────────────────────────────────

func TestLoad_WrongVersion(t *testing.T) {
	path := statePath(t)
	if err := os.WriteFile(path, []byte(`{"version":2,"identities":{}}`), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	f := NewFileStore(path)

	_, _, err := f.Load("https://a.example.com")
	if err == nil {
		t.Fatal("Load returned nil error, want error for unsupported version")
	}
	if !strings.Contains(err.Error(), "unsupported version") {
		t.Errorf("error %q lacks version context", err)
	}
}

// ── TestLoad_UnreadableFile ────────────────────────────────────────────────────

func TestLoad_UnreadableFile(t *testing.T) {
	// A directory is not a readable state file.
	dir := t.TempDir()
	f := NewFileStore(dir)

	_, _, err := f.Load("https://a.example.com")
	if err == nil {
		t.Fatal("Load returned nil error, want error for unreadable path")
	}
}

// ── TestSave_MissingDir ────────────────────────────────────────────────────────

func TestSave_MissingDir(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no-such-dir", "state.json")
	f := NewFileStore(path)

	err := f.Save("https://a.example.com", Identity{UUID: "u", Token: "t"})
	if err == nil {
		t.Fatal("Save returned nil error, want error for missing directory")
	}
}

// ── TestSave_Atomic ────────────────────────────────────────────────────────────

func TestSave_Atomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "forgery-state.json")
	f := NewFileStore(path)

	// Multiple saves; the file must be complete and parseable after each one.
	for i := 0; i < 5; i++ {
		id := Identity{UUID: fmt.Sprintf("uuid-%d", i), Token: fmt.Sprintf("token-%d", i)}
		if err := f.Save("https://a.example.com", id); err != nil {
			t.Fatalf("Save %d returned error: %v", i, err)
		}

		data := readRaw(t, path)
		if !json.Valid(data) {
			t.Fatalf("save %d left invalid JSON: %q", i, data)
		}
		var file fileFormat
		if err := json.Unmarshal(data, &file); err != nil {
			t.Fatalf("save %d left unparseable file: %v", i, err)
		}
		if file.Version != formatVersion {
			t.Errorf("save %d: version = %d, want %d", i, file.Version, formatVersion)
		}
		if file.Identities["https://a.example.com"] != id {
			t.Errorf("save %d: stored = %+v, want %+v", i, file.Identities["https://a.example.com"], id)
		}
	}

	// No temp files may be left behind.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("dir contains %d entries, want exactly the state file: %v", len(entries), names)
	}
}

// ── TestSave_Concurrent ────────────────────────────────────────────────────────

func TestSave_Concurrent(t *testing.T) {
	path := statePath(t)
	f := NewFileStore(path)

	urls := []string{
		"https://a.example.com",
		"https://b.example.com",
		"https://c.example.com",
		"https://d.example.com",
	}

	// Each goroutine repeatedly Saves its own URL and Loads a random one.
	var wg sync.WaitGroup
	for i, u := range urls {
		for j := 0; j < 20; j++ {
			wg.Add(1)
			go func(u string, i, j int) {
				defer wg.Done()
				id := Identity{UUID: fmt.Sprintf("uuid-%s-%d", u, j), Token: fmt.Sprintf("token-%d-%d", i, j)}
				if err := f.Save(u, id); err != nil {
					t.Errorf("Save(%s) error: %v", u, err)
					return
				}
				// A URL we never saved to must stay absent.
				if _, ok, err := f.Load("https://nope.example.com"); err != nil {
					t.Errorf("Load error: %v", err)
				} else if ok {
					t.Error("Load(nope) ok = true, want false")
				}
			}(u, i, j)
		}
	}
	wg.Wait()

	// Every URL must hold a complete, well-formed identity, and the final
	// file must be valid JSON with exactly the four keys.
	for _, u := range urls {
		id, ok, err := f.Load(u)
		if err != nil {
			t.Fatalf("Load(%s) error: %v", u, err)
		}
		if !ok {
			t.Errorf("Load(%s) ok = false, want true", u)
			continue
		}
		if id.UUID == "" || id.Token == "" {
			t.Errorf("Load(%s) = %+v, want non-empty uuid and token", u, id)
		}
	}

	var file fileFormat
	if err := json.Unmarshal(readRaw(t, path), &file); err != nil {
		t.Fatalf("final file unparseable: %v", err)
	}
	if file.Version != formatVersion {
		t.Errorf("version = %d, want %d", file.Version, formatVersion)
	}
	if len(file.Identities) != len(urls) {
		t.Errorf("identities = %d entries, want %d", len(file.Identities), len(urls))
	}
}

// ── TestFileFormat ─────────────────────────────────────────────────────────────

func TestFileFormat(t *testing.T) {
	// Golden check: the on-disk shape matches the documented contract.
	f := NewFileStore(statePath(t))
	if err := f.Save("https://a.example.com", Identity{UUID: "u-1", Token: "t-1"}); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(readRaw(t, f.path), &raw); err != nil {
		t.Fatalf("unmarshal raw file: %v", err)
	}
	if raw["version"] != float64(1) {
		t.Errorf("version = %v, want 1", raw["version"])
	}
	ids, ok := raw["identities"].(map[string]any)
	if !ok {
		t.Fatalf("identities = %v, want object", raw["identities"])
	}
	entry, ok := ids["https://a.example.com"].(map[string]any)
	if !ok {
		t.Fatalf("entry = %v, want object", ids["https://a.example.com"])
	}
	if entry["uuid"] != "u-1" || entry["token"] != "t-1" {
		t.Errorf("entry = %v, want uuid u-1 / token t-1", entry)
	}
}
