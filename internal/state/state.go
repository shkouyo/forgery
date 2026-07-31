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

// Package state implements the file-backed identity store that persists a
// runner's Forgejo identity (UUID + permanent token) across process restarts.
//
// Each Forgejo instance is keyed by its exact forgejo_url string, so a single
// state file can hold identities for multiple instances; C3's multi-instance
// core is expected to share one Store across all northbound clients. All
// operations are serialized by an internal mutex — a Store instance is safe
// for concurrent use.
//
// The on-disk format is versioned JSON:
//
//	{"version":1,"identities":{"<forgejo_url>":{"uuid":"...","token":"..."}}}
//
// Writes are atomic: the file is rewritten to a temporary file in the same
// directory and renamed over the target (0600 permissions), so a crash never
// leaves a partially written state file. A missing file means "no identity
// yet"; a present but unreadable, corrupt, or wrongly versioned file is a
// hard error and is never silently rebuilt — identity data is too valuable
// to discard on a hunch.
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Identity is a runner's Forgejo identity: the runner UUID and the permanent
// runner token issued by the Register RPC.
type Identity struct {
	UUID  string `json:"uuid"`
	Token string `json:"token"`
}

// Store persists runner identities keyed by Forgejo URL (the exact
// forgejo_url string). Implementations must be safe for concurrent use.
type Store interface {
	// Load returns the identity stored for forgejoURL, or ok=false when
	// no identity is stored for it yet.
	Load(forgejoURL string) (Identity, bool, error)
	// Save stores id for forgejoURL, replacing any previous identity while
	// preserving identities stored for other URLs.
	Save(forgejoURL string, id Identity) error
}

// ── file format ──

// fileFormat is the on-disk representation of the state file.
type fileFormat struct {
	Version    int                 `json:"version"`
	Identities map[string]Identity `json:"identities"`
}

// formatVersion is the only state file version this build understands.
const formatVersion = 1

// ── FileStore ──

// FileStore is a Store backed by a single JSON file. The zero value is not
// usable; construct it with NewFileStore.
type FileStore struct {
	mu   sync.Mutex
	path string
}

// NewFileStore creates a FileStore that reads and writes path.
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

// Load reads the identity for forgejoURL from the state file.
//
// A missing file or a missing key yields ok=false with no error. A file that
// exists but cannot be parsed (invalid JSON, unsupported version) is a hard
// error, as is any read failure that is not a missing file.
func (f *FileStore) Load(forgejoURL string) (Identity, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := f.read()
	if err != nil {
		return Identity{}, false, err
	}
	if file == nil {
		return Identity{}, false, nil
	}
	id, ok := file.Identities[forgejoURL]
	return id, ok, nil
}

// Save stores id for forgejoURL, merging into any existing state so other
// instances' identities survive. The write is atomic: temp file + rename.
func (f *FileStore) Save(forgejoURL string, id Identity) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := f.read()
	if err != nil {
		return err
	}
	if file == nil {
		file = &fileFormat{Version: formatVersion, Identities: map[string]Identity{}}
	} else if file.Identities == nil {
		file.Identities = map[string]Identity{}
	}
	file.Identities[forgejoURL] = id

	data, err := json.Marshal(file)
	if err != nil {
		return fmt.Errorf("state: marshal %s: %w", f.path, err)
	}
	return f.write(data)
}

// ── file helpers ──

// read parses the state file, returning (nil, nil) when the file does not
// exist. Any other read or parse failure is returned as an error.
func (f *FileStore) read() (*fileFormat, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("state: read %s: %w", f.path, err)
	}
	var file fileFormat
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("state: parse %s: %w", f.path, err)
	}
	if file.Version != formatVersion {
		return nil, fmt.Errorf("state: %s: unsupported version %d (want %d)", f.path, file.Version, formatVersion)
	}
	return &file, nil
}

// write atomically replaces the state file with data: the payload goes to a
// temp file in the same directory (0600 permissions), is synced and closed,
// then renamed over the target.
func (f *FileStore) write(data []byte) error {
	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(f.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("state: create temp in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename succeeds

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("state: write %s: %w", tmpName, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("state: sync %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("state: close %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		return fmt.Errorf("state: rename %s to %s: %w", tmpName, f.path, err)
	}
	return nil
}
