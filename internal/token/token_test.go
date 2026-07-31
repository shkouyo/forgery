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

package token

import (
	"regexp"
	"testing"
)

func TestGenerate_Length(t *testing.T) {
	tok, err := Generate()
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if len(tok) != 64 {
		t.Fatalf("Generate() length = %d, want 64", len(tok))
	}
}

func TestGenerate_Uniqueness(t *testing.T) {
	const n = 100
	seen := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		tok, err := Generate()
		if err != nil {
			t.Fatalf("Generate() at iteration %d: %v", i, err)
		}
		if seen[tok] {
			t.Fatalf("Generate() produced duplicate value at iteration %d", i)
		}
		seen[tok] = true
	}
}

func TestGenerate_HexOnly(t *testing.T) {
	hexRe := regexp.MustCompile(`^[0-9a-f]+$`)
	for i := 0; i < 50; i++ {
		tok, err := Generate()
		if err != nil {
			t.Fatalf("Generate() at iteration %d: %v", i, err)
		}
		if !hexRe.MatchString(tok) {
			t.Fatalf("Generate() = %q contains non-hex characters", tok)
		}
	}
}

func TestGenerateBytes_16(t *testing.T) {
	b, err := GenerateBytes(16)
	if err != nil {
		t.Fatalf("GenerateBytes(16) error = %v", err)
	}
	if len(b) != 16 {
		t.Fatalf("GenerateBytes(16) length = %d, want 16", len(b))
	}
}

func TestGenerateBytes_32(t *testing.T) {
	b, err := GenerateBytes(32)
	if err != nil {
		t.Fatalf("GenerateBytes(32) error = %v", err)
	}
	if len(b) != 32 {
		t.Fatalf("GenerateBytes(32) length = %d, want 32", len(b))
	}
}

func TestGenerateBytes_Uniqueness(t *testing.T) {
	const n = 100
	seen := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		b, err := GenerateBytes(16)
		if err != nil {
			t.Fatalf("GenerateBytes(16) at iteration %d: %v", i, err)
		}
		key := string(b)
		if seen[key] {
			t.Fatalf("GenerateBytes(16) produced duplicate value at iteration %d", i)
		}
		seen[key] = true
	}
}

func TestGenerateBytes_InvalidN(t *testing.T) {
	_, err := GenerateBytes(0)
	if err == nil {
		t.Fatal("GenerateBytes(0) should return an error")
	}
	_, err = GenerateBytes(-1)
	if err == nil {
		t.Fatal("GenerateBytes(-1) should return an error")
	}
}

func TestNoPanic(t *testing.T) {
	// Ensure neither function panics under normal use.
	for i := 0; i < 10; i++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Generate() panicked: %v", r)
				}
			}()
			Generate()
		}()
	}
	for i := 0; i < 10; i++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("GenerateBytes(16) panicked: %v", r)
				}
			}()
			GenerateBytes(16)
		}()
	}
}

// Benchmark for Generate
func BenchmarkGenerate(b *testing.B) {
	for b.Loop() {
		Generate()
	}
}

// Benchmark for GenerateBytes(16)
func BenchmarkGenerateBytes(b *testing.B) {
	for b.Loop() {
		GenerateBytes(16)
	}
}
