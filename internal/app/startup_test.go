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

package app

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"git.0x0f.dev/forgery/internal/state"
)

// registrar fakes instanceRegistrar: Register/Declare either return
// immediately (success or a preset error) or block until ctx is done
// (simulating a blackholed Forgejo that accepts connections but never
// replies). Call counts are recorded for assertions.
type registrar struct {
	mu            sync.Mutex
	hasIdentity   bool
	blockRegister bool
	blockDeclare  bool
	registerErr   error
	declareErr    error
	registerCalls int
	declareCalls  int
}

func (r *registrar) HasIdentity() bool { return r.hasIdentity }

func (r *registrar) Identity() state.Identity {
	return state.Identity{UUID: "uuid-1", Token: "token-1"}
}

func (r *registrar) Register(ctx context.Context) error {
	r.mu.Lock()
	r.registerCalls++
	block := r.blockRegister
	err := r.registerErr
	r.mu.Unlock()
	if err != nil {
		return err
	}
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (r *registrar) Declare(ctx context.Context) error {
	r.mu.Lock()
	r.declareCalls++
	block := r.blockDeclare
	err := r.declareErr
	r.mu.Unlock()
	if err != nil {
		return err
	}
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (r *registrar) callCounts() (register, declare int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.registerCalls, r.declareCalls
}

// TestRegisterInstance_Success verifies the happy path: Register and Declare
// both run and no error is returned.
func TestRegisterInstance_Success(t *testing.T) {
	r := &registrar{}
	err := registerInstance(context.Background(), r, time.Second, testLogger())
	if err != nil {
		t.Fatalf("registerInstance: %v", err)
	}
	regs, decls := r.callCounts()
	if regs != 1 || decls != 1 {
		t.Fatalf("call counts = (%d register, %d declare), want (1, 1)", regs, decls)
	}
}

// TestRegisterInstance_SkipsRegisterWithIdentity verifies the persisted
// identity path: Register is skipped but Declare still runs (and is still
// subject to the timeout budget).
func TestRegisterInstance_SkipsRegisterWithIdentity(t *testing.T) {
	r := &registrar{hasIdentity: true}
	err := registerInstance(context.Background(), r, time.Second, testLogger())
	if err != nil {
		t.Fatalf("registerInstance: %v", err)
	}
	regs, decls := r.callCounts()
	if regs != 0 || decls != 1 {
		t.Fatalf("call counts = (%d register, %d declare), want (0, 1)", regs, decls)
	}
}

// TestRegisterInstance_RegisterTimeout verifies the F7 fix: a Register that
// never returns (blackholed Forgejo) is cut off by the timeout and the
// error names the failing stage.
func TestRegisterInstance_RegisterTimeout(t *testing.T) {
	r := &registrar{blockRegister: true}
	start := time.Now()
	err := registerInstance(context.Background(), r, 50*time.Millisecond, testLogger())
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("registerInstance must fail when Register blocks past the timeout")
	}
	if !strings.Contains(err.Error(), "registration failed") {
		t.Errorf("error should name the registration stage, got %q", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("timeout did not fire: registerInstance took %v", elapsed)
	}
	regs, decls := r.callCounts()
	if decls != 0 {
		t.Errorf("Declare must not run after Register timed out, got %d declare calls", decls)
	}
	if regs != 1 {
		t.Errorf("Register calls = %d, want 1", regs)
	}
}

// TestRegisterInstance_DeclareTimeout verifies the F7 fix on the
// identity-present path: with Register skipped, a blocking Declare is cut
// off by the same timeout and the error names the declaration stage.
func TestRegisterInstance_DeclareTimeout(t *testing.T) {
	r := &registrar{hasIdentity: true, blockDeclare: true}
	start := time.Now()
	err := registerInstance(context.Background(), r, 50*time.Millisecond, testLogger())
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("registerInstance must fail when Declare blocks past the timeout")
	}
	if !strings.Contains(err.Error(), "declaration failed") {
		t.Errorf("error should name the declaration stage, got %q", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("timeout did not fire: registerInstance took %v", elapsed)
	}
}

// TestRegisterInstance_RegisterError verifies fail-fast: a returned error
// from Register propagates immediately (no timeout wait).
func TestRegisterInstance_RegisterError(t *testing.T) {
	r := &registrar{registerErr: errors.New("forgejo: permission denied")}
	err := registerInstance(context.Background(), r, time.Second, testLogger())
	if err == nil {
		t.Fatal("registerInstance must propagate the Register error")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("error should wrap the Register failure, got %q", err)
	}
}
