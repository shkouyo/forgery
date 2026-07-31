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

package slots

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// acquireWithTimeout attempts to acquire a slot, failing the test if the pool
// does not grant one within d. Returns the acquired slot (caller must
// Release it).
func acquireWithTimeout(t *testing.T, p *Pool, d time.Duration) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return p.Acquire(ctx) == nil
}

// ── capacity and acquire/release ──

func TestNew_AcquireRelease(t *testing.T) {
	p := New(2)

	for i := 0; i < 2; i++ {
		if err := p.Acquire(context.Background()); err != nil {
			t.Fatalf("Acquire #%d: %v", i, err)
		}
	}

	// Pool is now full: a further acquire must not succeed quickly.
	if acquireWithTimeout(t, p, 50*time.Millisecond) {
		t.Fatal("Acquire succeeded on a full pool, want block")
	}

	// Release one slot and re-acquire it.
	p.Release()
	if !acquireWithTimeout(t, p, 50*time.Millisecond) {
		t.Fatal("Acquire failed after Release, want free slot")
	}

	// Clean up the two held slots.
	p.Release()
	p.Release()
}

func TestNew_ZeroCapacity(t *testing.T) {
	p := New(0)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := p.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire on zero-capacity pool = %v, want DeadlineExceeded", err)
	}
}

// ── context cancellation ──

func TestAcquire_ContextCancelled(t *testing.T) {
	p := New(1)
	if err := p.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire with cancelled ctx = %v, want context.Canceled", err)
	}

	p.Release()
}

func TestAcquire_ContextDeadline(t *testing.T) {
	p := New(1)
	if err := p.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := p.Acquire(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Errorf("Acquire returned too early: %v", elapsed)
	}

	p.Release()
}

// ── Release contract ──

func TestRelease_NoOpOnEmptyPool(t *testing.T) {
	// Release without a matching Acquire must not panic or corrupt the pool:
	// a subsequent Acquire still works at full capacity.
	p := New(1)
	p.Release()
	if !acquireWithTimeout(t, p, 50*time.Millisecond) {
		t.Fatal("Acquire failed after spurious Release")
	}
	p.Release()
}

// ── concurrency ──

func TestConcurrentAcquireRelease(t *testing.T) {
	const (
		capacity   = 4
		workers    = 8
		iterations = 200
	)

	p := New(capacity)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if err := p.Acquire(context.Background()); err != nil {
					t.Errorf("Acquire: %v", err)
					return
				}
				p.Release()
			}
		}()
	}
	wg.Wait()

	// All workers released everything: the full capacity must be acquirable.
	held := 0
	for i := 0; i < capacity; i++ {
		if !acquireWithTimeout(t, p, 500*time.Millisecond) {
			t.Fatalf("only %d/%d slots available after workers finished, want all (leak)", held, capacity)
		}
		held++
	}
	// Release the held slots for hygiene.
	for i := 0; i < held; i++ {
		p.Release()
	}
}

// TestConcurrentHeldSlotsNeverExceedCapacity verifies that under concurrent
// load the number of simultaneously held slots never exceeds the capacity:
// each worker holds its slot for a random duration before releasing.
func TestConcurrentHeldSlotsNeverExceedCapacity(t *testing.T) {
	const (
		capacity = 3
		workers  = 6
	)

	p := New(capacity)
	held := make(chan struct{}, capacity*2)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if err := p.Acquire(context.Background()); err != nil {
					return
				}
				held <- struct{}{}
				time.Sleep(time.Duration(i%3) * time.Millisecond)
				<-held
				p.Release()
			}
		}()
	}

	// While the workers run, the number of held slots must stay within capacity.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if n := len(held); n > capacity {
				t.Fatalf("%d slots held simultaneously, capacity %d", n, capacity)
			}
		}
	}
}
