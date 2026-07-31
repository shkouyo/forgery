// Package slots implements the daemon-wide backpressure pool shared by all
// Forgejo instances. A task slot is acquired by a northbound PollLoop before
// it asks Forgejo for work and is held until that task reaches a terminal
// state, at which point the run module releases it. Because the pool is a
// single instance shared across every poller, max_parallel_tasks is a global
// budget across all configured Forgejo instances.
package slots

import "context"

// Pool is a concurrency-safe semaphore of fixed capacity. The zero value is
// not usable; construct it with New.
type Pool struct {
	sem chan struct{}
}

// New creates a Pool with the given capacity — the maximum number of slots
// that can be held simultaneously. A capacity of 0 makes Acquire block until
// the context is done (backpressure fully engaged).
func New(capacity int) *Pool {
	return &Pool{sem: make(chan struct{}, capacity)}
}

// Acquire blocks until a slot is available or ctx is cancelled. It returns an
// error (ctx.Err()) when the context is done before a slot could be taken;
// on success it returns nil and the caller holds one slot until it calls
// Release.
func (p *Pool) Acquire(ctx context.Context) error {
	select {
	case p.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release returns one slot to the pool. It never blocks: on an empty pool it
// is a no-op. The contract is that Release is called only for slots that were
// previously acquired via Acquire.
func (p *Pool) Release() {
	select {
	case <-p.sem:
	default:
	}
}
