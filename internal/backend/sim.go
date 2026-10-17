package backend

import (
	"sync"

	"github.com/BibekP/infersched/internal/lifecycle"
)

// SimBackend adapts the deterministic discrete-event simulator to the
// ExecutionBackend interface. Because the simulator is trace-oriented (it runs
// a whole arrival set in one pass), this adapter buffers submitted requests and
// executes them when RunAll or Close is called, streaming results on the
// Results channel.
//
// It exists so the coordinator can be driven end-to-end by the simulator
// through the same interface a real backend would use. It is NOT a live,
// incremental inference server.
type SimBackend struct {
	mu        sync.Mutex
	pending   []*lifecycle.Request
	results   chan Result
	runFn     func(reqs []*lifecycle.Request) []Result
	closed    bool
	submitted map[string]bool
	cancelled map[string]bool
}

// NewSimBackend constructs a SimBackend whose runFn executes the buffered
// request set and returns per-request results (typically simulator.Run wrapped).
func NewSimBackend(runFn func([]*lifecycle.Request) []Result) *SimBackend {
	return &SimBackend{
		results:   make(chan Result, 256),
		runFn:     runFn,
		submitted: make(map[string]bool),
		cancelled: make(map[string]bool),
	}
}

// Submit buffers a request for the next execution pass.
func (b *SimBackend) Submit(req *lifecycle.Request) (Handle, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return Handle{}, ErrBackendClosed
	}
	if b.submitted[req.ID] {
		return Handle{RequestID: req.ID}, nil
	}
	b.submitted[req.ID] = true
	b.pending = append(b.pending, req)
	return Handle{RequestID: req.ID, Token: uint64(len(b.pending))}, nil
}

// Cancel marks a buffered request cancelled; it will be dropped on the next run.
func (b *SimBackend) Cancel(h Handle) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cancelled[h.RequestID] = true
	return nil
}

// Results returns the result channel.
func (b *SimBackend) Results() <-chan Result { return b.results }

// InFlightPending reports how many requests are buffered for the next run (for
// tests and diagnostics).
func (b *SimBackend) InFlightPending() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

// RunAll executes all buffered requests and streams results. It is safe to call
// once; subsequent calls return immediately.
func (b *SimBackend) RunAll() {
	b.mu.Lock()
	if b.closed && len(b.pending) == 0 {
		b.mu.Unlock()
		return
	}
	reqs := b.pending
	b.pending = nil
	b.mu.Unlock()

	for _, r := range reqs {
		b.mu.Lock()
		cancelled := b.cancelled[r.ID]
		b.mu.Unlock()
		if cancelled {
			b.results <- Result{RequestID: r.ID, Outcome: OutcomeCancelled, WorkerID: -1}
		}
	}
	results := b.runFn(reqs)
	for _, res := range results {
		b.mu.Lock()
		cancelled := b.cancelled[res.RequestID]
		b.mu.Unlock()
		if cancelled && res.Outcome == OutcomeCompleted {
			res.Outcome = OutcomeCancelled
		}
		b.results <- res
	}
}

// Close runs any remaining buffered work, then closes the result channel.
func (b *SimBackend) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	remaining := len(b.pending)
	b.mu.Unlock()
	if remaining > 0 {
		b.RunAll()
	}
	close(b.results)
	return nil
}
