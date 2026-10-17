package backend

import (
	"context"
	"sync"
	"time"

	"github.com/BibekP/infersched/internal/lifecycle"
)

// MockBackend is a deterministic, injectable backend for lifecycle and failure
// testing. It does not model batching or cost: it accepts requests, waits a
// configurable nominal duration (or until cancelled), and reports the outcome.
//
// It is used to exercise the coordinator's admission, resource, cancellation,
// and shutdown paths without the simulator.
type MockBackend struct {
	// NominalDuration is how long a request "runs" before completing.
	NominalDuration time.Duration
	// FailEvery, when > 0, fails every Nth accepted request (1-based).
	FailEvery int

	mu       sync.Mutex
	next     uint64
	closed   bool
	inFlight map[string]context.CancelFunc
	results  chan Result
	now      func() int64
}

// NewMockBackend constructs a MockBackend. now supplies a clock in nanoseconds
// (defaults to wall time since process start).
func NewMockBackend(nominal time.Duration, now func() int64) *MockBackend {
	if now == nil {
		base := time.Now()
		now = func() int64 { return int64(time.Since(base)) }
	}
	return &MockBackend{
		NominalDuration: nominal,
		inFlight:        make(map[string]context.CancelFunc),
		results:         make(chan Result, 256),
		now:             now,
	}
}

// Submit starts a mock execution. It returns ErrBackendClosed after Close.
func (b *MockBackend) Submit(req *lifecycle.Request) (Handle, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return Handle{}, ErrBackendClosed
	}
	b.next++
	token := b.next
	acceptIndex := b.next
	ctx, cancel := context.WithCancel(context.Background())
	b.inFlight[req.ID] = cancel
	start := b.now()
	b.mu.Unlock()

	go func() {
		select {
		case <-time.After(b.NominalDuration):
			b.finish(req.ID, token, start, false)
		case <-ctx.Done():
			b.finish(req.ID, token, start, true)
		}
	}()

	if b.FailEvery > 0 && int(acceptIndex)%b.FailEvery == 0 {
		// Fail this one promptly.
		go func() {
			time.Sleep(time.Millisecond)
			b.fail(req.ID, token, start)
		}()
	}

	return Handle{RequestID: req.ID, Token: token}, nil
}

// Cancel cancels an in-flight mock execution. Unknown or finished requests are
// a no-op returning nil.
func (b *MockBackend) Cancel(h Handle) error {
	b.mu.Lock()
	cancel, ok := b.inFlight[h.RequestID]
	b.mu.Unlock()
	if ok {
		cancel()
	}
	return nil
}

func (b *MockBackend) finish(id string, token uint64, start int64, cancelled bool) {
	b.mu.Lock()
	_, ok := b.inFlight[id]
	if !ok {
		b.mu.Unlock()
		return
	}
	delete(b.inFlight, id)
	closed := b.closed
	b.mu.Unlock()
	if !ok {
		return
	}
	outcome := OutcomeCompleted
	if cancelled {
		outcome = OutcomeCancelled
	}
	res := Result{
		RequestID:    id,
		Outcome:      outcome,
		StartNS:      start,
		FirstTokenNS: start,
		EndNS:        b.now(),
		WorkerID:     -1,
	}
	if !closed {
		select {
		case b.results <- res:
		default:
		}
	}
}

func (b *MockBackend) fail(id string, token uint64, start int64) {
	b.mu.Lock()
	_, ok := b.inFlight[id]
	if !ok {
		b.mu.Unlock()
		return
	}
	delete(b.inFlight, id)
	b.mu.Unlock()
	res := Result{RequestID: id, Outcome: OutcomeFailed, StartNS: start, EndNS: b.now(), WorkerID: -1}
	select {
	case b.results <- res:
	default:
	}
}

// Results returns the result channel. Closing the backend closes this channel.
func (b *MockBackend) Results() <-chan Result { return b.results }

// Close cancels all in-flight work and closes the result channel.
func (b *MockBackend) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	for id, cancel := range b.inFlight {
		cancel()
		delete(b.inFlight, id)
	}
	b.mu.Unlock()
	close(b.results)
	return nil
}

// InFlight reports how many mock executions are still running (for tests).
func (b *MockBackend) InFlight() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.inFlight)
}
