package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func newTestReq(id string) *Request {
	return NewRequest(id, "t0", ClassInteractive, PriorityNormal, time.Time{}, 100, 0, CostEstimate{}, context.Background())
}

func TestHappyPath(t *testing.T) {
	r := newTestReq("r1")
	if r.State() != StateQueued {
		t.Fatalf("want QUEUED, got %s", r.State())
	}
	if err := r.Admit(); err != nil {
		t.Fatalf("admit: %v", err)
	}
	if err := r.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := r.Complete(); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if r.State() != StateCompleted {
		t.Fatalf("want COMPLETED, got %s", r.State())
	}
}

func TestIllegalTransitions(t *testing.T) {
	cases := []struct {
		name string
		from State
		to   State
	}{
		{"queued->running", StateQueued, StateRunning},
		{"queued->completed", StateQueued, StateCompleted},
		{"admitted->completed", StateAdmitted, StateCompleted},
		{"running->rejected", StateRunning, StateRejected},
		{"running->admitted", StateRunning, StateAdmitted},
		{"completed->running", StateCompleted, StateRunning},
		{"rejected->queued", StateRejected, StateQueued},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if CanTransition(tc.from, tc.to) {
				t.Fatalf("expected %s->%s to be illegal", tc.from, tc.to)
			}
		})
	}
}

func TestDoubleCompleteFails(t *testing.T) {
	r := newTestReq("r1")
	must(t, r.Admit())
	must(t, r.Start())
	must(t, r.Complete())
	if err := r.Complete(); err == nil {
		t.Fatal("second Complete should fail")
	}
}

func TestDoubleCancelDetected(t *testing.T) {
	r := newTestReq("r1")
	must(t, r.Cancel())
	var te *TransitionError
	if err := r.Cancel(); !errors.As(err, &te) {
		t.Fatalf("second Cancel should return TransitionError, got %v", err)
	}
}

func TestCancelFromEachActiveState(t *testing.T) {
	steps := []struct {
		name string
		run  func(*Request) error
	}{
		{"from queued", func(r *Request) error { return nil }},
		{"from admitted", func(r *Request) error { return r.Admit() }},
		{"from running", func(r *Request) error {
			err := r.Admit()
			if err != nil {
				return err
			}
			return r.Start()
		}},
	}
	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			r := newTestReq("r1")
			must(t, s.run(r))
			must(t, r.Cancel())
			if r.State() != StateCancelled {
				t.Fatalf("want CANCELLED, got %s", r.State())
			}
		})
	}
}

func TestExpireRules(t *testing.T) {
	r := newTestReq("r1")
	must(t, r.Admit())
	must(t, r.Expire())
	if r.State() != StateExpired {
		t.Fatalf("want EXPIRED, got %s", r.State())
	}
}

func TestConcurrentCancelExactlyOnce(t *testing.T) {
	r := newTestReq("r1")
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		success int
	)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Cancel(); err == nil {
				mu.Lock()
				success++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if success != 1 {
		t.Fatalf("exactly one Cancel should succeed, got %d", success)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
