package backend

import (
	"testing"
	"time"

	"github.com/BibekP/infersched/internal/lifecycle"
)

func mkReq(id string) *lifecycle.Request {
	return lifecycle.NewRequest(id, "t0", lifecycle.ClassInteractive, lifecycle.PriorityNormal,
		time.Time{}, 0, 0, lifecycle.CostEstimate{PrefillTokens: 1, ExpectedOutputTokens: 1}, nil)
}

func TestMockBackendCompletes(t *testing.T) {
	b := NewMockBackend(5*time.Millisecond, nil)
	if _, err := b.Submit(mkReq("r1")); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-b.Results():
		if res.Outcome != OutcomeCompleted {
			t.Fatalf("outcome=%s", res.Outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for result")
	}
	_ = b.Close()
}

func TestMockBackendCancel(t *testing.T) {
	b := NewMockBackend(time.Hour, nil)
	h, err := b.Submit(mkReq("r1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Cancel(h); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-b.Results():
		if res.Outcome != OutcomeCancelled {
			t.Fatalf("outcome=%s want cancelled", res.Outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cancellation")
	}
	_ = b.Close()
}

func TestMockBackendSubmitAfterClose(t *testing.T) {
	b := NewMockBackend(time.Millisecond, nil)
	_ = b.Close()
	if _, err := b.Submit(mkReq("r1")); err != ErrBackendClosed {
		t.Fatalf("want ErrBackendClosed, got %v", err)
	}
}

func TestMockBackendDoubleClose(t *testing.T) {
	b := NewMockBackend(time.Millisecond, nil)
	_ = b.Close()
	if err := b.Close(); err != nil {
		t.Fatalf("double close should be nil, got %v", err)
	}
}

func TestSimBackendRunsBuffered(t *testing.T) {
	b := NewSimBackend(func(reqs []*lifecycle.Request) []Result {
		out := make([]Result, 0, len(reqs))
		for _, r := range reqs {
			out = append(out, Result{RequestID: r.ID, Outcome: OutcomeCompleted, WorkerID: 0})
		}
		return out
	})
	for _, id := range []string{"a", "b", "c"} {
		if _, err := b.Submit(mkReq(id)); err != nil {
			t.Fatal(err)
		}
	}
	if b.InFlightPending() != 3 {
		t.Fatalf("expected 3 pending")
	}
	go b.RunAll()
	got := map[string]bool{}
	for i := 0; i < 3; i++ {
		res := <-b.Results()
		got[res.RequestID] = true
	}
	if len(got) != 3 {
		t.Fatalf("got %v", got)
	}
	_ = b.Close()
}
