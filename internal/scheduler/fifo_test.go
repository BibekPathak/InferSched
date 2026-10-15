package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/BibekP/infersched/internal/lifecycle"
)

func req(id string, arrival int64) *lifecycle.Request {
	return lifecycle.NewRequest(id, "t0", lifecycle.ClassInteractive, lifecycle.PriorityNormal,
		time.Time{}, arrival, 0, lifecycle.CostEstimate{}, context.Background())
}

func ids(reqs []*lifecycle.Request) []string {
	out := make([]string, len(reqs))
	for i, r := range reqs {
		out[i] = r.ID
	}
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFIFOArrivalOrder(t *testing.T) {
	in := []*lifecycle.Request{req("c", 30), req("a", 10), req("b", 20)}
	got := ids(NewFIFO().Order(in))
	want := []string{"a", "b", "c"}
	if !eq(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestFIFODeterministicTieBreak(t *testing.T) {
	in := []*lifecycle.Request{req("b", 10), req("a", 10), req("c", 10)}
	got := ids(NewFIFO().Order(in))
	want := []string{"a", "b", "c"}
	if !eq(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestFIFODoesNotMutateInput(t *testing.T) {
	in := []*lifecycle.Request{req("c", 30), req("a", 10), req("b", 20)}
	before := ids(in)
	_ = NewFIFO().Order(in)
	if after := ids(in); !eq(before, after) {
		t.Fatalf("input mutated: %v -> %v", before, after)
	}
}

func TestFIFOName(t *testing.T) {
	if NewFIFO().Name() != "fifo" {
		t.Fatal("unexpected policy name")
	}
}
