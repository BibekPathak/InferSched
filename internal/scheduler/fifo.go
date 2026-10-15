package scheduler

import (
	"sort"

	"github.com/BibekP/infersched/internal/lifecycle"
)

// FIFO schedules admitted requests strictly in arrival order. Ties on Arrival
// (which can occur with batched arrivals at the same monotonic instant) are
// broken by request ID so ordering is fully deterministic.
type FIFO struct{}

// NewFIFO returns a FIFO policy.
func NewFIFO() *FIFO { return &FIFO{} }

// Name implements Policy.
func (f *FIFO) Name() string { return "fifo" }

// Reset implements Policy. FIFO keeps no state.
func (f *FIFO) Reset() {}

// Order implements Policy.
func (f *FIFO) Order(reqs []*lifecycle.Request) []*lifecycle.Request {
	out := make([]*lifecycle.Request, len(reqs))
	copy(out, reqs)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Arrival != out[j].Arrival {
			return out[i].Arrival < out[j].Arrival
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// Registry returns the built-in policies keyed by name. Later phases extend
// this map with priority, SJF, EDF, hybrid and aging policies.
func Registry() map[string]Policy {
	return map[string]Policy{
		"fifo": NewFIFO(),
	}
}
