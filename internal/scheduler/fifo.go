package scheduler

import "github.com/BibekP/infersched/internal/lifecycle"

// FIFO schedules admitted requests strictly in arrival order. Ties on arrival
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
	return orderBy(reqs, byArrival)
}
