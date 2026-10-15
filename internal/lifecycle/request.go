// Package lifecycle defines the InferSched request model and its explicit
// state machine. All state changes flow through Request.transition so illegal
// transitions and double resource release are structurally prevented.
package lifecycle

import (
	"context"
	"sync"
	"time"
)

// WorkloadClass is a coarse label used for fairness accounting and metrics.
type WorkloadClass string

const (
	ClassInteractive WorkloadClass = "interactive"
	ClassBatch       WorkloadClass = "batch"
	ClassLongContext WorkloadClass = "long_context"
	ClassUnknown     WorkloadClass = "unknown"
)

// Priority is a small ordered priority. Higher values are more important.
type Priority int

const (
	PriorityLow      Priority = 0
	PriorityNormal   Priority = 50
	PriorityHigh     Priority = 100
	PriorityCritical Priority = 200
)

// CostEstimate captures the estimator's calibrated prediction of the work a
// request represents. These are ESTIMATES, never exact execution times; the
// cost model that produces them is configurable and calibrated to the backend.
type CostEstimate struct {
	// PrefillTokens is the number of prompt tokens (known exactly at ingress).
	PrefillTokens int
	// ExpectedOutputTokens is the predicted decode length. It is an estimate and
	// may be wrong; experiments must account for prediction error.
	ExpectedOutputTokens int
	// PrefillCostMS is the estimated wall time of the prefill phase.
	PrefillCostMS float64
	// DecodeCostPerTokenMS is the estimated per-token decode cost.
	DecodeCostPerTokenMS float64
	// KVTokens is the estimated peak number of cached tokens (prefill + output).
	KVTokens int
}

// TotalCostMS is the estimated total service time of the request.
func (c CostEstimate) TotalCostMS() float64 {
	return c.PrefillCostMS + float64(c.ExpectedOutputTokens)*c.DecodeCostPerTokenMS
}

// Request is the unit of work scheduled by InferSched. Internally all elapsed
// time and deadline math uses monotonic time (Arrival and Deadline are derived
// from a single monotonic baseline). WallClockArrival is retained only for
// external reporting/correlation.
type Request struct {
	ID       string
	TenantID string
	Class    WorkloadClass
	Priority Priority

	// WallClockArrival is a wall-clock timestamp used only for reporting.
	WallClockArrival time.Time
	// Arrival is the monotonic arrival time in nanoseconds since the runtime
	// baseline. It is the authoritative ordering key for arrival order.
	Arrival int64
	// Deadline is the monotonic deadline in nanoseconds since the same baseline.
	// Zero means the request has no deadline.
	Deadline int64

	Estimate CostEstimate

	// Ctx is cancelled by the coordinator if the request is cancelled. It is
	// propagated to execution so an in-flight request can be aborted.
	Ctx context.Context

	mu    sync.Mutex
	state State
}

// NewRequest constructs a request in the QUEUED state. cancel must be non-nil.
func NewRequest(
	id, tenant string,
	class WorkloadClass,
	priority Priority,
	wallArrival time.Time,
	arrival int64,
	deadline int64,
	est CostEstimate,
	cancel context.Context,
) *Request {
	if cancel == nil {
		cancel = context.Background()
	}
	return &Request{
		ID:               id,
		TenantID:         tenant,
		Class:            class,
		Priority:         priority,
		WallClockArrival: wallArrival,
		Arrival:          arrival,
		Deadline:         deadline,
		Estimate:         est,
		Ctx:              cancel,
		state:            StateQueued,
	}
}

// State returns the current lifecycle state.
func (r *Request) State() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// HasDeadline reports whether the request carries a deadline.
func (r *Request) HasDeadline() bool { return r.Deadline != 0 }

// transition atomically moves the request to next. It returns a
// *TransitionError if the move is illegal. On success it returns the previous
// state so callers can drive exactly-once side effects (e.g. resource release).
func (r *Request) transition(next State) (State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prev := r.state
	if !CanTransition(prev, next) {
		return prev, &TransitionError{RequestID: r.ID, From: prev, To: next}
	}
	r.state = next
	return prev, nil
}
