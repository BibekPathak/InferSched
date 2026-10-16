// Package backend defines the execution-backend interface. An ExecutionBackend
// is the thing that actually runs inference work. InferSched ships two
// implementations: the deterministic simulator (internal/simulator) and a mock
// backend for lifecycle and failure testing. A real inference engine can be
// added later without changing the coordinator, provided it implements this
// interface.
//
// The interface is deliberately narrow and does not depend on any specific
// model-serving framework. It deals in request-level operations and reports the
// measured milestones the scheduler needs (time to first token, completion,
// cancellation, failure). All reported times use the backend's own clock, which
// for the simulator is a virtual monotonic clock.
package backend

import (
	"errors"

	"github.com/BibekP/infersched/internal/lifecycle"
)

// ErrBackendClosed is returned by operations issued after the backend has been
// shut down.
var ErrBackendClosed = errors.New("backend: closed")

// Handle identifies one in-flight execution started by a backend.
type Handle struct {
	RequestID string
	// Token is an opaque, backend-defined token that is only meaningful to the
	// backend that issued it. It lets a backend correlate Submit and Cancel
	// without the coordinator knowing backend internals.
	Token uint64
}

// Outcome classifies how an execution ended.
type Outcome string

const (
	// OutcomeCompleted means the request generated its full output.
	OutcomeCompleted Outcome = "completed"
	// OutcomeCancelled means the request was cancelled before completion.
	OutcomeCancelled Outcome = "cancelled"
	// OutcomeFailed means the backend failed the request (e.g. worker failure).
	OutcomeFailed Outcome = "failed"
	// OutcomeExpired means the request's deadline passed before completion.
	OutcomeExpired Outcome = "expired"
)

// Result reports how an execution ended and the timing milestones observed. All
// times are backend-clock nanoseconds; durations are derived by the caller from
// these absolute marks.
type Result struct {
	RequestID string
	Outcome   Outcome

	// StartNS is when execution began (prefill start).
	StartNS int64
	// FirstTokenNS is when the first output token was produced.
	FirstTokenNS int64
	// EndNS is when the execution ended (completion, cancel, or failure).
	EndNS int64

	// OutputTokens is the number of tokens actually generated.
	OutputTokens int

	// WorkerID identifies the simulated worker that ran the request, or -1.
	WorkerID int
}

// ExecutionBackend runs inference requests. Implementations must be safe for
// concurrent Submit and Cancel calls unless documented otherwise.
type ExecutionBackend interface {
	// Submit starts executing req and returns a handle. It does not block until
	// completion; the caller learns the outcome from the result returned by
	// Results (or, for the simulator, from the run summary).
	Submit(req *lifecycle.Request) (Handle, error)
	// Cancel requests cooperative cancellation of an in-flight request. It is
	// idempotent and returns nil if the request is unknown or already finished.
	Cancel(h Handle) error
	// Results returns a channel of results for all submitted requests. The
	// channel is closed when the backend has no more results to report.
	Results() <-chan Result
	// Close shuts the backend down, cancelling in-flight work.
	Close() error
}
