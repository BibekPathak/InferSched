// Package scheduler defines the common SchedulingPolicy interface and the
// baseline policies. Policies receive an immutable view of a request set and
// return an ordered execution list; they never mutate requests or resources.
//
// A policy is expected to be pure with respect to ordering: given the same set
// of requests and the same metadata it must return a deterministic ordering.
// Any mutable state a policy keeps (e.g. aging counters) must be reset via
// Reset before a new trace replay so runs are reproducible.
package scheduler

import "github.com/BibekP/infersched/internal/lifecycle"

// Policy orders requests for execution. Implementations must be safe to call
// concurrently only if documented; the coordinator currently calls Order from a
// single scheduling goroutine.
type Policy interface {
	// Name returns a stable, bounded identifier for the policy (metric-safe).
	Name() string
	// Order returns the requests in the order they should be considered for
	// execution. The returned slice is a new slice; the input is not mutated.
	Order(reqs []*lifecycle.Request) []*lifecycle.Request
	// Reset clears any per-run state. Called before each trace replay so that
	// repeated runs with the same seed are identical.
	Reset()
}
