// Package scheduler defines the common SchedulingPolicy interface and the
// baseline policies. Policies receive an immutable view of a request set and
// return an ordered execution list; they never mutate requests or resources.
//
// A policy is expected to be pure with respect to ordering: given the same set
// of requests and the same metadata it must return a deterministic ordering.
// Any mutable state a policy keeps (e.g. aging counters) must be reset via
// Reset before a new trace replay so runs are reproducible.
package scheduler

import (
	"fmt"

	"github.com/BibekP/infersched/internal/lifecycle"
)

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

// Registry returns the built-in policies keyed by their stable name, each with
// default configuration. Callers that need non-default weights construct the
// policy directly (e.g. NewHybrid, NewSJF). The reference-time providers
// (NowFunc) are left nil; the coordinator/simulator wires a monotonic clock so
// that aging and deadline terms are evaluated at a deterministic instant.
func Registry() map[string]Policy {
	return map[string]Policy{
		"fifo":     NewFIFO(),
		"priority": NewPriority(Aging{}),
		"sjf":      NewSJF(DefaultCostModel()),
		"edf":      NewEDF(DefaultCostModel()),
		"hybrid":   NewHybrid(DefaultHybridWeights(), DefaultCostModel()),
	}
}

// Backend selects which implementation provides a policy's ordering.
type Backend string

const (
	// BackendGo uses the pure-Go implementation.
	BackendGo Backend = "go"
	// BackendRust uses the Rust scheduler-core over the C ABI. It is only
	// available in binaries built with the `rustcore` build tag.
	BackendRust Backend = "rust"
)

// ByBackend returns the policy named name implemented by the given backend.
// When backend is BackendRust and the Rust core is not linked, it returns an
// error so the caller can fail fast rather than silently using a different
// implementation.
func ByBackend(name string, backend Backend) (Policy, error) {
	switch backend {
	case "", BackendGo:
		p, ok := Registry()[name]
		if !ok {
			return nil, fmt.Errorf("scheduler: unknown policy %q", name)
		}
		return p, nil
	case BackendRust:
		return NewRustPolicy(name)
	default:
		return nil, fmt.Errorf("scheduler: unknown backend %q", backend)
	}
}
