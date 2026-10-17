//go:build rustcore

// Package scheduler: Rust backend adapter.
//
// This file is compiled only with the `rustcore` build tag, which is enabled
// when the Rust scheduler-core static library has been built (see the Makefile
// `rust-core` target). It implements the same Policy interface as the pure-Go
// policies, delegating ordering to the Rust core over a narrow C ABI.
//
// The boundary is deliberately small: it marshals request metadata into a flat
// C array, calls infersched_order once, and maps the returned position
// permutation back to requests. No allocation crosses the boundary.
package scheduler

/*
#cgo CFLAGS: -I${SRCDIR}/../../crates/scheduler-core/include
#cgo LDFLAGS: -L${SRCDIR}/../../crates/scheduler-core/target/release -lscheduler_core -lm -ldl -lpthread
#include <stdint.h>
#include <stdlib.h>
#include "scheduler_core.h"
*/
import "C"

import (
	"fmt"

	"github.com/BibekP/infersched/internal/lifecycle"
)

// rustPolicyKind maps a policy name to the Rust numeric kind.
func rustPolicyKind(name string) (uint32, error) {
	switch name {
	case "fifo":
		return uint32(C.INFERSCHED_POLICY_FIFO), nil
	case "priority":
		return uint32(C.INFERSCHED_POLICY_PRIORITY), nil
	case "sjf":
		return uint32(C.INFERSCHED_POLICY_SJF), nil
	case "edf":
		return uint32(C.INFERSCHED_POLICY_EDF), nil
	case "hybrid":
		return uint32(C.INFERSCHED_POLICY_HYBRID), nil
	default:
		return 0, fmt.Errorf("scheduler: rust core does not implement policy %q", name)
	}
}

// RustPolicy implements Policy by delegating ordering to the Rust scheduler
// core. The Go side owns the request lifetime; the core only sees a flat copy
// of metadata for the duration of one Order call.
type RustPolicy struct {
	name    string
	kind    uint32
	NowFunc func() int64
}

// NewRustPolicy constructs a Rust-backed policy for the named policy.
func NewRustPolicy(name string) (*RustPolicy, error) {
	kind, err := rustPolicyKind(name)
	if err != nil {
		return nil, err
	}
	return &RustPolicy{name: name, kind: kind, NowFunc: func() int64 { return 0 }}, nil
}

// Name implements Policy.
func (p *RustPolicy) Name() string { return p.name }

// Reset implements Policy. The Rust core is stateless for ordering.
func (p *RustPolicy) Reset() {}

// Order implements Policy by marshaling to the C ABI and mapping back.
func (p *RustPolicy) Order(reqs []*lifecycle.Request) []*lifecycle.Request {
	out := make([]*lifecycle.Request, len(reqs))
	if len(reqs) == 0 {
		return out
	}

	views := make([]C.infersched_request_view, len(reqs))
	for i, r := range reqs {
		views[i].index = C.uint32_t(i)
		views[i].arrival_ns = C.int64_t(r.Arrival)
		views[i].deadline_ns = C.int64_t(r.Deadline)
		views[i].priority = C.int32_t(r.Priority)
		views[i].prefill_cost_ms = C.double(r.Estimate.PrefillCostMS)
		views[i].decode_cost_per_token_ms = C.double(r.Estimate.DecodeCostPerTokenMS)
		views[i].expected_output_tokens = C.int64_t(r.Estimate.ExpectedOutputTokens)
		views[i].kv_tokens = C.int64_t(r.Estimate.KVTokens)
	}

	positions := make([]C.uint32_t, len(reqs))
	weights := C.infersched_default_hybrid_weights()
	now := C.int64_t(0)
	if p.NowFunc != nil {
		now = C.int64_t(p.NowFunc())
	}

	rc := C.infersched_order(
		C.uint32_t(p.kind),
		&weights,
		now,
		&views[0],
		C.size_t(len(reqs)),
		&positions[0],
	)
	if rc < 0 {
		// The C ABI should never fail for valid input; fall back to arrival
		// order rather than corrupting the scheduling decision.
		return orderBy(reqs, byArrival)
	}

	for i := 0; i < len(reqs); i++ {
		out[i] = reqs[int(positions[i])]
	}
	return out
}

// RustCoreVersion returns the linked scheduler-core version string.
func RustCoreVersion() string {
	return C.GoString(C.infersched_core_version())
}

// RustCoreAvailable reports whether the Rust core is linked.
func RustCoreAvailable() bool { return true }
