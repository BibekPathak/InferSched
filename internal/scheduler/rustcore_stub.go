//go:build !rustcore

package scheduler

import (
	"fmt"

	"github.com/BibekP/infersched/internal/lifecycle"
)

// RustPolicy is unavailable unless the binary is built with the `rustcore`
// build tag and the scheduler-core static library. This stub keeps the package
// buildable without a Rust toolchain; see rustcore.go for the real adapter.
type RustPolicy struct {
	name    string
	NowFunc func() int64
}

// NewRustPolicy returns an error in builds without the Rust core.
func NewRustPolicy(name string) (*RustPolicy, error) {
	return nil, fmt.Errorf("scheduler: rust core not built; rebuild with -tags rustcore (see `make rust-core`)")
}

// Name implements Policy.
func (p *RustPolicy) Name() string { return p.name }

// Reset implements Policy.
func (p *RustPolicy) Reset() {}

// Order implements Policy. It never runs in stub builds because NewRustPolicy
// returns an error, but it keeps *RustPolicy assignable to Policy.
func (p *RustPolicy) Order(reqs []*lifecycle.Request) []*lifecycle.Request {
	return orderBy(reqs, byArrival)
}

// RustCoreVersion reports that the Rust core is not linked.
func RustCoreVersion() string { return "unavailable (built without -tags rustcore)" }

// RustCoreAvailable reports whether the Rust core is linked.
func RustCoreAvailable() bool { return false }
