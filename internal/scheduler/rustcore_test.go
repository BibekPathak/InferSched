//go:build rustcore

package scheduler

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/BibekP/infersched/internal/lifecycle"
)

// TestRustGoEquivalence verifies that the Rust scheduler-core produces exactly
// the same ordering as the pure-Go implementation for every policy, across
// many randomized request sets. This is the core correctness argument for the
// cross-language boundary.
func TestRustGoEquivalence(t *testing.T) {
	policies := []string{"fifo", "priority", "sjf", "edf", "hybrid"}
	rng := rand.New(rand.NewSource(12345))

	for _, name := range policies {
		t.Run(name, func(t *testing.T) {
			for iter := 0; iter < 300; iter++ {
				reqs := randomRequests(rng, 1+rng.Intn(20))
				goPolicy := registryPolicy(name)
				rustPolicy, err := NewRustPolicy(name)
				if err != nil {
					t.Fatalf("NewRustPolicy(%q): %v", name, err)
				}
				// Use a fixed reference time so aging/deadline terms match.
				now := int64(0)
				setNow(goPolicy, now)
				rustPolicy.NowFunc = func() int64 { return now }

				want := ids(goPolicy.Order(reqs))
				got := ids(rustPolicy.Order(reqs))
				if !eq(want, got) {
					t.Fatalf("policy %s iter %d: Go=%v Rust=%v\nreqs=%s",
						name, iter, want, got, dump(reqs))
				}
			}
		})
	}
}

// TestRustPolicyNameAndReset exercises the adapter's trivial methods.
func TestRustPolicyNameAndReset(t *testing.T) {
	p, err := NewRustPolicy("edf")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name() != "edf" {
		t.Fatalf("name=%s", p.Name())
	}
	p.Reset() // must not panic
	if _, err := NewRustPolicy("bogus"); err == nil {
		t.Fatal("expected error for unsupported policy")
	}
}

// TestRustCoreVersion checks the linked core reports its version.
func TestRustCoreVersion(t *testing.T) {
	if v := RustCoreVersion(); v == "" || v == "unavailable (built without -tags rustcore)" {
		t.Fatalf("unexpected core version %q", v)
	}
	if !RustCoreAvailable() {
		t.Fatal("RustCoreAvailable should be true in rustcore builds")
	}
}

// registryPolicy returns a fresh, unconfigured Go policy by name.
func registryPolicy(name string) Policy {
	switch name {
	case "fifo":
		return NewFIFO()
	case "priority":
		return NewPriority(Aging{})
	case "sjf":
		return NewSJF(DefaultCostModel())
	case "edf":
		return NewEDF(DefaultCostModel())
	case "hybrid":
		return NewHybrid(DefaultHybridWeights(), DefaultCostModel())
	}
	panic("unknown policy " + name)
}

// setNow wires a policy's reference-time provider so equivalence is tested at a
// deterministic instant. FIFO keeps no clock.
func setNow(p Policy, now int64) {
	f := func() int64 { return now }
	switch pp := p.(type) {
	case *Priority:
		pp.Aging.NowFunc = f
	case *SJF:
		pp.Aging.NowFunc = f
	case *EDF:
		pp.NowFunc = f
	case *Hybrid:
		pp.NowFunc = f
	}
}

func randomRequests(rng *rand.Rand, n int) []*lifecycle.Request {
	out := make([]*lifecycle.Request, n)
	for i := 0; i < n; i++ {
		priority := []lifecycle.Priority{lifecycle.PriorityLow, lifecycle.PriorityNormal, lifecycle.PriorityHigh, lifecycle.PriorityCritical}[rng.Intn(4)]
		prefill := float64(rng.Intn(20))
		decode := 0.5
		outTokens := rng.Intn(500)
		var deadline int64
		if rng.Intn(2) == 0 {
			deadline = int64(1+rng.Intn(5000)) * 1e6
		}
		arrival := int64(rng.Intn(1000)) * 1e6
		out[i] = lifecycle.NewRequest(
			// IDs sort the same way as Rust's positional index tie-break.
			rngID(i), "t0", lifecycle.ClassInteractive, priority,
			time.Time{}, arrival, deadline,
			lifecycle.CostEstimate{
				PrefillTokens:        rng.Intn(2048),
				ExpectedOutputTokens: outTokens,
				PrefillCostMS:        prefill,
				DecodeCostPerTokenMS: decode,
				KVTokens:             rng.Intn(4096),
			},
			context.Background(),
		)
	}
	return out
}

func rngID(i int) string {
	// Fixed-width, zero-padded so lexicographic order equals numeric order.
	const digits = "0123456789"
	return "req-" + string([]byte{
		digits[(i/100000)%10], digits[(i/10000)%10], digits[(i/1000)%10],
		digits[(i/100)%10], digits[(i/10)%10], digits[i%10],
	})
}

func dump(reqs []*lifecycle.Request) string {
	s := ""
	for _, r := range reqs {
		s += r.ID + " "
	}
	return s
}
