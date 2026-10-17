//go:build rustcore

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/BibekP/infersched/internal/lifecycle"
)

// BenchmarkOrderGo benchmarks the pure-Go hybrid ordering path.
func BenchmarkOrderGo(b *testing.B) {
	reqs := benchRequests(256)
	p := NewHybrid(DefaultHybridWeights(), DefaultCostModel())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Order(reqs)
	}
}

// BenchmarkOrderRust benchmarks the Rust-core hybrid ordering path including
// the cgo marshaling cost.
func BenchmarkOrderRust(b *testing.B) {
	reqs := benchRequests(256)
	p, err := NewRustPolicy("hybrid")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Order(reqs)
	}
}

func benchRequests(n int) []*lifecycle.Request {
	out := make([]*lifecycle.Request, n)
	for i := 0; i < n; i++ {
		out[i] = lifecycle.NewRequest(
			rngID(i), "t0", lifecycle.ClassInteractive, lifecycle.Priority(i%200),
			time.Time{}, int64(i)*1e6, int64(i%50)*1e6,
			lifecycle.CostEstimate{
				PrefillTokens:        128 + i,
				ExpectedOutputTokens: 32 + (i % 256),
				PrefillCostMS:        float64(i%20) * 0.5,
				DecodeCostPerTokenMS: 0.5,
				KVTokens:             256 + i,
			},
			context.Background(),
		)
	}
	return out
}
