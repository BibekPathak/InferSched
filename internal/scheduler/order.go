package scheduler

import (
	"sort"

	"github.com/BibekP/infersched/internal/lifecycle"
)

// lessArrival is the universal deterministic tie-break: earlier monotonic
// arrival wins, then lexicographically smaller request ID. Every policy must
// fall back to this so orderings are total and reproducible regardless of input
// slice order or map iteration.
func lessArrival(a, b *lifecycle.Request) bool {
	if a.Arrival != b.Arrival {
		return a.Arrival < b.Arrival
	}
	return a.ID < b.ID
}

// orderBy sorts a copy of reqs using less, always breaking ties with
// lessArrival so the result is fully deterministic. The input is not mutated.
func orderBy(reqs []*lifecycle.Request, less func(a, b *lifecycle.Request) bool) []*lifecycle.Request {
	out := make([]*lifecycle.Request, len(reqs))
	copy(out, reqs)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if less(a, b) {
			return true
		}
		if less(b, a) {
			return false
		}
		return lessArrival(a, b)
	})
	return out
}

// byArrival is the FIFO ordering, exposed for reuse as a tie-break.
func byArrival(a, b *lifecycle.Request) bool { return lessArrival(a, b) }
