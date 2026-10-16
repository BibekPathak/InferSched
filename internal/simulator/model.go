// Package simulator implements InferSched's deterministic discrete-event
// simulator of continuous-batching LLM inference.
//
// It models request arrivals, queueing, prefill, decode iterations, batch
// formation, per-step execution time, KV-cache block allocation and release,
// cancellation, deadlines, worker availability, and worker failure.
//
// The simulator is a MODEL. Its numbers are not measurements of real hardware.
// All assumptions are stated in docs/benchmark-methodology.md. Given the same
// seed, configuration, and arrival trace, a run is bit-for-bit reproducible.
package simulator

import "math"

// PerfModel describes the cost of prefill and decode work. It captures the
// essential difference between prefill (compute-bound, scales with prompt
// tokens and is largely independent of batch size up to a point) and decode
// (memory-bandwidth-bound, one token per sequence per step, cost grows
// sub-linearly with batch size).
//
// Units: milliseconds.
type PerfModel struct {
	// PrefillBaseMS is a fixed per-request prefill overhead (kernel launch,
	// scheduling), applied once per request.
	PrefillBaseMS float64
	// PrefillPerTokenMS is the marginal prefill cost per prompt token when the
	// prefill batch is a single request. Batched prefill cost is divided by the
	// batching speedup, capped by PrefillMaxSpeedup.
	PrefillPerTokenMS float64
	// PrefillMaxSpeedup caps how much concurrent prefills amortize the per-token
	// cost. 1.0 means prefill does not batch (pessimistic).
	PrefillMaxSpeedup float64

	// DecodeBaseMS is the fixed per-step decode overhead (one step per active
	// sequence per iteration).
	DecodeBaseMS float64
	// DecodePerTokenMS is the marginal decode cost per sequence per step when
	// the batch is a single sequence.
	DecodePerTokenMS float64
	// DecodeMaxSpeedup caps how much the per-token decode cost is amortized as
	// batch size grows (memory-bandwidth sharing). 1.0 disables amortization.
	DecodeMaxSpeedup float64
}

// DefaultPerfModel returns a calibrated-but-arbitrary starting model. These
// constants reflect the *shape* of real inference (prefill per-token cost and
// decode per-step cost) and are meant to be calibrated to measured backend
// behavior; they are not measurements themselves.
func DefaultPerfModel() PerfModel {
	return PerfModel{
		PrefillBaseMS:     0.5,
		PrefillPerTokenMS: 0.02, // 2048-token prompt ~ 41.5ms at batch 1
		PrefillMaxSpeedup: 4.0,
		DecodeBaseMS:      1.0,
		DecodePerTokenMS:  0.5, // 1.0 + 0.5 = 1.5ms/step at batch 1
		DecodeMaxSpeedup:  8.0,
	}
}

// Validate reports whether the model's parameters are physically sensible.
func (m PerfModel) Validate() error {
	if m.PrefillPerTokenMS < 0 || m.DecodePerTokenMS < 0 || m.PrefillBaseMS < 0 || m.DecodeBaseMS < 0 {
		return errString("simulator: perf model costs must be non-negative")
	}
	if m.PrefillMaxSpeedup < 1 || m.DecodeMaxSpeedup < 1 {
		return errString("simulator: speedup factors must be >= 1")
	}
	return nil
}

// PrefillCostMS returns the wall time to prefill tokens shared across batch
// requests. Speedup saturates at PrefillMaxSpeedup as the batch grows.
func (m PerfModel) PrefillCostMS(tokens, batch int) float64 {
	if batch < 1 {
		batch = 1
	}
	speedup := math.Min(float64(batch), m.PrefillMaxSpeedup)
	perToken := m.PrefillPerTokenMS * float64(tokens) / speedup
	return m.PrefillBaseMS + perToken
}

// DecodeStepCostMS returns the wall time of one decode iteration for a batch of
// the given size, where each active sequence emits one token. Cost grows with
// batch size but sub-linearly up to DecodeMaxSpeedup.
func (m PerfModel) DecodeStepCostMS(batch int) float64 {
	if batch < 1 {
		batch = 1
	}
	speedup := math.Min(float64(batch), m.DecodeMaxSpeedup)
	perToken := m.DecodePerTokenMS * float64(batch) / speedup
	return m.DecodeBaseMS + perToken
}

type errString string

func (e errString) Error() string { return string(e) }
