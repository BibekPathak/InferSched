package simulator

import "github.com/BibekP/infersched/internal/lifecycle"

// msToNS converts milliseconds to virtual nanoseconds.
func msToNS(ms float64) int64 { return int64(ms * 1e6) }

// max returns the larger of a and b.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// kvTokensOf returns the estimated KV token footprint of r: the explicit
// KVTokens estimate if present, else prompt + expected output tokens.
func kvTokensOf(r *lifecycle.Request) int {
	if r.Estimate.KVTokens > 0 {
		return r.Estimate.KVTokens
	}
	return r.Estimate.PrefillTokens + r.Estimate.ExpectedOutputTokens
}

// result returns a pointer to the result slot for request index idx.
func (s *Simulator) result(idx int) *Result { return &s.results[idx] }

// indexOf returns the request slice index for r, or -1.
func (s *Simulator) indexOf(r *lifecycle.Request) int {
	for i, req := range s.requests {
		if req == r {
			return i
		}
	}
	return -1
}

// resultIndex maps a result pointer back to a request index.
func (s *Simulator) resultIndex(res *Result) int {
	for i := range s.results {
		if &s.results[i] == res {
			return i
		}
	}
	return -1
}

// pendingIndices returns the indices of requests admitted but not yet started.
func (s *Simulator) pendingIndices() []int {
	var out []int
	for i, r := range s.requests {
		if r.State() != lifecycle.StateQueued {
			continue
		}
		res := &s.results[i]
		if res.Admitted && res.StartNS == 0 && res.Outcome == "" {
			out = append(out, i)
		}
	}
	return out
}

// pendingRequests returns the request pointers for the given indices.
func (s *Simulator) pendingRequests(idxs []int) []*lifecycle.Request {
	out := make([]*lifecycle.Request, len(idxs))
	for i, idx := range idxs {
		out[i] = s.requests[idx]
	}
	return out
}

// workerOf returns the worker currently running idx, or nil.
func (s *Simulator) workerOf(idx int) *worker {
	for _, w := range s.workers {
		for _, rs := range w.active {
			if rs.reqIdx == idx {
				return w
			}
		}
	}
	return nil
}

// removeActive removes idx's runState from worker w.
func (s *Simulator) removeActive(w *worker, idx int) {
	out := w.active[:0]
	for _, rs := range w.active {
		if rs.reqIdx != idx {
			out = append(out, rs)
		}
	}
	w.active = out
}

// ScheduleCancel schedules a cancellation for request id at virtual time atNS.
// It is used to model client cancellation (queued or in-flight) and to test
// cancellation paths. Unknown IDs are ignored at delivery time.
func (s *Simulator) ScheduleCancel(id string, atNS int64) {
	idx := -1
	for i, r := range s.requests {
		if r.ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	s.push(EventCancel, atNS, idx, -1)
}

// observePeaks updates the run's peak-usage counters from current resource
// state. It is called after each event so peaks reflect all transient states.
func (s *Simulator) observePeaks() {
	snap := s.res.Snapshot()
	if snap.ActiveSequences > s.peakActive {
		s.peakActive = snap.ActiveSequences
	}
	if snap.KVBlocksUsed > s.peakKVBlocks {
		s.peakKVBlocks = snap.KVBlocksUsed
	}
	if snap.SlotsUsed > s.peakSlots {
		s.peakSlots = snap.SlotsUsed
	}
}
