package scheduler

import "github.com/BibekP/infersched/internal/lifecycle"

// EDF schedules the earliest deadline first. Requests without a deadline are
// ordered after all deadline-bearing requests (FIFO among themselves).
//
// Infeasibility: a request is infeasible when its slack
// (deadline - now - estimatedRemainingWork) is negative. EDF does NOT claim it
// can meet deadlines under overload. To keep ordering total and deterministic,
// infeasible requests are still ordered among themselves by deadline (a missed
// deadline with a sooner cutoff should not be delayed further), but the policy
// surfaces feasibility through Admit so admission control can reject or defer.
type EDF struct {
	// Model estimates remaining work for feasibility checks.
	Model CostModel
	// NowFunc returns the current monotonic time in nanoseconds. Nil => 0.
	NowFunc func() int64
	// SlackMS is subtracted from the effective deadline as a safety margin; a
	// request needs to finish SlackMS before its stated deadline to count as
	// feasible. Default 0.
	SlackMS float64
}

// NewEDF returns an EDF policy.
func NewEDF(model CostModel) *EDF { return &EDF{Model: model} }

// Name implements Policy.
func (e *EDF) Name() string { return "edf" }

// Reset implements Policy.
func (e *EDF) Reset() {}

func (e *EDF) now() int64 {
	if e.NowFunc == nil {
		return 0
	}
	return e.NowFunc()
}

// Feasible reports whether r can finish before its deadline given the current
// time and the estimated work. Requests without a deadline are always feasible.
func (e *EDF) Feasible(r *lifecycle.Request) bool {
	if !r.HasDeadline() {
		return true
	}
	remainingMS := float64(r.Deadline-e.now())/1e6 - e.SlackMS
	return e.Model.Work(r) <= remainingMS
}

// Order implements Policy.
func (e *EDF) Order(reqs []*lifecycle.Request) []*lifecycle.Request {
	return orderBy(reqs, func(a, b *lifecycle.Request) bool {
		ad, bd := a.HasDeadline(), b.HasDeadline()
		switch {
		case ad && bd:
			return a.Deadline < b.Deadline
		case ad && !bd:
			return true // deadline-bearing before deadline-less
		case !ad && bd:
			return false
		default:
			return false // both deadline-less: fall through to arrival tie-break
		}
	})
}
