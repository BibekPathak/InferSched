package scheduler

import "github.com/BibekP/infersched/internal/lifecycle"

// HybridWeights configures the deadline-aware hybrid scoring rule. Every term is
// normalised to [0,1] and combined additively; higher score runs first. No term
// is a magic constant: each is documented and individually configurable.
type HybridWeights struct {
	// DeadlineUrgency weights how close a request is to its deadline.
	DeadlineUrgency float64
	// Work weights preference for shorter estimated work.
	Work float64
	// QueueDelay weights how long a request has already waited.
	QueueDelay float64
	// Priority weights the request's static priority.
	Priority float64
	// Aging weights starvation protection.
	Aging float64

	// HorizonMS is the time window over which deadline urgency ramps from 0 to
	// 1. A request whose slack exceeds HorizonMS contributes 0 urgency.
	HorizonMS float64
	// WaitNormMS is the wait duration at which QueueDelay saturates to 1.
	WaitNormMS float64
	// WorkNormMS is the estimated work at which the Work term saturates to 1.
	WorkNormMS float64
	// MaxPriority is the priority value normalising the Priority term to 1.
	MaxPriority float64
	// AgingCapMS is the wait at which the Aging term saturates to 1.
	AgingCapMS float64
}

// DefaultHybridWeights returns a starting configuration. These values are NOT
// claimed optimal; the benchmark suite sweeps them and reports sensitivity.
func DefaultHybridWeights() HybridWeights {
	return HybridWeights{
		DeadlineUrgency: 1.0,
		Work:            0.5,
		QueueDelay:      0.5,
		Priority:        0.25,
		Aging:           0.25,
		HorizonMS:       2000,
		WaitNormMS:      1000,
		WorkNormMS:      5000,
		MaxPriority:     float64(lifecycle.PriorityCritical),
		AgingCapMS:      5000,
	}
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

// Hybrid is the deadline-aware hybrid policy. It scores each request and runs
// the highest-scoring first. The scoring rule is explicit:
//
//	score = w_dl*deadlineUrgency + w_work*work + w_wait*queueDelay
//	      + w_pri*priority + w_age*aging
type Hybrid struct {
	W       HybridWeights
	Model   CostModel
	NowFunc func() int64
}

// NewHybrid returns a hybrid policy with weights w and cost model m.
func NewHybrid(w HybridWeights, m CostModel) *Hybrid {
	return &Hybrid{W: w, Model: m}
}

// Name implements Policy.
func (h *Hybrid) Name() string { return "hybrid" }

// Reset implements Policy.
func (h *Hybrid) Reset() {}

func (h *Hybrid) now() int64 {
	if h.NowFunc == nil {
		return 0
	}
	return h.NowFunc()
}

func (h *Hybrid) deadlineUrgency(r *lifecycle.Request, nowNS int64) float64 {
	if !r.HasDeadline() || h.W.HorizonMS <= 0 {
		return 0
	}
	slackMS := float64(r.Deadline-nowNS) / 1e6
	return clamp01(1 - slackMS/h.W.HorizonMS)
}

func (h *Hybrid) workFactor(r *lifecycle.Request) float64 {
	if h.W.WorkNormMS <= 0 {
		return 0
	}
	return clamp01(1 - h.Model.Work(r)/h.W.WorkNormMS)
}

func (h *Hybrid) queueDelay(r *lifecycle.Request, nowNS int64) float64 {
	if h.W.WaitNormMS <= 0 {
		return 0
	}
	waitMS := float64(nowNS-r.Arrival) / 1e6
	return clamp01(waitMS / h.W.WaitNormMS)
}

func (h *Hybrid) priority(r *lifecycle.Request) float64 {
	if h.W.MaxPriority <= 0 {
		return 0
	}
	return clamp01(float64(r.Priority) / h.W.MaxPriority)
}

func (h *Hybrid) aging(r *lifecycle.Request, nowNS int64) float64 {
	if h.W.AgingCapMS <= 0 {
		return 0
	}
	waitMS := float64(nowNS-r.Arrival) / 1e6
	return clamp01(waitMS / h.W.AgingCapMS)
}

// Score returns r's hybrid score at nowNS. Higher is scheduled earlier.
func (h *Hybrid) Score(r *lifecycle.Request, nowNS int64) float64 {
	return h.W.DeadlineUrgency*h.deadlineUrgency(r, nowNS) +
		h.W.Work*h.workFactor(r) +
		h.W.QueueDelay*h.queueDelay(r, nowNS) +
		h.W.Priority*h.priority(r) +
		h.W.Aging*h.aging(r, nowNS)
}

// Order implements Policy.
func (h *Hybrid) Order(reqs []*lifecycle.Request) []*lifecycle.Request {
	now := h.now()
	return orderBy(reqs, func(a, b *lifecycle.Request) bool {
		return h.Score(a, now) > h.Score(b, now)
	})
}
