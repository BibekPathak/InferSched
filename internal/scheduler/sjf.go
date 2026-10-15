package scheduler

import "github.com/BibekP/infersched/internal/lifecycle"

// CostModel converts a request's estimated cost into a single comparable
// "work" number. It is deliberately injectable because the estimate is a
// prediction, not a measurement: the benchmark suite calibrates the model to
// observed backend behaviour and studies sensitivity to prediction error.
type CostModel struct {
	// PrefillWeight scales the estimated prefill cost. Default 1.0.
	PrefillWeight float64
	// DecodeWeight scales the estimated decode cost. Default 1.0.
	DecodeWeight float64
	// OutputErrorFactor inflates expected output length to model prediction
	// error (e.g. 1.1 assumes outputs 10% longer than predicted). Default 1.0.
	OutputErrorFactor float64
}

// DefaultCostModel is the identity model over the request's own estimate.
func DefaultCostModel() CostModel {
	return CostModel{PrefillWeight: 1, DecodeWeight: 1, OutputErrorFactor: 1}
}

// Work returns the estimated total work in milliseconds for r under m.
func (m CostModel) Work(r *lifecycle.Request) float64 {
	pw := m.PrefillWeight
	if pw == 0 {
		pw = 1
	}
	dw := m.DecodeWeight
	if dw == 0 {
		dw = 1
	}
	oef := m.OutputErrorFactor
	if oef == 0 {
		oef = 1
	}
	prefill := r.Estimate.PrefillCostMS * pw
	decode := float64(r.Estimate.ExpectedOutputTokens) * r.Estimate.DecodeCostPerTokenMS * dw * oef
	return prefill + decode
}

// SJF schedules the shortest estimated work first. "Shortest" means the smallest
// modeled cost, never a raw token count treated as an execution time. Ties are
// broken by arrival, then ID.
//
// Starvation: SJF can starve long requests indefinitely when short work keeps
// arriving (the classic convoy problem). Enable aging to bound this; the
// benchmark suite measures long-request tail latency explicitly.
type SJF struct {
	Model CostModel
	Aging Aging
}

// NewSJF returns an SJF policy using model. pass DefaultCostModel() for the
// identity model.
func NewSJF(model CostModel) *SJF { return &SJF{Model: model} }

// WithAging attaches an aging configuration and returns the policy.
func (s *SJF) WithAging(a Aging) *SJF { s.Aging = a; return s }

// Name implements Policy.
func (s *SJF) Name() string { return "sjf" }

// Reset implements Policy.
func (s *SJF) Reset() {}

// Order implements Policy.
func (s *SJF) Order(reqs []*lifecycle.Request) []*lifecycle.Request {
	return orderBy(reqs, func(a, b *lifecycle.Request) bool {
		wa := s.Model.Work(a) - s.Aging.boostFor(a)
		wb := s.Model.Work(b) - s.Aging.boostFor(b)
		return wa < wb
	})
}
