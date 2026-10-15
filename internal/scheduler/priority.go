package scheduler

import "github.com/BibekP/infersched/internal/lifecycle"

// Priority schedules by descending priority value. Higher lifecycle.Priority
// values are more important. Ties are broken by arrival, then request ID.
//
// Starvation: pure priority (or aging with AgingMS <= 0) can starve low-priority
// requests indefinitely under sustained load. Enable aging to bound this; the
// benchmark suite reports starvation incidents explicitly.
type Priority struct {
	Aging Aging
}

// NewPriority returns a priority policy. aging enables starvation protection;
// pass Aging{} for pure priority.
func NewPriority(aging Aging) *Priority { return &Priority{Aging: aging} }

// Name implements Policy.
func (p *Priority) Name() string { return "priority" }

// Reset implements Policy.
func (p *Priority) Reset() {}

// Order implements Policy.
func (p *Priority) Order(reqs []*lifecycle.Request) []*lifecycle.Request {
	if p.Aging.Config.Enabled() {
		return orderBy(reqs, func(a, b *lifecycle.Request) bool {
			ea := float64(a.Priority) + p.Aging.boostFor(a)
			eb := float64(b.Priority) + p.Aging.boostFor(b)
			return ea > eb
		})
	}
	return orderBy(reqs, func(a, b *lifecycle.Request) bool {
		return a.Priority > b.Priority
	})
}
