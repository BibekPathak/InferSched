// Package admission implements InferSched's bounded admission controller.
//
// The controller decides whether an arriving request may enter the system. It
// enforces hard bounds (queue length, queued tokens, outstanding requests,
// per-tenant limits) and applies one of two overload strategies: immediate
// rejection, or deadline-aware admission that rejects requests unlikely to
// finish before their deadline. Every rejection carries a bounded Reason so
// metrics never use user-supplied text.
package admission

import (
	"github.com/BibekP/infersched/internal/lifecycle"
	"github.com/BibekP/infersched/internal/resources"
)

// Strategy selects the overload behaviour.
type Strategy string

const (
	// StrategyReject rejects any request that would exceed a hard bound.
	StrategyReject Strategy = "reject"
	// StrategyDeadlineAware additionally rejects requests whose deadline is
	// infeasible under a conservative estimate, even if capacity remains.
	StrategyDeadlineAware Strategy = "deadline_aware"
)

// Config configures the admission controller.
type Config struct {
	// Strategy selects the overload behaviour.
	Strategy Strategy

	// MaxQueueLength bounds the number of requests waiting to be admitted.
	MaxQueueLength int
	// MaxQueuedTokens bounds the total estimated KV tokens represented by
	// queued (not-yet-admitted) requests.
	MaxQueuedTokens int
	// MaxOutstanding bounds requests that are admitted or running (not queued).
	MaxOutstanding int
	// MaxPerTenant bounds outstanding requests per tenant (0 = unlimited).
	MaxPerTenant int

	// FeasibilitySlackMS is a safety margin applied to deadline-feasibility
	// checks under StrategyDeadlineAware. A request needs to finish this many
	// milliseconds before its deadline to count as feasible.
	FeasibilitySlackMS float64
}

// NowFunc supplies the current monotonic time in nanoseconds. Nil => 0.
type NowFunc func() int64

// Controller tracks queue occupancy and tenant usage, and admits or rejects
// requests. It is not safe for concurrent use by itself; the coordinator owns
// serialization. The resource Manager, by contrast, is independently
// concurrent-safe.
type Controller struct {
	cfg     Config
	res     *resources.Manager
	nowFunc NowFunc

	queuedLen    int
	queuedTokens int
	outstanding  int
	perTenant    map[string]int
}

// Stats is a point-in-time view of controller occupancy, for metrics.
type Stats struct {
	Strategy        Strategy `json:"strategy"`
	QueuedLength    int      `json:"queued_length"`
	QueuedTokens    int      `json:"queued_tokens"`
	Outstanding     int      `json:"outstanding"`
	MaxQueueLength  int      `json:"max_queue_length"`
	MaxQueuedTokens int      `json:"max_queued_tokens"`
	MaxOutstanding  int      `json:"max_outstanding"`
}

// Decision is the outcome of evaluating a request.
type Decision struct {
	Admit   bool
	Reason  lifecycle.Reason
	Message string
}

// New constructs an admission Controller over res.
func New(cfg Config, res *resources.Manager, now NowFunc) *Controller {
	if cfg.Strategy == "" {
		cfg.Strategy = StrategyReject
	}
	if now == nil {
		now = func() int64 { return 0 }
	}
	return &Controller{
		cfg:       cfg,
		res:       res,
		nowFunc:   now,
		perTenant: make(map[string]int),
	}
}

// Config returns the controller configuration.
func (c *Controller) Config() Config { return c.cfg }

func (c *Controller) now() int64 { return c.nowFunc() }

// EstimateKVTokens returns the request's estimated KV-cache token footprint:
// prompt tokens + expected output tokens, as carried in the cost estimate.
func EstimateKVTokens(r *lifecycle.Request) int {
	if r.Estimate.KVTokens > 0 {
		return r.Estimate.KVTokens
	}
	return r.Estimate.PrefillTokens + r.Estimate.ExpectedOutputTokens
}

// Evaluate decides whether r may be admitted, without mutating any state.
// Callers that proceed must call Commit; callers that do not must call nothing.
// Evaluate is the read-only half of a two-phase admit so feasibility and limits
// are checked consistently.
func (c *Controller) Evaluate(r *lifecycle.Request) Decision {
	tokens := EstimateKVTokens(r)

	// Hard bounds first: reject before consuming any resources.
	if c.cfg.MaxQueueLength > 0 && c.queuedLen >= c.cfg.MaxQueueLength {
		return Decision{Reason: lifecycle.ReasonOverloadQueue, Message: "queue length limit reached"}
	}
	if c.cfg.MaxQueuedTokens > 0 && c.queuedTokens+tokens > c.cfg.MaxQueuedTokens {
		return Decision{Reason: lifecycle.ReasonOverloadTokens, Message: "queued token limit reached"}
	}
	if c.cfg.MaxOutstanding > 0 && c.outstanding >= c.cfg.MaxOutstanding {
		return Decision{Reason: lifecycle.ReasonOverloadFlights, Message: "outstanding request limit reached"}
	}
	if c.cfg.MaxPerTenant > 0 && c.perTenant[r.TenantID] >= c.cfg.MaxPerTenant {
		return Decision{Reason: lifecycle.ReasonOverloadTenant, Message: "tenant limit reached"}
	}

	// Deadline-aware strategy: reject requests unlikely to finish in time.
	if c.cfg.Strategy == StrategyDeadlineAware && r.HasDeadline() {
		if !c.deadlineFeasible(r) {
			return Decision{Reason: lifecycle.ReasonDeadlineMiss, Message: "deadline infeasible under conservative estimate"}
		}
	}

	return Decision{Admit: true, Reason: lifecycle.ReasonNone}
}

// deadlineFeasible conservatively checks whether r can finish before its
// deadline given the current time and an inflated estimate of its work.
func (c *Controller) deadlineFeasible(r *lifecycle.Request) bool {
	remainingMS := float64(r.Deadline-c.now())/1e6 - c.cfg.FeasibilitySlackMS
	// Conservative: inflate estimated work by a fixed safety factor of 1.0
	// (the estimate already carries the model's prediction). Configurable via
	// the cost model at higher layers; the controller adds only SlackMS margin.
	workMS := r.Estimate.TotalCostMS()
	return workMS <= remainingMS
}

// Commit records that an evaluated request was admitted: it updates occupancy
// and tenant counters. It must be called exactly once per successful Evaluate.
func (c *Controller) Commit(r *lifecycle.Request) {
	c.queuedLen++
	c.queuedTokens += EstimateKVTokens(r)
	c.perTenant[r.TenantID]++
}

// Dequeue records that a queued request left the queue (it is now being
// admitted or was cancelled/rejected after enqueueing).
func (c *Controller) Dequeue(r *lifecycle.Request) {
	if c.queuedLen > 0 {
		c.queuedLen--
	}
	c.queuedTokens -= EstimateKVTokens(r)
	if c.queuedTokens < 0 {
		c.queuedTokens = 0
	}
}

// MarkOutstanding records that a queued request became outstanding (admitted
// or running). It moves one unit from queued to outstanding.
func (c *Controller) MarkOutstanding() { c.outstanding++ }

// MarkDone records that an outstanding request finished (completed, cancelled,
// failed, or expired) and releases its outstanding slot and tenant count.
func (c *Controller) MarkDone(r *lifecycle.Request) {
	if c.outstanding > 0 {
		c.outstanding--
	}
	if t := c.perTenant[r.TenantID]; t > 0 {
		if t == 1 {
			delete(c.perTenant, r.TenantID)
		} else {
			c.perTenant[r.TenantID] = t - 1
		}
	}
}

// Stats returns a point-in-time occupancy snapshot.
func (c *Controller) Stats() Stats {
	return Stats{
		Strategy:        c.cfg.Strategy,
		QueuedLength:    c.queuedLen,
		QueuedTokens:    c.queuedTokens,
		Outstanding:     c.outstanding,
		MaxQueueLength:  c.cfg.MaxQueueLength,
		MaxQueuedTokens: c.cfg.MaxQueuedTokens,
		MaxOutstanding:  c.cfg.MaxOutstanding,
	}
}

// Invariants reports whether counters are non-negative and within bounds.
func (c *Controller) Invariants() error {
	if c.queuedLen < 0 || c.queuedTokens < 0 || c.outstanding < 0 {
		return errNegativeCounter
	}
	if c.cfg.MaxQueueLength > 0 && c.queuedLen > c.cfg.MaxQueueLength {
		return errQueueOverBound
	}
	if c.cfg.MaxOutstanding > 0 && c.outstanding > c.cfg.MaxOutstanding {
		return errOutstandingOverBound
	}
	return nil
}

var (
	errNegativeCounter      = errorString("admission: negative counter")
	errQueueOverBound       = errorString("admission: queue over bound")
	errOutstandingOverBound = errorString("admission: outstanding over bound")
)

type errorString string

func (e errorString) Error() string { return string(e) }
