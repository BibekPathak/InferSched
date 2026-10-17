package coordinator

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/BibekP/infersched/internal/lifecycle"
)

// Submission is a client request to run inference. All token counts are
// estimates except PromptTokens, which is known exactly at ingress.
type Submission struct {
	// ID is optional; the coordinator assigns one when empty.
	ID string
	// TenantID identifies the tenant for per-tenant limits; defaults when empty.
	TenantID string
	// Class is one of interactive, batch, long_context.
	Class string
	// Priority is a small ordered priority (higher = more important).
	Priority int
	// PromptTokens is the exact prompt length.
	PromptTokens int
	// ExpectedOutputTokens is the predicted output length (an estimate).
	ExpectedOutputTokens int
	// DeadlineMS is an optional deadline relative to submission (0 = none).
	DeadlineMS float64
	// KVTokens optionally overrides the KV footprint estimate
	// (default prompt + expected output).
	KVTokens int
	// PrefillCostMS and DecodeCostPerTokenMS optionally override the cost model.
	PrefillCostMS        float64
	DecodeCostPerTokenMS float64
}

// RequestStatus is a read-only view of a request's lifecycle.
type RequestStatus struct {
	ID         string `json:"id"`
	TenantID   string `json:"tenant_id"`
	Class      string `json:"class"`
	Priority   int    `json:"priority"`
	State      string `json:"state"`
	Outcome    string `json:"outcome,omitempty"`
	Reason     string `json:"reason,omitempty"`
	ArrivalNS  int64  `json:"arrival_ns"`
	DeadlineNS int64  `json:"deadline_ns"`
	AdmitNS    int64  `json:"admit_ns"`
	StartNS    int64  `json:"start_ns"`
	EndNS      int64  `json:"end_ns"`
	KVTokens   int    `json:"kv_tokens"`
}

// SchedulerStatus reports policy and queue state.
type SchedulerStatus struct {
	Policy    string `json:"policy"`
	Pending   int    `json:"pending"`
	Submitted int64  `json:"submitted"`
	Admitted  int64  `json:"admitted"`
	Rejected  int64  `json:"rejected"`
	Decisions int64  `json:"decisions"`
}

// ResourceStatus reports current resource occupancy.
type ResourceStatus struct {
	SlotsTotal      int `json:"slots_total"`
	SlotsUsed       int `json:"slots_used"`
	ActiveSequences int `json:"active_sequences"`
	KVBlocksTotal   int `json:"kv_blocks_total"`
	KVBlocksUsed    int `json:"kv_blocks_used"`
	Reservations    int `json:"reservations"`
}

// DefaultCostModel constants used when a submission does not override costs.
const (
	defaultPrefillPerTokenMS = 0.02
	defaultDecodePerTokenMS  = 0.5
)

var submissionSeq atomic.Uint64

// buildRequest validates a Submission and materializes a lifecycle.Request in
// the QUEUED state with a cancellable context.
func (c *Coordinator) buildRequest(s Submission) (*lifecycle.Request, error) {
	if s.PromptTokens <= 0 {
		return nil, fmt.Errorf("coordinator: prompt_tokens must be > 0")
	}
	if s.ExpectedOutputTokens < 0 {
		return nil, fmt.Errorf("coordinator: expected_output_tokens must be >= 0")
	}
	if s.DeadlineMS < 0 {
		return nil, fmt.Errorf("coordinator: deadline_ms must be >= 0")
	}
	class := lifecycle.WorkloadClass(s.Class)
	switch class {
	case lifecycle.ClassInteractive, lifecycle.ClassBatch, lifecycle.ClassLongContext:
	default:
		class = lifecycle.ClassUnknown
	}
	tenant := s.TenantID
	if tenant == "" {
		tenant = c.cfg.DefaultTenant
	}
	if tenant == "" {
		tenant = "default"
	}

	id := s.ID
	if id == "" {
		n := submissionSeq.Add(1)
		id = fmt.Sprintf("req-%d-%d", c.baseTime.UnixNano(), n)
	}

	arrival := c.clock()
	var deadline int64
	if s.DeadlineMS > 0 {
		deadline = arrival + int64(s.DeadlineMS*float64(time.Millisecond))
	}

	prefillCost := s.PrefillCostMS
	if prefillCost <= 0 {
		prefillCost = float64(s.PromptTokens) * defaultPrefillPerTokenMS
	}
	decodeCost := s.DecodeCostPerTokenMS
	if decodeCost <= 0 {
		decodeCost = defaultDecodePerTokenMS
	}
	kv := s.KVTokens
	if kv <= 0 {
		kv = s.PromptTokens + s.ExpectedOutputTokens
	}

	ctx, cancel := context.WithCancel(context.Background())
	_ = cancel // stored on the record via Ctx cancellation below

	est := lifecycle.CostEstimate{
		PrefillTokens:        s.PromptTokens,
		ExpectedOutputTokens: s.ExpectedOutputTokens,
		PrefillCostMS:        prefillCost,
		DecodeCostPerTokenMS: decodeCost,
		KVTokens:             kv,
	}
	return lifecycle.NewRequest(id, tenant, class, lifecycle.Priority(s.Priority),
		time.Now(), arrival, deadline, est, ctx), nil
}
