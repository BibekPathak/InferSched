// Package coordinator wires InferSched's components into a running runtime:
// request ingress and validation, bounded admission, the scheduling policy, the
// resource manager, and an execution backend. It owns request lifecycle and
// serializes state transitions on a single scheduling goroutine so the
// admission controller's counters need no internal locking.
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/BibekP/infersched/internal/admission"
	"github.com/BibekP/infersched/internal/backend"
	"github.com/BibekP/infersched/internal/lifecycle"
	"github.com/BibekP/infersched/internal/observability"
	"github.com/BibekP/infersched/internal/resources"
	"github.com/BibekP/infersched/internal/scheduler"
)

// ErrClosed is returned by Submit/Cancel after the coordinator has shut down.
var ErrClosed = errors.New("coordinator: closed")

// Config configures the coordinator.
type Config struct {
	// Policy is the scheduling policy name (see scheduler.Registry).
	Policy string

	// Resource capacity.
	Resources resources.Config

	// Admission bounds and strategy.
	Admission admission.Config

	// DefaultTenant is applied when a submission omits a tenant.
	DefaultTenant string

	// MaxPending bounds the internal admitted-queue slice. Requests beyond this
	// are rejected with a queue-length reason. 0 = bounded by admission only.
	MaxPending int

	// Backend is the execution backend. Required.
	Backend backend.ExecutionBackend

	// Logger is optional; a discard-ish default is used when nil.
	Logger *slog.Logger

	// Metrics is optional; one is created when nil.
	Metrics *observability.Metrics
}

// record tracks a request through the coordinator.
type record struct {
	req       *lifecycle.Request
	ctx       context.Context
	cancel    context.CancelFunc
	arrivalNS int64
	submitNS  int64
	admitNS   int64
	startNS   int64
	endNS     int64
	outcome   backend.Outcome
	reason    lifecycle.Reason
	released  bool
	admitted  bool
	started   bool
	kvTokens  int
}

// Coordinator is the running runtime. All lifecycle mutations happen on the
// single run loop (serialized), so Request state, admission counters, and
// resource reservations stay consistent without a global lock around logic.
type Coordinator struct {
	cfg Config
	log *slog.Logger
	met *observability.Metrics
	pol scheduler.Policy

	res *resources.Manager
	adm *admission.Controller

	mu       sync.Mutex
	records  map[string]*record
	order    []*record // admitted, not yet started, in arrival order
	incoming []*record // submitted, not yet evaluated for admission
	closed   bool
	wakeup   chan struct{}

	backendResults <-chan backend.Result

	baseTime time.Time
	baseNS   int64

	done chan struct{}
	wg   sync.WaitGroup

	// counters for status reporting.
	stats statsCounters
}

type statsCounters struct {
	submitted, admitted, rejected, completed, cancelled, failed, expired, deadlineMiss int64
	decisions                                                                          int64
}

// clock returns monotonic nanoseconds since the coordinator baseline.
func (c *Coordinator) clock() int64 { return int64(time.Since(c.baseTime)) }

// New constructs a Coordinator.
func New(cfg Config) (*Coordinator, error) {
	if cfg.Backend == nil {
		return nil, fmt.Errorf("coordinator: backend is required")
	}
	pol, ok := scheduler.Registry()[cfg.Policy]
	if !ok {
		return nil, fmt.Errorf("coordinator: unknown policy %q", cfg.Policy)
	}
	base := time.Now()
	res := resources.New(cfg.Resources)
	c := &Coordinator{
		cfg:      cfg,
		log:      cfg.Logger,
		met:      cfg.Metrics,
		pol:      pol,
		res:      res,
		records:  make(map[string]*record),
		wakeup:   make(chan struct{}, 1),
		done:     make(chan struct{}),
		baseTime: base,
	}
	if c.log == nil {
		c.log = observability.NewLogger(nil, slog.LevelInfo)
	}
	if c.met == nil {
		c.met = observability.New()
	}
	c.adm = admission.New(cfg.Admission, res, c.clock)
	c.wirePolicyClock()
	c.backendResults = cfg.Backend.Results()
	return c, nil
}

// wirePolicyClock points policy reference-time providers at the coordinator
// clock so aging/deadline/hybrid terms use monotonic time.
func (c *Coordinator) wirePolicyClock() {
	now := c.clock
	switch p := c.pol.(type) {
	case *scheduler.Priority:
		p.Aging.NowFunc = now
	case *scheduler.SJF:
		p.Aging.NowFunc = now
	case *scheduler.EDF:
		p.NowFunc = now
	case *scheduler.Hybrid:
		p.NowFunc = now
	}
}

// Start launches the run loop.
func (c *Coordinator) Start() {
	c.wg.Add(1)
	go c.run()
}

// Run blocks until the coordinator is shut down (convenience for main).
func (c *Coordinator) Run() { <-c.done }

// SubmitRequest validates and submits a new request. It returns the assigned
// request ID. Validation failures and rejections are reported explicitly.
func (c *Coordinator) SubmitRequest(spec Submission) (string, error) {
	req, err := c.buildRequest(spec)
	if err != nil {
		return "", err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return "", ErrClosed
	}
	if _, dup := c.records[req.ID]; dup {
		c.mu.Unlock()
		return "", fmt.Errorf("coordinator: duplicate request id %q", req.ID)
	}
	rec := &record{
		req:      req,
		ctx:      req.Ctx,
		cancel:   func() {},
		submitNS: c.clock(),
		kvTokens: kvTokensOf(req),
	}
	rec.arrivalNS = req.Arrival
	c.records[req.ID] = rec
	c.incoming = append(c.incoming, rec)
	c.stats.submitted++
	c.mu.Unlock()

	c.met.IncCounter(observability.MetricRequestsSubmitted, map[string]string{"state": "queued"})
	c.log.Info(string(observability.EventSubmitted), "id", req.ID, "tenant", req.TenantID, "priority", int(req.Priority))

	c.signal()
	return req.ID, nil
}

// signal wakes the run loop.
func (c *Coordinator) signal() {
	select {
	case c.wakeup <- struct{}{}:
	default:
	}
}

// CancelRequest requests cancellation of a request by ID.
func (c *Coordinator) CancelRequest(id string) error {
	c.mu.Lock()
	rec, ok := c.records[id]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("coordinator: unknown request %q", id)
	}
	if rec.req.State().Terminal() {
		c.mu.Unlock()
		return fmt.Errorf("coordinator: request %q already terminal (%s)", id, rec.req.State())
	}
	c.mu.Unlock()
	// The run loop performs the actual transition, releasing resources once.
	c.cancelRequest(id)
	c.signal()
	return nil
}

// cancelRequest marks a request for cancellation and, if running, tells the
// backend. Idempotent.
func (c *Coordinator) cancelRequest(id string) {
	c.mu.Lock()
	rec, ok := c.records[id]
	if !ok || rec.req.State().Terminal() {
		c.mu.Unlock()
		return
	}
	h := backend.Handle{RequestID: id}
	c.mu.Unlock()
	_ = c.cfg.Backend.Cancel(h)
}

// RequestStatus returns a snapshot of a request's status.
func (c *Coordinator) RequestStatus(id string) (RequestStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, ok := c.records[id]
	if !ok {
		return RequestStatus{}, fmt.Errorf("coordinator: unknown request %q", id)
	}
	r := rec.req
	return RequestStatus{
		ID:         r.ID,
		TenantID:   r.TenantID,
		Class:      string(r.Class),
		Priority:   int(r.Priority),
		State:      string(r.State()),
		Outcome:    string(rec.outcome),
		Reason:     string(rec.reason),
		ArrivalNS:  r.Arrival,
		DeadlineNS: r.Deadline,
		AdmitNS:    rec.admitNS,
		StartNS:    rec.startNS,
		EndNS:      rec.endNS,
		KVTokens:   rec.kvTokens,
	}, nil
}

// SchedulerStatus reports policy and queue state.
func (c *Coordinator) SchedulerStatus() SchedulerStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return SchedulerStatus{
		Policy:    c.pol.Name(),
		Pending:   len(c.order),
		Submitted: c.stats.submitted,
		Admitted:  c.stats.admitted,
		Rejected:  c.stats.rejected,
		Decisions: c.stats.decisions,
	}
}

// ResourceStatus returns current resource and admission occupancy.
func (c *Coordinator) ResourceStatus() ResourceStatus {
	snap := c.res.Snapshot()
	return ResourceStatus{
		SlotsTotal:      snap.SlotsTotal,
		SlotsUsed:       snap.SlotsUsed,
		ActiveSequences: snap.ActiveSequences,
		KVBlocksTotal:   snap.KVBlocksTotal,
		KVBlocksUsed:    snap.KVBlocksUsed,
		Reservations:    snap.Reservations,
	}
}

// Metrics returns the metrics registry, for the /metrics handler.
func (c *Coordinator) Metrics() *observability.Metrics { return c.met }

// Shutdown stops the coordinator gracefully: it cancels queued and in-flight
// work, waits for the run loop to drain, and returns. It is idempotent.
func (c *Coordinator) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	c.signal()
	_ = c.cfg.Backend.Close()

	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// kvTokensOf mirrors the simulator's estimate.
func kvTokensOf(r *lifecycle.Request) int {
	if r.Estimate.KVTokens > 0 {
		return r.Estimate.KVTokens
	}
	return r.Estimate.PrefillTokens + r.Estimate.ExpectedOutputTokens
}
