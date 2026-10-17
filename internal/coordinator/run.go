package coordinator

import (
	"time"

	"github.com/BibekP/infersched/internal/backend"
	"github.com/BibekP/infersched/internal/lifecycle"
	"github.com/BibekP/infersched/internal/observability"
	"github.com/BibekP/infersched/internal/resources"
)

// tickInterval bounds how long the loop sleeps without an event, so deadlines
// are checked even in the absence of arrivals or completions.
const tickInterval = 10 * time.Millisecond

// run is the single scheduling goroutine. All lifecycle mutations and resource
// releases happen here, serialized, so the admission controller and the
// pending/order slices need no additional locking.
func (c *Coordinator) run() {
	defer c.wg.Done()
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		c.processCancellations()
		c.drainBackendResults()
		c.expireDeadlines()
		c.admitIncoming()
		c.schedule()

		if c.isClosedAndDrained() {
			c.finalizeShutdown()
			close(c.done)
			return
		}

		select {
		case <-c.wakeup:
		case res, ok := <-c.backendResults:
			if !ok {
				c.backendResults = nil
				continue
			}
			c.applyResult(res)
		case <-ticker.C:
		}
	}
}

// processCancellations cancels queued (not-yet-started) requests that the API
// has asked to cancel. Running requests are cancelled via the backend and are
// finalized when their result arrives. This closes the gap where a request
// cancelled while queued would otherwise keep waiting for a worker slot.
func (c *Coordinator) processCancellations() {
	c.mu.Lock()
	if len(c.cancelIntent) == 0 {
		c.mu.Unlock()
		return
	}
	var toCancel []*record
	kept := c.order[:0]
	for _, rec := range c.order {
		if c.cancelIntent[rec.req.ID] && !rec.started {
			toCancel = append(toCancel, rec)
			continue
		}
		kept = append(kept, rec)
	}
	c.order = kept
	incomingKept := c.incoming[:0]
	for _, rec := range c.incoming {
		if c.cancelIntent[rec.req.ID] {
			toCancel = append(toCancel, rec)
			continue
		}
		incomingKept = append(incomingKept, rec)
	}
	c.incoming = incomingKept
	for _, rec := range toCancel {
		delete(c.cancelIntent, rec.req.ID)
	}
	c.mu.Unlock()

	for _, rec := range toCancel {
		if !c.recordOutcome(rec, backend.OutcomeCancelled, lifecycle.ReasonClientCancel) {
			continue
		}
		if rec.admitted {
			c.adm.Dequeue(rec.req)
			c.adm.MarkDone(rec.req)
		}
		rec.req.Cancel()
		c.finish(rec, backend.OutcomeCancelled, lifecycle.ReasonClientCancel)
	}
}

// isClosedAndDrained reports whether shutdown can complete. After Close the
// backend stops delivering results, so shutdown drives remaining records to a
// terminal state itself in finalizeShutdown; here we only require that no
// admission or scheduling work remains.
func (c *Coordinator) isClosedAndDrained() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed && len(c.incoming) == 0 && len(c.order) == 0
}

// finalizeShutdown cancels and finalizes any requests still outstanding, then
// closes the backend and reports final metrics. Running requests are cancelled
// because the runtime is stopping; their resources are released exactly once.
func (c *Coordinator) finalizeShutdown() {
	c.mu.Lock()
	var remaining []*record
	for _, rec := range c.records {
		if rec.outcome == "" {
			remaining = append(remaining, rec)
		}
	}
	c.mu.Unlock()

	for _, rec := range remaining {
		_ = c.cfg.Backend.Cancel(backend.Handle{RequestID: rec.req.ID})
		st := rec.req.State()
		switch st {
		case lifecycle.StateQueued:
			if !c.recordOutcome(rec, backend.OutcomeFailed, lifecycle.ReasonShutdown) {
				continue
			}
			c.adm.Dequeue(rec.req)
			c.adm.MarkDone(rec.req)
			rec.req.Reject()
			c.finish(rec, backend.OutcomeFailed, lifecycle.ReasonShutdown)
		case lifecycle.StateRunning:
			if !c.recordOutcome(rec, backend.OutcomeCancelled, lifecycle.ReasonShutdown) {
				continue
			}
			rec.req.Cancel()
			c.finish(rec, backend.OutcomeCancelled, lifecycle.ReasonShutdown)
		}
	}

	if c.cfg.Backend != nil {
		_ = c.cfg.Backend.Close()
	}
	c.publishResourceGauges()
}

// drainBackendResults applies any buffered backend results without blocking.
func (c *Coordinator) drainBackendResults() {
	if c.backendResults == nil {
		return
	}
	for {
		select {
		case res, ok := <-c.backendResults:
			if !ok {
				c.backendResults = nil
				return
			}
			c.applyResult(res)
		default:
			return
		}
	}
}

// admitIncoming evaluates admission for submitted requests in arrival order.
func (c *Coordinator) admitIncoming() {
	c.mu.Lock()
	incoming := c.incoming
	c.incoming = nil
	c.mu.Unlock()

	for _, rec := range incoming {
		d := c.adm.Evaluate(rec.req)
		if !d.Admit {
			c.reject(rec, d.Reason)
			continue
		}
		// A request larger than the entire pool can never run.
		blocks := c.res.Config().BlocksForTokens(rec.kvTokens)
		if blocks > c.res.TotalKVBlocks() {
			c.reject(rec, lifecycle.ReasonOverloadTokens)
			continue
		}
		c.adm.Commit(rec.req)
		c.mu.Lock()
		rec.admitted = true
		rec.admitNS = c.clock()
		rec.reason = lifecycle.ReasonNone
		c.stats.admitted++
		c.order = append(c.order, rec)
		c.mu.Unlock()
		c.met.IncCounter(observability.MetricRequestsAdmitted, nil)
		c.log.Info(string(observability.EventAdmitted), "id", rec.req.ID)
	}
}

// schedule orders admitted requests via the policy and starts as many as the
// resource capacity allows, submitting each to the backend.
func (c *Coordinator) schedule() {
	c.mu.Lock()
	pending := append([]*record(nil), c.order...)
	c.mu.Unlock()
	if len(pending) == 0 {
		c.publishResourceGauges()
		return
	}

	reqs := make([]*lifecycle.Request, len(pending))
	for i, rec := range pending {
		reqs[i] = rec.req
	}
	ordered := c.pol.Order(reqs)
	c.mu.Lock()
	c.stats.decisions++
	c.mu.Unlock()
	c.met.IncCounter(observability.MetricSchedulingDecisions, map[string]string{"policy": c.pol.Name()})

	started := make(map[string]bool)
	for _, req := range ordered {
		rec := c.recordFor(req.ID)
		if rec == nil || c.recordStarted(rec) {
			continue
		}
		if _, err := c.res.Reserve(resources.Need{RequestID: rec.req.ID, KVTokens: rec.kvTokens}); err != nil {
			// Capacity unavailable; leave it queued for a later pass.
			continue
		}
		if err := rec.req.Admit(); err != nil {
			c.res.Release(rec.req.ID)
			continue
		}
		if err := rec.req.Start(); err != nil {
			c.res.Release(rec.req.ID)
			continue
		}
		if _, err := c.cfg.Backend.Submit(rec.req); err != nil {
			c.recordOutcome(rec, backend.OutcomeFailed, lifecycle.ReasonWorkerFailure)
			rec.req.Fail()
			c.res.Release(rec.req.ID)
			c.finish(rec, backend.OutcomeFailed, lifecycle.ReasonWorkerFailure)
			continue
		}
		c.mu.Lock()
		rec.started = true
		rec.startNS = c.clock()
		c.mu.Unlock()
		started[rec.req.ID] = true
		c.adm.Dequeue(rec.req)
		c.adm.MarkOutstanding()
		c.met.IncCounter(observability.MetricRequestsSubmitted, map[string]string{"state": "running"})
		c.log.Info(string(observability.EventStarted), "id", rec.req.ID)
	}

	// Remove started requests from the pending order.
	if len(started) > 0 {
		c.mu.Lock()
		kept := c.order[:0]
		for _, rec := range c.order {
			if !started[rec.req.ID] {
				kept = append(kept, rec)
			}
		}
		c.order = kept
		c.mu.Unlock()
	}
	c.publishResourceGauges()
}

// applyResult applies a backend result, releasing resources exactly once.
//
// The record's terminal outcome is recorded under the coordinator lock BEFORE
// the request's own state transition, so a concurrent RequestStatus can never
// observe a terminal request state with an empty outcome. finish() then
// performs the exactly-once resource release and metric updates.
func (c *Coordinator) applyResult(res backend.Result) {
	rec := c.recordFor(res.RequestID)
	if rec == nil {
		return
	}
	switch res.Outcome {
	case backend.OutcomeCompleted:
		if !c.recordOutcome(rec, backend.OutcomeCompleted, lifecycle.ReasonCompleted) {
			return
		}
		if err := rec.req.Complete(); err != nil {
			c.log.Warn("complete_transition_failed", "id", rec.req.ID, "err", err.Error())
		}
		c.finish(rec, backend.OutcomeCompleted, lifecycle.ReasonCompleted)
		if rec.req.HasDeadline() && c.clock() > rec.req.Deadline {
			c.mu.Lock()
			c.stats.deadlineMiss++
			c.mu.Unlock()
			c.met.IncCounter(observability.MetricDeadlineMisses, nil)
		}
	case backend.OutcomeCancelled:
		if !c.recordOutcome(rec, backend.OutcomeCancelled, lifecycle.ReasonClientCancel) {
			return
		}
		rec.req.Cancel()
		c.finish(rec, backend.OutcomeCancelled, lifecycle.ReasonClientCancel)
	case backend.OutcomeFailed:
		if !c.recordOutcome(rec, backend.OutcomeFailed, lifecycle.ReasonWorkerFailure) {
			return
		}
		rec.req.Fail()
		c.finish(rec, backend.OutcomeFailed, lifecycle.ReasonWorkerFailure)
	case backend.OutcomeExpired:
		if !c.recordOutcome(rec, backend.OutcomeExpired, lifecycle.ReasonTimeout) {
			return
		}
		rec.req.Expire()
		c.finish(rec, backend.OutcomeExpired, lifecycle.ReasonTimeout)
	}
}

// recordOutcome records the terminal outcome on a record under the lock. It
// returns false if the record was already terminal (the caller must not
// transition or release again).
func (c *Coordinator) recordOutcome(rec *record, outcome backend.Outcome, reason lifecycle.Reason) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if rec.outcome != "" {
		return false
	}
	rec.outcome = outcome
	rec.reason = reason
	return true
}

// expireDeadlines expires admitted-but-not-started or running requests whose
// deadline has passed.
func (c *Coordinator) expireDeadlines() {
	now := c.clock()
	c.mu.Lock()
	var expiredQueued []*record
	kept := c.order[:0]
	for _, rec := range c.order {
		if rec.req.HasDeadline() && rec.req.Deadline <= now {
			expiredQueued = append(expiredQueued, rec)
			continue
		}
		kept = append(kept, rec)
	}
	c.order = kept
	c.mu.Unlock()

	for _, rec := range expiredQueued {
		if !c.recordOutcome(rec, backend.OutcomeExpired, lifecycle.ReasonTimeout) {
			continue
		}
		c.adm.Dequeue(rec.req)
		c.adm.MarkDone(rec.req)
		rec.req.Expire()
		c.finish(rec, backend.OutcomeExpired, lifecycle.ReasonTimeout)
	}

	// Running requests past their deadline are cancelled via the backend.
	c.mu.Lock()
	var runningExpired []*record
	for _, rec := range c.records {
		if rec.started && rec.outcome == "" && rec.req.HasDeadline() &&
			rec.req.State() == lifecycle.StateRunning && rec.req.Deadline <= now {
			runningExpired = append(runningExpired, rec)
		}
	}
	c.mu.Unlock()
	for _, rec := range runningExpired {
		_ = c.cfg.Backend.Cancel(backend.Handle{RequestID: rec.req.ID})
	}
}

// reject records a rejection and updates metrics.
func (c *Coordinator) reject(rec *record, reason lifecycle.Reason) {
	if !c.recordOutcome(rec, backend.OutcomeFailed, reason) {
		return
	}
	rec.req.Reject()
	c.finish(rec, backend.OutcomeFailed, reason)
	c.mu.Lock()
	c.stats.rejected++
	c.mu.Unlock()
	c.met.IncCounter(observability.MetricRequestsRejected, map[string]string{"reason": string(reason)})
	c.log.Info(string(observability.EventRejected), "id", rec.req.ID, "reason", string(reason))
}

// finish performs the exactly-once resource release and metric updates for a
// record whose terminal outcome has already been recorded (by recordOutcome or
// reject). It is idempotent: repeated calls release at most once.
func (c *Coordinator) finish(rec *record, outcome backend.Outcome, reason lifecycle.Reason) {
	c.mu.Lock()
	if rec.outcome == "" {
		// Defensive: callers normally record the outcome first.
		rec.outcome = outcome
		rec.reason = reason
	}
	if rec.endNS == 0 {
		rec.endNS = c.clock()
	}
	started := rec.started
	alreadyReleased := rec.released
	rec.released = true
	c.mu.Unlock()

	if alreadyReleased {
		return
	}
	if started {
		if _, err := c.res.Release(rec.req.ID); err != nil {
			c.log.Warn("release_failed", "id", rec.req.ID, "err", err.Error())
		}
		c.adm.MarkDone(rec.req)
	}

	switch outcome {
	case backend.OutcomeCompleted:
		c.met.IncCounter(observability.MetricRequestsCompleted, map[string]string{"outcome": "completed"})
	case backend.OutcomeCancelled:
		c.met.IncCounter(observability.MetricRequestsCompleted, map[string]string{"outcome": "cancelled"})
	case backend.OutcomeExpired:
		c.met.IncCounter(observability.MetricRequestsCompleted, map[string]string{"outcome": "expired"})
	case backend.OutcomeFailed:
		if !isRejection(reason) {
			c.met.IncCounter(observability.MetricRequestsCompleted, map[string]string{"outcome": "failed"})
		}
	}
	c.log.Info("request.terminal", "id", rec.req.ID, "outcome", string(outcome), "reason", string(reason))
}

// recordFor returns the record for id, or nil.
func (c *Coordinator) recordFor(id string) *record {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.records[id]
}

// recordStarted reports whether a record has been started, under the lock.
func (c *Coordinator) recordStarted(rec *record) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return rec.started
}

// publishResourceGauges updates the resource gauges.
func (c *Coordinator) publishResourceGauges() {
	snap := c.res.Snapshot()
	c.met.SetGauge(observability.MetricSlotsUsed, nil, int64(snap.SlotsUsed))
	c.met.SetGauge(observability.MetricKVBlocksUsed, nil, int64(snap.KVBlocksUsed))
	c.met.SetGauge(observability.MetricActiveSequences, nil, int64(snap.ActiveSequences))
	c.met.SetGauge(observability.MetricQueueDepth, nil, int64(len(c.order)))
}

func isRejection(r lifecycle.Reason) bool {
	switch r {
	case lifecycle.ReasonOverloadQueue, lifecycle.ReasonOverloadTokens,
		lifecycle.ReasonOverloadFlights, lifecycle.ReasonOverloadTenant,
		lifecycle.ReasonDeadlineMiss:
		return true
	}
	return false
}
