package simulator

import (
	"github.com/BibekP/infersched/internal/backend"
	"github.com/BibekP/infersched/internal/lifecycle"
	"github.com/BibekP/infersched/internal/resources"
)

// Run executes the simulation to completion (or until the horizon) and returns
// the per-request results plus aggregate stats. It is deterministic: the same
// Config and trace always produce the same results.
func (s *Simulator) Run() ([]Result, Stats) {
	s.policy.Reset()
	s.scheduleArrivals()
	s.scheduleFailures()

	for s.queue.Len() > 0 {
		e := s.queue.pop()
		if s.cfg.HorizonNS > 0 && e.time > s.cfg.HorizonNS {
			break
		}
		s.now = e.time
		switch e.kind {
		case EventArrival:
			s.handleArrival(e.reqIdx)
		case EventStepEnd:
			s.handleStepEnd(e.worker)
		case EventCancel:
			s.handleCancel(e.reqIdx)
		case EventDeadlineExpire:
			s.handleDeadline(e.reqIdx)
		case EventWorkerFailure:
			s.handleWorkerFailure(e.worker)
		}
		s.observePeaks()
		// After any state change, let idle workers pick up work.
		s.pump()
	}

	s.finalise()
	return s.results, s.stats()
}

// scheduleArrivals seeds the event queue with every request's arrival. Arrival
// times come from the trace and are authoritative; the simulator only orders
// them by (time, seq) where seq follows request order.
func (s *Simulator) scheduleArrivals() {
	for i, r := range s.requests {
		s.push(EventArrival, r.Arrival, i, -1)
	}
}

// scheduleFailures seeds synthetic worker-failure events. The number of
// injected failures is bounded by the configured rate applied to the request
// count, so a run always terminates regardless of horizon. Failures are spaced
// deterministically from the seeded RNG. A rate of 0 schedules none.
func (s *Simulator) scheduleFailures() {
	if s.cfg.FailureRatePerMillionSteps <= 0 || s.cfg.Workers == 0 {
		return
	}
	// expectedFailures scales with workload size; clamp to a sane bound so the
	// schedule is finite.
	expected := len(s.requests) * s.cfg.FailureRatePerMillionSteps / 1_000_000
	if expected < 1 {
		expected = 1
	}
	if expected > 100 {
		expected = 100
	}
	// Spread failures across the arrival span plus a tail.
	span := int64(1)
	if len(s.requests) > 0 {
		last := s.requests[len(s.requests)-1].Arrival
		if last > span {
			span = last
		}
	}
	step := span / int64(expected+1)
	if step < 1 {
		step = 1
	}
	for i := 1; i <= expected; i++ {
		t := int64(i) * step
		if s.cfg.HorizonNS > 0 && t >= s.cfg.HorizonNS {
			break
		}
		w := int(s.nextRNG() % uint64(s.cfg.Workers))
		s.push(EventWorkerFailure, t, -1, w)
	}
}

// push appends an event with an auto-assigned sequence number.
func (s *Simulator) push(kind EventKind, at int64, reqIdx, worker int) {
	s.seq++
	s.queue.push(event{time: at, seq: s.seq, kind: kind, reqIdx: reqIdx, worker: worker})
}

// handleArrival evaluates admission for the arriving request. On admission the
// request is counted as queued (admitted but not yet started); it becomes
// outstanding only when a worker actually starts it in tryStart. On rejection
// it is recorded and dropped.
func (s *Simulator) handleArrival(idx int) {
	r := s.requests[idx]
	res := s.result(idx)
	res.RequestID = r.ID
	res.TenantID = r.TenantID
	res.Class = r.Class
	res.Priority = r.Priority
	res.ArrivalNS = r.Arrival
	res.DeadlineNS = r.Deadline

	d := s.adm.Evaluate(r)
	if !d.Admit {
		s.reject(idx, d.Reason)
		return
	}
	// Commit places the request in the admission queue; it is dequeued and
	// marked outstanding in tryStart when a worker picks it up. This makes the
	// controller's queue-length and queued-token bounds meaningful.
	s.adm.Commit(r)

	// A request whose estimated KV footprint exceeds the entire pool can never
	// be scheduled. Reject it explicitly rather than letting it wait forever.
	if blocks := s.res.Config().BlocksForTokens(kvTokensOf(r)); blocks > s.res.TotalKVBlocks() {
		s.adm.Dequeue(r)
		s.adm.MarkDone(r)
		s.reject(idx, lifecycle.ReasonOverloadTokens)
		return
	}

	s.result(idx).Admitted = true
	s.result(idx).AdmitNS = s.now

	if r.HasDeadline() {
		s.push(EventDeadlineExpire, r.Deadline, idx, -1)
	}
}

// pump lets idle capacity pick up admitted-but-not-started requests. It forms
// batches per the scheduling policy and starts a step on each worker that has
// room. It is called after every event so work is never left idle.
func (s *Simulator) pump() {
	pending := s.pendingIndices()
	if len(pending) == 0 {
		s.startIdleWorkers()
		return
	}

	pol := s.policy.Order(s.pendingRequests(pending))
	// Map ordered requests back to indices.
	orderIdx := make([]int, 0, len(pol))
	for _, pr := range pol {
		orderIdx = append(orderIdx, s.indexOf(pr))
	}

	// Fill workers: a request may start only if its worker has a free slot and
	// enough KV capacity. The scheduler sees all pending requests, but a
	// request's feasibility depends on the specific worker it would land on, so
	// we greedily place the highest-priority request onto the first eligible
	// worker.
	remaining := append([]int(nil), orderIdx...)
	for {
		placed := false
		for wi := range s.workers {
			w := s.workers[wi]
			if w.failed || len(w.active) >= s.cfg.MaxBatchSize {
				continue
			}
			for k, idx := range remaining {
				if s.tryStart(w, idx) {
					remaining = append(remaining[:k], remaining[k+1:]...)
					placed = true
					break
				}
			}
		}
		if !placed {
			break
		}
	}
	s.startIdleWorkers()
}

// startIdleWorkers begins steps for any worker that has active sequences and is
// not currently executing.
func (s *Simulator) startIdleWorkers() {
	for _, w := range s.workers {
		if w.failed || len(w.active) == 0 {
			continue
		}
		// Worker is busy if any active run is mid-step.
		if s.workerBusy(w) {
			continue
		}
		s.startStep(w)
	}
}

// workerBusy reports whether w is mid-step (its active runs are marked down).
func (s *Simulator) workerBusy(w *worker) bool {
	for _, rs := range w.active {
		if rs.down {
			return true
		}
	}
	return false
}

// tryStart attempts to start idx's request on worker w: reserve resources,
// prefill, and add it to the worker's active set. Returns true on success.
func (s *Simulator) tryStart(w *worker, idx int) bool {
	r := s.requests[idx]
	kvTokens := kvTokensOf(r)
	if _, err := s.res.Reserve(resources.Need{RequestID: r.ID, KVTokens: kvTokens}); err != nil {
		return false
	}
	if err := r.Admit(); err != nil {
		s.res.Release(r.ID)
		return false
	}
	if err := r.Start(); err != nil {
		s.res.Release(r.ID)
		return false
	}
	// The request leaves the admission queue and becomes outstanding now that a
	// worker has actually started it.
	s.adm.Dequeue(r)
	s.adm.MarkOutstanding()

	rs := &runState{reqIdx: idx, worker: w.id, remaining: max(1, r.Estimate.ExpectedOutputTokens)}
	w.active = append(w.active, rs)

	res := s.result(idx)
	res.StartNS = s.now
	res.WorkerID = w.id
	res.KVTokens = kvTokens
	return true
}

// startStep begins one batched step on worker w. Prefill happens on the first
// step (cost depends on total prompt tokens and batch size); decode happens on
// subsequent steps. The step end is scheduled at now + stepCost.
func (s *Simulator) startStep(w *worker) {
	batch := len(w.active)
	if batch == 0 {
		return
	}
	var prefillTokens int
	for _, rs := range w.active {
		if !rs.started {
			prefillTokens += s.requests[rs.reqIdx].Estimate.PrefillTokens
		}
		rs.down = true
	}
	// Step cost: if any sequence still needs prefill, the step includes prefill
	// work; decode cost applies to sequences already decoding.
	cost := 0.0
	if prefillTokens > 0 {
		cost += s.perf.PrefillCostMS(prefillTokens, batch)
	}
	decodeBatch := 0
	for _, rs := range w.active {
		if rs.started {
			decodeBatch++
		}
	}
	if decodeBatch > 0 {
		cost += s.perf.DecodeStepCostMS(decodeBatch)
	}
	if cost <= 0 {
		cost = s.perf.DecodeStepCostMS(batch)
	}
	stepNS := msToNS(cost)
	if stepNS < 1 {
		stepNS = 1
	}
	s.push(EventStepEnd, s.now+stepNS, -1, w.id)
}

// handleStepEnd completes a step on worker w: newly-prefilled sequences emit
// their first token and begin decoding; decoding sequences emit one token and
// either finish or continue. The next step is then scheduled.
func (s *Simulator) handleStepEnd(workerIdx int) {
	w := s.workers[workerIdx]
	if w.failed {
		return
	}
	survivors := w.active[:0]
	for _, rs := range w.active {
		rs.down = false
		if !rs.started {
			rs.started = true
			s.result(rs.reqIdx).FirstTokenNS = s.now
		}
		rs.remaining--
		s.result(rs.reqIdx).OutputTokens++
		if rs.remaining <= 0 {
			s.completeRequest(w, rs)
			continue
		}
		survivors = append(survivors, rs)
	}
	w.active = survivors

	// If this worker still has active sequences, schedule the next step.
	if len(w.active) > 0 {
		s.startStep(w)
	}
}

// failWorker fails a worker, failing all its active requests and reconciling
// (releasing) their resources exactly once.
func (s *Simulator) failWorker(w *worker) {
	if w.failed {
		return
	}
	w.failed = true
	for _, rs := range w.active {
		s.failRequest(w, rs)
	}
	w.active = nil
}

// completeRequest finalises a normally-completed request.
func (s *Simulator) completeRequest(w *worker, rs *runState) {
	r := s.requests[rs.reqIdx]
	r.Complete()
	s.res.Release(r.ID)
	s.adm.MarkDone(r)

	res := s.result(rs.reqIdx)
	res.Outcome = backend.OutcomeCompleted
	res.EndNS = s.now
	s.finishTimes(res)
}

// failRequest finalises a request whose worker failed.
func (s *Simulator) failRequest(w *worker, rs *runState) {
	r := s.requests[rs.reqIdx]
	r.Fail()
	s.res.Release(r.ID)
	s.adm.MarkDone(r)

	res := s.result(rs.reqIdx)
	res.Outcome = backend.OutcomeFailed
	res.Reason = lifecycle.ReasonWorkerFailure
	res.EndNS = s.now
	s.finishTimes(res)
}

// handleCancel finalises a cancelled request whether queued or running.
func (s *Simulator) handleCancel(idx int) {
	r := s.requests[idx]
	st := r.State()
	if st.Terminal() {
		return
	}
	switch st {
	case lifecycle.StateRunning:
		if w := s.workerOf(idx); w != nil {
			s.removeActive(w, idx)
			s.res.Release(r.ID)
		}
		s.adm.MarkDone(r)
	case lifecycle.StateQueued:
		// Admitted but not yet started: it occupies the admission queue.
		s.adm.Dequeue(r)
		s.adm.MarkDone(r)
	}
	r.Cancel()
	res := s.result(idx)
	res.Outcome = backend.OutcomeCancelled
	res.Reason = lifecycle.ReasonClientCancel
	res.EndNS = s.now
	s.finishTimes(res)
}

// handleDeadline expires a queued or admitted request whose deadline passed.
func (s *Simulator) handleDeadline(idx int) {
	r := s.requests[idx]
	st := r.State()
	if st.Terminal() || st == lifecycle.StateRunning {
		// Running requests are not force-expired; they complete or are
		// cancelled. (A real runtime might preempt; documented limitation.)
		return
	}
	if st == lifecycle.StateQueued {
		s.adm.Dequeue(r)
	}
	s.adm.MarkDone(r)
	r.Expire()
	res := s.result(idx)
	res.Outcome = backend.OutcomeExpired
	res.Reason = lifecycle.ReasonTimeout
	res.EndNS = s.now
	s.finishTimes(res)
}

// handleWorkerFailure is the externally scheduled failure event.
func (s *Simulator) handleWorkerFailure(workerIdx int) {
	if workerIdx < 0 || workerIdx >= len(s.workers) {
		return
	}
	s.failWorker(s.workers[workerIdx])
}

// reject records a rejection for idx.
func (s *Simulator) reject(idx int, reason lifecycle.Reason) {
	r := s.requests[idx]
	r.Reject()
	res := s.result(idx)
	res.Outcome = backend.OutcomeFailed
	res.Reason = reason
	res.EndNS = s.now
}

// finalise reports requests that never reached a terminal state at run end.
func (s *Simulator) finalise() {
	for i, r := range s.requests {
		res := &s.results[i]
		res.RequestID = r.ID
		res.TenantID = r.TenantID
		res.Class = r.Class
		res.Priority = r.Priority
		res.ArrivalNS = r.Arrival
		res.DeadlineNS = r.Deadline

		if res.Outcome != "" {
			continue
		}
		if !res.Admitted {
			// Never arrived (beyond horizon) or arrived and was rejected. A
			// request that reached a terminal state was already recorded; one
			// that never arrived is reported as failed with reason "horizon".
			if r.State() == lifecycle.StateQueued {
				r.Reject()
				res.Outcome = backend.OutcomeFailed
				res.Reason = "horizon"
				res.EndNS = s.now
			}
			continue
		}
		// Admitted but not started at shutdown.
		if r.State() == lifecycle.StateQueued {
			r.Reject()
			res.Outcome = backend.OutcomeFailed
			res.Reason = lifecycle.ReasonShutdown
		} else {
			r.Cancel()
			res.Outcome = backend.OutcomeFailed
			res.Reason = "horizon"
		}
		res.EndNS = s.now
	}
}

// finishTimes computes derived latency fields from absolute marks.
func (s *Simulator) finishTimes(res *Result) {
	r := s.requests[s.resultIndex(res)]
	res.ArrivalNS = r.Arrival
	res.DeadlineNS = r.Deadline
	if res.Admitted {
		res.AdmissionDelayNS = res.AdmitNS - res.ArrivalNS
	}
	if res.StartNS > 0 {
		res.QueueDelayNS = res.StartNS - res.AdmitNS
	}
	if res.FirstTokenNS > 0 {
		res.TTFTNS = res.FirstTokenNS - res.ArrivalNS
	}
	if res.EndNS > 0 {
		res.CompletionNS = res.EndNS - res.ArrivalNS
	}
	if r.HasDeadline() {
		res.LatenessNS = res.EndNS - r.Deadline
	}
}

func (s *Simulator) stats() Stats {
	var st Stats
	for _, res := range s.results {
		st.Total++
		switch res.Outcome {
		case backend.OutcomeCompleted:
			st.Completed++
		case backend.OutcomeCancelled:
			st.Cancelled++
		case backend.OutcomeExpired:
			st.Expired++
		case backend.OutcomeFailed:
			if res.Reason == lifecycle.ReasonOverloadQueue ||
				res.Reason == lifecycle.ReasonOverloadTokens ||
				res.Reason == lifecycle.ReasonOverloadFlights ||
				res.Reason == lifecycle.ReasonOverloadTenant ||
				res.Reason == lifecycle.ReasonDeadlineMiss {
				st.Rejected++
			} else {
				st.Failed++
			}
		}
		if res.DeadlineNS != 0 && res.LatenessNS > 0 && res.Outcome == backend.OutcomeCompleted {
			st.DeadlineMiss++
		}
	}
	st.PeakActiveSequences = s.peakActive
	st.PeakKVBlocksUsed = s.peakKVBlocks
	st.PeakSlotsUsed = s.peakSlots
	return st
}
