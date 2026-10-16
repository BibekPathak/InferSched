package simulator

import (
	"testing"

	"github.com/BibekP/infersched/internal/backend"
	"github.com/BibekP/infersched/internal/lifecycle"
)

func TestCancelQueuedRequest(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Policy = "fifo"
	cfg.Workers = 1
	cfg.MaxBatchSize = 1
	cfg.KVBlocksPerWorker = 1024
	// Two requests arrive at t=0; the second queues behind the first.
	reqs := []*lifecycle.Request{
		mkReq("first", "t0", 0, 0, lifecycle.PriorityNormal, 128, 100),
		mkReq("second", "t0", 0, 0, lifecycle.PriorityNormal, 128, 100),
	}
	s := mustNew(t, cfg, reqs)
	s.ScheduleCancel("second", 1_000_000) // 1ms: likely still queued
	results, stats := s.Run()
	if stats.Cancelled == 0 {
		t.Fatalf("expected a cancellation, got %+v", stats)
	}
	var second *Result
	for i := range results {
		if results[i].RequestID == "second" {
			second = &results[i]
		}
	}
	if second == nil || second.Outcome != backend.OutcomeCancelled {
		t.Fatalf("second should be cancelled, got %+v", second)
	}
	if err := s.res.Invariants(); err != nil {
		t.Fatalf("leak after queued cancel: %v", err)
	}
}

func TestCancelRunningRequestReleasesResources(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Policy = "fifo"
	cfg.Workers = 1
	cfg.MaxBatchSize = 4
	cfg.KVBlocksPerWorker = 1024
	// A long-running request; cancel it mid-flight.
	reqs := []*lifecycle.Request{
		mkReq("run", "t0", 0, 0, lifecycle.PriorityNormal, 128, 100000),
	}
	s := mustNew(t, cfg, reqs)
	s.ScheduleCancel("run", 5_000_000) // 5ms in
	results, stats := s.Run()
	if stats.Cancelled != 1 {
		t.Fatalf("expected 1 cancelled, got %+v", stats)
	}
	if results[0].Outcome != backend.OutcomeCancelled {
		t.Fatalf("outcome=%s", results[0].Outcome)
	}
	// Resources must be released exactly once.
	if err := s.res.Invariants(); err != nil {
		t.Fatalf("leak after running cancel: %v", err)
	}
	if snap := s.res.Snapshot(); snap.Reservations != 0 {
		t.Fatalf("resources not released: %+v", snap)
	}
}

func TestCancelUnknownIDIsHarmless(t *testing.T) {
	cfg := DefaultConfig()
	reqs := linearReqs(3, 0, 128, 16)
	s := mustNew(t, cfg, reqs)
	s.ScheduleCancel("does-not-exist", 1_000_000)
	_, stats := s.Run()
	if stats.Completed != 3 {
		t.Fatalf("unknown cancel should not affect the run: %+v", stats)
	}
}
