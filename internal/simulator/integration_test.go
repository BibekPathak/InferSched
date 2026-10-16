package simulator

import (
	"path/filepath"
	"testing"

	"github.com/BibekP/infersched/internal/backend"
	"github.com/BibekP/infersched/internal/workload"
)

// TestReplayRealTraceFile loads the checked-in sample trace and replays it,
// exercising the full trace -> requests -> simulator path.
func TestReplayRealTraceFile(t *testing.T) {
	path := filepath.Join("..", "..", "benchmarks", "workloads", "sample-short-interactive.jsonl")
	tr, err := workload.Load(path)
	if err != nil {
		t.Fatalf("load trace: %v", err)
	}
	cfg := DefaultConfig()
	cfg.Policy = "fifo"
	sim, err := New(cfg, tr.BuildRequests(nil))
	if err != nil {
		t.Fatalf("new sim: %v", err)
	}
	results, stats := sim.Run()
	if stats.Total != len(tr.Records) {
		t.Fatalf("total=%d want %d", stats.Total, len(tr.Records))
	}
	// Every request must have a populated ID and a terminal outcome.
	for _, r := range results {
		if r.RequestID == "" {
			t.Fatal("result missing request id")
		}
		if r.Outcome == "" {
			t.Fatalf("request %s has no outcome", r.RequestID)
		}
	}
	if err := sim.res.Invariants(); err != nil {
		t.Fatalf("resource leak: %v", err)
	}
}

// TestAllPoliciesCompleteTheTrace ensures every registered policy can run the
// same trace without error and produces a valid partition of outcomes.
func TestAllPoliciesCompleteTheTrace(t *testing.T) {
	reqs := linearReqs(25, 300_000, 256, 32)
	for name := range policyNames(t) {
		cfg := DefaultConfig()
		cfg.Policy = name
		cfg.Workers = 2
		cfg.MaxBatchSize = 4
		sim, err := New(cfg, reqs)
		if err != nil {
			t.Fatalf("policy %s: new: %v", name, err)
		}
		results, stats := sim.Run()
		if stats.Completed+stats.Rejected+stats.Cancelled+stats.Expired+stats.Failed != stats.Total {
			t.Fatalf("policy %s: outcomes do not partition total: %+v", name, stats)
		}
		for _, r := range results {
			if r.Outcome == backend.OutcomeCompleted && r.OutputTokens <= 0 {
				t.Fatalf("policy %s: completed request %s has no output", name, r.RequestID)
			}
		}
		if err := sim.res.Invariants(); err != nil {
			t.Fatalf("policy %s: resource leak: %v", name, err)
		}
		if err := sim.adm.Invariants(); err != nil {
			t.Fatalf("policy %s: admission invariant: %v", name, err)
		}
	}
}

func policyNames(t *testing.T) map[string]struct{} {
	t.Helper()
	return map[string]struct{}{
		"fifo": {}, "priority": {}, "sjf": {}, "edf": {}, "hybrid": {},
	}
}
