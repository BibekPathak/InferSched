package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/BibekP/infersched/internal/workload"
)

// TestRunJSONProducesValidReport exercises the machine-readable output path the
// benchmark harness depends on: it loads the checked-in sample trace, runs all
// policies, and checks the JSON report is well-formed and self-consistent.
func TestRunJSONProducesValidReport(t *testing.T) {
	trace := filepath.Join("..", "..", "..", "benchmarks", "workloads", "sample-short-interactive.jsonl")

	// Reuse the CLI's own loading path by invoking runJSON through a temp file.
	out := filepath.Join(t.TempDir(), "report.json")
	tr, err := workload.Load(trace)
	if err != nil {
		t.Fatalf("load trace: %v", err)
	}

	p := simParams{
		Policies: []string{"fifo", "priority", "sjf", "edf", "hybrid"},
		Seed:     1, Workers: 2, BatchSize: 8, KVBlocks: 512, KVBlockSize: 16,
		Strategy: "reject",
	}
	if err := runJSON(out, tr, tr.BuildRequests(nil), p); err != nil {
		t.Fatalf("runJSON: %v", err)
	}

	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var rep jsonReport
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	if len(rep.PolicyRuns) != 5 {
		t.Fatalf("want 5 policy runs, got %d", len(rep.PolicyRuns))
	}
	for _, pr := range rep.PolicyRuns {
		if pr.Policy == "" {
			t.Fatal("policy run missing name")
		}
		if len(pr.Requests) != tr.Header.RequestCount {
			t.Fatalf("policy %s: got %d requests, want %d",
				pr.Policy, len(pr.Requests), tr.Header.RequestCount)
		}
		// Every request must carry an outcome and an id.
		for _, r := range pr.Requests {
			if r.ID == "" || r.Outcome == "" {
				t.Fatalf("policy %s: incomplete request record %+v", pr.Policy, r)
			}
		}
	}
	// Percentiles must be monotonic non-decreasing.
	for _, pr := range rep.PolicyRuns {
		if !(pr.Latency.TTFT.P50 <= pr.Latency.TTFT.P90 &&
			pr.Latency.TTFT.P90 <= pr.Latency.TTFT.P99) {
			t.Fatalf("policy %s: TTFT percentiles not monotonic: %+v", pr.Policy, pr.Latency.TTFT)
		}
	}
}
