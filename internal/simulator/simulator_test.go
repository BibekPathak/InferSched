package simulator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/BibekP/infersched/internal/backend"
	"github.com/BibekP/infersched/internal/lifecycle"
)

// mkReq builds a lifecycle request with explicit timing and cost metadata.
func mkReq(id, tenant string, arrival, deadline int64, pri lifecycle.Priority, prompt, out int) *lifecycle.Request {
	return lifecycle.NewRequest(id, tenant, lifecycle.ClassInteractive, pri,
		time.Time{}, arrival, deadline, lifecycle.CostEstimate{
			PrefillTokens:        prompt,
			ExpectedOutputTokens: out,
			PrefillCostMS:        float64(prompt) * 0.02,
			DecodeCostPerTokenMS: 0.5,
			KVTokens:             prompt + out,
		}, context.Background())
}

// linearReqs builds n requests arriving every gapNS.
func linearReqs(n int, gapNS int64, prompt, out int) []*lifecycle.Request {
	out2 := make([]*lifecycle.Request, n)
	for i := 0; i < n; i++ {
		out2[i] = mkReq(fmt.Sprintf("r%03d", i), "t0", int64(i)*gapNS, 0,
			lifecycle.PriorityNormal, prompt, out)
	}
	return out2
}

func mustNew(t *testing.T, cfg Config, reqs []*lifecycle.Request) *Simulator {
	t.Helper()
	s, err := New(cfg, reqs)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestSimulatorCompletesAllWork(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Policy = "fifo"
	reqs := linearReqs(10, 0, 128, 16)
	s := mustNew(t, cfg, reqs)
	results, stats := s.Run()

	if stats.Total != 10 {
		t.Fatalf("total=%d want 10", stats.Total)
	}
	if stats.Completed != 10 {
		t.Fatalf("completed=%d want 10 (stats=%+v)", stats.Completed, stats)
	}
	for _, r := range results {
		if r.Outcome != backend.OutcomeCompleted {
			t.Fatalf("request %s outcome=%s reason=%s", r.RequestID, r.Outcome, r.Reason)
		}
		if r.TTFTNS <= 0 {
			t.Fatalf("request %s has non-positive TTFT", r.RequestID)
		}
		if r.CompletionNS < r.TTFTNS {
			t.Fatalf("request %s completion < ttft", r.RequestID)
		}
		if r.OutputTokens != 16 {
			t.Fatalf("request %s output=%d want 16", r.RequestID, r.OutputTokens)
		}
	}
}

func TestDeterminismSameSeed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Policy = "edf"
	cfg.FailureRatePerMillionSteps = 50
	reqs := linearReqs(40, 500_000, 256, 32) // 0.5ms spacing

	h1 := runHash(t, cfg, reqs)
	h2 := runHash(t, cfg, reqs)
	h3 := runHash(t, cfg, reqs)
	if h1 != h2 || h2 != h3 {
		t.Fatalf("nondeterministic: %s %s %s", h1, h2, h3)
	}
}

func TestDeterminismAcrossFreshRequestSlice(t *testing.T) {
	// Two independently-built but identical traces must produce identical runs.
	cfg := DefaultConfig()
	cfg.Policy = "hybrid"
	a := linearReqs(30, 100_000, 128, 24)
	b := linearReqs(30, 100_000, 128, 24)
	if runHash(t, cfg, a) != runHash(t, cfg, b) {
		t.Fatal("identical traces produced different runs")
	}
}

func TestDifferentPoliciesProduceDifferentOrders(t *testing.T) {
	cfg := DefaultConfig()
	reqs := []*lifecycle.Request{
		mkReq("long", "t0", 0, 0, lifecycle.PriorityLow, 2048, 256),
		mkReq("short", "t0", 0, 0, lifecycle.PriorityCritical, 32, 8),
	}
	cfg.Policy = "sjf"
	_, sjfStats := mustNew(t, cfg, reqs).Run()
	cfg.Policy = "priority"
	_, priStats := mustNew(t, cfg, reqs).Run()
	if sjfStats.Completed != 2 || priStats.Completed != 2 {
		t.Fatalf("both policies should complete both requests: sjf=%+v pri=%+v", sjfStats, priStats)
	}
}

func TestKVBoundCausesQueueing(t *testing.T) {
	// One worker, batch 8, only 4 KV blocks of 16 => 64 tokens. Requests need
	// 128+16=144 tokens => 9 blocks each, so only 0 fit at all beyond... make
	// blocks fit exactly one at a time.
	cfg := DefaultConfig()
	cfg.Policy = "fifo"
	cfg.Workers = 1
	cfg.MaxBatchSize = 8
	cfg.KVBlockSize = 16
	cfg.KVBlocksPerWorker = 9 // exactly one 144-token request (9 blocks)
	reqs := linearReqs(4, 0, 128, 16)
	_, stats := mustNew(t, cfg, reqs).Run()
	if stats.Completed != 4 {
		t.Fatalf("serialized execution should still complete all: %+v", stats)
	}
	if stats.PeakActiveSequences > 1 {
		t.Fatalf("KV bound should serialize to 1 active sequence, got %d", stats.PeakActiveSequences)
	}
}

func TestRejectionUnderQueueBound(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Policy = "fifo"
	cfg.Admission.Strategy = "reject"
	cfg.Admission.MaxQueueLength = 2
	cfg.Admission.MaxOutstanding = 2
	cfg.Workers = 1
	cfg.MaxBatchSize = 1
	// All arrive at once; only a couple can be outstanding, rest rejected.
	reqs := linearReqs(20, 0, 128, 64)
	_, stats := mustNew(t, cfg, reqs).Run()
	if stats.Rejected == 0 {
		t.Fatalf("expected rejections under bound, got %+v", stats)
	}
	if stats.Completed+stats.Rejected+stats.Cancelled+stats.Expired+stats.Failed != stats.Total {
		t.Fatalf("outcomes do not partition total: %+v", stats)
	}
}

func TestDeadlineExpiry(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Policy = "fifo"
	cfg.Workers = 1
	cfg.MaxBatchSize = 1
	cfg.KVBlocksPerWorker = 1024
	// First request is long; second has an early deadline and will expire while
	// queued behind the first.
	long := mkReq("long", "t0", 0, 0, lifecycle.PriorityNormal, 512, 200)
	short := mkReq("short", "t0", 0, 50_000_000, lifecycle.PriorityNormal, 512, 200) // 50ms deadline
	_, stats := mustNew(t, cfg, []*lifecycle.Request{long, short}).Run()
	if stats.Expired == 0 {
		t.Fatalf("expected one expiry, got %+v", stats)
	}
}

func TestWorkerFailureFailsActiveRequests(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Policy = "fifo"
	cfg.Workers = 2
	cfg.MaxBatchSize = 4
	cfg.KVBlocksPerWorker = 1024
	cfg.FailureRatePerMillionSteps = 0
	reqs := linearReqs(8, 0, 256, 64)
	s := mustNew(t, cfg, reqs)
	// Schedule a failure at t=1ms on worker 0 deterministically.
	s.push(EventWorkerFailure, 1_000_000, -1, 0)
	_, stats := s.Run()
	if stats.Failed == 0 {
		t.Fatalf("expected some failover failures, got %+v", stats)
	}
	if stats.Completed == 0 {
		t.Fatalf("worker 1 should still complete work, got %+v", stats)
	}
	// Resources must not leak.
	if err := s.res.Invariants(); err != nil {
		t.Fatalf("resource leak after failures: %v", err)
	}
}

func TestNoResourceLeakAfterRun(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Policy = "edf"
	cfg.FailureRatePerMillionSteps = 100
	reqs := linearReqs(50, 200_000, 128, 32)
	s := mustNew(t, cfg, reqs)
	s.Run()
	if err := s.res.Invariants(); err != nil {
		t.Fatalf("resource leak: %v", err)
	}
	snap := s.res.Snapshot()
	if snap.Reservations != 0 || snap.KVBlocksUsed != 0 || snap.SlotsUsed != 0 {
		t.Fatalf("resources not fully released: %+v", snap)
	}
}

func TestAdmissionInvariantsAfterRun(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Admission.MaxQueueLength = 5
	cfg.Admission.MaxOutstanding = 10
	reqs := linearReqs(60, 100_000, 128, 16)
	s := mustNew(t, cfg, reqs)
	s.Run()
	if err := s.adm.Invariants(); err != nil {
		t.Fatalf("admission invariants: %v", err)
	}
}

func TestHorizonTerminates(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Policy = "fifo"
	cfg.HorizonNS = 1_000_000 // 1ms
	reqs := linearReqs(1000, 10_000, 128, 1000)
	_, stats := mustNew(t, cfg, reqs).Run()
	if stats.Total != 1000 {
		t.Fatalf("all requests must be accounted for, got %d", stats.Total)
	}
	// Later arrivals beyond the horizon should be unreported by arrival.
	if stats.Completed == 1000 {
		t.Fatalf("horizon should truncate the run")
	}
}

func TestRequestTooLargeToEverFitIsRejected(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Policy = "fifo"
	cfg.Workers = 1
	cfg.MaxBatchSize = 4
	cfg.KVBlockSize = 16
	cfg.KVBlocksPerWorker = 16 // 256 tokens total
	// Needs 128+1024 = 1152 tokens = 72 blocks > 16 available, can never fit.
	reqs := []*lifecycle.Request{
		mkReq("huge", "t0", 0, 0, lifecycle.PriorityNormal, 128, 1024),
	}
	_, stats := mustNew(t, cfg, reqs).Run()
	if stats.Rejected != 1 {
		t.Fatalf("oversized request should be rejected, got %+v", stats)
	}
	if stats.Failed != 0 || stats.Completed != 0 {
		t.Fatalf("oversized request should not fail/hang: %+v", stats)
	}
}

func TestInvalidConfig(t *testing.T) {
	if _, err := New(Config{Workers: 0}, nil); err == nil {
		t.Fatal("expected error for zero workers")
	}
	if _, err := New(Config{Workers: 1, MaxBatchSize: 1, KVBlockSize: 16, KVBlocksPerWorker: 1, Policy: "nope"}, nil); err == nil {
		t.Fatal("expected error for unknown policy")
	}
}

// runHash runs the simulator and returns a deterministic fingerprint of the
// results, used to detect nondeterminism.
func runHash(t *testing.T, cfg Config, reqs []*lifecycle.Request) string {
	t.Helper()
	results, stats := mustNew(t, cfg, reqs).Run()
	return fingerprint(results, stats)
}

func fingerprint(results []Result, stats Stats) string {
	var b []byte
	b = append(b, []byte(fmt.Sprintf("stats:%+v|", stats))...)
	for _, r := range results {
		b = append(b, []byte(fmt.Sprintf("%s:%s:%d:%d:%d:%d:%d;",
			r.RequestID, r.Outcome, r.StartNS, r.FirstTokenNS, r.EndNS, r.OutputTokens, r.WorkerID))...)
	}
	return string(b)
}
