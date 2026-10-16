package admission

import (
	"context"
	"testing"
	"time"

	"github.com/BibekP/infersched/internal/lifecycle"
	"github.com/BibekP/infersched/internal/resources"
)

func resPool() *resources.Manager {
	return resources.New(resources.Config{Workers: 2, MaxBatchSize: 8, KVBlockSize: 16, KVBlocksPerWorker: 64})
}

func req(id, tenant string, kvTokens, deadline int64) *lifecycle.Request {
	return lifecycle.NewRequest(id, tenant, lifecycle.ClassInteractive, lifecycle.PriorityNormal,
		time.Time{}, 0, deadline, lifecycle.CostEstimate{
			PrefillTokens:        int(kvTokens),
			ExpectedOutputTokens: 0,
			KVTokens:             int(kvTokens),
		}, context.Background())
}

func withWork(id string, kvTokens, deadline int64, workMS float64) *lifecycle.Request {
	r := req(id, "t0", kvTokens, deadline)
	r.Estimate.PrefillTokens = 1
	r.Estimate.PrefillCostMS = workMS
	return r
}

func TestAdmitWithinBounds(t *testing.T) {
	c := New(Config{MaxQueueLength: 10, MaxQueuedTokens: 1000, MaxOutstanding: 10}, resPool(), nil)
	d := c.Evaluate(req("r1", "t0", 100, 0))
	if !d.Admit {
		t.Fatalf("expected admit, got reject %s", d.Reason)
	}
	c.Commit(req("r1", "t0", 100, 0))
	if err := c.Invariants(); err != nil {
		t.Fatal(err)
	}
}

func TestRejectQueueLength(t *testing.T) {
	c := New(Config{MaxQueueLength: 2}, resPool(), nil)
	for i := 0; i < 2; i++ {
		r := req("q", "t0", 10, 0)
		if !c.Evaluate(r).Admit {
			t.Fatal("should admit within queue length")
		}
		c.Commit(r)
	}
	d := c.Evaluate(req("overflow", "t0", 10, 0))
	if d.Admit || d.Reason != lifecycle.ReasonOverloadQueue {
		t.Fatalf("want queue-length rejection, got %+v", d)
	}
}

func TestRejectQueuedTokens(t *testing.T) {
	c := New(Config{MaxQueuedTokens: 250}, resPool(), nil)
	c.Commit(req("r1", "t0", 200, 0))
	d := c.Evaluate(req("r2", "t0", 100, 0))
	if d.Admit || d.Reason != lifecycle.ReasonOverloadTokens {
		t.Fatalf("want token rejection, got %+v", d)
	}
}

func TestRejectOutstanding(t *testing.T) {
	c := New(Config{MaxOutstanding: 1}, resPool(), nil)
	c.MarkOutstanding()
	d := c.Evaluate(req("r1", "t0", 10, 0))
	if d.Admit || d.Reason != lifecycle.ReasonOverloadFlights {
		t.Fatalf("want outstanding rejection, got %+v", d)
	}
}

func TestRejectPerTenant(t *testing.T) {
	c := New(Config{MaxPerTenant: 1}, resPool(), nil)
	c.Commit(req("r1", "tenant-a", 10, 0))
	d := c.Evaluate(req("r2", "tenant-a", 10, 0))
	if d.Admit || d.Reason != lifecycle.ReasonOverloadTenant {
		t.Fatalf("want tenant rejection, got %+v", d)
	}
	// A different tenant is unaffected.
	if !c.Evaluate(req("r3", "tenant-b", 10, 0)).Admit {
		t.Fatal("other tenant should be admitted")
	}
}

func TestImmediateStrategyIgnoresDeadline(t *testing.T) {
	c := New(Config{Strategy: StrategyReject}, resPool(), nil)
	// A hopeless deadline still admits under the immediate strategy.
	d := c.Evaluate(withWork("r1", 10, 1, 999999))
	if !d.Admit {
		t.Fatalf("immediate strategy should not check deadlines, got %+v", d)
	}
}

func TestDeadlineAwareRejectsInfeasible(t *testing.T) {
	now := int64(1000) * 1e6 // 1000ms
	c := New(Config{Strategy: StrategyDeadlineAware, FeasibilitySlackMS: 0}, resPool(),
		func() int64 { return now })
	// 500ms work, only 100ms slack => infeasible.
	infeasible := withWork("late", 10, now+100*1e6, 500)
	d := c.Evaluate(infeasible)
	if d.Admit || d.Reason != lifecycle.ReasonDeadlineMiss {
		t.Fatalf("want deadline rejection, got %+v", d)
	}
	// 500ms work, 5000ms slack => feasible.
	feasible := withWork("ok", 10, now+5000*1e6, 500)
	if !c.Evaluate(feasible).Admit {
		t.Fatal("feasible request should be admitted")
	}
}

func TestDeadlineAwareSlackMargin(t *testing.T) {
	now := int64(0)
	c := New(Config{Strategy: StrategyDeadlineAware, FeasibilitySlackMS: 400}, resPool(),
		func() int64 { return now })
	// 500ms work, 800ms slack, but 400ms margin => 400ms available < 500 => reject.
	d := c.Evaluate(withWork("tight", 10, 800*1e6, 500))
	if d.Admit {
		t.Fatalf("slack margin should make this infeasible, got %+v", d)
	}
}

func TestDeadlineLessAlwaysFeasible(t *testing.T) {
	c := New(Config{Strategy: StrategyDeadlineAware}, resPool(), nil)
	if !c.Evaluate(withWork("nodl", 10, 0, 999999)).Admit {
		t.Fatal("deadline-less request should admit under deadline-aware strategy")
	}
}

func TestDequeueAndMarkDoneMaintainCounters(t *testing.T) {
	c := New(Config{MaxQueueLength: 10, MaxOutstanding: 10}, resPool(), nil)
	r := req("r1", "t0", 100, 0)
	c.Commit(r)
	if s := c.Stats(); s.QueuedLength != 1 || s.QueuedTokens != 100 {
		t.Fatalf("unexpected stats after commit: %+v", s)
	}
	c.Dequeue(r)
	c.MarkOutstanding()
	if s := c.Stats(); s.QueuedLength != 0 || s.QueuedTokens != 0 || s.Outstanding != 1 {
		t.Fatalf("unexpected stats after dequeue: %+v", s)
	}
	c.MarkDone(r)
	if s := c.Stats(); s.Outstanding != 0 {
		t.Fatalf("outstanding not decremented: %+v", s)
	}
	if err := c.Invariants(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryAfterOverload(t *testing.T) {
	c := New(Config{MaxQueueLength: 3, MaxOutstanding: 3}, resPool(), nil)
	// Fill the queue.
	reqs := make([]*lifecycle.Request, 0, 3)
	for i := 0; i < 3; i++ {
		r := req("q", "t0", 10, 0)
		c.Commit(r)
		reqs = append(reqs, r)
	}
	if c.Evaluate(req("x", "t0", 10, 0)).Admit {
		t.Fatal("should be overloaded")
	}
	// Drain and verify admission resumes.
	for _, r := range reqs {
		c.Dequeue(r)
		c.MarkOutstanding()
		c.MarkDone(r)
	}
	if !c.Evaluate(req("y", "t0", 10, 0)).Admit {
		t.Fatal("should recover after overload subsides")
	}
	if err := c.Invariants(); err != nil {
		t.Fatal(err)
	}
}
