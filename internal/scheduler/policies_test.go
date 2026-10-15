package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/BibekP/infersched/internal/lifecycle"
)

// mkReq builds a request with explicit scheduling metadata for policy tests.
func mkReq(id string, arrival, deadline int64, pri lifecycle.Priority, est lifecycle.CostEstimate) *lifecycle.Request {
	return lifecycle.NewRequest(id, "t0", lifecycle.ClassInteractive, pri,
		time.Time{}, arrival, deadline, est, context.Background())
}

func est(prefillMS float64, outTokens int, decodePerMS float64) lifecycle.CostEstimate {
	return lifecycle.CostEstimate{
		PrefillTokens:        1,
		ExpectedOutputTokens: outTokens,
		PrefillCostMS:        prefillMS,
		DecodeCostPerTokenMS: decodePerMS,
		KVTokens:             outTokens,
	}
}

func eqStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func wantOrder(t *testing.T, got []*lifecycle.Request, want []string) {
	t.Helper()
	g := make([]string, len(got))
	for i, r := range got {
		g[i] = r.ID
	}
	if !eqStrs(g, want) {
		t.Fatalf("order mismatch:\n got  %v\n want %v", g, want)
	}
}

func TestPriorityOrdering(t *testing.T) {
	p := NewPriority(Aging{})
	in := []*lifecycle.Request{
		mkReq("low", 10, 0, lifecycle.PriorityLow, est(1, 10, 1)),
		mkReq("crit", 30, 0, lifecycle.PriorityCritical, est(1, 10, 1)),
		mkReq("norm", 20, 0, lifecycle.PriorityNormal, est(1, 10, 1)),
	}
	wantOrder(t, p.Order(in), []string{"crit", "norm", "low"})
}

func TestPriorityTieBreakByArrival(t *testing.T) {
	p := NewPriority(Aging{})
	in := []*lifecycle.Request{
		mkReq("b", 20, 0, lifecycle.PriorityHigh, est(1, 10, 1)),
		mkReq("a", 10, 0, lifecycle.PriorityHigh, est(1, 10, 1)),
	}
	wantOrder(t, p.Order(in), []string{"a", "b"})
}

func TestPriorityAgingReorders(t *testing.T) {
	// "old" arrived long ago at low priority; "new" is high priority but fresh.
	// With aging, old should accumulate enough boost to overtake new.
	now := int64(60_000) * 1e6 // 60s in ns
	ag := Aging{
		Config:  AgingConfig{AgingMS: 1000, BoostPerSec: 100, MaxBoost: 1000},
		NowFunc: func() int64 { return now },
	}
	p := NewPriority(ag)
	old := mkReq("old", 0, 0, lifecycle.PriorityLow, est(1, 10, 1))             // waited 60s
	fresh := mkReq("new", 59_000*1e6, 0, lifecycle.PriorityHigh, est(1, 10, 1)) // waited 1s
	wantOrder(t, p.Order([]*lifecycle.Request{fresh, old}), []string{"old", "new"})
}

func TestSJFOrdering(t *testing.T) {
	s := NewSJF(DefaultCostModel())
	in := []*lifecycle.Request{
		mkReq("long", 10, 0, lifecycle.PriorityNormal, est(10, 1000, 1)), // ~1010ms
		mkReq("short", 20, 0, lifecycle.PriorityNormal, est(1, 10, 1)),   // ~11ms
		mkReq("mid", 30, 0, lifecycle.PriorityNormal, est(5, 100, 1)),    // ~105ms
	}
	wantOrder(t, s.Order(in), []string{"short", "mid", "long"})
}

func TestSJFIgnoresPriority(t *testing.T) {
	s := NewSJF(DefaultCostModel())
	in := []*lifecycle.Request{
		mkReq("hi-long", 10, 0, lifecycle.PriorityCritical, est(10, 1000, 1)),
		mkReq("lo-short", 20, 0, lifecycle.PriorityLow, est(1, 10, 1)),
	}
	wantOrder(t, s.Order(in), []string{"lo-short", "hi-long"})
}

func TestEDFOrdering(t *testing.T) {
	e := NewEDF(DefaultCostModel())
	in := []*lifecycle.Request{
		mkReq("d3", 10, 3000, lifecycle.PriorityNormal, est(1, 10, 1)),
		mkReq("d1", 10, 1000, lifecycle.PriorityNormal, est(1, 10, 1)),
		mkReq("d2", 10, 2000, lifecycle.PriorityNormal, est(1, 10, 1)),
	}
	wantOrder(t, e.Order(in), []string{"d1", "d2", "d3"})
}

func TestEDFDeadlineLessLast(t *testing.T) {
	e := NewEDF(DefaultCostModel())
	in := []*lifecycle.Request{
		mkReq("none1", 10, 0, lifecycle.PriorityNormal, est(1, 10, 1)),
		mkReq("dl1", 20, 5000, lifecycle.PriorityNormal, est(1, 10, 1)),
		mkReq("none2", 5, 0, lifecycle.PriorityNormal, est(1, 10, 1)),
	}
	// deadline-bearing first; deadline-less ordered by arrival (none2 before none1).
	wantOrder(t, e.Order(in), []string{"dl1", "none2", "none1"})
}

func TestEDFFeasibility(t *testing.T) {
	now := int64(1000) * 1e6 // 1000ms
	e := NewEDF(DefaultCostModel())
	e.NowFunc = func() int64 { return now }
	// 500ms of work, 200ms of slack => infeasible.
	late := mkReq("late", 0, now+200*1e6, lifecycle.PriorityNormal, est(500, 0, 1))
	// 500ms of work, 5000ms of slack => feasible.
	ok := mkReq("ok", 0, now+5000*1e6, lifecycle.PriorityNormal, est(500, 0, 1))
	none := mkReq("none", 0, 0, lifecycle.PriorityNormal, est(500, 0, 1))
	if e.Feasible(late) {
		t.Fatal("late should be infeasible")
	}
	if !e.Feasible(ok) {
		t.Fatal("ok should be feasible")
	}
	if !e.Feasible(none) {
		t.Fatal("deadline-less should always be feasible")
	}
}

func TestHybridPrefersUrgentDeadline(t *testing.T) {
	now := int64(0)
	h := NewHybrid(DefaultHybridWeights(), DefaultCostModel())
	h.NowFunc = func() int64 { return now }
	// Same work/priority; one much closer to its deadline.
	urgent := mkReq("urgent", 0, 500*1e6, lifecycle.PriorityNormal, est(1, 10, 1))
	relaxed := mkReq("relaxed", 0, 900_000*1e6, lifecycle.PriorityNormal, est(1, 10, 1))
	wantOrder(t, h.Order([]*lifecycle.Request{relaxed, urgent}), []string{"urgent", "relaxed"})
}

func TestHybridPrefersShortWorkWhenNoDeadlines(t *testing.T) {
	h := NewHybrid(DefaultHybridWeights(), DefaultCostModel())
	h.NowFunc = func() int64 { return 0 }
	short := mkReq("short", 0, 0, lifecycle.PriorityNormal, est(1, 10, 1))
	long := mkReq("long", 0, 0, lifecycle.PriorityNormal, est(100, 1000, 1))
	wantOrder(t, h.Order([]*lifecycle.Request{long, short}), []string{"short", "long"})
}

func TestHybridDeterministicTieBreak(t *testing.T) {
	h := NewHybrid(DefaultHybridWeights(), DefaultCostModel())
	h.NowFunc = func() int64 { return 0 }
	// Identical metadata: order must collapse to arrival-then-ID.
	in := []*lifecycle.Request{
		mkReq("b", 20, 0, lifecycle.PriorityNormal, est(1, 10, 1)),
		mkReq("a", 10, 0, lifecycle.PriorityNormal, est(1, 10, 1)),
		mkReq("c", 20, 0, lifecycle.PriorityNormal, est(1, 10, 1)),
	}
	wantOrder(t, h.Order(in), []string{"a", "b", "c"})
}

func TestAllPoliciesDoNotMutateInput(t *testing.T) {
	in := []*lifecycle.Request{
		mkReq("c", 30, 5000, lifecycle.PriorityHigh, est(5, 100, 1)),
		mkReq("a", 10, 1000, lifecycle.PriorityLow, est(1, 10, 1)),
		mkReq("b", 20, 0, lifecycle.PriorityNormal, est(10, 1000, 1)),
	}
	before := make([]string, len(in))
	for i, r := range in {
		before[i] = r.ID
	}
	for name, p := range Registry() {
		_ = p.Order(in)
		after := make([]string, len(in))
		for i, r := range in {
			after[i] = r.ID
		}
		if !eqStrs(before, after) {
			t.Fatalf("policy %s mutated input: %v -> %v", name, before, after)
		}
	}
}

func TestRegistryNames(t *testing.T) {
	reg := Registry()
	for _, want := range []string{"fifo", "priority", "sjf", "edf", "hybrid"} {
		p, ok := reg[want]
		if !ok {
			t.Fatalf("registry missing %q", want)
		}
		if p.Name() != want {
			t.Fatalf("policy keyed %q reports name %q", want, p.Name())
		}
	}
}

func TestOrderDeterminismRepeated(t *testing.T) {
	// Same input, many repetitions: identical output every time.
	in := []*lifecycle.Request{
		mkReq("d", 40, 5000, lifecycle.PriorityNormal, est(5, 100, 1)),
		mkReq("a", 10, 1000, lifecycle.PriorityHigh, est(1, 10, 1)),
		mkReq("c", 30, 0, lifecycle.PriorityLow, est(10, 1000, 1)),
		mkReq("b", 20, 2000, lifecycle.PriorityNormal, est(3, 50, 1)),
	}
	for name, p := range Registry() {
		p.(interface{ Reset() }).Reset()
		first := idSeq(p.Order(in))
		for i := 0; i < 50; i++ {
			if got := idSeq(p.Order(in)); !eqStrs(got, first) {
				t.Fatalf("policy %s nondeterministic: %v vs %v", name, first, got)
			}
		}
	}
}

func idSeq(reqs []*lifecycle.Request) []string {
	out := make([]string, len(reqs))
	for i, r := range reqs {
		out[i] = r.ID
	}
	return out
}
