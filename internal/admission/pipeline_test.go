package admission

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/BibekP/infersched/internal/lifecycle"
	"github.com/BibekP/infersched/internal/resources"
)

// admissionPipeline models the coordinator's admit path: Evaluate -> Reserve ->
// Commit, with a resource rollback if reservation fails. It exists to prove the
// two components compose safely under concurrency.
type admissionPipeline struct {
	c   *Controller
	res *resources.Manager
}

// tryAdmit returns the admitted request or a rejection reason. On resource
// failure it rolls back cleanly (nothing was committed before Reserve).
func (p *admissionPipeline) tryAdmit(r *lifecycle.Request) (bool, lifecycle.Reason, error) {
	d := p.c.Evaluate(r)
	if !d.Admit {
		return false, d.Reason, nil
	}
	if _, err := p.res.Reserve(resources.Need{RequestID: r.ID, KVTokens: EstimateKVTokens(r)}); err != nil {
		if errors.Is(err, resources.ErrInsufficientCapacity) {
			return false, lifecycle.ReasonOverloadTokens, nil
		}
		return false, lifecycle.ReasonNone, err
	}
	p.c.Commit(r)
	p.c.Dequeue(r)
	p.c.MarkOutstanding()
	return true, lifecycle.ReasonNone, nil
}

// finish releases resources and updates counters exactly once.
func (p *admissionPipeline) finish(r *lifecycle.Request) error {
	if _, err := p.res.Release(r.ID); err != nil {
		return err
	}
	p.c.MarkDone(r)
	return nil
}

func TestPipelineAdmitAndFinish(t *testing.T) {
	res := resources.New(resources.Config{Workers: 1, MaxBatchSize: 4, KVBlockSize: 16, KVBlocksPerWorker: 8})
	c := New(Config{MaxQueueLength: 8, MaxOutstanding: 4}, res, nil)
	p := &admissionPipeline{c: c, res: res}

	r := req("r1", "t0", 16, 0) // 1 KV block
	ok, reason, err := p.tryAdmit(r)
	if err != nil || !ok {
		t.Fatalf("admit failed: ok=%v reason=%s err=%v", ok, reason, err)
	}
	if s := res.Snapshot(); s.Reservations != 1 || s.KVBlocksUsed != 1 {
		t.Fatalf("resource not reserved: %+v", s)
	}
	if err := p.finish(r); err != nil {
		t.Fatal(err)
	}
	if s := res.Snapshot(); s.Reservations != 0 || s.KVBlocksUsed != 0 {
		t.Fatalf("resource not released: %+v", s)
	}
	if err := res.Invariants(); err != nil {
		t.Fatal(err)
	}
	if err := c.Invariants(); err != nil {
		t.Fatal(err)
	}
}

func TestPipelineResourceBoundRollsBackCleanly(t *testing.T) {
	// 8 KV blocks total, requests of 32 tokens => 2 blocks each => 4 fit.
	res := resources.New(resources.Config{Workers: 1, MaxBatchSize: 100, KVBlockSize: 16, KVBlocksPerWorker: 8})
	c := New(Config{MaxQueueLength: 100, MaxOutstanding: 100}, res, nil)
	p := &admissionPipeline{c: c, res: res}

	admitted := 0
	for i := 0; i < 6; i++ {
		r := req(string(rune('a'+i)), "t0", 32, 0)
		ok, reason, err := p.tryAdmit(r)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if ok {
			admitted++
		} else if reason != lifecycle.ReasonOverloadTokens {
			t.Fatalf("expected token overload reason, got %s", reason)
		}
	}
	if admitted != 4 {
		t.Fatalf("want 4 admitted (KV-bound), got %d", admitted)
	}
	// The controller must not have counted rejected requests as outstanding.
	if s := c.Stats(); s.Outstanding != 4 {
		t.Fatalf("outstanding should be 4, got %d", s.Outstanding)
	}
	if err := res.Invariants(); err != nil {
		t.Fatal(err)
	}
}

func TestPipelineConcurrentSubmissionNoLeak(t *testing.T) {
	res := resources.New(resources.Config{Workers: 2, MaxBatchSize: 8, KVBlockSize: 16, KVBlocksPerWorker: 16})
	c := New(Config{MaxQueueLength: 64, MaxOutstanding: 16}, res, nil)
	// Controller counters are not concurrent-safe by contract; serialize the
	// controller path with a mutex, mirroring the coordinator's single scheduler
	// goroutine, while exercising the concurrent-safe resource Manager.
	var mu sync.Mutex
	p := &admissionPipeline{c: c, res: res}

	const goroutines = 16
	const iters = 100
	var wg sync.WaitGroup
	var admitted, rejected int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				r := req(idFor(g, i), "t0", 16, 0)
				mu.Lock()
				ok, _, err := p.tryAdmit(r)
				mu.Unlock()
				if err != nil {
					t.Errorf("admit: %v", err)
					return
				}
				if ok {
					atomic.AddInt64(&admitted, 1)
					// Simulate brief execution then finish.
					mu.Lock()
					err := p.finish(r)
					mu.Unlock()
					if err != nil {
						t.Errorf("finish: %v", err)
						return
					}
				} else {
					atomic.AddInt64(&rejected, 1)
				}
			}
		}(g)
	}
	wg.Wait()

	if err := res.Invariants(); err != nil {
		t.Fatalf("resource invariants after stress: %v", err)
	}
	if err := c.Invariants(); err != nil {
		t.Fatalf("admission invariants after stress: %v", err)
	}
	if s := res.Snapshot(); s.Reservations != 0 || s.KVBlocksUsed != 0 || s.SlotsUsed != 0 {
		t.Fatalf("resources leaked: %+v", s)
	}
	if s := c.Stats(); s.Outstanding != 0 || s.QueuedLength != 0 {
		t.Fatalf("admission counters leaked: %+v", s)
	}
	t.Logf("admitted=%d rejected=%d", admitted, rejected)
}

func idFor(g, i int) string {
	return "g" + itoa(g) + "-i" + itoa(i)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
