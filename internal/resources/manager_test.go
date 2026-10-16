package resources

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func testConfig() Config {
	return Config{Workers: 2, MaxBatchSize: 4, KVBlockSize: 16, KVBlocksPerWorker: 8}
}

func TestBlocksForTokensCeiling(t *testing.T) {
	c := Config{KVBlockSize: 16}
	cases := []struct {
		tokens, want int
	}{
		{0, 0}, {1, 1}, {15, 1}, {16, 1}, {17, 2}, {32, 2}, {33, 3},
	}
	for _, tc := range cases {
		if got := c.BlocksForTokens(tc.tokens); got != tc.want {
			t.Fatalf("tokens=%d blocks=%d want %d", tc.tokens, got, tc.want)
		}
	}
}

func TestReserveAndRelease(t *testing.T) {
	m := New(testConfig())
	r, err := m.Reserve(Need{RequestID: "r1", KVTokens: 20}) // ceil(20/16)=2 blocks
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if r.KVBlocks != 2 || r.Slots != 1 || r.Sequences != 1 {
		t.Fatalf("unexpected reservation: %+v", r)
	}
	s := m.Snapshot()
	if s.SlotsUsed != 1 || s.KVBlocksUsed != 2 || s.Reservations != 1 {
		t.Fatalf("unexpected snapshot: %+v", s)
	}
	if _, err := m.Release("r1"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if s := m.Snapshot(); s.SlotsUsed != 0 || s.KVBlocksUsed != 0 || s.Reservations != 0 {
		t.Fatalf("release did not free resources: %+v", s)
	}
}

func TestReserveIdempotent(t *testing.T) {
	m := New(testConfig())
	r1, err := m.Reserve(Need{RequestID: "r1", KVTokens: 20})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := m.Reserve(Need{RequestID: "r1", KVTokens: 20})
	if err != nil {
		t.Fatal(err)
	}
	if r1 != r2 {
		t.Fatalf("idempotent reserve returned different reservation: %+v vs %+v", r1, r2)
	}
	if s := m.Snapshot(); s.Reservations != 1 || s.KVBlocksUsed != 2 {
		t.Fatalf("idempotent reserve double-counted: %+v", s)
	}
}

func TestReserveRejectsOverCapacity(t *testing.T) {
	m := New(testConfig()) // 16 total KV blocks
	// 15 requests * 2 blocks = 30 > 16; first 8 succeed (16 blocks), rest fail.
	ok, fail := 0, 0
	for i := 0; i < 15; i++ {
		id := "r" + string(rune('a'+i))
		_, err := m.Reserve(Need{RequestID: id, KVTokens: 20})
		if err == nil {
			ok++
		} else if errors.Is(err, ErrInsufficientCapacity) {
			fail++
		} else {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 8 || fail != 7 {
		t.Fatalf("want 8 ok / 7 fail, got %d / %d", ok, fail)
	}
	if err := m.Invariants(); err != nil {
		t.Fatalf("invariants: %v", err)
	}
}

func TestFailedReserveLeavesNoPartialState(t *testing.T) {
	m := New(testConfig())
	for i := 0; i < 8; i++ {
		id := "r" + string(rune('a'+i))
		if _, err := m.Reserve(Need{RequestID: id, KVTokens: 32}); err != nil {
			t.Fatalf("reserve %s: %v", id, err)
		}
	}
	before := m.Snapshot()
	if _, err := m.Reserve(Need{RequestID: "overflow", KVTokens: 32}); !errors.Is(err, ErrInsufficientCapacity) {
		t.Fatalf("want capacity error, got %v", err)
	}
	after := m.Snapshot()
	if before != after {
		t.Fatalf("failed reserve mutated state:\n before %+v\n after  %+v", before, after)
	}
	if m.Holds("overflow") {
		t.Fatal("failed reserve left a reservation record")
	}
}

func TestDoubleReleaseDetected(t *testing.T) {
	m := New(testConfig())
	if _, err := m.Reserve(Need{RequestID: "r1", KVTokens: 16}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Release("r1"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Release("r1"); !errors.Is(err, ErrUnknownReservation) {
		t.Fatalf("double release should fail, got %v", err)
	}
	if err := m.Invariants(); err != nil {
		t.Fatalf("invariants after double release: %v", err)
	}
}

func TestReleaseUnknown(t *testing.T) {
	m := New(testConfig())
	if _, err := m.Release("ghost"); !errors.Is(err, ErrUnknownReservation) {
		t.Fatalf("want unknown reservation error, got %v", err)
	}
}

func TestSlotCapacitySeparateFromKV(t *testing.T) {
	// 1 worker, batch 2, huge KV: slot limit binds first.
	m := New(Config{Workers: 1, MaxBatchSize: 2, KVBlockSize: 16, KVBlocksPerWorker: 1000})
	if _, err := m.Reserve(Need{RequestID: "r1", KVTokens: 16}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Reserve(Need{RequestID: "r2", KVTokens: 16}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Reserve(Need{RequestID: "r3", KVTokens: 16}); !errors.Is(err, ErrInsufficientCapacity) {
		t.Fatalf("third request should exceed slots, got %v", err)
	}
	if err := m.Invariants(); err != nil {
		t.Fatal(err)
	}
}

func TestNegativeUsagePanicsGuarded(t *testing.T) {
	// Ensure the defensive panic path is not reachable under normal use.
	m := New(testConfig())
	if _, err := m.Reserve(Need{RequestID: "r1", KVTokens: 16}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Release("r1"); err != nil {
		t.Fatal(err)
	}
	if err := m.Invariants(); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidConfigPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on invalid config")
		}
	}()
	New(Config{Workers: 0})
}

// TestConcurrentReserveRelease hammers the manager from many goroutines to
// verify capacity is never exceeded and accounting stays consistent. Run with
// -race to also catch data races.
func TestConcurrentReserveRelease(t *testing.T) {
	m := New(testConfig()) // 8 slots, 16 KV blocks
	const goroutines = 32
	const iters = 200

	var (
		wg         sync.WaitGroup
		admits     int64
		rejects    int64
		overallMax int64
	)

	checkMax := func() {
		s := m.Snapshot()
		if s.SlotsUsed > s.SlotsTotal || s.KVBlocksUsed > s.KVBlocksTotal {
			t.Errorf("capacity exceeded: %+v", s)
		}
		if int64(s.SlotsUsed) > atomic.LoadInt64(&overallMax) {
			atomic.StoreInt64(&overallMax, int64(s.SlotsUsed))
		}
	}

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				id := fmt.Sprintf("g%d-i%d", g, i)
				if _, err := m.Reserve(Need{RequestID: id, KVTokens: 20}); err == nil {
					atomic.AddInt64(&admits, 1)
					checkMax()
					if _, err := m.Release(id); err != nil {
						t.Errorf("release %s: %v", id, err)
					}
				} else {
					atomic.AddInt64(&rejects, 1)
				}
			}
		}(g)
	}
	wg.Wait()

	if err := m.Invariants(); err != nil {
		t.Fatalf("invariants after stress: %v", err)
	}
	s := m.Snapshot()
	if s.SlotsUsed != 0 || s.KVBlocksUsed != 0 || s.Reservations != 0 {
		t.Fatalf("resources leaked after stress: %+v", s)
	}
	if admits == 0 || rejects == 0 {
		t.Logf("admits=%d rejects=%d (contention may not have been exercised)", admits, rejects)
	}
}
