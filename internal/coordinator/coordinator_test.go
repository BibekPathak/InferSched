package coordinator

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/BibekP/infersched/internal/admission"
	"github.com/BibekP/infersched/internal/backend"
	"github.com/BibekP/infersched/internal/lifecycle"
	"github.com/BibekP/infersched/internal/observability"
	"github.com/BibekP/infersched/internal/resources"
)

func testConfig(mock *backend.MockBackend) Config {
	return Config{
		Policy: "fifo",
		Resources: resources.Config{
			Workers: 1, MaxBatchSize: 4, KVBlockSize: 16, KVBlocksPerWorker: 64,
		},
		Admission: admission.Config{Strategy: admission.StrategyReject},
		Backend:   mock,
		Metrics:   observability.New(),
		Logger:    observability.NewLogger(io.Discard, slog.LevelError),
	}
}

func waitForState(t *testing.T, c *Coordinator, id string, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := c.RequestStatus(id)
		if err == nil && st.State == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	st, _ := c.RequestStatus(id)
	t.Fatalf("request %s state=%s want %s", id, st.State, want)
}

func TestCoordinatorBasicLifecycle(t *testing.T) {
	mock := backend.NewMockBackend(10*time.Millisecond, nil)
	c, err := New(testConfig(mock))
	if err != nil {
		t.Fatal(err)
	}
	c.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Shutdown(ctx)
	}()

	id, err := c.SubmitRequest(Submission{PromptTokens: 128, ExpectedOutputTokens: 16})
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, c, id, string(lifecycle.StateCompleted), 2*time.Second)

	st, _ := c.RequestStatus(id)
	if st.Outcome != string(backend.OutcomeCompleted) {
		t.Fatalf("outcome=%s", st.Outcome)
	}
	if st.AdmitNS == 0 || st.StartNS == 0 || st.EndNS == 0 {
		t.Fatalf("missing timestamps: %+v", st)
	}
	if got := c.Metrics().CounterValue(observability.MetricRequestsCompleted, map[string]string{"outcome": "completed"}); got != 1 {
		t.Fatalf("completed metric=%d want 1", got)
	}
}

func TestCoordinatorRejectsInvalid(t *testing.T) {
	mock := backend.NewMockBackend(time.Millisecond, nil)
	c, _ := New(testConfig(mock))
	c.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Shutdown(ctx)
	}()

	if _, err := c.SubmitRequest(Submission{PromptTokens: 0}); err == nil {
		t.Fatal("expected error for zero prompt tokens")
	}
	if _, err := c.SubmitRequest(Submission{PromptTokens: 10, ExpectedOutputTokens: -1}); err == nil {
		t.Fatal("expected error for negative output tokens")
	}
}

func TestCoordinatorDuplicateID(t *testing.T) {
	mock := backend.NewMockBackend(time.Hour, nil)
	c, _ := New(testConfig(mock))
	c.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Shutdown(ctx)
	}()

	if _, err := c.SubmitRequest(Submission{ID: "dup", PromptTokens: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SubmitRequest(Submission{ID: "dup", PromptTokens: 10}); err == nil {
		t.Fatal("expected duplicate id error")
	}
}

func TestCoordinatorCancelRunning(t *testing.T) {
	mock := backend.NewMockBackend(time.Hour, nil)
	c, _ := New(testConfig(mock))
	c.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Shutdown(ctx)
	}()

	id, err := c.SubmitRequest(Submission{PromptTokens: 64, ExpectedOutputTokens: 8})
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, c, id, string(lifecycle.StateRunning), 2*time.Second)
	if err := c.CancelRequest(id); err != nil {
		t.Fatal(err)
	}
	waitForState(t, c, id, string(lifecycle.StateCancelled), 2*time.Second)
}

func TestCoordinatorReleasesResourcesOnCompletion(t *testing.T) {
	mock := backend.NewMockBackend(5*time.Millisecond, nil)
	c, _ := New(testConfig(mock))
	c.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Shutdown(ctx)
	}()

	var ids []string
	for i := 0; i < 4; i++ {
		id, err := c.SubmitRequest(Submission{PromptTokens: 64, ExpectedOutputTokens: 8})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	for _, id := range ids {
		waitForState(t, c, id, string(lifecycle.StateCompleted), 3*time.Second)
	}
	// Give the loop a moment to publish gauges.
	time.Sleep(20 * time.Millisecond)
	rs := c.ResourceStatus()
	if rs.KVBlocksUsed != 0 || rs.SlotsUsed != 0 || rs.Reservations != 0 {
		t.Fatalf("resources leaked: %+v", rs)
	}
}

func TestCoordinatorAdmissionRejection(t *testing.T) {
	cfg := testConfig(backend.NewMockBackend(time.Hour, nil))
	cfg.Admission.MaxOutstanding = 1
	mock := cfg.Backend.(*backend.MockBackend)
	c, _ := New(cfg)
	c.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Shutdown(ctx)
	}()
	_ = mock

	// First request runs (long); second is rejected on outstanding limit.
	id1, _ := c.SubmitRequest(Submission{PromptTokens: 64, ExpectedOutputTokens: 8})
	waitForState(t, c, id1, string(lifecycle.StateRunning), 2*time.Second)
	id2, err := c.SubmitRequest(Submission{PromptTokens: 64, ExpectedOutputTokens: 8})
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, c, id2, string(lifecycle.StateRejected), 2*time.Second)
	st, _ := c.RequestStatus(id2)
	if st.Reason != string(lifecycle.ReasonOverloadFlights) {
		t.Fatalf("reason=%s want overload", st.Reason)
	}
}

func TestCoordinatorShutdownWithOutstanding(t *testing.T) {
	mock := backend.NewMockBackend(time.Hour, nil)
	c, _ := New(testConfig(mock))
	c.Start()

	for i := 0; i < 3; i++ {
		if _, err := c.SubmitRequest(Submission{PromptTokens: 64, ExpectedOutputTokens: 8}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	// After shutdown, submissions are refused.
	if _, err := c.SubmitRequest(Submission{PromptTokens: 10}); err != ErrClosed {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

func TestCoordinatorConcurrentSubmit(t *testing.T) {
	mock := backend.NewMockBackend(2*time.Millisecond, nil)
	cfg := testConfig(mock)
	cfg.Resources.MaxBatchSize = 8
	cfg.Resources.KVBlocksPerWorker = 512
	cfg.Admission.MaxOutstanding = 1000
	cfg.Admission.MaxQueueLength = 1000
	c, _ := New(cfg)
	c.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = c.Shutdown(ctx)
	}()

	const n = 200
	done := make(chan string, n)
	for i := 0; i < n; i++ {
		go func() {
			id, err := c.SubmitRequest(Submission{PromptTokens: 32, ExpectedOutputTokens: 8})
			if err != nil {
				done <- ""
				return
			}
			done <- id
		}()
	}
	var ids []string
	for i := 0; i < n; i++ {
		if id := <-done; id != "" {
			ids = append(ids, id)
		}
	}
	for _, id := range ids {
		waitForState(t, c, id, string(lifecycle.StateCompleted), 5*time.Second)
	}
	snap := c.ResourceStatus()
	if snap.KVBlocksUsed != 0 || snap.SlotsUsed != 0 {
		t.Fatalf("resources leaked after concurrent load: %+v", snap)
	}
}

func TestCoordinatorCancelQueued(t *testing.T) {
	// One worker, batch size 1: the second request stays queued behind the
	// first (which runs for an hour). Cancelling the queued request must move it
	// to CANCELLED without a worker ever running it, and must not leak queue
	// accounting.
	cfg := testConfig(backend.NewMockBackend(time.Hour, nil))
	cfg.Resources.MaxBatchSize = 1
	cfg.Admission.MaxOutstanding = 100
	c, _ := New(cfg)
	c.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Shutdown(ctx)
	}()

	id1, err := c.SubmitRequest(Submission{ID: "running", PromptTokens: 64, ExpectedOutputTokens: 8})
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, c, id1, string(lifecycle.StateRunning), 2*time.Second)

	id2, err := c.SubmitRequest(Submission{ID: "queued", PromptTokens: 64, ExpectedOutputTokens: 8})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := c.RequestStatus(id2)
		if st.State == string(lifecycle.StateQueued) && st.AdmitNS > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if err := c.CancelRequest(id2); err != nil {
		t.Fatalf("cancel queued: %v", err)
	}
	waitForState(t, c, id2, string(lifecycle.StateCancelled), 2*time.Second)

	st, _ := c.RequestStatus(id2)
	if st.Outcome != string(backend.OutcomeCancelled) {
		t.Fatalf("outcome=%s want cancelled", st.Outcome)
	}
	if st.Reason != string(lifecycle.ReasonClientCancel) {
		t.Fatalf("reason=%s want client cancel", st.Reason)
	}
	if err := c.adm.Invariants(); err != nil {
		t.Fatalf("admission invariants after queued cancel: %v", err)
	}
}
