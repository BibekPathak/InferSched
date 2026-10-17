package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/BibekP/infersched/internal/admission"
	"github.com/BibekP/infersched/internal/backend"
	"github.com/BibekP/infersched/internal/coordinator"
	"github.com/BibekP/infersched/internal/observability"
	"github.com/BibekP/infersched/internal/resources"
)

func newTestServer(t *testing.T, mock *backend.MockBackend) (*httptest.Server, *coordinator.Coordinator) {
	t.Helper()
	coord, err := coordinator.New(coordinator.Config{
		Policy: "fifo",
		Resources: resources.Config{
			Workers: 1, MaxBatchSize: 4, KVBlockSize: 16, KVBlocksPerWorker: 64,
		},
		Admission: admission.Config{Strategy: admission.StrategyReject},
		Backend:   mock,
		Metrics:   observability.New(),
		Logger:    observability.NewLogger(io.Discard, slog.LevelError),
	})
	if err != nil {
		t.Fatal(err)
	}
	coord.Start()
	srv := httptest.NewServer(NewServer(coord))
	t.Cleanup(func() {
		srv.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = coord.Shutdown(ctx)
	})
	return srv, coord
}

func TestHealth(t *testing.T) {
	srv, _ := newTestServer(t, backend.NewMockBackend(time.Millisecond, nil))
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestSubmitAndQueryLifecycle(t *testing.T) {
	srv, _ := newTestServer(t, backend.NewMockBackend(20*time.Millisecond, nil))

	body := `{"id":"api-1","prompt_tokens":128,"expected_output_tokens":16,"priority":50}`
	resp, err := http.Post(srv.URL+"/v1/requests", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("submit status=%d body=%s", resp.StatusCode, b)
	}
	resp.Body.Close()

	// Poll until completed.
	deadline := time.Now().Add(2 * time.Second)
	var last coordinator.RequestStatus
	for time.Now().Before(deadline) {
		r, err := http.Get(srv.URL + "/v1/requests/api-1")
		if err != nil {
			t.Fatal(err)
		}
		json.NewDecoder(r.Body).Decode(&last)
		r.Body.Close()
		if last.State == "COMPLETED" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if last.State != "COMPLETED" {
		t.Fatalf("request did not complete: %+v", last)
	}
	if last.Outcome != "completed" {
		t.Fatalf("outcome=%s", last.Outcome)
	}
}

func TestSubmitInvalidJSON(t *testing.T) {
	srv, _ := newTestServer(t, backend.NewMockBackend(time.Millisecond, nil))
	resp, err := http.Post(srv.URL+"/v1/requests", "application/json", strings.NewReader(`{"prompt_tokens":`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", resp.StatusCode)
	}
}

func TestSubmitRejectsUnknownFields(t *testing.T) {
	srv, _ := newTestServer(t, backend.NewMockBackend(time.Millisecond, nil))
	resp, err := http.Post(srv.URL+"/v1/requests", "application/json",
		strings.NewReader(`{"prompt_tokens":10,"bogus":1}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", resp.StatusCode)
	}
}

func TestSubmitValidation(t *testing.T) {
	srv, _ := newTestServer(t, backend.NewMockBackend(time.Millisecond, nil))
	resp, err := http.Post(srv.URL+"/v1/requests", "application/json",
		strings.NewReader(`{"prompt_tokens":0}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", resp.StatusCode)
	}
}

func TestCancelEndpoint(t *testing.T) {
	srv, _ := newTestServer(t, backend.NewMockBackend(time.Hour, nil))
	body := `{"id":"c-1","prompt_tokens":64,"expected_output_tokens":8}`
	resp, _ := http.Post(srv.URL+"/v1/requests", "application/json", strings.NewReader(body))
	resp.Body.Close()

	// Wait until running.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r, _ := http.Get(srv.URL + "/v1/requests/c-1")
		var st coordinator.RequestStatus
		json.NewDecoder(r.Body).Decode(&st)
		r.Body.Close()
		if st.State == "RUNNING" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/requests/c-1/cancel", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("cancel status=%d", resp.StatusCode)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r, _ := http.Get(srv.URL + "/v1/requests/c-1")
		var st coordinator.RequestStatus
		json.NewDecoder(r.Body).Decode(&st)
		r.Body.Close()
		if st.State == "CANCELLED" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("request not cancelled")
}

func TestUnknownRequest404(t *testing.T) {
	srv, _ := newTestServer(t, backend.NewMockBackend(time.Millisecond, nil))
	resp, err := http.Get(srv.URL + "/v1/requests/ghost")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d want 404", resp.StatusCode)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	srv, _ := newTestServer(t, backend.NewMockBackend(time.Millisecond, nil))
	resp, err := http.Get(srv.URL + "/v1/requests")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d want 405", resp.StatusCode)
	}
}

func TestStatusEndpoints(t *testing.T) {
	srv, _ := newTestServer(t, backend.NewMockBackend(time.Millisecond, nil))
	for _, path := range []string{"/v1/scheduler", "/v1/resources"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status=%d", path, resp.StatusCode)
		}
		resp.Body.Close()
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv, _ := newTestServer(t, backend.NewMockBackend(time.Millisecond, nil))
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(body, []byte("infersched_requests_admitted_total")) {
		t.Fatalf("metrics missing expected series:\n%s", body)
	}
}
