// Package api exposes InferSched's runtime over HTTP. It is deliberately thin:
// it validates and decodes client input, delegates to the coordinator, and
// serializes responses. It holds no scheduling logic.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/BibekP/infersched/internal/coordinator"
)

// Server serves the InferSched HTTP API for a Coordinator.
type Server struct {
	coord *coordinator.Coordinator
	mux   *http.ServeMux
}

// NewServer constructs an HTTP server around coord.
func NewServer(coord *coordinator.Coordinator) *Server {
	s := &Server{coord: coord, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/v1/requests", s.handleRequests)
	s.mux.HandleFunc("/v1/requests/", s.handleRequestByID)
	s.mux.HandleFunc("/v1/scheduler", s.handleScheduler)
	s.mux.HandleFunc("/v1/resources", s.handleResources)
	s.mux.HandleFunc("/metrics", s.handleMetrics)
}

// Handler returns the root HTTP handler.
func (s *Server) Handler() http.Handler { return s.mux }

// ServeHTTP makes Server itself an http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// submitRequest is the JSON body for POST /v1/requests.
type submitRequest struct {
	ID                   string  `json:"id"`
	TenantID             string  `json:"tenant_id"`
	Class                string  `json:"class"`
	Priority             int     `json:"priority"`
	PromptTokens         int     `json:"prompt_tokens"`
	ExpectedOutputTokens int     `json:"expected_output_tokens"`
	DeadlineMS           float64 `json:"deadline_ms"`
	KVTokens             int     `json:"kv_tokens"`
	PrefillCostMS        float64 `json:"prefill_cost_ms"`
	DecodeCostPerTokenMS float64 `json:"decode_cost_per_token_ms"`
}

func (s *Server) handleRequests(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.handleSubmit(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var body submitRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	id, err := s.coord.SubmitRequest(coordinator.Submission{
		ID:                   body.ID,
		TenantID:             body.TenantID,
		Class:                body.Class,
		Priority:             body.Priority,
		PromptTokens:         body.PromptTokens,
		ExpectedOutputTokens: body.ExpectedOutputTokens,
		DeadlineMS:           body.DeadlineMS,
		KVTokens:             body.KVTokens,
		PrefillCostMS:        body.PrefillCostMS,
		DecodeCostPerTokenMS: body.DecodeCostPerTokenMS,
	})
	if err != nil {
		if errors.Is(err, coordinator.ErrClosed) {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"id": id, "state": "QUEUED"})
}

// handleRequestByID serves /v1/requests/{id} and /v1/requests/{id}/cancel.
func (s *Server) handleRequestByID(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/requests/")
	if rest == "" {
		writeError(w, http.StatusNotFound, "missing request id")
		return
	}
	parts := strings.SplitN(rest, "/", 2)
	id := parts[0]
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}

	switch {
	case action == "" && r.Method == http.MethodGet:
		st, err := s.coord.RequestStatus(id)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, st)
	case action == "cancel" && r.Method == http.MethodPost:
		if err := s.coord.CancelRequest(id); err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"id": id, "state": "CANCELLING"})
	case action == "" && r.Method == http.MethodDelete:
		if err := s.coord.CancelRequest(id); err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"id": id, "state": "CANCELLING"})
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleScheduler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, s.coord.SchedulerStatus())
}

func (s *Server) handleResources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, s.coord.ResourceStatus())
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := s.coord.Metrics().WritePrometheus(w); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// HTTPServer wraps the API in an *http.Server with sane timeouts.
func (s *Server) HTTPServer(addr string) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
