package observability

import (
	"io"
	"log/slog"
	"os"
)

// NewLogger returns a structured JSON logger writing to w at the given level.
// InferSched logs the lifecycle of a request through admission, scheduling,
// execution, and completion using the request ID as a field (not a label).
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	if w == nil {
		w = os.Stderr
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(h)
}

// Event names used in structured logs. They form a small, stable vocabulary so
// request lifecycle traces can be filtered reliably.
const (
	EventSubmitted  = "request.submitted"
	EventAdmitted   = "request.admitted"
	EventRejected   = "request.rejected"
	EventQueued     = "request.queued"
	EventStarted    = "request.started"
	EventFirstToken = "request.first_token"
	EventCompleted  = "request.completed"
	EventCancelled  = "request.cancelled"
	EventFailed     = "request.failed"
	EventExpired    = "request.expired"
)
