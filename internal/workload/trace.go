// Package workload defines the versioned, reproducible request-trace format
// exchanged between the Python workload generator and the Go simulator.
//
// A trace is a JSONL file: the first line is a header record carrying the
// format version and generation configuration, and every subsequent line is a
// single request record. Traces are replayable: the simulator reconstructs
// identical arrivals from the file alone, so every scheduling policy is scored
// against byte-identical input regardless of language or process.
package workload

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/BibekP/infersched/internal/lifecycle"
)

// TraceFormatVersion identifies the on-disk schema. Bump on breaking change.
const TraceFormatVersion = "v1"

// Header is the first record of a trace file.
type Header struct {
	Kind            string         `json:"kind"` // always "header"
	FormatVersion   string         `json:"format_version"`
	Profile         string         `json:"profile"`
	Seed            int64          `json:"seed"`
	RequestCount    int            `json:"request_count"`
	GeneratorConfig map[string]any `json:"generator_config,omitempty"`
	// TimeUnit documents the unit of Arrival/Deadline in the trace.
	TimeUnit string `json:"time_unit"` // "ns"
}

// Record is one request arrival in a trace.
type Record struct {
	Kind             string  `json:"kind"` // always "request"
	ID               string  `json:"id"`
	TenantID         string  `json:"tenant_id"`
	Class            string  `json:"class"`
	Priority         int     `json:"priority"`
	Arrival          int64   `json:"arrival_ns"`
	Deadline         int64   `json:"deadline_ns"` // 0 = none
	PromptTokens     int     `json:"prompt_tokens"`
	ExpectedOutput   int     `json:"expected_output_tokens"`
	PrefillCostMS    float64 `json:"prefill_cost_ms"`
	DecodeCostPerTok float64 `json:"decode_cost_per_token_ms"`
	KVTokens         int     `json:"kv_tokens"`
}

// Trace is a parsed, in-memory trace.
type Trace struct {
	Header  Header
	Records []Record
	source  string
}

// Load reads and parses a JSONL trace from path.
func Load(path string) (*Trace, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("workload: open %s: %w", path, err)
	}
	defer f.Close()
	t, err := Parse(f)
	if err != nil {
		return nil, fmt.Errorf("workload: parse %s: %w", path, err)
	}
	t.source = path
	return t, nil
}

// Parse reads a JSONL trace from r.
func Parse(r io.Reader) (*Trace, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	t := &Trace{}
	line := 0
	for sc.Scan() {
		line++
		raw := sc.Bytes()
		if len(raw) == 0 {
			continue
		}
		if line == 1 {
			var h Header
			if err := json.Unmarshal(raw, &h); err != nil {
				return nil, fmt.Errorf("header line %d: %w", line, err)
			}
			if h.Kind != "header" {
				return nil, fmt.Errorf("line 1: expected kind=header, got %q", h.Kind)
			}
			if h.FormatVersion != TraceFormatVersion {
				return nil, fmt.Errorf("unsupported trace format version %q (want %q)", h.FormatVersion, TraceFormatVersion)
			}
			t.Header = h
			continue
		}
		var rec Record
		if err := json.Unmarshal(raw, &rec); err != nil {
			return nil, fmt.Errorf("record line %d: %w", line, err)
		}
		if rec.Kind != "request" {
			return nil, fmt.Errorf("line %d: expected kind=request, got %q", line, rec.Kind)
		}
		t.Records = append(t.Records, rec)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	if t.Header.Kind == "" {
		return nil, fmt.Errorf("empty trace: no header record")
	}
	if t.Header.RequestCount != 0 && t.Header.RequestCount != len(t.Records) {
		return nil, fmt.Errorf("header request_count=%d but parsed %d records", t.Header.RequestCount, len(t.Records))
	}
	return t, nil
}

// Write serializes the trace back to w as JSONL (header first). The output is
// deterministic: records are emitted in file order.
func (t *Trace) Write(w io.Writer) error {
	enc := json.NewEncoder(w)
	if err := enc.Encode(t.Header); err != nil {
		return err
	}
	for i := range t.Records {
		if err := enc.Encode(&t.Records[i]); err != nil {
			return err
		}
	}
	return nil
}

// Source returns the path the trace was loaded from, if any.
func (t *Trace) Source() string { return t.source }

// BuildRequests converts the trace into lifecycle requests using bg as the base
// cancellation context for every request. Arrival and Deadline are copied
// verbatim from the trace so replays are identical across runs. WallClockArrival
// is left zero; the simulator derives wall time from its own monotonic baseline.
func (t *Trace) BuildRequests(bg context.Context) []*lifecycle.Request {
	reqs := make([]*lifecycle.Request, 0, len(t.Records))
	for i := range t.Records {
		reqs = append(reqs, t.Records[i].toRequest(bg))
	}
	return reqs
}

func (rec *Record) toRequest(bg context.Context) *lifecycle.Request {
	class := lifecycle.WorkloadClass(rec.Class)
	switch class {
	case lifecycle.ClassInteractive, lifecycle.ClassBatch, lifecycle.ClassLongContext:
	default:
		class = lifecycle.ClassUnknown
	}
	return lifecycle.NewRequest(
		rec.ID,
		rec.TenantID,
		class,
		lifecycle.Priority(rec.Priority),
		time.Time{},
		rec.Arrival,
		rec.Deadline,
		lifecycle.CostEstimate{
			PrefillTokens:        rec.PromptTokens,
			ExpectedOutputTokens: rec.ExpectedOutput,
			PrefillCostMS:        rec.PrefillCostMS,
			DecodeCostPerTokenMS: rec.DecodeCostPerTok,
			KVTokens:             rec.KVTokens,
		},
		bg,
	)
}
