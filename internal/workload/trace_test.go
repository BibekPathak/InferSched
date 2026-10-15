package workload

import (
	"bytes"
	"context"
	"testing"

	"github.com/BibekP/infersched/internal/lifecycle"
)

func sampleTrace() *Trace {
	return &Trace{
		Header: Header{
			Kind:            "header",
			FormatVersion:   TraceFormatVersion,
			Profile:         "A-short-interactive",
			Seed:            42,
			RequestCount:    2,
			GeneratorConfig: map[string]any{"arrival": "poisson", "lambda": 5.0},
			TimeUnit:        "ns",
		},
		Records: []Record{
			{Kind: "request", ID: "r0", TenantID: "t0", Class: "interactive", Priority: 50,
				Arrival: 1000, Deadline: 0, PromptTokens: 128, ExpectedOutput: 32,
				PrefillCostMS: 1.0, DecodeCostPerTok: 2.0, KVTokens: 160},
			{Kind: "request", ID: "r1", TenantID: "t1", Class: "batch", Priority: 10,
				Arrival: 2000, Deadline: 9000, PromptTokens: 2048, ExpectedOutput: 512,
				PrefillCostMS: 16.0, DecodeCostPerTok: 2.0, KVTokens: 2560},
		},
	}
}

func TestRoundTrip(t *testing.T) {
	orig := sampleTrace()
	var buf bytes.Buffer
	if err := orig.Write(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := Parse(&buf)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.Header.Seed != orig.Header.Seed || got.Header.Profile != orig.Header.Profile {
		t.Fatalf("header mismatch: %+v", got.Header)
	}
	if len(got.Records) != len(orig.Records) {
		t.Fatalf("record count mismatch: %d != %d", len(got.Records), len(orig.Records))
	}
	if got.Records[1].PromptTokens != 2048 {
		t.Fatalf("record content mismatch: %+v", got.Records[1])
	}
}

func TestWriteDeterministic(t *testing.T) {
	tr := sampleTrace()
	var a, b bytes.Buffer
	if err := tr.Write(&a); err != nil {
		t.Fatal(err)
	}
	if err := tr.Write(&b); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatal("trace serialization not deterministic")
	}
}

func TestParseRejectsBadHeader(t *testing.T) {
	_, err := Parse(bytes.NewBufferString(`{"kind":"nope"}`))
	if err == nil {
		t.Fatal("expected error for non-header first line")
	}
}

func TestParseRejectsWrongVersion(t *testing.T) {
	_, err := Parse(bytes.NewBufferString(`{"kind":"header","format_version":"v999"}`))
	if err == nil {
		t.Fatal("expected error for unsupported version")
	}
}

func TestParseRejectsCountMismatch(t *testing.T) {
	_, err := Parse(bytes.NewBufferString(
		`{"kind":"header","format_version":"v1","request_count":5}` + "\n" +
			`{"kind":"request","id":"r0"}`))
	if err == nil {
		t.Fatal("expected error for count mismatch")
	}
}

func TestBuildRequests(t *testing.T) {
	reqs := sampleTrace().BuildRequests(context.Background())
	if len(reqs) != 2 {
		t.Fatalf("want 2 requests, got %d", len(reqs))
	}
	r := reqs[1]
	if r.Class != lifecycle.ClassBatch {
		t.Fatalf("class: %s", r.Class)
	}
	if r.Arrival != 2000 || r.Deadline != 9000 {
		t.Fatalf("timing: arrival=%d deadline=%d", r.Arrival, r.Deadline)
	}
	if !r.HasDeadline() {
		t.Fatal("r1 should have a deadline")
	}
	if reqs[0].HasDeadline() {
		t.Fatal("r0 should have no deadline")
	}
	if r.Estimate.KVTokens != 2560 {
		t.Fatalf("kv tokens: %d", r.Estimate.KVTokens)
	}
}

func TestUnknownClassCoerced(t *testing.T) {
	tr := sampleTrace()
	tr.Records[0].Class = "mystery"
	reqs := tr.BuildRequests(context.Background())
	if reqs[0].Class != lifecycle.ClassUnknown {
		t.Fatalf("want unknown, got %s", reqs[0].Class)
	}
}
