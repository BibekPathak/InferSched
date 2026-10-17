package observability

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

func TestCounterAndGauge(t *testing.T) {
	m := New()
	m.IncCounter(MetricRequestsAdmitted, nil)
	m.AddCounter(MetricRequestsAdmitted, nil, 4)
	if got := m.CounterValue(MetricRequestsAdmitted, nil); got != 5 {
		t.Fatalf("counter=%d want 5", got)
	}
	m.SetGauge(MetricQueueDepth, nil, 7)
	if got := m.GaugeValue(MetricQueueDepth, nil); got != 7 {
		t.Fatalf("gauge=%d want 7", got)
	}
	m.AddGauge(MetricQueueDepth, nil, -3)
	if got := m.GaugeValue(MetricQueueDepth, nil); got != 4 {
		t.Fatalf("gauge=%d want 4", got)
	}
}

func TestExpositionFormat(t *testing.T) {
	m := New()
	m.AddCounter(MetricRequestsRejected, map[string]string{"reason": "OVERLOAD_QUEUE_LENGTH"}, 3)
	m.SetGauge(MetricKVBlocksUsed, nil, 42)

	var buf bytes.Buffer
	if err := m.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "# TYPE "+MetricRequestsRejected+" counter") {
		t.Fatalf("missing TYPE line:\n%s", out)
	}
	if !strings.Contains(out, `reason="OVERLOAD_QUEUE_LENGTH"`) {
		t.Fatalf("missing labeled series:\n%s", out)
	}
	if !strings.Contains(out, MetricKVBlocksUsed+" 42") {
		t.Fatalf("missing gauge value:\n%s", out)
	}
}

func TestExpositionDeterministic(t *testing.T) {
	m := New()
	for i := 0; i < 10; i++ {
		m.IncCounter(MetricRequestsAdmitted, nil)
	}
	var a, b bytes.Buffer
	_ = m.WritePrometheus(&a)
	_ = m.WritePrometheus(&b)
	if a.String() != b.String() {
		t.Fatal("exposition not deterministic")
	}
}

func TestConcurrentMetricUpdates(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				m.IncCounter(MetricRequestsAdmitted, nil)
				m.SetGauge(MetricQueueDepth, nil, int64(i))
			}
		}()
	}
	wg.Wait()
	if got := m.CounterValue(MetricRequestsAdmitted, nil); got != 32*500 {
		t.Fatalf("counter=%d want %d", got, 32*500)
	}
}

func TestDeclaredSeriesPresentAtZero(t *testing.T) {
	m := New()
	var buf bytes.Buffer
	_ = m.WritePrometheus(&buf)
	if !strings.Contains(buf.String(), MetricRequestsRejected) {
		t.Fatal("rejected counter not pre-declared")
	}
}
