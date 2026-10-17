// Package observability provides InferSched's metrics and structured logging.
//
// Metrics are exposed in the Prometheus text exposition format with a minimal,
// dependency-free implementation. Every label value is bounded and drawn from a
// closed set (lifecycle states, bounded reasons, policies, outcomes); request
// IDs, tenant names, and other user-supplied strings are NEVER used as labels,
// which would let a client create unbounded cardinality.
package observability

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Metrics is a thread-safe collection of InferSched's counters and gauges.
//
// Counters are monotonic; gauges reflect a current value that may rise or fall.
// The set of label values is closed, so the number of time series is bounded.
type Metrics struct {
	mu sync.Mutex

	// Counters, keyed by metric name then serialized labels.
	counters map[string]map[string]*counterSeries

	// Gauges, keyed the same way.
	gauges map[string]map[string]*gaugeSeries
}

type counterSeries struct {
	help   string
	labels map[string]string
	value  atomic.Int64
}

type gaugeSeries struct {
	help   string
	labels map[string]string
	value  atomic.Int64
}

// New constructs an empty Metrics registry with all known series pre-declared,
// so a scrape before any traffic still shows zero-valued series.
func New() *Metrics {
	m := &Metrics{
		counters: make(map[string]map[string]*counterSeries),
		gauges:   make(map[string]map[string]*gaugeSeries),
	}
	m.declareAll()
	return m
}

// labelKey serializes a label set into a stable key.
func labelKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%q", k, labels[k])
	}
	return b.String()
}

// IncCounter increments a counter series by 1.
func (m *Metrics) IncCounter(name string, labels map[string]string) {
	m.AddCounter(name, labels, 1)
}

// AddCounter adds delta to a counter series.
func (m *Metrics) AddCounter(name string, labels map[string]string, delta int64) {
	if delta == 0 {
		return
	}
	m.mu.Lock()
	series, ok := m.counters[name]
	if !ok {
		series = make(map[string]*counterSeries)
		m.counters[name] = series
	}
	key := labelKey(labels)
	cs, ok := series[key]
	if !ok {
		cs = &counterSeries{help: helpFor(name), labels: cloneLabels(labels)}
		series[key] = cs
	}
	m.mu.Unlock()
	cs.value.Add(delta)
}

// SetGauge sets a gauge series to v.
func (m *Metrics) SetGauge(name string, labels map[string]string, v int64) {
	m.mu.Lock()
	series, ok := m.gauges[name]
	if !ok {
		series = make(map[string]*gaugeSeries)
		m.gauges[name] = series
	}
	key := labelKey(labels)
	gs, ok := series[key]
	if !ok {
		gs = &gaugeSeries{help: helpFor(name), labels: cloneLabels(labels)}
		series[key] = gs
	}
	m.mu.Unlock()
	gs.value.Store(v)
}

// AddGauge adds delta to a gauge series.
func (m *Metrics) AddGauge(name string, labels map[string]string, delta int64) {
	if delta == 0 {
		return
	}
	m.mu.Lock()
	series, ok := m.gauges[name]
	if !ok {
		series = make(map[string]*gaugeSeries)
		m.gauges[name] = series
	}
	key := labelKey(labels)
	gs, ok := series[key]
	if !ok {
		gs = &gaugeSeries{help: helpFor(name), labels: cloneLabels(labels)}
		series[key] = gs
	}
	m.mu.Unlock()
	gs.value.Add(delta)
}

// CounterValue returns the current value of a counter series (for tests).
func (m *Metrics) CounterValue(name string, labels map[string]string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if series, ok := m.counters[name]; ok {
		if cs, ok := series[labelKey(labels)]; ok {
			return cs.value.Load()
		}
	}
	return 0
}

// GaugeValue returns the current value of a gauge series (for tests).
func (m *Metrics) GaugeValue(name string, labels map[string]string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if series, ok := m.gauges[name]; ok {
		if gs, ok := series[labelKey(labels)]; ok {
			return gs.value.Load()
		}
	}
	return 0
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}

// WriteTo writes the registry in Prometheus text exposition format.
func (m *Metrics) WritePrometheus(w io.Writer) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	names := make([]string, 0, len(m.counters)+len(m.gauges))
	for n := range m.counters {
		names = append(names, n)
	}
	for n := range m.gauges {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, name := range names {
		if series, ok := m.counters[name]; ok {
			if err := writeCounters(w, name, series); err != nil {
				return err
			}
			continue
		}
		if series, ok := m.gauges[name]; ok {
			if err := writeGauges(w, name, series); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeCounters(w io.Writer, name string, series map[string]*counterSeries) error {
	keys := make([]string, 0, len(series))
	for k := range series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		cs := series[k]
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", name, cs.help, name); err != nil {
			return err
		}
		break
	}
	for _, k := range keys {
		cs := series[k]
		if _, err := fmt.Fprintf(w, "%s%s %d\n", name, formatLabels(cs.labels), cs.value.Load()); err != nil {
			return err
		}
	}
	return nil
}

func writeGauges(w io.Writer, name string, series map[string]*gaugeSeries) error {
	keys := make([]string, 0, len(series))
	for k := range series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		gs := series[k]
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", name, gs.help, name); err != nil {
			return err
		}
		break
	}
	for _, k := range keys {
		gs := series[k]
		if _, err := fmt.Fprintf(w, "%s%s %d\n", name, formatLabels(gs.labels), gs.value.Load()); err != nil {
			return err
		}
	}
	return nil
}

// formatLabels renders a label set as `{k="v",...}` or "" when empty.
func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%q", k, labels[k])
	}
	b.WriteByte('}')
	return b.String()
}
