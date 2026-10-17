package observability

// Metric names are stable identifiers used by dashboards and alerts. They are
// declared up front so a scrape always shows every series, even at zero.
const (
	MetricRequestsSubmitted   = "infersched_requests_submitted_total"
	MetricRequestsAdmitted    = "infersched_requests_admitted_total"
	MetricRequestsRejected    = "infersched_requests_rejected_total"
	MetricRequestsCompleted   = "infersched_requests_completed_total"
	MetricRequestsCancelled   = "infersched_requests_cancelled_total"
	MetricRequestsFailed      = "infersched_requests_failed_total"
	MetricRequestsExpired     = "infersched_requests_expired_total"
	MetricDeadlineMisses      = "infersched_deadline_misses_total"
	MetricSchedulingDecisions = "infersched_scheduling_decisions_total"
	MetricBackendLatencyMS    = "infersched_backend_latency_ms_total"

	MetricQueueDepth       = "infersched_queue_depth"
	MetricQueueWaitMS      = "infersched_queue_wait_ms"
	MetricInFlight         = "infersched_in_flight"
	MetricActiveSequences  = "infersched_active_sequences"
	MetricKVBlocksUsed     = "infersched_kv_blocks_used"
	MetricSlotsUsed        = "infersched_slots_used"
	MetricWorkersAvailable = "infersched_workers_available"
)

// metricHelp maps each metric name to its HELP text. Any series created at
// runtime (from traffic) looks its help up here, so the exposition is always
// well-formed.
var metricHelp = map[string]string{
	MetricRequestsSubmitted:   "requests submitted by lifecycle state",
	MetricRequestsAdmitted:    "requests admitted",
	MetricRequestsRejected:    "requests rejected by reason",
	MetricRequestsCompleted:   "requests completed by outcome",
	MetricRequestsCancelled:   "requests cancelled",
	MetricRequestsFailed:      "requests failed",
	MetricRequestsExpired:     "requests expired",
	MetricDeadlineMisses:      "completed requests that missed their deadline",
	MetricSchedulingDecisions: "scheduling decisions by policy",
	MetricBackendLatencyMS:    "backend execution latency by outcome (milliseconds)",
	MetricQueueDepth:          "requests admitted but not yet started",
	MetricQueueWaitMS:         "current queue wait placeholder (milliseconds)",
	MetricInFlight:            "requests currently executing",
	MetricActiveSequences:     "active sequences across workers",
	MetricKVBlocksUsed:        "KV blocks currently reserved",
	MetricSlotsUsed:           "execution slots currently used",
	MetricWorkersAvailable:    "workers available",
}

func helpFor(name string) string {
	if h, ok := metricHelp[name]; ok {
		return h
	}
	return name
}

// declareAll pre-declares the zero-valued series that use fixed label sets, so
// the exposition includes them before any traffic arrives.
func (m *Metrics) declareAll() {
	// lifecycle states (closed set).
	states := []string{"queued", "admitted", "running", "completed", "rejected", "cancelled", "expired", "failed"}
	for _, s := range states {
		m.declareCounter(MetricRequestsSubmitted, "requests submitted by lifecycle state", map[string]string{"state": s})
	}

	// rejection reasons (closed set).
	reasons := []string{
		"OVERLOAD_QUEUE_LENGTH", "OVERLOAD_QUEUED_TOKENS", "OVERLOAD_OUTSTANDING_REQUESTS",
		"OVERLOAD_TENANT_LIMIT", "DEADLINE_INFEASIBLE", "CLIENT_CANCELLED", "TIMEOUT",
		"WORKER_FAILURE", "SHUTDOWN", "NONE",
	}
	for _, r := range reasons {
		m.declareCounter(MetricRequestsRejected, "requests rejected by reason", map[string]string{"reason": r})
	}

	// outcomes (closed set).
	outcomes := []string{"completed", "cancelled", "failed", "expired"}
	for _, o := range outcomes {
		m.declareCounter(MetricRequestsCompleted, "requests completed by outcome", map[string]string{"outcome": o})
	}

	// admission is a single always-present series.
	m.declareCounter(MetricRequestsAdmitted, "requests admitted", nil)
	m.declareCounter(MetricSchedulingDecisions, "scheduling decisions by policy", map[string]string{"policy": "fifo"})
	m.declareCounter(MetricDeadlineMisses, "completed requests that missed their deadline", nil)

	// gauges.
	m.declareGauge(MetricQueueDepth, "requests admitted but not yet started")
	m.declareGauge(MetricInFlight, "requests currently executing")
	m.declareGauge(MetricActiveSequences, "active sequences across workers")
	m.declareGauge(MetricKVBlocksUsed, "KV blocks currently reserved")
	m.declareGauge(MetricSlotsUsed, "execution slots currently used")
	m.declareGauge(MetricWorkersAvailable, "workers available")
}

func (m *Metrics) declareCounter(name, help string, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	series, ok := m.counters[name]
	if !ok {
		series = make(map[string]*counterSeries)
		m.counters[name] = series
	}
	key := labelKey(labels)
	if _, ok := series[key]; !ok {
		series[key] = &counterSeries{help: help, labels: cloneLabels(labels)}
	}
}

func (m *Metrics) declareGauge(name, help string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	series, ok := m.gauges[name]
	if !ok {
		series = make(map[string]*gaugeSeries)
		m.gauges[name] = series
	}
	if _, ok := series[""]; !ok {
		series[""] = &gaugeSeries{help: help}
	}
}
