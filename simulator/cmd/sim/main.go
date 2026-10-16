// Command sim replays a workload trace through the deterministic simulator and
// prints per-policy summary statistics. It is the Phase 4 driver: it lets a
// policy's scheduling behaviour be inspected before the Phase 5 benchmark
// harness automates multi-seed comparisons.
//
// The numbers this prints are SIMULATION output. They are not measurements of
// real hardware.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/BibekP/infersched/internal/admission"
	"github.com/BibekP/infersched/internal/backend"
	"github.com/BibekP/infersched/internal/lifecycle"
	"github.com/BibekP/infersched/internal/simulator"
	"github.com/BibekP/infersched/internal/workload"
)

func main() {
	var (
		tracePath = flag.String("trace", "", "path to a workload JSONL trace (required)")
		policies  = flag.String("policies", "fifo,priority,sjf,edf,hybrid", "comma-separated policies to compare")
		seed      = flag.Int64("seed", 1, "simulation seed")
		workers   = flag.Int("workers", 2, "number of simulated workers")
		batchSize = flag.Int("batch-size", 8, "max batch size per worker")
		kvBlocks  = flag.Int("kv-blocks-per-worker", 512, "KV blocks per worker")
		kvBlockSz = flag.Int("kv-block-size", 16, "KV block size in tokens")
		maxQueue  = flag.Int("max-queue", 0, "max queued requests (0 = unlimited)")
		maxOut    = flag.Int("max-outstanding", 0, "max outstanding requests (0 = unlimited)")
		strategy  = flag.String("strategy", "reject", "admission strategy: reject|deadline_aware")
		failRate  = flag.Int("failure-rate", 0, "synthetic worker failures per million steps")
		horizonMS = flag.Int64("horizon-ms", 0, "run horizon in milliseconds (0 = until drained)")
		detail    = flag.Bool("detail", false, "print per-request results for each policy")
		jsonOut   = flag.String("json-out", "", "write machine-readable results to this path (or '-' for stdout)")
	)
	flag.Parse()

	if *tracePath == "" {
		fmt.Fprintln(os.Stderr, "sim: -trace is required")
		flag.Usage()
		os.Exit(2)
	}

	tr, err := workload.Load(*tracePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sim: %v\n", err)
		os.Exit(1)
	}
	// Build requests once per policy from the same trace so every policy sees
	// identical arrivals and characteristics.
	base := tr.BuildRequests(nil)

	if *jsonOut != "" {
		if err := runJSON(*jsonOut, tr, base, simParams{
			Policies: strings.Split(*policies, ","), Seed: *seed, Workers: *workers,
			BatchSize: *batchSize, KVBlocks: *kvBlocks, KVBlockSize: *kvBlockSz,
			MaxQueue: *maxQueue, MaxOutstanding: *maxOut, Strategy: *strategy,
			FailRate: *failRate, HorizonNS: *horizonMS * 1_000_000,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "sim: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("simulator replay: %s\n", *tracePath)
	fmt.Printf("  profile=%s seed=%d requests=%d workers=%d batch=%d kvBlocks=%d kvBlockSize=%d\n",
		tr.Header.Profile, *seed, len(base), *workers, *batchSize, *kvBlocks, *kvBlockSz)

	names := strings.Split(*policies, ",")
	var rows []policyRow
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		cfg := simulator.DefaultConfig()
		cfg.Seed = *seed
		cfg.Policy = name
		cfg.Workers = *workers
		cfg.MaxBatchSize = *batchSize
		cfg.KVBlocksPerWorker = *kvBlocks
		cfg.KVBlockSize = *kvBlockSz
		cfg.FailureRatePerMillionSteps = *failRate
		cfg.HorizonNS = *horizonMS * 1_000_000
		cfg.Admission = admission.Config{
			Strategy:       admission.Strategy(*strategy),
			MaxQueueLength: *maxQueue,
			MaxOutstanding: *maxOut,
		}

		sim, err := simulator.New(cfg, base)
		if err != nil {
			fmt.Fprintf(os.Stderr, "sim: policy %s: %v\n", name, err)
			os.Exit(1)
		}
		results, stats := sim.Run()
		rows = append(rows, policyRow{policy: name, stats: stats, sum: summarize(results)})
		if *detail {
			printDetail(name, results)
		}
	}

	printTable(rows)
}

// policyRow is one policy's result in the comparison table.
type policyRow struct {
	policy string
	stats  simulator.Stats
	sum    summary
}

type summary struct {
	p50TTFT, p90TTFT, p99TTFT float64
	p50Comp, p90Comp, p99Comp float64
	throughputPerSec          float64
	deadlineMissRate          float64
	rejectRate                float64
}

func summarize(results []simulator.Result) summary {
	var ttft, comp []float64
	completed := 0
	var deadlineEligible, deadlineMiss int
	rejected := 0
	var maxEnd float64
	for _, r := range results {
		maxEnd = maxf(maxEnd, float64(r.EndNS)/1e9)
		if r.Outcome == backend.OutcomeCompleted {
			completed++
			ttft = append(ttft, float64(r.TTFTNS)/1e6)
			comp = append(comp, float64(r.CompletionNS)/1e6)
		}
		if r.DeadlineNS != 0 {
			deadlineEligible++
			if r.LatenessNS > 0 {
				deadlineMiss++
			}
		}
		switch r.Reason {
		case "OVERLOAD_QUEUE_LENGTH", "OVERLOAD_QUEUED_TOKENS",
			"OVERLOAD_OUTSTANDING_REQUESTS", "OVERLOAD_TENANT_LIMIT", "DEADLINE_INFEASIBLE":
			rejected++
		}
	}
	sort.Float64s(ttft)
	sort.Float64s(comp)
	s := summary{
		p50TTFT: pct(ttft, 50),
		p90TTFT: pct(ttft, 90),
		p99TTFT: pct(ttft, 99),
		p50Comp: pct(comp, 50),
		p90Comp: pct(comp, 90),
		p99Comp: pct(comp, 99),
	}
	if maxEnd > 0 {
		s.throughputPerSec = float64(completed) / maxEnd
	}
	if deadlineEligible > 0 {
		s.deadlineMissRate = float64(deadlineMiss) / float64(deadlineEligible)
	}
	if len(results) > 0 {
		s.rejectRate = float64(rejected) / float64(len(results))
	}
	return s
}

// pct returns the p-th percentile of a sorted slice using the nearest-rank
// method: ceil(p/100 * N) with 1-based indexing. Method is stated so results are
// unambiguous.
func pct(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := (p*len(sorted) + 99) / 100
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func printTable(rows []policyRow) {
	fmt.Println()
	fmt.Printf("%-9s %6s %6s %6s %6s %6s | %7s %7s %7s | %8s %8s %8s | %6s\n",
		"policy", "done", "rej", "exp", "fail", "canc",
		"p50TTFT", "p90TTFT", "p99TTFT",
		"p50comp", "p90comp", "p99comp",
		"thr/s")
	for _, r := range rows {
		fmt.Printf("%-9s %6d %6d %6d %6d %6d | %7.1f %7.1f %7.1f | %8.1f %8.1f %8.1f | %6.1f\n",
			r.policy,
			r.stats.Completed, r.stats.Rejected, r.stats.Expired, r.stats.Failed, r.stats.Cancelled,
			r.sum.p50TTFT, r.sum.p90TTFT, r.sum.p99TTFT,
			r.sum.p50Comp, r.sum.p90Comp, r.sum.p99Comp,
			r.sum.throughputPerSec)
	}
	fmt.Println()
	fmt.Println("latencies in milliseconds; percentile method: nearest-rank (ceil(p/100*N), 1-based)")
	fmt.Println("note: SIMULATION output, not real-hardware measurements")
}

func printDetail(policy string, results []simulator.Result) {
	fmt.Printf("\n-- %s per-request --\n", policy)
	for _, r := range results {
		fmt.Printf("  %-8s %-9s reason=%-22s start=%8.2fms ttft=%8.2fms comp=%8.2fms out=%d\n",
			r.RequestID, r.Outcome, r.Reason,
			float64(r.StartNS)/1e6, float64(r.TTFTNS)/1e6, float64(r.CompletionNS)/1e6, r.OutputTokens)
	}
}

// simParams captures the simulation configuration for the JSON runner.
type simParams struct {
	Policies       []string
	Seed           int64
	Workers        int
	BatchSize      int
	KVBlocks       int
	KVBlockSize    int
	MaxQueue       int
	MaxOutstanding int
	Strategy       string
	FailRate       int
	HorizonNS      int64
}

// jsonReport is the machine-readable benchmark record for one trace + config.
type jsonReport struct {
	Trace      string          `json:"trace"`
	Profile    string          `json:"profile"`
	TraceSeed  int64           `json:"trace_seed"`
	Config     jsonConfig      `json:"config"`
	PolicyRuns []jsonPolicyRun `json:"policy_runs"`
	Note       string          `json:"note"`
}

type jsonConfig struct {
	Workers        int    `json:"workers"`
	MaxBatchSize   int    `json:"max_batch_size"`
	KVBlocksPerW   int    `json:"kv_blocks_per_worker"`
	KVBlockSize    int    `json:"kv_block_size"`
	MaxQueue       int    `json:"max_queue"`
	MaxOutstanding int    `json:"max_outstanding"`
	Strategy       string `json:"admission_strategy"`
	FailureRate    int    `json:"failure_rate_per_million_steps"`
	HorizonNS      int64  `json:"horizon_ns"`
	SimSeed        int64  `json:"sim_seed"`
}

// jsonPolicyRun is one policy's result under one config.
type jsonPolicyRun struct {
	Policy   string      `json:"policy"`
	Stats    jsonStats   `json:"stats"`
	Latency  jsonLatency `json:"latency_ms"`
	Requests []jsonReq   `json:"requests"`
}

type jsonStats struct {
	Total        int `json:"total"`
	Completed    int `json:"completed"`
	Rejected     int `json:"rejected"`
	Cancelled    int `json:"cancelled"`
	Expired      int `json:"expired"`
	Failed       int `json:"failed"`
	DeadlineMiss int `json:"deadline_miss"`
	PeakActive   int `json:"peak_active_sequences"`
	PeakKVBlocks int `json:"peak_kv_blocks_used"`
	PeakSlots    int `json:"peak_slots_used"`
}

type jsonLatency struct {
	TTFT       jsonPcts `json:"ttft"`
	Completion jsonPcts `json:"completion"`
	QueueDelay jsonPcts `json:"queue_delay"`
}

type jsonPcts struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

// jsonReq is a per-request record used for distribution analysis.
type jsonReq struct {
	ID           string  `json:"id"`
	TenantID     string  `json:"tenant"`
	Class        string  `json:"class"`
	Priority     int     `json:"priority"`
	Outcome      string  `json:"outcome"`
	Reason       string  `json:"reason"`
	ArrivalNS    int64   `json:"arrival_ns"`
	DeadlineNS   int64   `json:"deadline_ns"`
	AdmitNS      int64   `json:"admit_ns"`
	StartNS      int64   `json:"start_ns"`
	FirstTokenNS int64   `json:"first_token_ns"`
	EndNS        int64   `json:"end_ns"`
	AdmitDelayMS float64 `json:"admit_delay_ms"`
	QueueDelayMS float64 `json:"queue_delay_ms"`
	TTFTMS       float64 `json:"ttft_ms"`
	CompletionMS float64 `json:"completion_ms"`
	LatenessMS   float64 `json:"lateness_ms"`
	OutputTokens int     `json:"output_tokens"`
	WorkerID     int     `json:"worker_id"`
}

func runJSON(out string, tr *workload.Trace, base []*lifecycle.Request, p simParams) error {
	rep := jsonReport{
		Trace:     tr.Source(),
		Profile:   tr.Header.Profile,
		TraceSeed: tr.Header.Seed,
		Config: jsonConfig{
			Workers: p.Workers, MaxBatchSize: p.BatchSize, KVBlocksPerW: p.KVBlocks,
			KVBlockSize: p.KVBlockSize, MaxQueue: p.MaxQueue, MaxOutstanding: p.MaxOutstanding,
			Strategy: p.Strategy, FailureRate: p.FailRate, HorizonNS: p.HorizonNS, SimSeed: p.Seed,
		},
		Note: "SIMULATION output, not real-hardware measurements",
	}
	for _, name := range p.Policies {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		cfg := simulator.DefaultConfig()
		cfg.Seed = p.Seed
		cfg.Policy = name
		cfg.Workers = p.Workers
		cfg.MaxBatchSize = p.BatchSize
		cfg.KVBlocksPerWorker = p.KVBlocks
		cfg.KVBlockSize = p.KVBlockSize
		cfg.FailureRatePerMillionSteps = p.FailRate
		cfg.HorizonNS = p.HorizonNS
		cfg.Admission = admission.Config{
			Strategy:       admission.Strategy(p.Strategy),
			MaxQueueLength: p.MaxQueue,
			MaxOutstanding: p.MaxOutstanding,
		}
		sim, err := simulator.New(cfg, base)
		if err != nil {
			return fmt.Errorf("policy %s: %w", name, err)
		}
		results, stats := sim.Run()
		rep.PolicyRuns = append(rep.PolicyRuns, buildPolicyRun(name, results, stats))
	}

	var w *os.File
	if out == "-" {
		w = os.Stdout
	} else {
		f, err := os.Create(out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}

func buildPolicyRun(name string, results []simulator.Result, stats simulator.Stats) jsonPolicyRun {
	run := jsonPolicyRun{
		Policy: name,
		Stats: jsonStats{
			Total: stats.Total, Completed: stats.Completed, Rejected: stats.Rejected,
			Cancelled: stats.Cancelled, Expired: stats.Expired, Failed: stats.Failed,
			DeadlineMiss: stats.DeadlineMiss, PeakActive: stats.PeakActiveSequences,
			PeakKVBlocks: stats.PeakKVBlocksUsed, PeakSlots: stats.PeakSlotsUsed,
		},
	}
	var ttft, comp, qd []float64
	for _, r := range results {
		jr := jsonReq{
			ID: r.RequestID, TenantID: r.TenantID, Class: string(r.Class), Priority: int(r.Priority),
			Outcome: string(r.Outcome), Reason: string(r.Reason),
			ArrivalNS: r.ArrivalNS, DeadlineNS: r.DeadlineNS, AdmitNS: r.AdmitNS,
			StartNS: r.StartNS, FirstTokenNS: r.FirstTokenNS, EndNS: r.EndNS,
			AdmitDelayMS: float64(r.AdmissionDelayNS) / 1e6,
			QueueDelayMS: float64(r.QueueDelayNS) / 1e6,
			TTFTMS:       float64(r.TTFTNS) / 1e6,
			CompletionMS: float64(r.CompletionNS) / 1e6,
			LatenessMS:   float64(r.LatenessNS) / 1e6,
			OutputTokens: r.OutputTokens, WorkerID: r.WorkerID,
		}
		run.Requests = append(run.Requests, jr)
		if r.Outcome == backend.OutcomeCompleted {
			ttft = append(ttft, jr.TTFTMS)
			comp = append(comp, jr.CompletionMS)
			qd = append(qd, jr.QueueDelayMS)
		}
	}
	run.Latency = jsonLatency{
		TTFT:       pcts(ttft),
		Completion: pcts(comp),
		QueueDelay: pcts(qd),
	}
	return run
}

func pcts(v []float64) jsonPcts {
	sort.Float64s(v)
	return jsonPcts{
		P50: pct(v, 50), P90: pct(v, 90), P95: pct(v, 95), P99: pct(v, 99),
	}
}
