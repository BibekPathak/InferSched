// Command sim replays a workload trace through the deterministic simulator and
// prints per-policy summary statistics. It is the Phase 4 driver: it lets a
// policy's scheduling behaviour be inspected before the Phase 5 benchmark
// harness automates multi-seed comparisons.
//
// The numbers this prints are SIMULATION output. They are not measurements of
// real hardware.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/BibekP/infersched/internal/admission"
	"github.com/BibekP/infersched/internal/backend"
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
