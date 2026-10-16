// Command infersched is the InferSched runtime coordinator entrypoint.
//
// Phase 3 scope: this binary validates the build wiring, the workload-trace
// tooling, and a dry-run admission/resource pipeline. The HTTP API and the
// simulator backend are wired in through the coordinator in later phases.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BibekP/infersched/internal/admission"
	"github.com/BibekP/infersched/internal/lifecycle"
	"github.com/BibekP/infersched/internal/resources"
	"github.com/BibekP/infersched/internal/scheduler"
	"github.com/BibekP/infersched/internal/workload"
)

const version = "0.3.0-phase3"

func main() {
	var (
		showVersion = flag.Bool("version", false, "print version and exit")
		tracePath   = flag.String("validate-trace", "", "validate a workload JSONL trace and print a summary")
		admitTrace  = flag.String("admission-demo", "", "run a trace through admission + resource accounting and print a summary")
		policyName  = flag.String("policy", "fifo", "scheduling policy for -admission-demo")
		workers     = flag.Int("workers", 2, "simulated workers for -admission-demo")
		batchSize   = flag.Int("batch-size", 8, "max batch size per worker")
		kvBlocks    = flag.Int("kv-blocks-per-worker", 64, "KV blocks per worker")
		kvBlockSize = flag.Int("kv-block-size", 16, "KV block size in tokens")
		outstanding = flag.Int("max-outstanding", 0, "max outstanding requests (0 = unlimited)")
		queueLen    = flag.Int("max-queue", 0, "max queued requests (0 = unlimited)")
	)
	flag.Parse()

	switch {
	case *showVersion:
		fmt.Printf("infersched %s\n", version)
	case *tracePath != "":
		if err := validateTrace(*tracePath); err != nil {
			fmt.Fprintf(os.Stderr, "infersched: %v\n", err)
			os.Exit(1)
		}
	case *admitTrace != "":
		cfg := demoConfig{
			Policy:      *policyName,
			Workers:     *workers,
			BatchSize:   *batchSize,
			KVBlocks:    *kvBlocks,
			KVBlockSize: *kvBlockSize,
			Outstanding: *outstanding,
			QueueLen:    *queueLen,
		}
		if err := admissionDemo(*admitTrace, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "infersched: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Printf("infersched %s\n", version)
		fmt.Println("Phase 3 build: lifecycle, policies, admission and resource accounting available.")
		fmt.Println("Commands: -validate-trace <file>, -admission-demo <file>")
	}
}

type demoConfig struct {
	Policy      string
	Workers     int
	BatchSize   int
	KVBlocks    int
	KVBlockSize int
	Outstanding int
	QueueLen    int
}

// admissionDemo replays a trace in scheduling order through the admission
// controller and resource manager, reporting admission, rejection, and final
// accounting. It does not execute requests; it exercises the admit path and
// resource reservations only.
func admissionDemo(path string, cfg demoConfig) error {
	tr, err := workload.Load(path)
	if err != nil {
		return err
	}
	reqs := tr.BuildRequests(nil)

	pol, ok := scheduler.Registry()[cfg.Policy]
	if !ok {
		return fmt.Errorf("unknown policy %q", cfg.Policy)
	}

	res := resources.New(resources.Config{
		Workers:           cfg.Workers,
		MaxBatchSize:      cfg.BatchSize,
		KVBlockSize:       cfg.KVBlockSize,
		KVBlocksPerWorker: cfg.KVBlocks,
	})
	adm := admission.New(admission.Config{
		Strategy:       admission.StrategyDeadlineAware,
		MaxOutstanding: cfg.Outstanding,
		MaxQueueLength: cfg.QueueLen,
	}, res, nil)

	var admitted, rejected, reserveFailed int
	reasons := map[lifecycle.Reason]int{}
	for _, r := range pol.Order(reqs) {
		d := adm.Evaluate(r)
		if !d.Admit {
			rejected++
			reasons[d.Reason]++
			continue
		}
		if _, err := res.Reserve(resources.Need{RequestID: r.ID, KVTokens: admission.EstimateKVTokens(r)}); err != nil {
			reserveFailed++
			reasons[lifecycle.ReasonOverloadTokens]++
			continue
		}
		admitted++
	}
	snap := res.Snapshot()
	fmt.Printf("admission demo: %s (policy=%s)\n", path, cfg.Policy)
	fmt.Printf("  requests       : %d\n", len(reqs))
	fmt.Printf("  admitted       : %d\n", admitted)
	fmt.Printf("  rejected       : %d\n", rejected)
	fmt.Printf("  reserveFailed  : %d\n", reserveFailed)
	fmt.Printf("  slots          : %d/%d used\n", snap.SlotsUsed, snap.SlotsTotal)
	fmt.Printf("  kv blocks      : %d/%d used\n", snap.KVBlocksUsed, snap.KVBlocksTotal)
	fmt.Printf("  reject reasons : %v\n", reasons)
	return nil
}

func validateTrace(path string) error {
	tr, err := workload.Load(path)
	if err != nil {
		return err
	}
	h := tr.Header
	fmt.Printf("trace %s: valid %s\n", path, h.FormatVersion)
	fmt.Printf("  profile        : %s\n", h.Profile)
	fmt.Printf("  seed           : %d\n", h.Seed)
	fmt.Printf("  requests       : %d\n", len(tr.Records))
	var withDeadline, totalPrompt, totalOutput int
	for _, r := range tr.Records {
		if r.Deadline != 0 {
			withDeadline++
		}
		totalPrompt += r.PromptTokens
		totalOutput += r.ExpectedOutput
	}
	fmt.Printf("  with deadline  : %d\n", withDeadline)
	fmt.Printf("  total prompt tk: %d\n", totalPrompt)
	fmt.Printf("  total output tk: %d\n", totalOutput)
	return nil
}
