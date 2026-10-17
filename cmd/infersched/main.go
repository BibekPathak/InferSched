// Command infersched is the InferSched runtime coordinator entrypoint.
//
// Phase 6 scope: this binary can serve the HTTP API backed by the coordinator
// and the mock execution backend, run the admission/resource dry-run demo, and
// validate workload traces.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BibekP/infersched/internal/admission"
	"github.com/BibekP/infersched/internal/api"
	"github.com/BibekP/infersched/internal/backend"
	"github.com/BibekP/infersched/internal/coordinator"
	"github.com/BibekP/infersched/internal/lifecycle"
	"github.com/BibekP/infersched/internal/observability"
	"github.com/BibekP/infersched/internal/resources"
	"github.com/BibekP/infersched/internal/scheduler"
	"github.com/BibekP/infersched/internal/workload"
)

const version = "0.6.0-phase6"

func main() {
	var (
		showVersion = flag.Bool("version", false, "print version and exit")
		tracePath   = flag.String("validate-trace", "", "validate a workload JSONL trace and print a summary")
		admitTrace  = flag.String("admission-demo", "", "run a trace through admission + resource accounting and print a summary")
		serve       = flag.Bool("serve", false, "run the HTTP API server backed by the mock execution backend")
		addr        = flag.String("addr", ":8080", "HTTP listen address for -serve")
		policyName  = flag.String("policy", "fifo", "scheduling policy (fifo|priority|sjf|edf|hybrid)")
		workers     = flag.Int("workers", 2, "simulated workers")
		batchSize   = flag.Int("batch-size", 8, "max batch size per worker")
		kvBlocks    = flag.Int("kv-blocks-per-worker", 64, "KV blocks per worker")
		kvBlockSize = flag.Int("kv-block-size", 16, "KV block size in tokens")
		outstanding = flag.Int("max-outstanding", 0, "max outstanding requests (0 = unlimited)")
		queueLen    = flag.Int("max-queue", 0, "max queued requests (0 = unlimited)")
		strategy    = flag.String("strategy", "reject", "admission strategy (reject|deadline_aware)")
		workMS      = flag.Int("mock-work-ms", 50, "mock backend nominal execution time in ms (-serve)")
		logLevel    = flag.String("log-level", "info", "log level (debug|info|warn|error)")
	)
	flag.Parse()

	switch {
	case *showVersion:
		fmt.Printf("infersched %s\n", version)
	case *serve:
		if err := runServer(serverConfig{
			Addr:        *addr,
			Policy:      *policyName,
			Workers:     *workers,
			BatchSize:   *batchSize,
			KVBlocks:    *kvBlocks,
			KVBlockSize: *kvBlockSize,
			Outstanding: *outstanding,
			QueueLen:    *queueLen,
			Strategy:    *strategy,
			MockWorkMS:  *workMS,
			LogLevel:    *logLevel,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "infersched: %v\n", err)
			os.Exit(1)
		}
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
		fmt.Println("Commands: -serve, -validate-trace <file>, -admission-demo <file>")
	}
}

// serverConfig configures the HTTP server run.
type serverConfig struct {
	Addr        string
	Policy      string
	Workers     int
	BatchSize   int
	KVBlocks    int
	KVBlockSize int
	Outstanding int
	QueueLen    int
	Strategy    string
	MockWorkMS  int
	LogLevel    string
}

// runServer builds the coordinator with a mock backend, serves the HTTP API,
// and shuts down gracefully on SIGINT/SIGTERM.
func runServer(cfg serverConfig) error {
	level := slog.LevelInfo
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	logger := observability.NewLogger(os.Stderr, level)
	metrics := observability.New()

	mock := backend.NewMockBackend(time.Duration(cfg.MockWorkMS)*time.Millisecond, nil)
	coord, err := coordinator.New(coordinator.Config{
		Policy: cfg.Policy,
		Resources: resources.Config{
			Workers:           cfg.Workers,
			MaxBatchSize:      cfg.BatchSize,
			KVBlockSize:       cfg.KVBlockSize,
			KVBlocksPerWorker: cfg.KVBlocks,
		},
		Admission: admission.Config{
			Strategy:       admission.Strategy(cfg.Strategy),
			MaxOutstanding: cfg.Outstanding,
			MaxQueueLength: cfg.QueueLen,
		},
		Backend: mock,
		Logger:  logger,
		Metrics: metrics,
	})
	if err != nil {
		return err
	}
	coord.Start()

	srv := api.NewServer(coord).HTTPServer(cfg.Addr)
	errCh := make(chan error, 1)
	go func() {
		logger.Info("http_listening", "addr", cfg.Addr, "policy", cfg.Policy)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case sig := <-sigCh:
		logger.Info("shutdown_signal", "signal", sig.String())
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http_shutdown_error", "err", err.Error())
	}
	if err := coord.Shutdown(shutdownCtx); err != nil {
		logger.Warn("coordinator_shutdown_error", "err", err.Error())
	}
	logger.Info("shutdown_complete")
	return nil
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
