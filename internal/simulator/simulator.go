package simulator

import (
	"fmt"

	"github.com/BibekP/infersched/internal/admission"
	"github.com/BibekP/infersched/internal/backend"
	"github.com/BibekP/infersched/internal/lifecycle"
	"github.com/BibekP/infersched/internal/resources"
	"github.com/BibekP/infersched/internal/scheduler"
)

// Config fully specifies a simulation run. Two runs with the same Config and
// the same request trace produce identical results.
type Config struct {
	// Seed seeds any stochastic elements (currently only synthetic worker
	// failures). Determinism holds for any fixed seed.
	Seed int64

	// Policy selects the scheduling policy by name (see scheduler.Registry).
	Policy string

	// Resource capacity.
	Workers           int
	MaxBatchSize      int
	KVBlockSize       int
	KVBlocksPerWorker int

	// Perf model for prefill/decode cost.
	Perf PerfModel

	// Admission bounds (0 = unlimited where applicable).
	Admission admission.Config

	// FailureRatePerMillionSteps configures synthetic worker failures: on each
	// completed step, each worker independently fails with probability
	// FailureRatePerMillionSteps / 1e6. 0 disables failures. Uses Seed.
	FailureRatePerMillionSteps int

	// HorizonNS bounds the simulation: once virtual time passes HorizonNS and
	// no further events are pending before it, the run ends. Requests still
	// queued or running at termination are reported as OutcomeFailed with
	// reason "horizon". 0 means "run until all events drain".
	HorizonNS int64
}

// DefaultConfig returns a reasonable single-worker configuration.
func DefaultConfig() Config {
	return Config{
		Seed:              1,
		Policy:            "fifo",
		Workers:           2,
		MaxBatchSize:      8,
		KVBlockSize:       16,
		KVBlocksPerWorker: 512,
		Perf:              DefaultPerfModel(),
		Admission: admission.Config{
			Strategy:        admission.StrategyReject,
			MaxQueueLength:  0,
			MaxQueuedTokens: 0,
			MaxOutstanding:  0,
		},
	}
}

// Validate checks the configuration for internal consistency.
func (c Config) Validate() error {
	if c.Workers <= 0 {
		return fmt.Errorf("simulator: workers must be > 0")
	}
	if c.MaxBatchSize <= 0 {
		return fmt.Errorf("simulator: max batch size must be > 0")
	}
	if _, ok := scheduler.Registry()[c.Policy]; !ok {
		return fmt.Errorf("simulator: unknown policy %q", c.Policy)
	}
	if err := c.Perf.Validate(); err != nil {
		return err
	}
	if err := (resources.Config{
		Workers:           c.Workers,
		MaxBatchSize:      c.MaxBatchSize,
		KVBlockSize:       c.KVBlockSize,
		KVBlocksPerWorker: c.KVBlocksPerWorker,
	}).Validate(); err != nil {
		return err
	}
	return nil
}

// Result is the simulator's record of one request's fate. It is the primary
// output the benchmark harness analyses.
type Result struct {
	RequestID string
	TenantID  string
	Class     lifecycle.WorkloadClass
	Priority  lifecycle.Priority
	Outcome   backend.Outcome
	Reason    lifecycle.Reason

	ArrivalNS    int64
	DeadlineNS   int64 // 0 = none
	Admitted     bool
	AdmitNS      int64 // when admitted (valid iff Admitted)
	StartNS      int64 // prefill start (0 if never)
	FirstTokenNS int64 // 0 if never
	EndNS        int64 // final transition time

	QueueDelayNS int64 // AdmitNS - ArrivalNS
	TTFTNS       int64 // FirstTokenNS - ArrivalNS
	CompletionNS int64 // EndNS - ArrivalNS
	LatenessNS   int64 // EndNS - DeadlineNS (positive = missed), if deadline

	OutputTokens int
	KVTokens     int
	WorkerID     int
}

// Stats summarises a whole run.
type Stats struct {
	Total        int
	Completed    int
	Rejected     int
	Cancelled    int
	Expired      int
	Failed       int
	DeadlineMiss int

	// Peak usage observed.
	PeakActiveSequences int
	PeakKVBlocksUsed    int
	PeakSlotsUsed       int
}

// Simulator runs one deterministic simulation. Construct with New, drive with
// Run.
type Simulator struct {
	cfg Config

	perf   PerfModel
	res    *resources.Manager
	adm    *admission.Controller
	policy scheduler.Policy

	requests []*lifecycle.Request
	results  []Result

	now int64 // virtual nanoseconds
	seq uint64

	queue eventQueue

	workers []*worker

	// rng state for synthetic failures (xorshift64*), deterministic in Seed.
	rngState uint64

	// Peak usage observed during the run.
	peakActive   int
	peakKVBlocks int
	peakSlots    int
}

// worker tracks one simulated inference worker's resident sequences.
type worker struct {
	id     int
	active []*runState
	failed bool
}

// runState is the simulator's per-request execution state while admitted.
type runState struct {
	reqIdx int
	worker int
	// remaining decode steps.
	remaining int
	started   bool
	down      bool // true while its worker is executing a step
}

// New constructs a Simulator from cfg and a request trace.
func New(cfg Config, reqs []*lifecycle.Request) (*Simulator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	pol := scheduler.Registry()[cfg.Policy]
	// Clone the caller's requests so repeated runs are independent and the
	// caller's slice is never mutated. Two runs over the same inputs are
	// therefore identical.
	cloned := make([]*lifecycle.Request, len(reqs))
	for i, r := range reqs {
		cloned[i] = r.Clone()
	}
	// Ensure the policy has a reference-time provider wired to virtual time.
	s := &Simulator{
		cfg:      cfg,
		perf:     cfg.Perf,
		res:      resources.New(resources.Config{Workers: cfg.Workers, MaxBatchSize: cfg.MaxBatchSize, KVBlockSize: cfg.KVBlockSize, KVBlocksPerWorker: cfg.KVBlocksPerWorker}),
		policy:   pol,
		requests: cloned,
		results:  make([]Result, len(cloned)),
		rngState: uint64(cfg.Seed)*2654435761 + 1,
	}
	s.adm = admission.New(cfg.Admission, s.res, func() int64 { return s.now })
	for i := 0; i < cfg.Workers; i++ {
		s.workers = append(s.workers, &worker{id: i})
	}
	if err := s.wirePolicyClock(); err != nil {
		return nil, err
	}
	return s, nil
}

// wirePolicyClock points the policy's reference-time provider at the simulator
// clock so aging/deadline/hybrid terms use virtual time deterministically.
func (s *Simulator) wirePolicyClock() error {
	// Wire all clock-bearing policies to virtual time from the very start so the
	// simulator engine is the single source of now().
	now := func() int64 { return s.now }
	switch p := s.policy.(type) {
	case *scheduler.Priority:
		p.Aging.NowFunc = now
	case *scheduler.SJF:
		p.Aging.NowFunc = now
	case *scheduler.EDF:
		p.NowFunc = now
	case *scheduler.Hybrid:
		p.NowFunc = now
	case *scheduler.FIFO:
		// no clock
	default:
		return fmt.Errorf("simulator: unsupported policy type %T", s.policy)
	}
	return nil
}

// nextRNG returns a deterministic pseudo-random uint64 (xorshift64*).
func (s *Simulator) nextRNG() uint64 {
	x := s.rngState
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	s.rngState = x
	return x * 2685821657736338717
}
