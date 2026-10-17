# InferSched

A deadline-aware LLM inference **scheduling runtime** that decides which request
executes next, when a request is admitted, and when it is rejected or deferred —
under finite compute and KV-cache memory.

This is a systems project: it implements the scheduling core, resource
accounting, admission control, a continuous-batching simulator, and a
reproducible benchmark suite. It is **not** an API wrapper around a model server.

## Status

**Phase 8 complete — portfolio-ready.** Implemented:

- Request model and an explicit, validated lifecycle state machine.
- The `SchedulingPolicy` interface and five policies: FIFO, Priority, SJF,
  EDF, and the deadline-aware Hybrid — all sharing deterministic tie-breaking.
- An optional aging mechanism for starvation protection.
- A bounded admission controller with immediate-reject and deadline-aware
  overload strategies and distinct rejection reasons.
- A concurrency-safe resource manager tracking execution slots, active
  sequences, and KV-cache blocks with atomic reservations and exactly-once
  release.
- A deterministic discrete-event continuous-batching simulator, plus a narrow
  `ExecutionBackend` interface with mock and simulator adapters.
- A runtime coordinator (ingress → admission → policy → resources → backend)
  with graceful shutdown.
- An HTTP API (submit, query, cancel, scheduler status, resources) and
  Prometheus `/metrics` with bounded labels and structured logs.
- A reproducible Python workload generator and benchmark harness, and a
  performance report with measured simulation results.
- An **optional Rust scheduling core** behind the same `Policy` interface via a
  narrow C ABI, with Go↔Rust equivalence tests.
- Docker for reproducible experiments.

The remaining optional work is a real inference backend (vLLM/llama.cpp), which
requires GPU hardware and is explicitly out of scope for the default
deliverable. See the [Definition of done](#definition-of-done) for the
criterion-by-criterion status.

## Requirements

- Go 1.24+ (developed against Go 1.27)
- Rust (Phase 7, optional)
- Python 3.10+ with `numpy` and `matplotlib` (Phase 5, benchmark tooling)
- Docker + Docker Compose (reproducible experiments)

## Build and test

```sh
make build          # build bin/infersched
make test           # run Go unit tests
make test-race      # run tests with the race detector
make fmt vet        # format and vet
```

Or directly:

```sh
go build ./...
go test ./...
go test -race ./...
```

## Demo

Run the HTTP runtime (mock execution backend):

```sh
make build
./bin/infersched -serve -addr :8080 -policy sjf
```

Then, from another shell:

```sh
# submit a request
curl -s -X POST localhost:8080/v1/requests \
  -d '{"prompt_tokens":128,"expected_output_tokens":32,"priority":50}'
# {"id":"req-...","state":"QUEUED"}

# query it, scheduler and resources
curl -s localhost:8080/v1/requests/<id>
curl -s localhost:8080/v1/scheduler
curl -s localhost:8080/v1/resources

# cancel and scrape metrics
curl -s -X POST localhost:8080/v1/requests/<id>/cancel
curl -s localhost:8080/metrics
```

Validate a workload trace:

```sh
go run ./cmd/infersched -validate-trace benchmarks/workloads/sample-short-interactive.jsonl
```

```
trace benchmarks/workloads/sample-short-interactive.jsonl: valid v1
  profile        : A-short-interactive
  seed           : 42
  requests       : 5
  with deadline  : 2
  total prompt tk: 10720
  total output tk: 888
```

Run a trace through admission control + resource accounting (no execution):

```sh
go run ./cmd/infersched -admission-demo benchmarks/workloads/sample-short-interactive.jsonl -policy sjf -kv-blocks-per-worker 8
```

```
admission demo: benchmarks/workloads/sample-short-interactive.jsonl (policy=sjf)
  requests       : 5
  admitted       : 1
  rejected       : 2
  reserveFailed  : 2
  slots          : 1/16 used
  kv blocks      : 10/16 used
  reject reasons : map[DEADLINE_INFEASIBLE:2 OVERLOAD_QUEUED_TOKENS:2]
```

Replay a trace through the deterministic simulator and compare policies:

```sh
go run ./simulator/cmd/sim -trace benchmarks/workloads/sample-short-interactive.jsonl
```

```
policy      done    rej    exp   fail   canc | p50TTFT p90TTFT p99TTFT |  p50comp  p90comp  p99comp |  thr/s
fifo           5      0      0      0      0 |     3.0     3.1     3.1 |     97.5    769.5    769.5 |    6.5
...
note: SIMULATION output, not real-hardware measurements
```

Generate a workload, run the benchmark suite, and produce plots:

```sh
make gen-workloads        # profiles A-G at seed 1 under benchmarks/workloads/
make bench                # full sweep across seeds + plots (SIMULATION output)
```

Results land in `benchmarks/results/` (`summary.md`, `summary.json`, `raw/`,
`plots/`, `environment.json`). Conclusions are written up in
[docs/performance-report.md](docs/performance-report.md).

### Optional Rust scheduling core

The scheduling policies also exist in Rust behind the same interface, linked
through a narrow C ABI. The default build does **not** require a Rust toolchain.

```sh
make rust-test          # cargo unit tests for the core
make rust-equivalence   # Go vs Rust ordering equivalence tests
make build-rust         # build the coordinator with -tags rustcore

# replay a trace using the Rust policy backend
go run -tags rustcore ./simulator/cmd/sim \
  -trace benchmarks/workloads/sample-short-interactive.jsonl -policy-backend rust
```

See the cross-language boundary section in
[docs/architecture.md](docs/architecture.md).

## Repository layout

```
cmd/infersched/        runtime coordinator entrypoint
internal/lifecycle/    request model + state machine            (Phase 1)
internal/scheduler/    SchedulingPolicy interface + policies      (Phase 1-2)
internal/workload/     versioned JSONL trace format             (Phase 1)
internal/admission/    bounded admission control                (Phase 3)
internal/resources/    compute + KV-cache accounting            (Phase 3)
internal/backend/      execution backend interface              (Phase 4)
internal/api/          HTTP ingress                             (Phase 6)
internal/coordinator/  runtime wiring + lifecycle               (Phase 6)
internal/observability/ Prometheus metrics + logs               (Phase 6)
internal/simulator/    discrete-event continuous batching        (Phase 4)
simulator/cmd/sim/     simulator replay / benchmark CLI          (Phase 4-5)
benchmarks/scripts/    workload generator + benchmark harness   (Phase 5)
benchmarks/workloads/  generated JSONL traces                   (Phase 5)
benchmarks/results/    summary, plots, environment              (Phase 5)
crates/scheduler-core/ Rust scheduling core (C ABI)             (Phase 7)
docs/                  architecture, policies, methodology, perf report
```

## Verification

```sh
make fmt vet test-race        # formatting, vet, and Go tests under the race detector
make build-rust rust-test     # optional Rust core: build + cargo tests
make rust-equivalence         # Go <-> Rust ordering equivalence tests
make bench                    # full benchmark sweep + plots (SIMULATION output)
```

## Definition of done

| # | Criterion | Status |
|---|---|---|
| 1 | Multiple policies behind one scheduler interface | ✅ FIFO, Priority, SJF, EDF, Hybrid (+ aging) |
| 2 | Reproducible workload generator | ✅ profiles A–G, seeded, JSONL |
| 3 | Simulator models batching, prefill, decode, KV limits | ✅ `internal/simulator` |
| 4 | Bounded admission, explicit overload rejection | ✅ `internal/admission` |
| 5 | Cancellation + resource accounting pass concurrency tests | ✅ race-tested |
| 6 | Benchmark suite compares all policies on identical traces | ✅ `benchmarks/` |
| 7 | Performance report with measured trade-offs | ✅ `docs/performance-report.md` |
| 8 | API lifecycle ops and metrics work | ✅ `internal/api` + `/metrics` |
| 9 | Docs explain design and simulation limits | ✅ `docs/` |
| 10 | Build & test via documented commands | ✅ `make` targets |

## Limitations and honesty

- All latency, throughput, and memory figures come from a **simulator**, never
  from real hardware. Simulation results are labelled distinctly and are never
  presented as hardware measurements.
- Output lengths are **predictions**. The cost model is configurable and its
  error is an experimental variable.
- Deadlines are not guaranteed under overload. Admission control may reject or
  defer requests that cannot finish in time, and rejection reasons are recorded.
- No claim of GPU scheduling or real KV-cache utilization is made while only a
  CPU-side simulation exists.
- The Rust scheduling core is optional and implements ordering only; it is
  verified equivalent to Go but is not faster in an unconditional sense (see
  `docs/architecture.md`).
