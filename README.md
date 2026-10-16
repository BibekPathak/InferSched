# InferSched

A deadline-aware LLM inference **scheduling runtime** that decides which request
executes next, when a request is admitted, and when it is rejected or deferred —
under finite compute and KV-cache memory.

This is a systems project: it implements the scheduling core, resource
accounting, admission control, a continuous-batching simulator, and a
reproducible benchmark suite. It is **not** an API wrapper around a model server.

## Status

**Phase 3 complete.** Implemented:

- Request model and an explicit, validated lifecycle state machine.
- The `SchedulingPolicy` interface and five policies: FIFO, Priority, SJF,
  EDF, and the deadline-aware Hybrid — all sharing deterministic tie-breaking.
- An optional aging mechanism for starvation protection.
- A bounded admission controller with queue-length, queued-token, outstanding,
  and per-tenant limits, plus immediate-reject and deadline-aware overload
  strategies with distinct, recorded rejection reasons.
- A concurrency-safe resource manager tracking execution slots, active
  sequences, and KV-cache blocks with atomic all-or-nothing reservations and
  exactly-once release.
- A versioned, reproducible JSONL workload-trace format with load/validate/write.
- Architecture and scheduling-policy documentation.
- Unit, invariant, and concurrency (race-tested) tests for the state machine,
  all policies, resource accounting, admission control, and their integration.

Not yet implemented (see [docs/architecture.md](docs/architecture.md) for the
full phase map): the continuous-batching simulator, the HTTP API, metrics, the
benchmark harness, and the Rust core.

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

## Repository layout

```
cmd/infersched/        runtime coordinator entrypoint
internal/lifecycle/    request model + state machine            (Phase 1)
internal/scheduler/    SchedulingPolicy interface + policies      (Phase 1-2)
internal/workload/     versioned JSONL trace format             (Phase 1)
internal/admission/    bounded admission control                (Phase 3)
internal/resources/    compute + KV-cache accounting            (Phase 3)internal/backend/      execution backend interface              (Phase 4/6)
internal/api/          HTTP ingress                             (Phase 6)
internal/observability/ Prometheus metrics + logs               (Phase 6)
simulator/             discrete-event continuous batching       (Phase 4)
benchmarks/            workload traces, scripts, results        (Phase 5)
crates/scheduler-core/ Rust scheduling core (C ABI)             (Phase 7)
docs/                  architecture, policies, methodology
```

## Limitations and honesty

- All latency, throughput, and memory figures will come from a **simulator**
  unless an actual inference backend is integrated and measured. Simulation
  results are labelled distinctly and are never presented as real hardware
  measurements.
- Output lengths are **predictions**. The cost model is configurable and its
  error is an experimental variable.
- Deadlines are not guaranteed under overload. Admission control may reject or
  defer requests that cannot finish in time, and rejection reasons are recorded.
- No claim of GPU scheduling or real KV-cache utilization is made while only a
  CPU-side simulation exists.

See [docs/architecture.md](docs/architecture.md) and
[docs/scheduling-policies.md](docs/scheduling-policies.md).
