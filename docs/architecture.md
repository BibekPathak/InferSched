# InferSched Architecture

InferSched is a deadline-aware inference scheduling runtime that decides *which*
request runs next, *when* a request is admitted, and *when* a request is rejected
or deferred — under finite compute and KV-cache memory.

This document describes the system as designed. Phases are marked so a reader
can tell what is implemented from what is planned.

## Component map

| # | Component | Package | Phase |
|---|-----------|---------|-------|
| 1 | Request ingress + validation | `internal/api` | **6** |
| 2 | Bounded admission controller | `internal/admission` | **3** |
| 3 | Request metadata + lifecycle | `internal/lifecycle` | **1** |
| 4 | Scheduling policy engine | `internal/scheduler` | 1 (interface + FIFO), **2** (priority, SJF, EDF, hybrid, aging) |
| 5 | Compute + memory resource manager | `internal/resources` | **3** |
| 6 | Continuous-batching simulator | `internal/simulator` | **4** |
| 7 | Execution backend interface | `internal/backend` | **4** (mock + sim adapters **6**) |
| 8 | Metrics + tracing | `internal/observability` | **6** |
| 9 | Workload generator + benchmark harness | `benchmarks/`, `internal/workload` | 1 (format), **5** (generator) |
| — | Coordinator wiring | `internal/coordinator` | **6** |
| — | Rust scheduling core | `crates/scheduler-core` | **7** (optional) |

## Data flow (target)

```
        client
          │  (1) submit
          ▼
   ┌─────────────┐   reject (2)   ┌──────────────────────┐
   │  ingress /  │───────────────▶│  admission controller │
   │ validation  │                │  bounds: queue len,   │
   └─────────────┘                │  queued tokens,        │
          │ accept                │  outstanding, tenants │
          ▼                       └──────────┬───────────┘
   ┌─────────────┐   order (4)              │ admitted
   │  scheduler  │◀─────────────────────────┤
   │  (policy)   │                          ▼
   └──────┬──────┘                ┌──────────────────────┐
          │ schedule              │ (5) resource manager  │
          ▼                       │ slots, seqs, KV blocks│
   ┌─────────────┐  reserve/free └──────────────────────┘
   │  backend    │◀──────────────┐
   │ (simulator) │               │
   └──────┬──────┘               │
          │ complete/cancel/fail │
          ▼                      │
   lifecycle state machine ──────┘ (release exactly once)
          │
          ▼
   observability (8): metrics + structured logs
```

## Admission control and resource accounting (components 2 & 5)

### Admission controller (`internal/admission`)

The controller bounds the system *before* resources are consumed. `Evaluate(r)`
is the read-only half of a two-phase admit and returns a `Decision` with a
bounded `lifecycle.Reason`; the coordinator then reserves resources and calls
`Commit` (or, on resource failure, commits nothing). This ordering guarantees a
rejected request never consumes resources and a request that fails reservation
leaves no partial state.

Hard bounds (all optional, `0` = unlimited):

- `MaxQueueLength` — requests waiting to be admitted.
- `MaxQueuedTokens` — estimated KV tokens represented by queued requests.
- `MaxOutstanding` — requests admitted or running.
- `MaxPerTenant` — outstanding requests per tenant.

Two overload strategies:

1. **`StrategyReject`** — reject any request that would exceed a hard bound.
2. **`StrategyDeadlineAware`** — additionally reject requests whose deadline is
   infeasible under a conservative estimate (`remaining < work + slack`). It does
   not claim deadlines can be met under overload; it rejects what cannot finish.

Every rejection records a distinct reason (`OVERLOAD_QUEUE_LENGTH`,
`OVERLOAD_QUEUED_TOKENS`, `OVERLOAD_OUTSTANDING_REQUESTS`, `OVERLOAD_TENANT_LIMIT`,
`DEADLINE_INFEASIBLE`). Requests are never silently dropped.

`Evaluate`/`Commit` counters are single-goroutine by contract (the coordinator
serializes them); the resource manager is independently concurrent-safe.

### Resource manager (`internal/resources`)

Models `Workers × MaxBatchSize` execution slots and `Workers × KVBlocksPerWorker`
KV blocks. A request's KV footprint is `ceil(KVTokens / KVBlockSize)` blocks.

- `Reserve` is atomic and all-or-nothing: if any dimension would exceed capacity
  it returns `ErrInsufficientCapacity` and mutates nothing, so a failed admission
  rolls back by construction. Reserving an already-reserved request is a no-op
  returning the existing reservation (idempotent admission).
- `Release` frees a reservation exactly once; a second release returns
  `ErrUnknownReservation` and leaves state unchanged, making double release
  detectable and harmless.
- `Invariants()` re-derives all totals from the per-request reservations and
  checks they match the running counters and never exceed capacity. It is used by
  the concurrency stress tests.

This is a **model** of resource consumption. It does not allocate GPU memory and
makes no claim about real device occupancy.

## Continuous-batching simulator and backend interface (components 6 & 7)

### Execution backend interface (`internal/backend`)

`ExecutionBackend` is the narrow seam between the coordinator and whatever runs
inference: the deterministic simulator, a mock backend for lifecycle/failure
testing, or a real engine. It deals in `Submit`/`Cancel`/`Results` and reports
request-level milestones (start, first token, end, outcome). It does **not**
depend on any model-serving framework.

### Simulator (`internal/simulator`)

A deterministic discrete-event model of continuous-batching inference:

- Events are ordered by `(virtual_time, insertion_sequence)`, so ties break
  deterministically and a run is reproducible for a fixed seed + trace.
- Input requests are **cloned**, so repeated runs over the same slice are
  independent and the caller's slice is never mutated.
- Workers run up to `MaxBatchSize` sequences; requests join mid-flight and are
  included in the next step (continuous batching).
- Prefill and decode have distinct cost curves (`PerfModel`), capturing that
  prefill scales with prompt tokens and decode costs one step per active
  sequence, both with configurable batching speedups.
- KV-cache is block-accounted; a request cannot start without free blocks and
  releases exactly once on completion, cancellation, expiry, or failure.
- Cancellation and deadline events are delivered through the same event queue;
  worker failures fail a worker's active requests and reconcile resources.
- The scheduling policy's reference clock is wired to virtual time, so aging,
  deadline, and hybrid terms are evaluated at deterministic instants.

The simulator's assumptions and limitations are specified in
[`benchmark-methodology.md`](benchmark-methodology.md). **All simulator output is
modelled, not measured.**

## Runtime coordinator, API, and observability (components 1, 8, and wiring — Phase 6)

### Coordinator (`internal/coordinator`)

The coordinator owns the running runtime. It runs a **single scheduling
goroutine** (`run.go`) so that all lifecycle mutations, admission-counter
updates, and resource release happen serialized; the admission controller's
counters therefore need no internal locking, while the resource manager remains
independently concurrency-safe.

Each loop iteration: drains backend results, expires deadlines, evaluates
admission for newly submitted requests, then orders admitted requests with the
policy and starts as many as capacity allows by reserving resources and calling
`Backend.Submit`. Terminal transitions release resources **exactly once**
(guarded by a per-record flag and the request's own state machine).

Shutdown cancels queued and running work, drives every outstanding record to a
terminal state, releases resources, closes the backend, and wakes waiting
callers. It is idempotent and context-bounded.

### Execution backends (`internal/backend`)

- `MockBackend` — deterministic, configurable duration, supports cancellation
  and a fail-every-N mode for lifecycle/failure tests.
- `SimBackend` — adapts the discrete-event simulator to the `ExecutionBackend`
  interface (trace-oriented: buffers submissions, executes on `RunAll`/`Close`).

### HTTP API (`internal/api`)

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/requests` | submit a request (JSON; unknown fields rejected) |
| GET | `/v1/requests/{id}` | query request state |
| POST | `/v1/requests/{id}/cancel` | cancel a request |
| DELETE | `/v1/requests/{id}` | cancel a request |
| GET | `/v1/scheduler` | scheduler/policy status and counters |
| GET | `/v1/resources` | resource occupancy |
| GET | `/metrics` | Prometheus text exposition |
| GET | `/healthz` | liveness |

The API is thin: it validates input, delegates to the coordinator, and
serializes responses. `http.Server` timeouts are set; request bodies are
size-bounded (1 MiB) and unknown JSON fields are rejected.

### Metrics and logging (`internal/observability`)

Prometheus text exposition with a **dependency-free** implementation. Every
label value is drawn from a closed set (lifecycle states, bounded rejection
reasons, outcomes, policy names); request IDs and user-supplied strings are
**never** used as labels, so clients cannot create unbounded cardinality.
Structured JSON logs record the request lifecycle (`request.submitted`,
`request.admitted`, `request.started`, `request.terminal`, ...) with the request
ID as a field.

## Cross-language boundary (Phase 7)

The scheduling core exists in two implementations behind the same
`scheduler.Policy` interface:

- **Go** (`internal/scheduler`) — the default, always built.
- **Rust** (`crates/scheduler-core`) — the same policies, linked in a binary
  built with the `rustcore` build tag.

### Why a second implementation

The Rust core demonstrates a narrow, low-risk cross-language seam with
ownership-friendly sorting. It is **optional**: the project builds, tests, and
benchmarks fully without a Rust toolchain. Nothing in the coordinator, simulator,
or benchmark harness depends on it.

### The ABI

The boundary is deliberately minimal and allocation-free:

```c
typedef struct { uint32 index; int64 arrival_ns; int64 deadline_ns; int32 priority;
                 double prefill_cost_ms; double decode_cost_per_token_ms;
                 int64 expected_output_tokens; int64 kv_tokens; } infersched_request_view;

int32_t infersched_order(uint32_t policy_kind, const infersched_hybrid_weights *w,
                         int64_t now_ns, const infersched_request_view *reqs,
                         size_t n, uint32_t *out);
```

- The caller passes a flat array of request **metadata** and receives a
  permutation of input **positions**. No Rust types, allocators, or error values
  cross the boundary.
- `Go → C`: the adapter (`internal/scheduler/rustcore.go`, `//go:build rustcore`)
  marshals metadata into a C array and calls `infersched_order` once per
  `Order`.
- `C → Rust`: `unsafe extern "C" fn infersched_order` reconstructs slices and
  sorts stable indices with the same comparator as Go.
- The C header (`crates/scheduler-core/include/scheduler_core.h`) and the Rust
  `#[repr(C)]` structs are the single source of truth for layout; field order
  must match exactly.

### Determinism and equivalence

Both implementations break ties identically (arrival, then a stable position
that mirrors the Go ID order). The build-tagged test
`TestRustGoEquivalence` runs randomized request sets through every policy on
both implementations and asserts byte-identical orderings; it passes for all
five policies over hundreds of sets. A full trace replay through the simulator
with `-policy-backend go` and `-policy-backend rust` produces identical
statistics.

### Building

```sh
make rust-test           # cargo test in crates/scheduler-core
make build-rust          # build the coordinator with -tags rustcore
make rust-equivalence    # Go vs Rust ordering equivalence tests
```

### Limits

- Only request **ordering** is delegated; resource accounting, admission, and
  the event loop remain in Go.
- The Rust policy is stateless per call (no aging counters of its own); aging
  terms are computed from the reference time passed in.
- The `.a` is linked statically; there is no dynamic-library or versioned-symbol
  scheme.

## Request lifecycle (component 3)

A request is created in `QUEUED` and moves through an explicit, tabulated state
machine (`internal/lifecycle/state.go`). Every transition is validated against
`transitionTable`; anything not listed is rejected with a `TransitionError`.
This is the mechanism that prevents illegal transitions and double resource
release — release side effects are driven off the *previous* state returned by
the guarded transition.

```
QUEUED ──admit──▶ ADMITTED ──start──▶ RUNNING ──complete──▶ COMPLETED
   │                  │                   │
   ├─reject──▶ REJECTED                   ├─fail──▶ FAILED
   ├─cancel──▶ CANCELLED ◀──cancel────────┤
   └─expire──▶ EXPIRED  ◀──expire─────────┘
```

Terminal states (`COMPLETED`, `REJECTED`, `CANCELLED`, `EXPIRED`, `FAILED`) have
no outgoing transitions. `Cancel` is idempotent from the caller's perspective:
the first call succeeds, subsequent calls return a `TransitionError`, so the
coordinator can detect double cancellation without panicking.

### Time model

- **Monotonic time** (nanoseconds since a runtime baseline) is authoritative for
  `Arrival`, `Deadline`, elapsed time, and all policy ordering.
- **Wall-clock** (`WallClockArrival`) is retained only for external reporting and
  correlations. It never participates in scheduling decisions.

### Cost is an estimate

`CostEstimate` carries `PrefillTokens`, `ExpectedOutputTokens`,
`PrefillCostMS`, `DecodeCostPerTokenMS`, and `KVTokens`. Expected output length
is never known exactly; the cost model that fills these fields is configurable
and calibrated to backend behaviour, and prediction error is a first-class
experimental variable.

## Scheduling policy engine (component 4)

Every policy implements the same interface (`internal/scheduler/policy.go`):

```go
type Policy interface {
    Name() string
    Order(reqs []*lifecycle.Request) []*lifecycle.Request
    Reset()
}
```

Policies are pure with respect to ordering: given the same requests and metadata
they return a deterministic ordering. Ties are always broken by a stable key
(arrival, then request ID) so that runs are reproducible. `Reset` clears any
per-run state (e.g. aging counters) before a trace replay.

Phase 1 shipped `FIFO`. Phase 2 adds priority, shortest-estimated-work-first,
earliest-deadline-first, a deadline-aware hybrid, and optional aging, all sharing
the same deterministic tie-breaking (`orderBy` falls back to arrival then ID).
See [`scheduling-policies.md`](scheduling-policies.md).

## Workload traces

Workload traces are versioned JSONL. Line 1 is a header (format version, profile,
seed, count, generator config); each subsequent line is a request arrival. The
format lives in `internal/workload` and is validated on load; a mismatched
`request_count` or unsupported `format_version` is a hard error. Because traces
are files, the *same* arrivals are replayed against every policy, in any language
or process, guaranteeing identical input.

## Correctness posture

- State transitions are the single choke point for lifecycle changes.
- Resource release is driven off guarded transitions so it happens exactly once.
- Monotonic time everywhere internally.
- Determinism is a testable property: same seed + same trace ⇒ identical output.

## Limitations (through Phase 7)

- The default `-serve` execution backend is a mock; the simulator backend is
  available through `ExecutionBackend` but has not yet been wired as the default
  server backend. No real inference engine is integrated.
- The Rust core is optional and implements ordering only; it is not on the
  default build path and does not accelerate anything by itself.
- All cost figures are model parameters; no real backend has been measured.
- Metrics are dependency-free rather than using `client_golang`; the exposition
  format is standard, but histograms/summaries are not implemented.
- The coordinator exposes request-level lifecycle (submit/status/cancel); there
  is no streaming/token-level API.
