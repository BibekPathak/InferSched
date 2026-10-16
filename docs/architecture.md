# InferSched Architecture

InferSched is a deadline-aware inference scheduling runtime that decides *which*
request runs next, *when* a request is admitted, and *when* a request is rejected
or deferred — under finite compute and KV-cache memory.

This document describes the system as designed. Phases are marked so a reader
can tell what is implemented from what is planned.

## Component map

| # | Component | Package | Phase |
|---|-----------|---------|-------|
| 1 | Request ingress + validation | `internal/api` | 6 |
| 2 | Bounded admission controller | `internal/admission` | **3** |
| 3 | Request metadata + lifecycle | `internal/lifecycle` | **1** |
| 4 | Scheduling policy engine | `internal/scheduler` | 1 (interface + FIFO), **2** (priority, SJF, EDF, hybrid, aging) |
| 5 | Compute + memory resource manager | `internal/resources` | **3** |
| 6 | Continuous-batching simulator | `simulator/` | 4 |
| 7 | Execution backend interface | `internal/backend` | 4/6 |
| 8 | Metrics + tracing | `internal/observability` | 6 |
| 9 | Workload generator + benchmark harness | `benchmarks/`, `internal/workload` | **1** (format), 5 (generator) |
| — | Coordinator wiring | `internal/coordinator` | 6 |
| — | Rust scheduling core | `crates/scheduler-core` | 7 (optional) |

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

## Cross-language boundary

The scheduling core is written in Go for Phases 1–6. Phase 7 adds a Rust
`scheduler-core` crate behind the same `Policy` interface, bridged through a
narrow C ABI (`c-archive` + cgo adapter). The interface is deliberately small —
ordering in, ordering out — so the FFI does not leak into the coordinator, the
simulator, or the benchmark harness.

## Correctness posture

- State transitions are the single choke point for lifecycle changes.
- Resource release is driven off guarded transitions so it happens exactly once.
- Monotonic time everywhere internally.
- Determinism is a testable property: same seed + same trace ⇒ identical output.

## Limitations (through Phase 3)

- No HTTP API, simulator, or metrics yet.
- Scheduling policies are implemented but not yet driven end-to-end by a
  simulator.
- Admission and resource accounting are implemented and tested in isolation and
  through an integration pipeline, but are not yet wired into the coordinator.
- All cost figures are estimates; no real backend has been measured.
