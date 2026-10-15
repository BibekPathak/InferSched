# Scheduling Policies

All policies implement the same interface (`internal/scheduler/policy.go`) and
are compared on identical arrival traces, worker capacity, and resource limits.
A policy returns an ordered list of requests; the coordinator then attempts to
admit and dispatch them in that order.

Ordering is always fully deterministic: ties are broken by `Arrival` (monotonic
nanoseconds), then by `ID`. No policy may depend on map iteration order, wall
clock, or floating-point nondeterminism.

> **Cost is estimated, not measured.** "Shortest estimated work" means shortest
> *estimated* work. Expected output length is a prediction; its error is an
> experimental variable, not an assumption of exactness.

## Implemented

### FIFO
- **Order:** ascending `Arrival`, ties by `ID`.
- **Rationale:** simplest correct baseline; establishes the reference point that
  every other policy must beat to justify its complexity.
- **Known weaknesses:** head-of-line blocking — a single long request delays all
  shorter requests behind it; ignores deadlines, priority, and cost entirely.

### Priority
- **Order:** descending `Priority`, ties by arrival then `ID`.
- **Starvation:** priority alone starves low-priority work under sustained load.
  Enable aging to bound this; the benchmark suite reports starvation incidents.

### Shortest Estimated Work First (SJF)
- **Order:** ascending `CostModel.Work(req)` =
  `PrefillWeight·PrefillCostMS + DecodeWeight·OutputErrorFactor·ExpectedOutputTokens·DecodeCostPerTokenMS`.
  Token counts are never treated as execution times; the model is injectable and
  `OutputErrorFactor` explicitly models output-length prediction error.
- **Rationale:** minimises mean queueing delay when estimates are accurate.
- **Known weaknesses:** long-request tail latency (convoy effect); sensitive to
  prediction error. Both are measured, not assumed away.

### Earliest Deadline First (EDF)
- **Order:** ascending `Deadline`; requests with no deadline are ordered *after*
  all deadline-bearing requests (FIFO among themselves).
- **Infeasible deadlines:** `Feasible(req)` returns false when
  `Deadline − now − SlackMS < CostModel.Work(req)`. The policy does **not** claim
  it can meet deadlines under overload; infeasibility is surfaced so admission
  control can reject or defer, and infeasible requests executed anyway count as
  deadline misses.

### Aging (fairness option)
- A request waiting beyond `AgingMS` accumulates boost
  `(waitSec − AgingMS·1e-3)·BoostPerSec`, capped at `MaxBoost`.
- Configurable per policy (`Aging`) and applied to Priority, SJF, and hybrid.
- Evaluated at an explicit reference instant (`NowFunc`) so ordering stays
  deterministic under replay. A nil `NowFunc` disables aging in effect.
- Success criterion is a measured reduction in starvation incidents and a better
  service-distribution tail, *not* a throughput claim.

### Deadline-Aware Hybrid
An explicit, additive scoring rule — no unexplained magic constants. Higher
score runs first:

```
score(req, now) =
      w_dl   * deadline_urgency(req, now)   // clamp01(1 − slack / HorizonMS)
    + w_work * work_factor(req)             // clamp01(1 − work / WorkNormMS)
    + w_wait * queue_delay(req, now)        // clamp01(wait / WaitNormMS)
    + w_pri  * priority_term(req)           // clamp01(pri / MaxPriority)
    + w_age  * aging_term(req, now)         // clamp01(wait / AgingCapMS)
```

- `deadline_urgency` is `0` for requests with no deadline.
- Every weight and normalisation constant lives in `HybridWeights` and is
  configurable. `DefaultHybridWeights()` is a starting point and is **not**
  claimed optimal: the benchmark suite sweeps the weights and reports
  sensitivity.
- The hybrid policy is evaluated against every baseline; negative results and
  baseline wins are reported.

## Comparison protocol

1. One trace is generated and its seed + config saved.
2. The identical trace is replayed against every policy.
3. Worker capacity and resource limits are identical across policies.
4. Multiple seeds per configuration; report median and spread.
5. p50/p90/p95/p99 reported with sample counts and a stated percentile method.
6. Negative results and baseline wins are documented explicitly.

A policy that improves p99 by starving long requests, or maximises throughput
while missing most interactive deadlines, is **not** an unconditional
improvement. No universal winner is declared; the report states which policy wins
for which workload and under what assumptions.
