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

## Implemented (Phase 1)

### FIFO
- **Order:** ascending `Arrival`, ties by `ID`.
- **Rationale:** simplest correct baseline; establishes the reference point that
  every other policy must beat to justify its complexity.
- **Known weaknesses:** head-of-line blocking — a single long request delays all
  shorter requests behind it; ignores deadlines, priority, and cost entirely.

Deprecation note: `Registry()` returns the built-in policies by name and is
extended as new policies land.

## Planned (Phase 2)

The following are specified here so their intent is fixed before implementation.
Weights and constants are configurable and documented at implementation time;
none are asserted to be optimal in advance.

### Priority
- **Order:** descending `Priority`, ties by FIFO.
- **Starvation:** priority alone starves low-priority work under sustained load.
  Mitigated by the aging mechanism below; reported as a starvation metric.

### Shortest Estimated Work First (SJF)
- **Order:** ascending `CostEstimate.TotalCostMS()` =
  `PrefillCostMS + ExpectedOutputTokens × DecodeCostPerTokenMS`.
- **Rationale:** minimises mean queueing delay when cost estimates are accurate.
- **Known weaknesses:** long-request tail latency; sensitive to output-length
  prediction error. Both are measured, not assumed away.

### Earliest Deadline First (EDF)
- **Order:** ascending `Deadline`; requests with no deadline are ordered *after*
  all deadline-bearing requests (FIFO among themselves).
- **Infeasible deadlines:** if `Deadline < now + estimated remaining work`, the
  request is treated as infeasible. The policy does **not** claim it can meet
  deadlines under overload; infeasibility is surfaced to admission control (which
  may reject or defer) and counted as a deadline miss if executed anyway.

### Deadline-Aware Hybrid
An explicit, additive scoring rule — no unexplained magic constants:

```
score(req, now) =
      w_dl   * deadline_urgency(req, now)      // 0..1, higher = more urgent
    + w_work * work_factor(req)                // shorter work => higher
    + w_wait * normalized_queue_delay(req, now)
    + w_pri  * normalized_priority(req)
    + w_age  * aging_boost(req, now)           // starvation protection
```

- `deadline_urgency` = `1 - slack / horizon`, clamped to `[0,1]`; requests with
  no deadline contribute `0`.
- Each weight and the `horizon` are configurable. The policy is **not** assumed
  to be best: it is evaluated against every baseline, and negative results are
  reported.

### Aging (fairness option)
- Requests accumulate a boost proportional to time waited beyond a configurable
  threshold, eventually outranking fresher low-cost work.
- Applies to Priority, SJF and EDF configurations.
- Success criterion is a measured reduction in starvation incidents and an
  improvement in the service-distribution tail, *not* a throughput claim.

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
