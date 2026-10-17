# InferSched Performance Report

> **All numbers in this report are SIMULATION output from
> `internal/simulator`, not measurements of real hardware.** They come from the
> deterministic discrete-event model documented in
> [`benchmark-methodology.md`](benchmark-methodology.md), using
> `simulator.DefaultPerfModel()`, which is *uncalibrated*. Absolute latencies
> are illustrative; only the **relative** behaviour of policies, under the
> stated assumptions, should be read. No GPU scheduling, TTFT, or memory figure
> here is a real measurement.

## Environment and method

| Field | Value |
|---|---|
| Host OS | Linux x86_64 |
| Policies | fifo, priority, sjf, edf, hybrid |
| Seeds | 1, 2, 3 (medians reported across seeds) |
| Base capacity | 2 workers, batch 8, 512 KV blocks/worker, block size 16 |
| Percentile method | nearest-rank (`ceil(p/100·N)`, 1-based) |

Each trace was generated once per seed, then replayed against every policy under
identical capacity and resource limits. Raw per-run JSON, the summary,
environment metadata, and plots are stored under `benchmarks/results/`. The full
table is `benchmarks/results/summary.md`; plots are in
`benchmarks/results/plots/`.

Reproduce with:

```sh
python3 benchmarks/scripts/run_benchmarks.py --out benchmarks/results \
    --seeds 1,2,3 --suite profiles,sweep,capacity
python3 benchmarks/scripts/plot_results.py benchmarks/results
```

## Summary of findings

1. **No policy wins universally.**
2. **Under-loaded, all policies are equivalent** — when there is no queueing,
   ordering cannot matter (profiles A, E, F).
3. **EDF is the worst policy under deadline overload** — worse than FIFO —
   because it prioritises the earliest deadlines, which under overload are
   already infeasible, burning capacity on doomed work. A genuine negative
   result for a policy one might assume is best for deadlines.
4. **SJF is the best all-rounder under deadline overload**: highest throughput,
   lowest queue delay, lowest p95 TTFT, and a deadline-miss rate far below
   priority and EDF.
5. **SJF/Hybrid penalise long-request tail latency** (long-context profile): the
   classic shortest-job fairness cost, visible as increased p95 TTFT for long
   requests.
6. **KV capacity, not scheduling, dominates** when requests cannot fit
   concurrently: policies converge because the bottleneck is memory admission.

## Workload profiles (base capacity)

### A — short interactive (under-loaded)
All five policies are identical: 300/300 completed, p95 TTFT ≈ 1 ms, p99
completion ≈ 190 ms, ~38.5 rps. No contention, so order is irrelevant. **Expected
null result.**

### B — long-context (memory-bound)
~79/80 completed, 1 rejected (a request whose KV footprint exceeded the pool,
correctly rejected as infeasible rather than hung). FIFO/priority/EDF: p95 TTFT
≈ 23.5 s. **SJF and Hybrid are worse** (p95 TTFT ≈ 29–31 s): preferring short
work delays long-context requests further. A scheduling policy that improves
short-request latency at the cost of long-request tail latency is not an
unconditional improvement.

### C — mixed interactive + batch
All policies complete ~326 requests. SJF has the lowest p90 queue delay
(1.08 s vs 4.93 s for FIFO) and slightly best throughput (9.2 vs 9.1 rps);
Hybrid has the best p99 completion (24.0 s vs 25.1 s).

### D — bursty
All policies complete 300/300. Hybrid has the best p99 completion (1.30 s vs
1.42 s FIFO) at slightly lower throughput (85.4 vs 87.8 rps). **SJF has the
worst p99 completion (1.85 s)** despite the best p95 TTFT — again trading
long-request tail for short-request latency.

### E — deadline-constrained (base)
All policies identical (300 done, 32% miss, p95 TTFT ≈ 7 ms): at this arrival
rate and capacity the system is not contended, so deadlines are missed because
the SLAs are intrinsically tight, not because of ordering.

### F — overload then recovery
All policies complete 480/480 with near-identical tails; the recovery window
drains the backlog before it accumulates. No policy advantage claimed.

### G — multi-tenant
SJF completes the same work with the best p95 TTFT (15.7 s vs 18.2 s FIFO) and
best p99 completion (21.3 s). FIFO/EDF are identical. Per-tenant admission
limits were not enabled, so fairness across tenants is not differentiated here.

## Utilization sweep (sweep-C, sweep-E)

### sweep-C — saturation

| policy | done | p95 TTFT ms | p99 comp ms | p90 qdelay ms | rps |
|---|---|---|---|---|---|
| fifo | 514 | 21404 | 37359 | 10932 | 11.0 |
| priority | 514 | 20983 | 39895 | 9158 | 10.9 |
| sjf | 514 | **16407** | 35075 | **5447** | 10.8 |
| edf | 514 | 21404 | 37359 | 10932 | 11.0 |
| hybrid | 514 | **15358** | 36295 | 6701 | 11.0 |

SJF cuts p95 TTFT ≈ 23% and p90 queue delay ≈ 50% versus FIFO at a small
throughput cost. Hybrid cuts p95 TTFT ≈ 28% with throughput equal to FIFO.

### sweep-C — overload
Same ordering of results: SJF/Hybrid reduce p95 TTFT (33.0 s / 32.9 s vs 43.0 s
FIFO) and queue delay, throughput approximately unchanged.

### sweep-E — the most informative result

**Saturation** (≈60 rps, 400 requests):

| policy | done | expired | dlmiss% | p95 TTFT | p99 comp | p90 qdelay | rps |
|---|---|---|---|---|---|---|---|
| fifo | 388 | 12 | 32.8 | 203 | 576 | 149 | 54.6 |
| priority | 397 | 3 | 32.2 | 500 | 1110 | 259 | 55.9 |
| sjf | 390 | 10 | **29.8** | 157 | 768 | 96 | 55.4 |
| edf | 398 | 2 | 32.0 | 420 | 1156 | 307 | 55.8 |
| hybrid | 398 | 2 | 33.2 | 611 | 1223 | 323 | 55.5 |

**Overload** (≈120 rps, 600 requests):

| policy | done | expired | dlmiss% | p95 TTFT | p99 comp | p90 qdelay | rps |
|---|---|---|---|---|---|---|---|
| fifo | 397 | 203 | **13.8** | 800 | 1361 | 714 | 67.6 |
| priority | 456 | 144 | 37.5 | 1977 | 2427 | 1758 | 66.3 |
| sjf | **480** | 120 | 20.3 | **646** | 1945 | **236** | **78.4** |
| edf | 479 | 121 | **40.7** | 2055 | 2425 | 1995 | 69.4 |
| hybrid | 452 | 148 | 23.0 | 1816 | 2363 | 1644 | 67.2 |

- **SJF is the best all-rounder under deadline overload**: highest throughput
  (78.4 rps), lowest queue delay (236 ms), lowest p95 TTFT (646 ms), and a
  deadline-miss rate (20.3%) far below priority (37.5%) and EDF (40.7%).
- **EDF has the worst deadline-miss rate (40.7%)** — worse than plain FIFO. Under
  overload the earliest deadlines are already infeasible, so EDF systematically
  burns capacity on doomed requests and delays feasible ones.
- **FIFO has the lowest raw deadline-miss rate (13.8%)** but also the lowest
  completion count (397) because it lets 203 requests expire untouched rather
  than rejecting them. Deadline-miss rate alone is misleading.
- **Hybrid sits between SJF and FIFO**; its default weights are not tuned for
  this workload — a finding, not a defect.

## Capacity sensitivity (cap-C)

Holding traffic constant and varying KV capacity:

| variant | policy | done | rejected | p95 TTFT | p90 qdelay |
|---|---|---|---|---|---|
| tiny-kv (64 blocks/w, batch 4) | fifo | 308 | 92 | 978 | 585 |
| tiny-kv | priority | 308 | 92 | 747 | 409 |
| tiny-kv | sjf | 308 | 92 | 1039 | 448 |
| large-kv (2048 blocks/w, batch 16) | fifo | 400 | 0 | 534 | 274 |
| large-kv | sjf | 400 | 0 | **229** | **68** |

- With **tiny KV**, ~23% of requests are rejected and policies converge: the
  bottleneck is memory admission, not ordering.
- With **large KV**, all requests fit and SJF's advantage is largest (p95 TTFT
  229 ms vs 534 ms, p90 queue delay 68 ms vs 274 ms).

## Conclusions (workload-specific)

| Workload | Best policy | Under what assumptions |
|---|---|---|
| Short interactive, under-loaded | tie (any) | no contention |
| Long-context, memory-bound | FIFO / priority / EDF over SJF | long-request tail latency matters |
| Mixed, saturated/overloaded | SJF or Hybrid | short-request latency prioritised |
| Deadline-constrained, overloaded | SJF | throughput + deadline-miss both matter |
| Multi-tenant (no per-tenant limits) | SJF | fairness not yet exercised |

**There is no universal winner.** SJF and Hybrid win on queueing delay and
short-request latency under contention but penalise long-request tails. FIFO is
the safest default when the system is not saturated. EDF, despite its
theoretical appeal for deadlines, is the worst policy in the deadline-overload
case because it cannot distinguish feasible from infeasible deadlines.

## Limitations of these results

- The performance model is **uncalibrated**. Relative comparisons are meaningful
  only within this model; absolute values are not.
- Output length is a **perfect prediction** in these traces. Sensitivity to
  prediction error (`CostModel.OutputErrorFactor`) is not yet swept.
- No per-tenant admission limits were enabled, so fairness across tenants is not
  differentiated.
- EDF does not perform deadline-feasibility filtering before ordering; a
  feasibility-aware admission policy (`admission.StrategyDeadlineAware`) is
  expected to change its numbers and is a documented next step.
- Only 3 seeds were used; medians are reported but per-percentile spread is not
  yet plotted.
- Percentiles use the nearest-rank method; with ~400 samples the p99 is
  estimated from ~4 observations and is noisy.

## Reproducing

```sh
make build
python3 benchmarks/scripts/run_benchmarks.py --out benchmarks/results \
    --seeds 1,2,3 --suite profiles,sweep,capacity
python3 benchmarks/scripts/plot_results.py benchmarks/results
```

Raw results: `benchmarks/results/raw/*.json`. Summary:
`benchmarks/results/summary.{json,md}`. Environment:
`benchmarks/results/environment.json`. Manifest:
`benchmarks/results/manifest.json`.
