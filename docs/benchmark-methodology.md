# Benchmark Methodology

This document specifies how InferSched is measured, and — equally important —
the assumptions and limitations of the deterministic simulator. **All results
produced before a real backend is integrated are simulation output.** They are
never presented as real-hardware measurements.

## Simulation model

The simulator (`internal/simulator`) is a deterministic discrete-event model of
continuous-batching LLM inference. It advances a virtual monotonic clock through
an event queue and honours resource limits exactly like the real resource
manager.

### What is modelled

- **Arrivals** from a versioned workload trace, replayed verbatim.
- **Admission** through the same bounded admission controller used in
  production paths (queue length, queued tokens, outstanding, tenant limits,
  deadline-aware feasibility).
- **Queueing**: admitted-but-not-started requests wait for a worker slot.
- **Continuous batching**: each worker runs up to `MaxBatchSize` sequences; a
  request may join a worker mid-flight and is included in the next step.
- **Prefill**: cost = `PrefillBaseMS + PrefillPerTokenMS · promptTokens /
  min(batch, PrefillMaxSpeedup)`. Prefill batches only within a step.
- **Decode**: one token per active sequence per step; cost =
  `DecodeBaseMS + DecodePerTokenMS · decodeBatch / min(decodeBatch,
  DecodeMaxSpeedup)`.
- **KV-cache**: block accounting with `blocks = ceil(tokens / KVBlockSize)`;
  a request cannot start without free blocks and releases them on completion,
  cancellation, expiry, or failure.
- **Cancellation**: delivered as a scheduled event, modelled for both queued and
  in-flight requests.
- **Deadlines**: expiry events for admitted-but-not-started requests.
- **Worker failure**: deterministic scheduled failure events fail a worker's
  active requests and reconcile their resources exactly once.

### Determinism

Given the same `Config` (including `Seed`) and the same request trace, a run is
reproducible. Events are ordered by `(virtual_time, insertion_sequence)`, so
ties resolve deterministically. The simulator clones its input requests, so
repeated runs over the same slice are independent. This is covered by
`TestDeterminismSameSeed` and `TestDeterminismAcrossFreshRequestSlice`.

### Performance model

`PerfModel` encodes the *shape* of inference cost:

| Parameter | Meaning |
|---|---|
| `PrefillBaseMS` | fixed per-request prefill overhead |
| `PrefillPerTokenMS` | marginal prefill cost per prompt token |
| `PrefillMaxSpeedup` | cap on prefill batching amortization |
| `DecodeBaseMS` | fixed per-step decode overhead |
| `DecodePerTokenMS` | marginal decode cost per sequence per step |
| `DecodeMaxSpeedup` | cap on decode batching amortization |

`DefaultPerfModel()` is a **plausible but uncalibrated** starting point. It is
meant to be calibrated against a measured backend; until then, absolute
latencies are illustrative and only *relative* policy behaviour should be read,
with care.

## Assumptions

1. Only one token is produced per sequence per decode step.
2. Prefill and decode share a fixed per-step cost structure; no overlap between
   prefill and decode across workers.
3. KV usage is estimated from the trace (`KVTokens`), not measured.
4. Running requests are not preempted at deadlines; only queued requests expire.
5. Worker failures fail a worker's active requests; no migration or retry.
6. Memory is modelled in integer KV blocks; there is no fragmentation model.
7. The cost model does not model attention quadratic cost explicitly; prompt
   length enters via `PrefillPerTokenMS`.

## Limitations

- **No real hardware.** TTFT, ITL, throughput, and memory are modelled. Do not
  cite them as measurements.
- **No GPU scheduler.** Slot/batch limits are a software model.
- **Estimate error.** `ExpectedOutputTokens` is a prediction; `CostModel`'s
  `OutputErrorFactor` lets experiments inflate it to study sensitivity.
- **Percentiles** use the nearest-rank method (`ceil(p/100·N)`, 1-based), stated
  on every report.

## Comparison protocol

1. Generate a trace once; save its seed and generator config.
2. Replay the *identical* trace against every policy.
3. Keep worker capacity and resource limits identical across policies.
4. Repeat with multiple seeds; report medians and spread.
5. Save raw results plus machine-readable environment metadata.
6. Emit Markdown tables and lightweight plots.
7. State the percentile method and sample counts.

## What a "win" means

A policy that improves p99 by starving long requests, or maximises throughput
while missing most interactive deadlines, is **not** an unconditional
improvement. Reports identify which policy wins for which workload and under
what assumptions, and document negative results and baseline wins.

## Distinguishing simulation from reality

Every table and plot produced by the Phase 4/5 tooling is labelled
`SIMULATION`. When an optional real backend (Phase 7) is integrated, its
measurements are reported in a separate, explicitly-labelled section and are
never mixed with simulation numbers.
