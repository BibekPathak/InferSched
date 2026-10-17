/*
 * InferSched scheduler-core C ABI.
 *
 * This header declares the narrow interface between the Go coordinator and the
 * Rust scheduling core. The boundary passes a flat array of request metadata in
 * and receives a permutation of input positions (the execution order) out. No
 * allocation crosses the boundary and no Rust types leak.
 */
#ifndef INFERSCHED_SCHEDULER_CORE_H
#define INFERSCHED_SCHEDULER_CORE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* A request's scheduling metadata. Field order and types must match the Rust
 * `RequestView` #[repr(C)] struct exactly. */
typedef struct {
    uint32_t index;                  /* position in the caller's slice */
    int64_t  arrival_ns;             /* monotonic arrival time */
    int64_t  deadline_ns;            /* monotonic deadline, 0 = none */
    int32_t  priority;               /* higher = more important */
    double   prefill_cost_ms;        /* estimated prefill cost */
    double   decode_cost_per_token_ms;/* estimated decode cost per token */
    int64_t  expected_output_tokens; /* predicted output length */
    int64_t  kv_tokens;              /* estimated KV footprint */
} infersched_request_view;

/* Hybrid policy weights, must match Rust `HybridWeightsFfi`. */
typedef struct {
    double deadline_urgency;
    double work;
    double queue_delay;
    double priority;
    double aging;
    double horizon_ms;
    double wait_norm_ms;
    double work_norm_ms;
    double max_priority;
    double aging_cap_ms;
} infersched_hybrid_weights;

/* Policy kinds, must match Rust `PolicyKind`. */
enum {
    INFERSCHED_POLICY_FIFO     = 0,
    INFERSCHED_POLICY_PRIORITY = 1,
    INFERSCHED_POLICY_SJF      = 2,
    INFERSCHED_POLICY_EDF      = 3,
    INFERSCHED_POLICY_HYBRID   = 4
};

/* Order `n` requests under `policy_kind`, writing `n` input positions into
 * `out`. Returns the number written, or a negative error code:
 *   -1 null pointer, -2 unknown policy, -3 output buffer too small. */
int32_t infersched_order(
    uint32_t policy_kind,
    const infersched_hybrid_weights *weights,
    int64_t now_ns,
    const infersched_request_view *reqs,
    size_t n,
    uint32_t *out);

/* Return the default hybrid weights. */
infersched_hybrid_weights infersched_default_hybrid_weights(void);

/* Return the scheduler-core version as a static C string. */
const char *infersched_core_version(void);

#ifdef __cplusplus
}
#endif

#endif /* INFERSCHED_SCHEDULER_CORE_H */
