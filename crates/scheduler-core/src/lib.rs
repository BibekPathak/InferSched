//! InferSched scheduling core.
//!
//! This crate implements the same scheduling policies as the Go
//! `internal/scheduler` package, exposed through a narrow C ABI so the Go
//! coordinator can use them behind the identical `scheduler.Policy` interface.
//!
//! The boundary is deliberately small: the caller passes a flat array of
//! request metadata plus a policy name and reference time, and receives a
//! permutation of the input indices (the execution order). No allocation
//! crosses the boundary; no Rust types leak.
//!
//! Ordering is fully deterministic and matches the Go implementation
//! field-for-field, so equivalence tests over random request sets pass.

use std::cmp::Ordering;

/// A request's scheduling metadata, laid out to match the Go adapter's
/// C-compatible struct.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct RequestView {
    /// Index of the request in the caller's slice (echoed back in the order).
    pub index: u32,
    /// Monotonic arrival time in nanoseconds.
    pub arrival_ns: i64,
    /// Monotonic deadline in nanoseconds (0 = none).
    pub deadline_ns: i64,
    /// Static priority (higher = more important).
    pub priority: i32,
    /// Estimated prefill cost in milliseconds.
    pub prefill_cost_ms: f64,
    /// Estimated decode cost per token in milliseconds.
    pub decode_cost_per_token_ms: f64,
    /// Expected output tokens (an estimate).
    pub expected_output_tokens: i64,
    /// Estimated KV token footprint.
    pub kv_tokens: i64,
}

/// Hybrid policy weights, matching Go's `HybridWeights`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct HybridWeightsFfi {
    pub deadline_urgency: f64,
    pub work: f64,
    pub queue_delay: f64,
    pub priority: f64,
    pub aging: f64,
    pub horizon_ms: f64,
    pub wait_norm_ms: f64,
    pub work_norm_ms: f64,
    pub max_priority: f64,
    pub aging_cap_ms: f64,
}

/// Policy identifiers. Kept in sync with the Go adapter.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PolicyKind {
    Fifo,
    Priority,
    Sjf,
    Edf,
    Hybrid,
}

impl PolicyKind {
    fn from_u32(v: u32) -> Option<PolicyKind> {
        match v {
            0 => Some(PolicyKind::Fifo),
            1 => Some(PolicyKind::Priority),
            2 => Some(PolicyKind::Sjf),
            3 => Some(PolicyKind::Edf),
            4 => Some(PolicyKind::Hybrid),
            _ => None,
        }
    }
}

fn clamp01(x: f64) -> f64 {
    x.clamp(0.0, 1.0)
}

/// Universal deterministic tie-break: earlier arrival, then smaller index.
fn less_arrival(a: &RequestView, b: &RequestView) -> Ordering {
    if a.arrival_ns != b.arrival_ns {
        return a.arrival_ns.cmp(&b.arrival_ns);
    }
    a.index.cmp(&b.index)
}

/// Total-cost estimate in milliseconds (the SJF work model, identity weights).
fn work(r: &RequestView) -> f64 {
    r.prefill_cost_ms + (r.expected_output_tokens as f64) * r.decode_cost_per_token_ms
}

/// Compare two requests under a policy. Returns Ordering::Less if `a` should be
/// scheduled before `b`. Ties always fall back to `less_arrival`.
fn compare(
    kind: PolicyKind,
    w: &HybridWeightsFfi,
    now_ns: i64,
    a: &RequestView,
    b: &RequestView,
) -> Ordering {
    let primary = match kind {
        PolicyKind::Fifo => less_arrival(a, b),
        PolicyKind::Priority => {
            if a.priority != b.priority {
                b.priority.cmp(&a.priority)
            } else {
                Ordering::Equal
            }
        }
        PolicyKind::Sjf => {
            let wa = work(a);
            let wb = work(b);
            if wa != wb {
                wa.partial_cmp(&wb).unwrap_or(Ordering::Equal)
            } else {
                Ordering::Equal
            }
        }
        PolicyKind::Edf => {
            let ad = a.deadline_ns != 0;
            let bd = b.deadline_ns != 0;
            match (ad, bd) {
                (true, true) => {
                    if a.deadline_ns != b.deadline_ns {
                        a.deadline_ns.cmp(&b.deadline_ns)
                    } else {
                        Ordering::Equal
                    }
                }
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => Ordering::Equal,
            }
        }
        PolicyKind::Hybrid => {
            let sa = hybrid_score(w, now_ns, a);
            let sb = hybrid_score(w, now_ns, b);
            if sa > sb {
                Ordering::Less
            } else if sa < sb {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
    };
    if primary == Ordering::Equal {
        less_arrival(a, b)
    } else {
        primary
    }
}

fn deadline_urgency(w: &HybridWeightsFfi, now_ns: i64, r: &RequestView) -> f64 {
    if r.deadline_ns == 0 || w.horizon_ms <= 0.0 {
        return 0.0;
    }
    let slack_ms = (r.deadline_ns - now_ns) as f64 / 1e6;
    clamp01(1.0 - slack_ms / w.horizon_ms)
}

fn work_factor(w: &HybridWeightsFfi, r: &RequestView) -> f64 {
    if w.work_norm_ms <= 0.0 {
        return 0.0;
    }
    clamp01(1.0 - work(r) / w.work_norm_ms)
}

fn queue_delay(w: &HybridWeightsFfi, now_ns: i64, r: &RequestView) -> f64 {
    if w.wait_norm_ms <= 0.0 {
        return 0.0;
    }
    let wait_ms = (now_ns - r.arrival_ns) as f64 / 1e6;
    clamp01(wait_ms / w.wait_norm_ms)
}

fn priority_term(w: &HybridWeightsFfi, r: &RequestView) -> f64 {
    if w.max_priority <= 0.0 {
        return 0.0;
    }
    clamp01((r.priority as f64) / w.max_priority)
}

fn aging_term(w: &HybridWeightsFfi, now_ns: i64, r: &RequestView) -> f64 {
    if w.aging_cap_ms <= 0.0 {
        return 0.0;
    }
    let wait_ms = (now_ns - r.arrival_ns) as f64 / 1e6;
    clamp01(wait_ms / w.aging_cap_ms)
}

fn hybrid_score(w: &HybridWeightsFfi, now_ns: i64, r: &RequestView) -> f64 {
    w.deadline_urgency * deadline_urgency(w, now_ns, r)
        + w.work * work_factor(w, r)
        + w.queue_delay * queue_delay(w, now_ns, r)
        + w.priority * priority_term(w, r)
        + w.aging * aging_term(w, now_ns, r)
}

/// Order a slice of request views under a policy, writing the resulting index
/// permutation into `out`. `out` must have length >= reqs.len().
pub fn order_into(
    kind: PolicyKind,
    weights: &HybridWeightsFfi,
    now_ns: i64,
    reqs: &[RequestView],
    out: &mut [u32],
) -> i32 {
    if out.len() < reqs.len() {
        return -3;
    }
    let mut idx: Vec<u32> = (0..reqs.len() as u32).collect();
    idx.sort_by(|&i, &j| {
        let a = &reqs[i as usize];
        let b = &reqs[j as usize];
        compare(kind, weights, now_ns, a, b)
    });
    for (k, &i) in idx.iter().enumerate() {
        out[k] = i;
    }
    reqs.len() as i32
}

/// Default hybrid weights, mirroring Go's `DefaultHybridWeights()`.
pub fn default_hybrid_weights() -> HybridWeightsFfi {
    HybridWeightsFfi {
        deadline_urgency: 1.0,
        work: 0.5,
        queue_delay: 0.5,
        priority: 0.25,
        aging: 0.25,
        horizon_ms: 2000.0,
        wait_norm_ms: 1000.0,
        work_norm_ms: 5000.0,
        max_priority: 200.0,
        aging_cap_ms: 5000.0,
    }
}

/// Map a policy name byte string to a PolicyKind.
pub fn policy_from_name(name: &[u8]) -> Option<PolicyKind> {
    match name {
        b"fifo" => Some(PolicyKind::Fifo),
        b"priority" => Some(PolicyKind::Priority),
        b"sjf" => Some(PolicyKind::Sjf),
        b"edf" => Some(PolicyKind::Edf),
        b"hybrid" => Some(PolicyKind::Hybrid),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// C ABI
// ---------------------------------------------------------------------------

/// Order requests under a policy identified by its numeric kind.
///
/// Returns the number of indices written, or a negative error code:
///   -1: null pointer argument
///   -2: unknown policy
///   -3: output buffer too small
///
/// # Safety
/// `reqs` must point to `n` valid `RequestView` values, and `out` must point to
/// at least `n` `u32` slots.
#[no_mangle]
pub unsafe extern "C" fn infersched_order(
    policy_kind: u32,
    weights: *const HybridWeightsFfi,
    now_ns: i64,
    reqs: *const RequestView,
    n: usize,
    out: *mut u32,
) -> i32 {
    if reqs.is_null() || out.is_null() {
        return -1;
    }
    let kind = match PolicyKind::from_u32(policy_kind) {
        Some(k) => k,
        None => return -2,
    };
    let w = if weights.is_null() {
        default_hybrid_weights()
    } else {
        *weights
    };
    let req_slice = unsafe { std::slice::from_raw_parts(reqs, n) };
    let out_slice = unsafe { std::slice::from_raw_parts_mut(out, n) };
    order_into(kind, &w, now_ns, req_slice, out_slice)
}

/// Expose the default hybrid weights to the caller.
#[no_mangle]
pub extern "C" fn infersched_default_hybrid_weights() -> HybridWeightsFfi {
    default_hybrid_weights()
}

/// Return the scheduler-core version as a static C string.
#[no_mangle]
pub extern "C" fn infersched_core_version() -> *const std::os::raw::c_char {
    c"0.1.0".as_ptr()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rv(
        index: u32,
        arrival: i64,
        deadline: i64,
        priority: i32,
        prefill: f64,
        out_tok: i64,
    ) -> RequestView {
        RequestView {
            index,
            arrival_ns: arrival,
            deadline_ns: deadline,
            priority,
            prefill_cost_ms: prefill,
            decode_cost_per_token_ms: 0.5,
            expected_output_tokens: out_tok,
            kv_tokens: 100,
        }
    }

    fn order(kind: PolicyKind, reqs: &[RequestView]) -> Vec<u32> {
        let mut out = vec![0u32; reqs.len()];
        let w = default_hybrid_weights();
        let rc = order_into(kind, &w, 0, reqs, &mut out);
        assert_eq!(rc, reqs.len() as i32);
        out
    }

    #[test]
    fn fifo_by_arrival() {
        // Positions into the input slice, sorted by arrival: pos1(10), pos2(20), pos0(30).
        let reqs = vec![
            rv(2, 30, 0, 0, 1.0, 10),
            rv(0, 10, 0, 0, 1.0, 10),
            rv(1, 20, 0, 0, 1.0, 10),
        ];
        assert_eq!(order(PolicyKind::Fifo, &reqs), vec![1, 2, 0]);
    }

    #[test]
    fn fifo_tie_break_by_index() {
        // Both arrive at 10; tie-break is by the `index` field: index 0 at pos1, index 1 at pos0.
        let reqs = vec![rv(1, 10, 0, 0, 1.0, 10), rv(0, 10, 0, 0, 1.0, 10)];
        assert_eq!(order(PolicyKind::Fifo, &reqs), vec![1, 0]);
    }

    #[test]
    fn priority_desc() {
        let reqs = vec![
            rv(0, 1, 0, 10, 1.0, 10),
            rv(1, 2, 0, 100, 1.0, 10),
            rv(2, 3, 0, 50, 1.0, 10),
        ];
        assert_eq!(order(PolicyKind::Priority, &reqs), vec![1, 2, 0]);
    }

    #[test]
    fn sjf_shortest_first() {
        let reqs = vec![rv(0, 1, 0, 0, 100.0, 1000), rv(1, 2, 0, 0, 1.0, 10)];
        assert_eq!(order(PolicyKind::Sjf, &reqs), vec![1, 0]);
    }

    #[test]
    fn edf_deadline_less_last() {
        let reqs = vec![
            rv(0, 1, 0, 0, 1.0, 10),
            rv(1, 1, 5000, 0, 1.0, 10),
            rv(2, 1, 1000, 0, 1.0, 10),
        ];
        assert_eq!(order(PolicyKind::Edf, &reqs), vec![2, 1, 0]);
    }

    #[test]
    fn hybrid_prefers_urgent() {
        let w = default_hybrid_weights();
        let reqs = vec![
            rv(0, 0, 900_000 * 1_000_000, 50, 1.0, 10),
            rv(1, 0, 500 * 1_000_000, 50, 1.0, 10),
        ];
        let mut out = vec![0u32; reqs.len()];
        order_into(PolicyKind::Hybrid, &w, 0, &reqs, &mut out);
        assert_eq!(out, vec![1, 0]);
    }

    fn order_via_c_abi(
        kind: u32,
        w: &HybridWeightsFfi,
        now: i64,
        reqs: &[RequestView],
        out: &mut [u32],
    ) -> i32 {
        unsafe {
            infersched_order(
                kind,
                w as *const HybridWeightsFfi,
                now,
                reqs.as_ptr(),
                reqs.len(),
                out.as_mut_ptr(),
            )
        }
    }

    #[test]
    fn unknown_policy_is_error() {
        let reqs = vec![rv(0, 1, 0, 0, 1.0, 10)];
        let mut out = vec![0u32; 1];
        let w = default_hybrid_weights();
        assert_eq!(order_via_c_abi(99, &w, 0, &reqs, &mut out), -2);
    }

    #[test]
    fn c_abi_null_returns_error() {
        let w = default_hybrid_weights();
        let mut out = vec![0u32; 1];
        let rc = unsafe { infersched_order(0, &w, 0, std::ptr::null(), 0, out.as_mut_ptr()) };
        assert_eq!(rc, -1);
    }

    #[test]
    fn c_abi_orders() {
        let reqs = vec![rv(0, 30, 0, 0, 1.0, 10), rv(1, 10, 0, 0, 1.0, 10)];
        let w = default_hybrid_weights();
        let mut out = vec![0u32; 2];
        let rc = order_via_c_abi(0, &w, 0, &reqs, &mut out);
        assert_eq!(rc, 2);
        assert_eq!(out, vec![1, 0]);
    }

    #[test]
    fn policy_name_mapping() {
        assert_eq!(policy_from_name(b"fifo"), Some(PolicyKind::Fifo));
        assert_eq!(policy_from_name(b"hybrid"), Some(PolicyKind::Hybrid));
        assert_eq!(policy_from_name(b"nope"), None);
    }
}
