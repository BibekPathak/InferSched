#!/usr/bin/env python3
"""InferSched reproducible workload generator.

Generates versioned JSONL workload traces (format v1) for the simulator and
benchmark harness. Every trace is fully determined by a seed and a profile, so
regenerating with the same inputs yields byte-identical output.

Profiles:
    A  short interactive requests
    B  long-context requests
    C  mixed interactive + batch workloads
    D  bursty traffic
    E  deadline-constrained requests
    F  overload and recovery
    G  multi-tenant fairness

The cost fields written into each record match the simulator's documented
prefill/decode model shape: prefill_cost_ms = prompt_tokens * PREFILL_PER_TOKEN_MS,
decode_cost_per_token_ms = DECODE_PER_TOKEN_MS. They are estimates, not
measurements.

Usage:
    python3 workload_gen.py --profile A --seed 1 --out workloads/A-seed1.jsonl
    python3 workload_gen.py --list-profiles
"""
from __future__ import annotations

import argparse
import json
import math
import random
from dataclasses import dataclass, field
from typing import Callable

FORMAT_VERSION = "v1"
TIME_UNIT = "ns"
MS = 1_000_000  # nanoseconds per millisecond

# Cost-model constants, matching simulator DefaultPerfModel shape.
PREFILL_PER_TOKEN_MS = 0.02
DECODE_PER_TOKEN_MS = 0.5


@dataclass
class RequestSpec:
    """A single generated request, before serialization."""

    id: str
    tenant: str
    cls: str
    priority: int
    arrival_ns: int
    deadline_ns: int  # 0 = none
    prompt_tokens: int
    expected_output_tokens: int
    kv_tokens: int = 0
    prefill_cost_ms: float = 0.0
    decode_cost_per_token_ms: float = DECODE_PER_TOKEN_MS
    cancel_at_ns: int = 0  # 0 = no scheduled cancellation

    def finalize(self) -> "RequestSpec":
        self.kv_tokens = self.prompt_tokens + self.expected_output_tokens
        self.prefill_cost_ms = self.prompt_tokens * PREFILL_PER_TOKEN_MS
        return self

    def to_record(self) -> dict:
        return {
            "kind": "request",
            "id": self.id,
            "tenant_id": self.tenant,
            "class": self.cls,
            "priority": self.priority,
            "arrival_ns": int(self.arrival_ns),
            "deadline_ns": int(self.deadline_ns),
            "prompt_tokens": int(self.prompt_tokens),
            "expected_output_tokens": int(self.expected_output_tokens),
            "prefill_cost_ms": round(self.prefill_cost_ms, 6),
            "decode_cost_per_token_ms": self.decode_cost_per_token_ms,
            "kv_tokens": int(self.kv_tokens),
            "cancel_at_ns": int(self.cancel_at_ns),
        }


@dataclass
class Profile:
    name: str
    description: str
    generate: Callable[[random.Random, dict], tuple[list[RequestSpec], dict]] = field(repr=False)


def _poisson_arrivals(rng: random.Random, n: int, rate_rps: float, start_ns: int = 0) -> list[int]:
    """Exponential inter-arrival gaps for a Poisson process at rate_rps."""
    mean_gap_ns = int(1e9 / rate_rps)
    t = start_ns
    out = []
    for _ in range(n):
        t += max(1, int(rng.expovariate(1.0 / mean_gap_ns)))
        out.append(t)
    return out


def _uniform(rng: random.Random, lo: int, hi: int) -> int:
    return rng.randint(lo, hi)


def _lognormal_int(rng: random.Random, median: float, sigma: float, lo: int, hi: int) -> int:
    """Heavy-tailed integer: lognormal with median, clamped to [lo, hi]."""
    v = median * math.exp(rng.gauss(0, sigma))
    return max(lo, min(hi, int(v)))


# ---------------------------------------------------------------------------
# Profiles
# ---------------------------------------------------------------------------


def profile_A(rng: random.Random, opts: dict) -> tuple[list[RequestSpec], dict]:
    n = opts.get("count", 200)
    rate = opts.get("rate_rps", 40.0)
    arrivals = _poisson_arrivals(rng, n, rate)
    reqs = []
    for i, a in enumerate(arrivals):
        prompt = _uniform(rng, 32, 512)
        out = _uniform(rng, 8, 128)
        reqs.append(
            RequestSpec(
                id=f"A-{i:05d}",
                tenant=f"t{i % 2}",
                cls="interactive",
                priority=100 if rng.random() < 0.3 else 50,
                arrival_ns=a,
                deadline_ns=0,
                prompt_tokens=prompt,
                expected_output_tokens=out,
            ).finalize()
        )
    return reqs, {"arrival": "poisson", "rate_rps": rate, "count": n}


def profile_B(rng: random.Random, opts: dict) -> tuple[list[RequestSpec], dict]:
    n = opts.get("count", 80)
    rate = opts.get("rate_rps", 6.0)
    arrivals = _poisson_arrivals(rng, n, rate)
    reqs = []
    for i, a in enumerate(arrivals):
        prompt = _uniform(rng, 4096, 16384)
        out = _uniform(rng, 64, 512)
        reqs.append(
            RequestSpec(
                id=f"B-{i:05d}",
                tenant=f"t{i % 2}",
                cls="long_context",
                priority=50,
                arrival_ns=a,
                deadline_ns=0,
                prompt_tokens=prompt,
                expected_output_tokens=out,
            ).finalize()
        )
    return reqs, {"arrival": "poisson", "rate_rps": rate, "prompt": "uniform(4096,16384)"}


def profile_C(rng: random.Random, opts: dict) -> tuple[list[RequestSpec], dict]:
    """Mixed interactive (low latency) and batch (throughput) traffic."""
    n_int = opts.get("interactive", 150)
    n_batch = opts.get("batch", 50)
    int_arr = _poisson_arrivals(rng, n_int, opts.get("interactive_rate", 30.0))
    batch_arr = _poisson_arrivals(rng, n_batch, opts.get("batch_rate", 8.0))
    reqs = []
    for i, a in enumerate(int_arr):
        reqs.append(
            RequestSpec(
                id=f"C-i{i:05d}", tenant="interactive", cls="interactive", priority=100,
                arrival_ns=a, deadline_ns=0,
                prompt_tokens=_uniform(rng, 64, 512),
                expected_output_tokens=_uniform(rng, 16, 128),
            ).finalize()
        )
    for i, a in enumerate(batch_arr):
        reqs.append(
            RequestSpec(
                id=f"C-b{i:05d}", tenant="batch", cls="batch", priority=10,
                arrival_ns=a, deadline_ns=0,
                prompt_tokens=_lognormal_int(rng, 4096, 0.8, 1024, 16384),
                expected_output_tokens=_lognormal_int(rng, 512, 0.8, 64, 2048),
            ).finalize()
        )
    reqs.sort(key=lambda r: (r.arrival_ns, r.id))
    return reqs, {
        "arrival": "mixed_poisson",
        "interactive": n_int, "batch": n_batch,
        "interactive_rate": opts.get("interactive_rate", 30.0),
        "batch_rate": opts.get("batch_rate", 8.0),
    }


def profile_D(rng: random.Random, opts: dict) -> tuple[list[RequestSpec], dict]:
    """Bursty: periods of quiescence punctuated by spikes."""
    bursts = opts.get("bursts", 5)
    per_burst = opts.get("per_burst", 40)
    gap_ns = opts.get("burst_gap_ms", 500) * MS
    burst_len_ns = opts.get("burst_len_ms", 50) * MS
    reqs = []
    idx = 0
    t0 = 0
    for b in range(bursts):
        for _ in range(per_burst):
            a = t0 + rng.randint(0, burst_len_ns)
            prompt = _lognormal_int(rng, 512, 1.0, 64, 8192)
            out = _lognormal_int(rng, 64, 1.0, 8, 1024)
            reqs.append(
                RequestSpec(
                    id=f"D-{idx:05d}", tenant=f"t{idx % 3}", cls="interactive",
                    priority=100,
                    arrival_ns=a, deadline_ns=0,
                    prompt_tokens=prompt, expected_output_tokens=out,
                ).finalize()
            )
            idx += 1
        t0 += gap_ns
    reqs.sort(key=lambda r: (r.arrival_ns, r.id))
    return reqs, {
        "arrival": "bursty",
        "bursts": bursts, "per_burst": per_burst,
        "burst_gap_ms": opts.get("burst_gap_ms", 500),
        "burst_len_ms": opts.get("burst_len_ms", 50),
    }


def profile_E(rng: random.Random, opts: dict) -> tuple[list[RequestSpec], dict]:
    """Deadline-constrained requests with a mix of tight and loose SLAs."""
    n = opts.get("count", 200)
    rate = opts.get("rate_rps", 30.0)
    arrivals = _poisson_arrivals(rng, n, rate)
    reqs = []
    for i, a in enumerate(arrivals):
        prompt = _uniform(rng, 128, 2048)
        out = _uniform(rng, 16, 256)
        # Estimated service time + slack; some tight, some loose.
        service_ms = prompt * PREFILL_PER_TOKEN_MS + out * DECODE_PER_TOKEN_MS
        slack_ms = opts.get("tight_ms", 100) if i % 2 == 0 else opts.get("loose_ms", 2000)
        deadline = a + int((service_ms + slack_ms) * MS)
        reqs.append(
            RequestSpec(
                id=f"E-{i:05d}", tenant=f"t{i % 2}", cls="interactive",
                priority=100 if i % 2 == 0 else 50,
                arrival_ns=a, deadline_ns=deadline,
                prompt_tokens=prompt, expected_output_tokens=out,
            ).finalize()
        )
    return reqs, {
        "arrival": "poisson", "rate_rps": rate,
        "tight_ms": opts.get("tight_ms", 100), "loose_ms": opts.get("loose_ms", 2000),
    }


def profile_F(rng: random.Random, opts: dict) -> tuple[list[RequestSpec], dict]:
    """Overload then recovery: a high-rate window followed by a calm window."""
    overload_n = opts.get("overload_count", 300)
    recover_n = opts.get("recover_count", 60)
    overload_rate = opts.get("overload_rate", 120.0)
    recover_rate = opts.get("recover_rate", 10.0)
    overload_arr = _poisson_arrivals(rng, overload_n, overload_rate)
    end_overload = overload_arr[-1] + 100 * MS if overload_arr else 0
    recover_arr = _poisson_arrivals(rng, recover_n, recover_rate, start_ns=end_overload)
    reqs = []
    for i, a in enumerate(overload_arr):
        reqs.append(
            RequestSpec(
                id=f"F-o{i:05d}", tenant=f"t{i % 3}", cls="interactive", priority=100,
                arrival_ns=a, deadline_ns=0,
                prompt_tokens=_uniform(rng, 128, 1024),
                expected_output_tokens=_uniform(rng, 16, 128),
            ).finalize()
        )
    for i, a in enumerate(recover_arr):
        reqs.append(
            RequestSpec(
                id=f"F-r{i:05d}", tenant=f"t{i % 3}", cls="interactive", priority=100,
                arrival_ns=a, deadline_ns=0,
                prompt_tokens=_uniform(rng, 128, 1024),
                expected_output_tokens=_uniform(rng, 16, 128),
            ).finalize()
        )
    reqs.sort(key=lambda r: (r.arrival_ns, r.id))
    return reqs, {
        "arrival": "overload_then_recovery",
        "overload_count": overload_n, "recover_count": recover_n,
        "overload_rate": overload_rate, "recover_rate": recover_rate,
    }


def profile_G(rng: random.Random, opts: dict) -> tuple[list[RequestSpec], dict]:
    """Multi-tenant fairness: tenants with different rates and sizes."""
    tenants = opts.get("tenants", ["alpha", "beta", "gamma", "delta"])
    per_tenant = opts.get("per_tenant", 60)
    reqs = []
    for ti, tenant in enumerate(tenants):
        # Tenants alternate between light-short and heavy-long offerings.
        heavy = ti % 2 == 1
        rate = opts.get("heavy_rate", 10.0) if heavy else opts.get("light_rate", 30.0)
        arrivals = _poisson_arrivals(rng, per_tenant, rate)
        for i, a in enumerate(arrivals):
            if heavy:
                prompt = _lognormal_int(rng, 4096, 0.7, 1024, 16384)
                out = _lognormal_int(rng, 256, 0.7, 32, 1024)
                cls = "batch"
            else:
                prompt = _uniform(rng, 64, 512)
                out = _uniform(rng, 16, 128)
                cls = "interactive"
            reqs.append(
                RequestSpec(
                    id=f"G-{tenant}-{i:05d}", tenant=tenant, cls=cls, priority=50,
                    arrival_ns=a, deadline_ns=0,
                    prompt_tokens=prompt, expected_output_tokens=out,
                ).finalize()
            )
    reqs.sort(key=lambda r: (r.arrival_ns, r.id))
    return reqs, {"arrival": "multi_tenant", "tenants": tenants, "per_tenant": per_tenant}


PROFILES: dict[str, Profile] = {
    "A": Profile("A", "short interactive requests", profile_A),
    "B": Profile("B", "long-context requests", profile_B),
    "C": Profile("C", "mixed interactive + batch", profile_C),
    "D": Profile("D", "bursty traffic", profile_D),
    "E": Profile("E", "deadline-constrained requests", profile_E),
    "F": Profile("F", "overload and recovery", profile_F),
    "G": Profile("G", "multi-tenant fairness", profile_G),
}


def generate(profile: str, seed: int, opts: dict) -> tuple[dict, list[dict]]:
    if profile not in PROFILES:
        raise SystemExit(f"unknown profile {profile!r}; choose from {sorted(PROFILES)}")
    rng = random.Random(seed)
    reqs, gen_cfg = PROFILES[profile].generate(rng, opts)
    header = {
        "kind": "header",
        "format_version": FORMAT_VERSION,
        "profile": profile,
        "seed": seed,
        "request_count": len(reqs),
        "time_unit": TIME_UNIT,
        "generator_config": gen_cfg,
    }
    return header, [r.to_record() for r in reqs]


def write_trace(path: str, header: dict, records: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(header, sort_keys=True) + "\n")
        for rec in records:
            f.write(json.dumps(rec, sort_keys=True) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="InferSched workload generator")
    ap.add_argument("--profile", required=False, help="profile letter A-G")
    ap.add_argument("--seed", type=int, default=1, help="RNG seed")
    ap.add_argument("--out", required=False, help="output JSONL path")
    ap.add_argument("--set", action="append", default=[], metavar="KEY=VAL",
                    help="profile option override, e.g. count=500")
    ap.add_argument("--list-profiles", action="store_true", help="list profiles and exit")
    args = ap.parse_args()

    if args.list_profiles:
        for letter, prof in sorted(PROFILES.items()):
            print(f"{letter}: {prof.description}")
        return

    if not args.profile or not args.out:
        ap.error("--profile and --out are required unless --list-profiles")

    opts: dict = {}
    for kv in args.set:
        k, _, v = kv.partition("=")
        try:
            opts[k] = int(v)
        except ValueError:
            try:
                opts[k] = float(v)
            except ValueError:
                opts[k] = v

    header, records = generate(args.profile, args.seed, opts)
    write_trace(args.out, header, records)
    print(f"wrote {len(records)} requests to {args.out} (profile {args.profile}, seed {args.seed})")


if __name__ == "__main__":
    main()
