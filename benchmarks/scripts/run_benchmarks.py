#!/usr/bin/env python3
"""InferSched benchmark harness.

Orchestrates reproducible comparisons of scheduling policies:

1. Generate each workload trace once (delegated to workload_gen) and record its
   seed and configuration.
2. Replay the identical trace against every policy under identical worker
   capacity and resource limits.
3. Repeat across multiple seeds.
4. Save raw per-run JSON, an aggregated summary, environment metadata, and
   Markdown tables.

All results are SIMULATION output, never real-hardware measurements. The
harness labels them as such and the environment metadata records the machine so
runs remain attributable.

Usage:
    python3 run_benchmarks.py --out ../results --policies fifo,priority,sjf,edf,hybrid
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import shutil
import sys
import time
from dataclasses import dataclass, field

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
WORKLOADS = os.path.join(ROOT, "benchmarks", "workloads")
GEN = os.path.join(HERE, "workload_gen.py")

MS = 1_000_000


@dataclass
class Capacity:
    """Worker capacity and resource limits, held identical across policies."""

    workers: int = 2
    batch_size: int = 8
    kv_blocks_per_worker: int = 512
    kv_block_size: int = 16
    max_queue: int = 0
    max_outstanding: int = 0
    strategy: str = "reject"
    failure_rate: int = 0

    def to_cli(self) -> list[str]:
        return [
            "-workers", str(self.workers),
            "-batch-size", str(self.batch_size),
            "-kv-blocks-per-worker", str(self.kv_blocks_per_worker),
            "-kv-block-size", str(self.kv_block_size),
            "-max-queue", str(self.max_queue),
            "-max-outstanding", str(self.max_outstanding),
            "-strategy", self.strategy,
            "-failure-rate", str(self.failure_rate),
        ]


@dataclass
class Scenario:
    """A named workload profile with generation options."""

    name: str
    profile: str
    opts: dict = field(default_factory=dict)
    capacity: Capacity = field(default_factory=Capacity)


def default_scenarios(cap: Capacity) -> list[Scenario]:
    """Traffic profiles at a single, moderate capacity.

    For a utilization sweep across low/moderate/saturation/overload see
    `sweep_scenarios`.
    """
    return [
        Scenario("A-short-interactive", "A", {"count": 300, "rate_rps": 40.0}, cap),
        Scenario("B-long-context", "B", {"count": 80, "rate_rps": 6.0}, cap),
        Scenario("C-mixed", "C", {"interactive": 250, "batch": 80}, cap),
        Scenario("D-bursty", "D", {"bursts": 6, "per_burst": 50}, cap),
        Scenario("E-deadline", "E", {"count": 300, "rate_rps": 30.0}, cap),
        Scenario("F-overload-recovery", "F", {"overload_count": 400, "recover_count": 80}, cap),
        Scenario("G-multitenant", "G", {"per_tenant": 80}, cap),
    ]


def sweep_scenarios(base: Capacity) -> list[Scenario]:
    """Utilization sweep per spec section 11.

    Uses the mixed interactive+batch profile (C) and the deadline profile (E)
    because they best differentiate policies, and sweeps capacity/arrival
    intensity across four levels: low, moderate, saturation, overload.
    Arrival rate is scaled; capacity is held at the base to isolate load.
    """
    scen_list = []
    # (label, interactive_count, interactive_rate, batch_count, batch_rate)
    loads = [
        ("low", 120, 15.0, 40, 4.0),
        ("moderate", 250, 35.0, 80, 8.0),
        ("saturation", 400, 70.0, 120, 16.0),
        ("overload", 600, 140.0, 200, 32.0),
    ]
    for label, ic, ir, bc, br in loads:
        scen_list.append(Scenario(
            f"sweep-C-{label}", "C",
            {"interactive": ic, "interactive_rate": ir, "batch": bc, "batch_rate": br},
            base,
        ))
    for label, count, rate in [("low", 120, 12.0), ("moderate", 250, 30.0),
                               ("saturation", 400, 60.0), ("overload", 600, 120.0)]:
        scen_list.append(Scenario(
            f"sweep-E-{label}", "E", {"count": count, "rate_rps": rate}, base,
        ))
    return scen_list


def capacity_scenarios(base: Capacity) -> list[Scenario]:
    """KV-cache and batch-size sensitivity, holding traffic constant.

    Same trace parameters across capacities so the effect of memory/batch limits
    is isolated.
    """
    out = []
    variants = [
        ("tiny-kv", Capacity(**{**vars(base), "kv_blocks_per_worker": 64, "batch_size": 4})),
        ("small-kv", Capacity(**{**vars(base), "kv_blocks_per_worker": 128, "batch_size": 8})),
        ("large-kv", Capacity(**{**vars(base), "kv_blocks_per_worker": 2048, "batch_size": 16})),
    ]
    for label, c in variants:
        out.append(Scenario(f"cap-C-{label}", "C", {"interactive": 300, "batch": 100}, c))
    return out


def run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, **kw)


def go_binary() -> str:
    """Return the simulator CLI path.

    Prefers a prebuilt binary (e.g. bin/sim baked into the Docker image). If
    none exists and a Go toolchain is available, builds one. This keeps the
    harness usable both in a dev checkout and in a minimal runtime container.
    """
    candidates = [
        os.path.join(ROOT, "bin", "sim"),
        shutil.which("sim"),
    ]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    # Fall back to building with the Go toolchain.
    go = shutil.which("go")
    if not go:
        raise SystemExit("no prebuilt 'sim' binary and no 'go' toolchain found")
    bin_path = os.path.join(ROOT, "bin", "sim")
    os.makedirs(os.path.dirname(bin_path), exist_ok=True)
    res = run([go, "build", "-o", bin_path, "./simulator/cmd/sim"])
    if res.returncode != 0:
        sys.stderr.write(res.stderr)
        raise SystemExit("failed to build simulator CLI")
    return bin_path


def gen_trace(bin_path: str, scen: Scenario, seed: int, workdir: str) -> str:
    path = os.path.join(workdir, f"{scen.name}-seed{seed}.jsonl")
    cmd = [sys.executable, GEN, "--profile", scen.profile, "--seed", str(seed), "--out", path]
    for k, v in scen.opts.items():
        cmd += ["--set", f"{k}={v}"]
    res = run(cmd)
    if res.returncode != 0:
        sys.stderr.write(res.stderr)
        raise SystemExit(f"trace generation failed for {scen.name}")
    return path


def run_sim(bin_path: str, trace: str, scen: Scenario, seed: int, policies: list[str]) -> dict:
    out_path = trace + f".policy.seed{seed}.json"
    cmd = [bin_path, "-trace", trace, "-seed", str(seed),
           "-policies", ",".join(policies), "-json-out", out_path]
    cmd += scen.capacity.to_cli()
    res = run(cmd)
    if res.returncode != 0:
        sys.stderr.write(res.stderr)
        raise SystemExit(f"simulation failed for {scen.name}")
    with open(out_path) as f:
        return json.load(f)


def env_metadata() -> dict:

    meta = {
        "os": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "go_version": None,
        "note": "All latency/throughput numbers are SIMULATION output.",
    }
    try:
        r = run(["go", "version"])
        if r.returncode == 0:
            meta["go_version"] = r.stdout.strip()
    except Exception:
        pass
    try:
        import numpy
        meta["numpy"] = numpy.__version__
    except Exception:
        ns = shutil.which("numpy")  # not important
    return meta


def percentile(values: list[float], p: int) -> float:
    """Nearest-rank percentile (ceil(p/100*N), 1-based), matching the Go CLI."""
    if not values:
        return 0.0
    s = sorted(values)
    rank = (p * len(s) + 99) // 100
    rank = max(1, min(len(s), rank))
    return s[rank - 1]


def median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    if n % 2 == 1:
        return s[n // 2]
    return 0.5 * (s[n // 2 - 1] + s[n // 2])


def main() -> None:
    ap = argparse.ArgumentParser(description="InferSched benchmark harness")
    ap.add_argument("--out", default=os.path.join(ROOT, "benchmarks", "results"),
                    help="output directory for raw + summary results")
    ap.add_argument("--policies", default="fifo,priority,sjf,edf,hybrid")
    ap.add_argument("--seeds", default="1,2,3", help="comma-separated simulation seeds")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--batch-size", type=int, default=8)
    ap.add_argument("--kv-blocks", type=int, default=512)
    ap.add_argument("--quick", action="store_true", help="single seed, fewer requests")
    ap.add_argument("--suite", default="profiles,sweep,capacity",
                    help="comma-separated: profiles,sweep,capacity")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    traces_dir = os.path.join(args.out, "traces")
    raw_dir = os.path.join(args.out, "raw")
    os.makedirs(traces_dir, exist_ok=True)
    os.makedirs(raw_dir, exist_ok=True)

    policies = [p.strip() for p in args.policies.split(",") if p.strip()]
    seeds = [int(s) for s in args.seeds.split(",")]
    if args.quick:
        seeds = seeds[:1]

    cap = Capacity(workers=args.workers, batch_size=args.batch_size,
                   kv_blocks_per_worker=args.kv_blocks)
    suites = [s.strip() for s in args.suite.split(",") if s.strip()]
    scenarios: list[Scenario] = []
    if "profiles" in suites:
        scenarios += default_scenarios(cap)
    if "sweep" in suites:
        scenarios += sweep_scenarios(cap)
    if "capacity" in suites:
        scenarios += capacity_scenarios(cap)
    if args.quick:
        for s in scenarios:
            for k in list(s.opts):
                if k in ("count", "interactive", "batch", "per_burst", "per_tenant", "overload_count", "recover_count"):
                    s.opts[k] = max(20, s.opts[k] // 4)

    bin_path = go_binary()
    meta = env_metadata()
    meta["capacity"] = vars(cap)
    meta["policies"] = policies
    meta["seeds"] = seeds
    meta["generated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(os.path.join(args.out, "environment.json"), "w") as f:
        json.dump(meta, f, indent=2)

    # raw[scenario][policy] = list of per-seed runs
    raw: dict = {}
    print(f"running {len(scenarios)} scenarios x {len(policies)} policies x {len(seeds)} seeds")

    for scen in scenarios:
        raw[scen.name] = {p: [] for p in policies}
        for seed in seeds:
            trace = gen_trace(bin_path, scen, seed, traces_dir)
            report = run_sim(bin_path, trace, scen, seed, policies)
            run_path = os.path.join(raw_dir, f"{scen.name}-seed{seed}.json")
            with open(run_path, "w") as f:
                json.dump(report, f, indent=2)
            for pr in report["policy_runs"]:
                raw[scen.name][pr["policy"]].append(pr)
            print(f"  {scen.name} seed={seed} done")

    summarize(raw, policies, seeds, args.out)

    # Write a run manifest for provenance.
    with open(os.path.join(args.out, "manifest.json"), "w") as f:
        json.dump({
            "scenarios": [s.name for s in scenarios],
            "policies": policies,
            "seeds": seeds,
            "note": "SIMULATION output; see environment.json",
        }, f, indent=2)

    print(f"results written under {args.out}")


def summarize(raw: dict, policies: list[str], seeds: list[int], out_dir: str) -> None:
    """Aggregate per-scenario stats across seeds into a summary JSON + table."""
    summary = {"scenarios": {}, "policies": policies, "seeds": seeds,
               "note": "SIMULATION output; nearest-rank percentiles; medians across seeds"}
    for scen, by_policy in raw.items():
        scen_sum = {}
        for pol, runs in by_policy.items():
            completed = [r["stats"]["completed"] for r in runs]
            rejected = [r["stats"]["rejected"] for r in runs]
            expired = [r["stats"]["expired"] for r in runs]
            failed = [r["stats"]["failed"] for r in runs]
            dlmiss = [r["stats"]["deadline_miss"] for r in runs]
            total = runs[0]["stats"]["total"] if runs else 0
            # Percentiles: gather per-seed values and report the median of seeds.
            ttfts = [r["latency_ms"]["ttft"]["p95"] for r in runs]
            comps = [r["latency_ms"]["completion"]["p99"] for r in runs]
            qds = [r["latency_ms"]["queue_delay"]["p90"] for r in runs]
            # Throughput from max completion mark.
            thr = []
            for r in runs:
                max_end = max((req["end_ns"] for req in r["requests"]), default=0) / 1e9
                done = r["stats"]["completed"]
                thr.append(done / max_end if max_end > 0 else 0.0)
            scen_sum[pol] = {
                "completed_median": median(completed),
                "rejected_median": median(rejected),
                "expired_median": median(expired),
                "failed_median": median(failed),
                "deadline_miss_median": median(dlmiss),
                "deadline_miss_rate_median": (median(dlmiss) / total) if total else 0.0,
                "reject_rate_median": (median(rejected) / total) if total else 0.0,
                "p95_ttft_ms_median": median(ttfts),
                "p99_completion_ms_median": median(comps),
                "p90_queue_delay_ms_median": median(qds),
                "throughput_rps_median": median(thr),
            }
        summary["scenarios"][scen] = scen_sum
    with open(os.path.join(out_dir, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    write_markdown(summary, out_dir)


def write_markdown(summary: dict, out_dir: str) -> None:
    lines = ["# InferSched benchmark summary", "",
             "SIMULATION output — not real-hardware measurements.",
             "Percentiles use the nearest-rank method; cells are medians across seeds.",
             ""]
    for scen, by_policy in summary["scenarios"].items():
        lines.append(f"## {scen}")
        lines.append("")
        lines.append("| policy | done | rej | exp | fail | dlmiss% | p95 TTFT ms | p99 comp ms | p90 qdelay ms | rps |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|")
        for pol in summary["policies"]:
            s = by_policy.get(pol)
            if not s:
                continue
            lines.append(
                f"| {pol} | {s['completed_median']:.0f} | {s['rejected_median']:.0f} | "
                f"{s['expired_median']:.0f} | {s['failed_median']:.0f} | "
                f"{100*s['deadline_miss_rate_median']:.1f} | {s['p95_ttft_ms_median']:.1f} | "
                f"{s['p99_completion_ms_median']:.1f} | {s['p90_queue_delay_ms_median']:.1f} | "
                f"{s['throughput_rps_median']:.1f} |"
            )
        lines.append("")
    with open(os.path.join(out_dir, "summary.md"), "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
