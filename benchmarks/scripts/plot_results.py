#!/usr/bin/env python3
"""Produce lightweight plots from InferSched benchmark results.

Reads summary.json produced by run_benchmarks.py and emits PNG plots under
results/plots/. Plots visualise trade-offs (throughput vs p99, deadline-miss
rate, rejection rate) that the Markdown tables state numerically.

All plots are SIMULATION output, not real-hardware measurements; titles say so.
"""
from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))


def main() -> None:
    out_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "benchmarks", "results")
    summary_path = os.path.join(out_dir, "summary.json")
    if not os.path.exists(summary_path):
        raise SystemExit(f"missing {summary_path}; run run_benchmarks.py first")

    with open(summary_path) as f:
        summary = json.load(f)

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:
        print(f"matplotlib unavailable ({e}); skipping plots", file=sys.stderr)
        return

    plots_dir = os.path.join(out_dir, "plots")
    os.makedirs(plots_dir, exist_ok=True)

    policies = summary["policies"]
    scenarios = list(summary["scenarios"].keys())

    # 1. Throughput vs p99 completion time, one point per (scenario, policy).
    plt.figure(figsize=(9, 6))
    markers = ["o", "s", "^", "D", "v"]
    for i, pol in enumerate(policies):
        xs, ys = [], []
        for scen in scenarios:
            s = summary["scenarios"][scen].get(pol)
            if not s:
                continue
            xs.append(s["throughput_rps_median"])
            ys.append(s["p99_completion_ms_median"])
        plt.scatter(xs, ys, label=pol, marker=markers[i % len(markers)], s=70)
    plt.xlabel("throughput (completed req/s, median)")
    plt.ylabel("p99 completion latency (ms, median)")
    plt.title("Throughput vs p99 latency (SIMULATION)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "throughput-vs-p99.png"), dpi=110)
    plt.close()

    # 2. Deadline-miss rate by policy, grouped by scenario.
    plt.figure(figsize=(10, 6))
    width = 0.15
    for i, pol in enumerate(policies):
        vals = []
        for scen in scenarios:
            s = summary["scenarios"][scen].get(pol)
            vals.append(100 * s["deadline_miss_rate_median"] if s else 0.0)
        x = [j + i * width for j in range(len(scenarios))]
        plt.bar(x, vals, width=width, label=pol)
    plt.xticks([j + width * (len(policies) - 1) / 2 for j in range(len(scenarios))],
               scenarios, rotation=20, ha="right")
    plt.ylabel("deadline-miss rate (%)")
    plt.title("Deadline-miss rate by policy (SIMULATION)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "deadline-miss-rate.png"), dpi=110)
    plt.close()

    # 3. Rejection rate by policy, grouped by scenario.
    plt.figure(figsize=(10, 6))
    for i, pol in enumerate(policies):
        vals = []
        for scen in scenarios:
            s = summary["scenarios"][scen].get(pol)
            vals.append(100 * s["reject_rate_median"] if s else 0.0)
        x = [j + i * width for j in range(len(scenarios))]
        plt.bar(x, vals, width=width, label=pol)
    plt.xticks([j + width * (len(policies) - 1) / 2 for j in range(len(scenarios))],
               scenarios, rotation=20, ha="right")
    plt.ylabel("rejection rate (%)")
    plt.title("Rejection rate by policy (SIMULATION)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "rejection-rate.png"), dpi=110)
    plt.close()

    print(f"wrote plots to {plots_dir}")


if __name__ == "__main__":
    main()
