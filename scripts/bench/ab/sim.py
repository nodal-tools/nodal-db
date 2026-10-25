"""Error rates of the perf gate's pre-fixed sequential rules, by
simulation through the gate's own decision code (`gatelib.gate_metric`:
looks, first-look rule, extension, confirm-on-FAIL).

Model: per-pair ratio = true ratio x exp(sigma Z), Z standard normal
(log-normal pair noise; sigma = the per-pair ratio spread, ~1.5-3 % on a
quiet host, 8-15 % at loads 10-20 per the bench records). The warm-up
samples are simulated too.

usage: python3 scripts/bench/ab/sim.py [--runs 4000] [--looks 5,11,21]
       [--margin 0.0] [--seed 1]

Prints, per threshold T, sigma and true ratio: P(PASS), P(FAIL) (after
confirmation), P(FLAKY), P(INCONCLUSIVE) and the mean number of pairs a
metric costs (incl. warm-ups and confirmation runs).
"""

import argparse
import math
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gatelib as g  # noqa: E402

CLASSES = {"hot": 1.05, "io": 1.10}


def simulate(threshold, sigma, true, plan, runs, seed):
    rng = random.Random(seed)
    m = g.Metric("sim", {"class": "x", "threshold": threshold, "check": "sim"},
                 {"x": threshold})
    counts = {}
    pairs = 0
    for _ in range(runs):
        calls = [0]

        def sampler(arm):
            calls[0] += 1
            if arm == "base":
                return 100.0, {}
            return 100.0 * true * math.exp(sigma * rng.gauss(0.0, 1.0)), {}
        r = g.gate_metric(m, sampler, plan, rng, lambda: 1.0)
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
        pairs += calls[0] / 2
    return {v: c / runs for v, c in counts.items()}, pairs / runs


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--runs", type=int, default=4000)
    ap.add_argument("--looks", default=",".join(map(str, g.DEFAULT_LOOKS)))
    ap.add_argument("--margin", type=float, default=0.0)
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    plan = g.Plan([int(x) for x in a.looks.split(",")], a.margin)
    print(f"looks {plan.looks}, first-look margin {plan.first_margin}, {a.runs} runs per cell")
    print(f"{'T':>5} {'sigma':>6} {'true':>5} {'PASS':>7} {'FAIL':>7} {'FLAKY':>7} {'INCONC':>7} {'pairs':>6}")
    for threshold in sorted(set(CLASSES.values())):
        for sigma in (0.015, 0.03, 0.08, 0.15):
            for true in (1.00, 1.02, threshold, 1.10, 1.15):
                if true == 1.10 and threshold == 1.10:
                    pass
                p, pairs = simulate(threshold, sigma, true, plan, a.runs, a.seed)
                print(f"{threshold:5.2f} {sigma:6.3f} {true:5.2f} "
                      f"{p.get(g.PASS, 0):7.3f} {p.get(g.FAIL, 0):7.4f} {p.get(g.FLAKY, 0):7.3f} "
                      f"{p.get(g.INCONCLUSIVE, 0):7.3f} {pairs:6.1f}", flush=True)


if __name__ == "__main__":
    main()
