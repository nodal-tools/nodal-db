"""Paired, interleaved A/B of two ab_write_path builds (appender_write_path_PLAN §8).

Per mode: N pairs; each pair runs old and new once, in random order; the
statistic is the median per-pair ratio new/old with a bootstrap 95 % CI.
Load average is recorded at the start and end of every mode.

usage: ab.py OLD_BIN NEW_BIN N mode [mode ...]

Building the two arms (each in its own target directory, release):

    git archive <old-rev> | tar -x -C /tmp/old
    cp -r scripts/bench/ab_write_path /tmp/old/scripts/bench/
    (cd /tmp/old/scripts/bench/ab_write_path &&
     CARGO_TARGET_DIR=/tmp/ab-old cargo build --release)
    (cd scripts/bench/ab_write_path &&
     CARGO_TARGET_DIR=/tmp/ab-new cargo build --release)
    python3 scripts/bench/ab_write_path/ab.py /tmp/ab-old/release/ab_write_path \
        /tmp/ab-new/release/ab_write_path 21 long_insert array_insert ...

The harness's default feature `lookup_const` needs a core that exports
`api::ENTITY_NAME_LOOKUP_SQL` (2026-10-24 on). An old arm on an earlier
core fails to compile with it, whatever the modes: build that arm with
`--no-default-features` (plus any other feature it needs, e.g.
`--features save_api`).

Discard a mode whose 1-minute load average changes by more than 2x
between its start and end. The core opens its own connections, so the
DuckDB thread count is the engine default in both arms.
"""
import json
import os
import random
import statistics
import subprocess
import sys
import time

old_bin, new_bin, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
modes = sys.argv[4:]
rng = random.Random(20261023)


def sample(binary, mode):
    out = subprocess.run([binary, mode], check=True, capture_output=True, text=True)
    return float(out.stdout.strip().splitlines()[-1])


def boot_ci(ratios, b=10000):
    meds = []
    for _ in range(b):
        s = [rng.choice(ratios) for _ in ratios]
        meds.append(statistics.median(s))
    meds.sort()
    return meds[int(0.025 * b)], meds[int(0.975 * b) - 1]


results = []
for mode in modes:
    la0 = os.getloadavg()
    sample(old_bin, mode)  # warm-up (page cache, binary)
    sample(new_bin, mode)
    olds, news, ratios = [], [], []
    t0 = time.time()
    for _ in range(n):
        order = [("old", old_bin), ("new", new_bin)]
        rng.shuffle(order)
        got = {}
        for name, b in order:
            got[name] = sample(b, mode)
        olds.append(got["old"])
        news.append(got["new"])
        ratios.append(got["new"] / got["old"])
    la1 = os.getloadavg()
    lo, hi = boot_ci(ratios)
    r = {
        "mode": mode,
        "n": n,
        "old_median_ms": statistics.median(olds),
        "new_median_ms": statistics.median(news),
        "ratio_median": statistics.median(ratios),
        "ratio_ci95": [lo, hi],
        "speedup": 1 / statistics.median(ratios),
        "load_start": la0[0],
        "load_end": la1[0],
        "wall_s": time.time() - t0,
    }
    results.append(r)
    print(json.dumps(r), flush=True)
