"""Integration self-test of the perf gate (ab_ratio_gates_PLAN §9 items
7-10): real builds and real samples, a handful of metrics each. Not part
of `just test` (minutes, needs a quiet host); `just perf-gate-selftest`.

1. A/A: HEAD vs HEAD (salted) on a few metrics must exit 0.
2. Injected regression: the candidate busy-spins x1.15 in one io metric
   and one hot metric (NODAL_AB_INJECT via --self-test --inject): those
   FAIL (exit 1), the uninjected ones PASS.
3. Small injection: x1.03 on a 1.05 metric must not FAIL.
4. Wrong result: `wrong` (a row-count check fails) and `wrongvalue` (the
   digest differs) each give WRONG RESULT, exit 4, also with --allow.
5. Old baseline (OLD_BASE, default 40e16a5, before lookup_const and
   skip-unchanged): builds, SKIPs the metrics that need those, completes.

usage: python3 scripts/bench/ab/selftest.py [--work DIR] [--old-base REV]
       [--only 1,2,...] [--force]
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))

SUBSET = "wr_scalar_1_replace_100k,wr_20x1_scalars_100k,lk_core_sql_100k,rd_point_scalar"


def gate(args, work, force):
    fd, path = tempfile.mkstemp(prefix="ab-selftest-", suffix=".json")
    os.close(fd)
    cmd = [sys.executable, os.path.join(HERE, "ab.py"), *args, "--work", work, "--json", path]
    if force:
        cmd.append("--force")
    print("$ " + " ".join(cmd[1:]), flush=True)
    rc = subprocess.run(cmd, cwd=REPO).returncode
    try:
        with open(path) as f:
            out = json.load(f)
    except (OSError, ValueError):
        out = None
    os.remove(path)
    return rc, out


def verdicts(out):
    return {r["metric"]: r["verdict"] for r in out["results"]} if out else {}


def expect(name, cond, detail):
    print(f"[{'ok' if cond else 'FAILED'}] {name}: {detail}", flush=True)
    return cond


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--work", default=os.path.join(REPO, "target-ab"))
    ap.add_argument("--old-base", default="40e16a5")
    ap.add_argument("--only", default="1,2,3,4,5")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    only = {int(x) for x in a.only.split(",")}
    ok = True
    aa = ["HEAD", "HEAD", "--aa", "--quick", "--self-test"]

    if 1 in only:
        rc, out = gate(aa + ["--metrics", SUBSET], a.work, a.force)
        ok &= expect("A/A", rc == 0, f"exit {rc}, {verdicts(out)}")
    if 2 in only:
        rc, out = gate(aa + ["--metrics", SUBSET, "--inject", "wr_20x1_scalars_100k=1.15"],
                       a.work, a.force)
        v = verdicts(out)
        ok &= expect("x1.15 on an io metric", rc == 1 and v.get("wr_20x1_scalars_100k") == "FAIL"
                     and all(x == "PASS" for m, x in v.items() if m != "wr_20x1_scalars_100k"),
                     f"exit {rc}, {v}")
        rc, out = gate(aa + ["--metrics", SUBSET, "--inject", "rd_point_scalar=1.15"],
                       a.work, a.force)
        v = verdicts(out)
        ok &= expect("x1.15 on a hot metric", rc == 1 and v.get("rd_point_scalar") == "FAIL",
                     f"exit {rc}, {v}")
    if 3 in only:
        rc, out = gate(aa + ["--metrics", "rd_point_scalar,lk_core_sql_100k",
                             "--inject", "rd_point_scalar=1.03"], a.work, a.force)
        v = verdicts(out)
        ok &= expect("x1.03 on a 1.05 metric does not FAIL", v.get("rd_point_scalar") != "FAIL"
                     and rc in (0, 2), f"exit {rc}, {v}")
    if 4 in only:
        for spec in ("wrong", "wrongvalue"):
            rc, out = gate(aa + ["--metrics", SUBSET, "--inject", f"wr_scalar_1_replace_100k={spec}",
                                 "--allow", "wr_scalar_1_replace_100k=trying to allow it"],
                           a.work, a.force)
            v = verdicts(out)
            r = next((x for x in out["results"] if x["metric"] == "wr_scalar_1_replace_100k"), {}) if out else {}
            ok &= expect(f"{spec} -> WRONG RESULT", rc == 4 and v.get("wr_scalar_1_replace_100k") == "WRONG RESULT"
                         and "ratio" not in r and "allow_refused" in r, f"exit {rc}, {v}, {r.get('error')}")
    if 5 in only:
        rc, out = gate([a.old_base, "HEAD", "--quick", "--metrics",
                        SUBSET + ",wr_unchanged_scalar_10k,wr_delete_1000_scalar_big,rd_dense"],
                       a.work, a.force)
        v = verdicts(out)
        ok &= expect("old baseline builds, skips what it lacks, completes",
                     rc in (0, 1, 2) and v.get("lk_core_sql_100k") == "SKIPPED"
                     and v.get("wr_unchanged_scalar_10k") == "SKIPPED"
                     and all(x not in ("ERROR", "WRONG RESULT") for x in v.values()),
                     f"exit {rc}, {v}")
    print("SELFTEST " + ("PASSED" if ok else "FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
