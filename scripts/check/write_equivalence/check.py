"""Randomized write-path equivalence check between two git revisions.

Builds the workload in this directory (src/main.rs) against the core of
each revision, runs the same seeded random API workload with both, and
diffs the outputs: every commit's report or error, every row of every
base table (without the commit timestamps and the db_identity row), the
sequence counters and the audit view counts. Use it to show that a
write-path change that is meant to be invisible (a refactor, a
performance change) writes the same data as before.

usage:
    python3 scripts/check/write_equivalence/check.py OLD [NEW]
        [--seeds 1,2,3] [--commits 100] [--big] [--work DIR]

OLD and NEW are git revisions (NEW defaults to HEAD); NEW may also be
the literal WORKTREE, the uncommitted working tree. Each revision is
extracted with `git archive` and this directory (the workload of the
current checkout, not of the revision) is copied into it, so both arms
run the same workload. Both build in release mode in one shared target
directory (`--work`/target), so the bundled DuckDB compiles once; the
binaries are cached per commit id. Exit status 0 when every seed matches,
1 otherwise (the first differing lines are printed; the full outputs stay
in the work directory).

`--big` adds large rounds now and then (a 700-entity long segment,
31 x 8 760-element arrays, a 16 385-key delete): slower, but it crosses
the write path's chunk and round thresholds.
"""

import argparse
import difflib
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
REL = os.path.relpath(HERE, REPO)


def git(*args):
    return subprocess.run(
        ["git", "-C", REPO, *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def tree_id(rev):
    """A cache key for the revision's source: the commit id, or a hash of
    the working tree's diff for WORKTREE."""
    if rev == "WORKTREE":
        diff = subprocess.run(
            ["git", "-C", REPO, "diff", "HEAD", "--", "crates", "schema"],
            check=True, capture_output=True,
        ).stdout
        return "wt-" + git("rev-parse", "--short=12", "HEAD") + "-" + hashlib.sha1(diff).hexdigest()[:8]
    return git("rev-parse", "--short=12", rev + "^{commit}")


def extract(rev, dest):
    shutil.rmtree(dest, ignore_errors=True)
    os.makedirs(dest)
    if rev == "WORKTREE":
        for part in ("Cargo.toml", "Cargo.lock", "crates", "schema"):
            src = os.path.join(REPO, part)
            if os.path.isdir(src):
                shutil.copytree(src, os.path.join(dest, part),
                                ignore=shutil.ignore_patterns("target", "__pycache__"))
            elif os.path.exists(src):
                shutil.copy2(src, dest)
    else:
        archive = subprocess.run(
            ["git", "-C", REPO, "archive", rev], check=True, capture_output=True
        ).stdout
        subprocess.run(["tar", "-x", "-C", dest], input=archive, check=True)
    tool = os.path.join(dest, REL)
    shutil.rmtree(tool, ignore_errors=True)
    shutil.copytree(HERE, tool, ignore=shutil.ignore_patterns("target", "__pycache__", "Cargo.lock"))
    return tool


def build(rev, work):
    tid = tree_id(rev)
    binary = os.path.join(work, "bin", f"write_equivalence-{tid}")
    if rev != "WORKTREE" and os.path.exists(binary):
        print(f"{rev} ({tid}): cached binary", flush=True)
        return binary
    tool = extract(rev, os.path.join(work, "src", tid))
    target = os.path.join(work, "target")
    t0 = time.time()
    print(f"{rev} ({tid}): building ...", flush=True)
    subprocess.run(
        ["cargo", "build", "--release", "--quiet"],
        cwd=tool, check=True, env={**os.environ, "CARGO_TARGET_DIR": target},
    )
    os.makedirs(os.path.dirname(binary), exist_ok=True)
    shutil.copy2(os.path.join(target, "release", "write_equivalence"), binary)
    print(f"{rev} ({tid}): built in {time.time() - t0:.0f} s", flush=True)
    return binary


def run(binary, seed, commits, big, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    t0 = time.time()
    out = subprocess.run(
        [binary, str(seed), str(commits), "1" if big else "0", out_dir],
        check=True, capture_output=True, text=True,
    ).stdout
    with open(out_dir + ".txt", "w") as f:
        f.write(out)
    return out, time.time() - t0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("old")
    ap.add_argument("new", nargs="?", default="HEAD")
    ap.add_argument("--seeds", default="1,2,3")
    ap.add_argument("--commits", type=int, default=100)
    ap.add_argument("--big", action="store_true")
    ap.add_argument("--work", default=os.path.join(tempfile.gettempdir(), "nodal-db-write-equivalence"))
    a = ap.parse_args()
    seeds = [int(s) for s in a.seeds.split(",") if s]
    os.makedirs(a.work, exist_ok=True)

    bins = {arm: build(rev, a.work) for arm, rev in (("old", a.old), ("new", a.new))}
    failed = 0
    for seed in seeds:
        outs = {}
        secs = {}
        for arm in ("old", "new"):
            d = os.path.join(a.work, "runs", f"{arm}-{seed}")
            shutil.rmtree(d, ignore_errors=True)
            outs[arm], secs[arm] = run(bins[arm], seed, a.commits, a.big, d)
        n_commits = sum(1 for line in outs["new"].splitlines() if line.startswith("## ") and "(" in line)
        n_err = outs["new"].count("Err(")
        if outs["old"] == outs["new"]:
            print(
                f"seed {seed}: identical ({len(outs['new'].splitlines())} lines, "
                f"{n_commits} commit reports, {n_err} errors; "
                f"old {secs['old']:.1f} s, new {secs['new']:.1f} s)",
                flush=True,
            )
        else:
            failed += 1
            print(f"seed {seed}: DIFFERENT (outputs: {a.work}/runs/{{old,new}}-{seed}.txt)")
            diff = difflib.unified_diff(
                outs["old"].splitlines(), outs["new"].splitlines(), "old", "new", lineterm="", n=1
            )
            for i, line in enumerate(diff):
                if i >= 40:
                    print("...")
                    break
                print(line[:300])
    print(f"{len(seeds) - failed}/{len(seeds)} seeds identical ({a.old} vs {a.new})")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
