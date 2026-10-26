"""The A/B perf gate (ab_ratio_gates_PLAN): judge a revision's performance
by paired, interleaved ratios against a baseline on this host, never
against absolute numbers.

usage:
    python3 scripts/bench/ab/ab.py [BASE] [CAND] [--quick | --full] [--release]
        [--metrics GLOB[,GLOB]] [--allow METRIC=reason]... [--force]
        [--aa [--aa-salt K]] [--cpus LIST] [--work DIR] [--json PATH]
    python3 scripts/bench/ab/ab.py --list
    python3 scripts/bench/ab/ab.py --bins OLD_BIN NEW_BIN --modes M [M ...] [-n N]

BASE defaults to `git merge-base HEAD main` (give it explicitly on main
itself), CAND to WORKTREE (the uncommitted working tree). `--release` (the
release gate, `just full`): BASE = the commit in scripts/bench/ab/BASELINE,
CAND = HEAD when the working tree is clean, and each metric runs at its
`release_scale` (metrics.toml: quick unless its cost depends on file
size; perf_gate_footprint_PLAN D4; `--quick` / `--full` force one scale);
`--advance-baseline` then moves BASELINE to CAND after an exit-0, unforced
run of every metric against BASELINE itself, with the gate's own code
committed (a ratchet, plan D2). A size metric with `size_from` is gated
from the `size_bytes` its timed metric's samples printed when that metric
ran (else its own mode runs). In a release run, a metric whose capability the base
has and the candidate lacks is an ERROR, not SKIPPED. Both arms build
the harness in `harness/` of *this* checkout against their own core
(scripts/lib/revbuild.py: one shared target dir under --work, the harness
lock seeded from the revision's root Cargo.lock, binaries cached by tree,
harness, features, lock, rustc and salt), with the cargo features of the
capabilities both arms provide (capabilities.toml, source probes). Each
arm builds its own fixtures with its own core (cached under --work).
Builds run at most 8 cargo jobs, through sccache when it is installed.

DuckDB caps (metrics.toml [settings], perf_gate_footprint_PLAN D1, D2):
every sample runs with SLURM_CPUS_ON_NODE = duckdb_threads (4; a metric's
own `threads` wins) and SLURM_MEM_PER_NODE = duckdb_memory_mb, every
fixture build with fixture_threads (8); DuckDB sizes each instance from
them. Before the fixtures, each arm's binary must report exactly these
caps (harness mode `threads`), else the run is refused (exit 3).

Per metric (metrics.toml): one warm-up sample per arm, then pairs in
counterbalanced order up to pre-fixed looks (default 6, 11, 21 pairs);
ratio = cand / base, statistic = the median ratio with its exact
order-statistic 95 % CI. At each look: PASS if the CI's upper bound <= the
metric's threshold, FAIL if its lower bound is above it, else the next
look; after the last, INCONCLUSIVE. (At 6 pairs the CI is the range: all
six ratios <= T passes at once.) A FAIL is confirmed by an independent
rerun (FAIL again -> FAIL, else FLAKY). A run whose foreign load (below)
drifts > 2x (the median of its first 3 pairs vs its last 3, floored at
nproc/16 cores) is rerun once, then marked LOAD-DRIFT. Sizes take 3
samples per arm and gate max(C) / min(B). Every sample's result check must
pass in both arms and the arms' result digests must be equal; otherwise
WRONG RESULT, and no ratio.

Exit status: 0 every metric PASS / ALLOWED / SKIPPED; 1 a FAIL; 2 an
INCONCLUSIVE / FLAKY / LOAD-DRIFT and no FAIL; 3 a build or harness error
(or a refused start); 4 WRONG RESULT, which overrides every other code and
which --allow cannot cover.

The host: the load rules use the *foreign* load, the busy cores of every
other process (/proc/stat busy CPU minus the gate's own and its children's
CPU; `HostLoad`), so the gate's own samples and builds never count. The
run refuses to start when the foreign load over 5 s exceeds nproc/4 and
warns above nproc/8 (--force runs anyway; results are marked FORCED); the
same check repeats after the builds, with no wait. Per pair, the foreign
and gate cores, PSI cpu `some avg10` and the 1-min load average
(information only) are recorded; the CPU governor and boost flag too.

`--aa` (A/A calibration): BASE and CAND may be the same revision; each arm
gets its own salt (`--aa-salt K` picks a different pair), so two extract
directories, two binaries and two fixture sets; the binaries are compared
byte for byte and the result is recorded.

`--self-test --inject METRIC=SPEC` (tool tests only) sets NODAL_AB_INJECT
for the candidate arm: SPEC = a factor such as 1.15 (busy-spin), `wrong`
(a result check fails) or `wrongvalue` (the digest differs). A gate run
refuses to start when NODAL_AB_INJECT is set in the runner's environment.

`--bins OLD NEW --modes ...` is the ad-hoc mode of the former
ab_write_path/ab.py (two prebuilt binaries, the caller's environment, no
verdicts): JSON lines with the median ratio, the order-statistic CI and
the old bootstrap CI.
"""

import argparse
import datetime
import filecmp
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
HARNESS = os.path.join(HERE, "harness")
HARNESS_REL = os.path.relpath(HARNESS, REPO)
sys.path.insert(0, os.path.join(REPO, "scripts", "lib"))
sys.path.insert(0, HERE)
import gatelib  # noqa: E402
import revbuild  # noqa: E402

BIN = "ab_harness"


def log(msg):
    print(msg, flush=True)


# ------------------------------------------------------------------ host

def read_text(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return None


def host_info():
    model = None
    for line in (read_text("/proc/cpuinfo") or "").splitlines():
        if line.startswith("model name"):
            model = line.split(":", 1)[1].strip()
            break
    return {
        "nproc": os.cpu_count(),
        "cpu": model,
        "kernel": os.uname().release,
        "governor": read_text("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
        "boost": read_text("/sys/devices/system/cpu/cpufreq/boost"),
        "load_start": os.getloadavg(),
    }


def proc_stat_busy(text):
    """Busy jiffies of all CPUs from /proc/stat's `cpu` line: user, nice,
    system, irq, softirq, steal (guest time is inside user; idle and
    iowait are not busy)."""
    for line in text.splitlines():
        if line.startswith("cpu "):
            v = [int(x) for x in line.split()[1:]]
            v += [0] * (8 - len(v))
            return v[0] + v[1] + v[2] + v[5] + v[6] + v[7]
    raise ValueError("/proc/stat: no cpu line")


def psi_cpu_some_avg10(text):
    """`some avg10` of /proc/pressure/cpu, or None."""
    for line in (text or "").splitlines():
        if line.startswith("some "):
            for kv in line.split()[1:]:
                k, _, v = kv.partition("=")
                if k == "avg10":
                    return float(v)
    return None


START_WINDOW_S = 5.0


def make_meter():
    return HostLoad()


class HostLoad:
    """The foreign load meter (perf_gate_footprint_PLAN D3). Each call
    returns the busy cores of every *other* process since the previous
    call: (Δ host busy jiffies / HZ - Δ CPU of this process and its waited
    children) / Δ wall. The gate's samples are waited children, so their
    CPU (and their own children's, e.g. the arm's CLI) is subtracted; the
    1-min load average counted it. Kernel work our IO triggers counts as
    foreign (small; the drift floor absorbs it). `last` holds the detail of
    the latest call: foreign and gate cores, PSI cpu `some avg10`, and the
    1-min load average (information only)."""

    def __init__(self, stat_path="/proc/stat", psi_path="/proc/pressure/cpu"):
        self.stat_path = stat_path
        self.psi_path = psi_path
        self.hz = os.sysconf("SC_CLK_TCK")
        self.last = None
        self._prev = self._snap()

    def _snap(self):
        t = os.times()
        return (time.monotonic(), proc_stat_busy(read_text(self.stat_path) or ""),
                t.user + t.system + t.children_user + t.children_system)

    def __call__(self):
        now = self._snap()
        (t0, b0, g0), (t1, b1, g1) = self._prev, now
        self._prev = now
        wall = max(t1 - t0, 1e-6)
        host = (b1 - b0) / self.hz / wall
        gate = (g1 - g0) / wall
        self.last = {"foreign": round(max(host - gate, 0.0), 3), "gate": round(gate, 3),
                     "psi_some_avg10": psi_cpu_some_avg10(read_text(self.psi_path)),
                     "load1": os.getloadavg()[0]}
        return self.last["foreign"]

    def window(self, seconds=5.0):
        """The foreign busy cores over the next `seconds` (the gate idle)."""
        self()
        time.sleep(seconds)
        return self()


# ----------------------------------------------------------------- arms

class Arm:
    def __init__(self, name, rev, salt):
        self.name = name
        self.rev = rev
        self.salt = salt
        self.tree = revbuild.tree_id(REPO, rev)
        self.commit = (revbuild.commit_id(REPO, "HEAD", 0) if rev == "WORKTREE"
                       else revbuild.commit_id(REPO, rev, 0))
        self.src = None
        self.tool = None
        self.key = None
        self.binary = None
        self.cli = None
        self.envs = {}  # scale -> {fixture env var: path}

    def prepare(self, work, harness_hash):
        """Extract the revision (a commit's extract is reused) and install
        the current harness into it."""
        self.src = os.path.join(
            work, "src", f"{self.tree}-w{harness_hash}" + (f"-s{self.salt}" if self.salt else ""))
        marker = os.path.join(self.src, ".ab-extracted")
        if self.rev == "WORKTREE" or not os.path.exists(marker):
            revbuild.extract(REPO, self.rev, self.src)
            open(marker, "w").close()
        self.tool = revbuild.install_tool(self.src, HARNESS, HARNESS_REL)

    def build(self, work, harness_hash, features):
        lock = revbuild.resolve_lock(self.tool)
        self.key = revbuild.build_key(self.tree, harness_hash, features,
                                      revbuild.rustc_vv(self.tool), lock, self.salt)
        self.binary = os.path.join(work, "bin", f"{BIN}-{self.key}")
        if os.path.exists(self.binary):
            log(f"{self.name} {self.rev} ({self.key}): cached harness")
            return
        t0 = time.time()
        log(f"{self.name} {self.rev} ({self.key}): building the harness ...")
        built = revbuild.cargo_build(self.tool, os.path.join(work, "target"), BIN, features)
        install(built, self.binary)
        log(f"{self.name}: built in {time.time() - t0:.0f} s")

    def build_cli(self, work):
        with open(os.path.join(self.src, "Cargo.lock"), "rb") as f:
            lock = f.read()
        key = revbuild.build_key(self.tree, "cli", ["nodal-db-cli"],
                                 revbuild.rustc_vv(self.src), lock, self.salt)
        self.cli = os.path.join(work, "bin", f"nodal-db-{key}")
        if os.path.exists(self.cli):
            log(f"{self.name}: cached nodal-db CLI")
            return
        t0 = time.time()
        log(f"{self.name}: building the nodal-db CLI ...")
        built = revbuild.cargo_build(self.src, os.path.join(work, "target"), "nodal-db",
                                     package="nodal-db-cli")
        install(built, self.cli)
        log(f"{self.name}: CLI built in {time.time() - t0:.0f} s")


def install(src, dest):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy2(src, dest + ".tmp")
    os.replace(dest + ".tmp", dest)


def clean_env():
    return {k: v for k, v in os.environ.items() if not k.startswith("NODAL_AB_")}


def build_fixtures(cfg, metrics, arms, work, scale):
    """Build (or reuse) every fixture the metrics need at `scale`, per arm
    with that arm's own binary; a `shared` fixture is built once by the
    candidate. Fills each arm's `envs[scale]`."""
    for arm in arms.values():
        arm.envs.setdefault(scale, {})
    needed = []
    for m in metrics:
        for f in m.fixtures:
            if f not in needed:
                needed.append(f)
    for fid in needed:
        fx = cfg.fixtures[fid]
        if fx.build is None:  # the arm's CLI
            for arm in arms.values():
                arm.build_cli(work)
                arm.envs[scale][fx.env] = arm.cli
            continue
        tag = scale if fx.scaled else "fixed"
        owners = [arms["cand"]] if fx.shared else list(arms.values())
        for arm in owners:
            path = os.path.join(work, "fixtures", arm.key, f"{fid}-{tag}.db")
            if not os.path.exists(path):
                os.makedirs(os.path.dirname(path), exist_ok=True)
                tmp = path + ".tmp"
                if os.path.exists(tmp):
                    os.remove(tmp)
                t0 = time.time()
                log(f"{arm.name}: building fixture {fid} ({tag}) ...")
                args = [arm.binary, fx.build, tmp] + ([scale] if fx.scaled else [])
                env = clean_env()
                env.update(gatelib.duckdb_env(cfg.caps.fixture_threads, cfg.caps.memory_mb))
                subprocess.run(args, check=True, env=env, stdout=subprocess.DEVNULL)
                os.replace(tmp, path)
                log(f"{arm.name}: fixture {fid} built in {time.time() - t0:.0f} s")
            if fx.shared:
                for a in arms.values():
                    a.envs[scale][fx.env] = path
            else:
                arm.envs[scale][fx.env] = path


# --------------------------------------------------------------- samples

def parse_sample(arm, metric, proc):
    if proc.returncode == gatelib.CHECK_FAILED_EXIT:
        why = [ln for ln in proc.stderr.splitlines() if ln.startswith("CHECK FAILED")]
        raise gatelib.WrongResult(f"{arm}: " + (why[-1] if why else "result check failed"))
    if proc.returncode == gatelib.QUERY_FAILED_EXIT:
        why = [ln for ln in proc.stderr.splitlines() if ln.startswith("QUERY FAILED")]
        raise gatelib.HarnessError(f"{arm}: " + (why[-1] if why else "a query failed")[:600])
    if proc.returncode != 0:
        tail = " | ".join(proc.stderr.strip().splitlines()[-3:])
        raise gatelib.HarnessError(f"{arm}: {metric.mode} exited {proc.returncode}: {tail}")
    lines = proc.stdout.strip().splitlines()
    digest = None
    for ln in lines:
        if ln.startswith("digest"):
            kvs = [kv.split("=", 1) for kv in ln.split()[1:]]
            if any(len(kv) != 2 for kv in kvs):
                raise gatelib.HarnessError(f"{arm}: {metric.mode}: malformed digest line {ln!r}")
            digest = dict(kvs)
    if digest is None:
        raise gatelib.HarnessError(f"{arm}: {metric.mode} printed no digest (no result check)")
    try:
        value = float(lines[-1])
    except (IndexError, ValueError):
        raise gatelib.HarnessError(f"{arm}: {metric.mode}: no value on the last line")
    return value, digest


def check_arm_caps(arms, caps, threads_used):
    """Start-up check (perf_gate_footprint_PLAN D1): each arm's binary,
    run with the caps' environment, must report exactly those DuckDB
    threads and that memory_limit; else the run is refused (exit 3)."""
    for arm in arms.values():
        for t in sorted(set(threads_used)):
            env = clean_env()
            env.update(gatelib.duckdb_env(t, caps.memory_mb))
            proc = subprocess.run([arm.binary, "threads"], capture_output=True, text=True, env=env)
            why = (gatelib.check_caps(proc.stdout, t, caps.memory_mb) if proc.returncode == 0
                   else f"`threads` mode exited {proc.returncode}")
            if why:
                raise gatelib.ConfigError(
                    f"{arm.name}: the DuckDB caps did not take effect: {why}. A DuckDB bump "
                    "may have changed its SLURM_* detection (CONTRIBUTING \"DuckDB upgrades\")")
    log(f"DuckDB caps verified in both arms: threads {sorted(set(threads_used))}, "
        f"memory {caps.memory_mb} MB (limit {0.8 * caps.memory_mb * 1e6 / 1024 ** 3:.1f} GiB)")


def size_lines(stdout):
    """The `size_bytes N` values a sample printed (mt_* modes)."""
    out = []
    for ln in (stdout or "").splitlines():
        if ln.startswith("size_bytes "):
            try:
                out.append(int(ln.split()[1]))
            except (IndexError, ValueError):
                raise gatelib.HarnessError(f"malformed size line {ln!r}")
    return out


def make_sampler(metric, arms, scale, cpus, inject, caps):
    """sampler(arm) -> (value, digest); `sampler.sizes[arm]` collects the
    sizes the samples printed (gated by a `size_from` metric)."""
    def sampler(arm):
        a = arms[arm]
        env = clean_env()
        env.update(a.envs.get(scale, {}))
        env.update(gatelib.duckdb_env(caps.for_metric(metric), caps.memory_mb))
        env["NODAL_AB_SCALE"] = scale
        if inject and arm == "cand" and inject[0] == metric.id:
            env["NODAL_AB_INJECT"] = f"{metric.mode}:{inject[1]}"
        cmd = [a.binary, metric.mode]
        if cpus:
            cmd = ["taskset", "-c", cpus] + cmd
        proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
        got = parse_sample(arm, metric, proc)
        sampler.sizes[arm] += size_lines(proc.stdout)
        return got
    sampler.sizes = {"base": [], "cand": []}
    return sampler


# ----------------------------------------------------------------- adhoc

def bootstrap_ci(ratios, rng, b=10000):
    meds = sorted(statistics.median(rng.choice(ratios) for _ in ratios) for _ in range(b))
    return meds[int(0.025 * b)], meds[int(0.975 * b) - 1]


def adhoc(a):
    """`--bins OLD NEW --modes ...`: the former ab_write_path/ab.py."""
    old_bin, new_bin = a.bins
    rng = random.Random(a.seed)

    def sample(binary, mode):
        out = subprocess.run([binary, mode], check=True, capture_output=True, text=True)
        return float(out.stdout.strip().splitlines()[-1])

    for mode in a.modes:
        la0 = os.getloadavg()
        sample(old_bin, mode)
        sample(new_bin, mode)
        olds, news, ratios = [], [], []
        t0 = time.time()
        for old_first in gatelib.balanced_orders(a.n, rng):
            got = {}
            for name, b in ([("old", old_bin), ("new", new_bin)] if old_first
                            else [("new", new_bin), ("old", old_bin)]):
                got[name] = sample(b, mode)
            olds.append(got["old"])
            news.append(got["new"])
            ratios.append(got["new"] / got["old"])
        la1 = os.getloadavg()
        lo, hi, k, cov = gatelib.order_stat_ci(ratios)
        r = {
            "mode": mode,
            "n": a.n,
            "old_median_ms": statistics.median(olds),
            "new_median_ms": statistics.median(news),
            "ratio_median": statistics.median(ratios),
            "ratio_ci_order_stat": [lo, hi],
            "ratio_ci_coverage": cov,
            "ratio_ci95_bootstrap": list(bootstrap_ci(ratios, rng)),
            "speedup": 1 / statistics.median(ratios),
            "load_start": la0[0],
            "load_end": la1[0],
            "load_drift": gatelib.drifted(la0[0], la1[0]),
            "wall_s": time.time() - t0,
        }
        print(json.dumps(r), flush=True)
    return 0


# ------------------------------------------------------------------ main

def parse_args(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("base", nargs="?")
    ap.add_argument("cand", nargs="?")
    scale = ap.add_mutually_exclusive_group()
    scale.add_argument("--quick", action="store_true", help="N = 11, small fixtures (default)")
    scale.add_argument("--full", action="store_true", help="N = 21, gate-scale fixtures")
    ap.add_argument("--metrics", help="comma-separated globs over metric ids")
    ap.add_argument("--allow", action="append", default=[], metavar="METRIC=reason")
    ap.add_argument("--force", action="store_true", help="run on a loaded host (marked FORCED)")
    ap.add_argument("--aa", action="store_true", help="A/A calibration (salted arms)")
    ap.add_argument("--aa-salt", default="1")
    ap.add_argument("--cpus", help="taskset CPU list for both arms (see CONTRIBUTING)")
    ap.add_argument("--work", default=os.path.join(REPO, "target-ab"))
    ap.add_argument("--json", help="also write results.json here")
    ap.add_argument("--seed", type=int, default=None, help="pair-order RNG seed")
    ap.add_argument("--list", action="store_true", help="list the metrics and exit")
    ap.add_argument("--release", action="store_true",
                    help="the release gate: BASE = the BASELINE file, CAND = HEAD if the tree is clean")
    ap.add_argument("--advance-baseline", action="store_true",
                    help="after an all-PASS run of a committed CAND, write it to BASELINE")
    ap.add_argument("--self-test", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--inject", help=argparse.SUPPRESS)
    ap.add_argument("--bins", nargs=2, metavar=("OLD_BIN", "NEW_BIN"))
    ap.add_argument("--modes", nargs="+", default=[])
    ap.add_argument("-n", type=int, default=21)
    return ap.parse_args(argv)


def default_base():
    mb = revbuild.git(REPO, "merge-base", "HEAD", "main")
    if mb == revbuild.commit_id(REPO, "HEAD", 0):
        raise gatelib.ConfigError(
            "HEAD is main's tip (merge-base = HEAD): give BASE explicitly "
            "(e.g. the commit before your change, or the release baseline)")
    return mb


BASELINE = os.path.join(HERE, "BASELINE")


def read_baseline():
    """The release gate's baseline commit (scripts/bench/ab/BASELINE: one
    commit id; advanced by `--advance-baseline` after a passing release
    run, plan D2)."""
    try:
        with open(BASELINE) as f:
            rev = f.read().split()[0]
    except (OSError, IndexError):
        raise gatelib.ConfigError(f"{BASELINE}: no baseline commit")
    return rev


def worktree_clean():
    """No uncommitted change in what the arms build (revbuild.CORE_PATHS)."""
    return revbuild.git(REPO, "status", "--porcelain", "--untracked-files=normal", "--",
                        *revbuild.CORE_PATHS) == ""


# The gate's own code and configuration (harness, metrics, thresholds, the
# runner and revbuild): taken from the working tree for both arms, so they
# do not decide CAND, but a BASELINE advance must not rest on uncommitted
# thresholds or checks. BASELINE itself is excluded (the previous advance
# may still be waiting for its commit).
GATE_PATHS = (os.path.relpath(HERE, REPO), os.path.join("scripts", "lib"),
              ":!" + os.path.relpath(os.path.join(HERE, "BASELINE"), REPO))


def gate_tool_clean():
    return revbuild.git(REPO, "status", "--porcelain", "--untracked-files=normal", "--",
                        *GATE_PATHS) == ""


def advance_baseline(out, code):
    """After a passing, complete release run (release scales) of a committed
    candidate against the current BASELINE: BASELINE = CAND."""
    why = None
    baseline = read_baseline()
    if code != gatelib.EXIT_PASS:
        why = f"exit {code}"
    elif not out.get("release") or out.get("scale") not in ("release", "full"):
        why = "only a --release run of the release suite (or at --full) advances it"
    elif out.get("metrics_filter"):
        why = f"a --metrics subset ({out['metrics_filter']})"
    elif not out["base"]["commit"].startswith(baseline):
        why = f"BASE {out['base']['commit'][:12]} is not the BASELINE commit {baseline[:12]}"
    elif out["cand"]["rev"] == "WORKTREE":
        why = "the candidate is an uncommitted working tree"
    elif not out.get("gate_tool_clean", True):
        why = "uncommitted changes in the gate's own code or metrics (" + ", ".join(
            p for p in GATE_PATHS if not p.startswith(":!")) + ")"
    elif out["forced"] or out["aa"] or out["injection"]:
        why = "a forced, A/A or self-test run"
    if why:
        print(f"BASELINE not advanced ({why})")
        return
    with open(BASELINE, "w") as f:
        f.write(out["cand"]["commit"] + "\n")
    print(f"BASELINE advanced to {out['cand']['commit'][:12]}: commit it together with "
          "the run's block in crates/nodal-db-core/benches/RESULTS.md (\"A/B gate runs\")")
    for r in out.get("results", []):
        if r.get("allowed_reason"):
            print(f"  record the accepted regression: {r['metric']} "
                  f"({r.get('verdict_unallowed')}): {r['allowed_reason']}")


def main(argv=None):
    a = parse_args(argv if argv is not None else sys.argv[1:])
    if a.bins:
        if not a.modes:
            print("--bins needs --modes", file=sys.stderr)
            return gatelib.EXIT_ERROR
        return adhoc(a)
    try:
        return gate(a)
    except gatelib.ConfigError as e:
        print(f"error: {e}", file=sys.stderr)
        return gatelib.EXIT_ERROR
    except subprocess.CalledProcessError as e:
        print(f"error: {' '.join(map(str, e.cmd))} failed ({e.returncode})", file=sys.stderr)
        return gatelib.EXIT_ERROR
    except Exception:
        # An uncaught exception would exit 1, which means FAIL; it is an
        # error of the tool or the host (exit 3).
        import traceback
        traceback.print_exc()
        return gatelib.EXIT_ERROR


def gate(a):
    cfg = gatelib.load_config(os.path.join(HERE, "metrics.toml"))
    caps = gatelib.load_capabilities(os.path.join(HERE, "capabilities.toml"))
    if a.list:
        for m in cfg.metrics.values():
            needs = f" needs {','.join(m.needs)}" if m.needs else ""
            needs += "" if len(m.scales) == 2 else f" ({'/'.join(m.scales)} only)"
            needs += f"; release {m.release_scale}" + (f" (from {m.size_from})" if m.size_from else "")
            print(f"{m.id:32} {m.cls:4} T={m.threshold:.2f}  fixtures={','.join(m.fixtures) or '-'}{needs}")
            print(f"{'':32} check: {m.check}")
        return 0
    if os.environ.get("NODAL_AB_INJECT") and not a.self_test:
        raise gatelib.ConfigError("NODAL_AB_INJECT is set: a gate run refuses to start (unset it)")
    inject = None
    if a.inject:
        if not a.self_test:
            raise gatelib.ConfigError("--inject is for --self-test runs only")
        mid, spec = a.inject.split("=", 1)
        if mid not in cfg.metrics:
            raise gatelib.ConfigError(f"--inject: no metric {mid!r}")
        inject = (mid, spec)
    metrics = cfg.select(a.metrics)
    allows = gatelib.parse_allows(a.allow, cfg.metrics)
    # The release gate runs each metric at its release_scale (D4) unless a
    # scale is given; any other run at --quick (default) or --full.
    scale = "release" if a.release and not (a.quick or a.full) else ("full" if a.full else "quick")

    def scale_of(m):
        return m.release_scale if scale == "release" else scale
    plans = {s: cfg.plan(s) for s in ("quick", "full")}

    host = host_info()
    for p in plans.values():
        p.drift_floor = host["nproc"] / 16
    meter = make_meter()
    host["foreign_start"] = meter.window(START_WINDOW_S)
    log(f"foreign load: {host['foreign_start']:.1f} busy cores over {START_WINDOW_S:g} s "
        f"(1-min load average {host['load_start'][0]:.1f}, information only)")
    decision, msg = gatelib.load_gate(host["foreign_start"], host["nproc"], a.force)
    forced = False
    if decision == "refuse":
        log(f"refusing to start: {msg}. Wait for a quieter host or pass --force.")
        return gatelib.EXIT_ERROR
    if msg:
        log(f"warning: {msg}")
        forced = a.force and host["foreign_start"] > host["nproc"] / 4
    if host["governor"] and host["governor"] != "performance":
        log(f"warning: CPU governor is {host['governor']!r}, not 'performance' (not changed)")

    if a.release:
        base = a.base or read_baseline()
        cand = a.cand or ("HEAD" if worktree_clean() else "WORKTREE")
    else:
        base = a.base or default_base()
        cand = a.cand or "WORKTREE"
    salts = (f"aa{a.aa_salt}b", f"aa{a.aa_salt}c") if a.aa else ("", "")
    arms = {"base": Arm("base", base, salts[0]), "cand": Arm("cand", cand, salts[1])}
    if arms["base"].tree == arms["cand"].tree and not a.aa:
        raise gatelib.ConfigError(
            f"BASE and CAND are the same tree ({arms['base'].tree}): that is an A/A run, use --aa")
    work = os.path.abspath(a.work)
    os.makedirs(work, exist_ok=True)
    hh = revbuild.dir_hash(HARNESS)
    log(f"base {base} = {arms['base'].tree}; cand {cand} = {arms['cand'].tree}; "
        f"harness {hh}; {scale}, looks {plans['quick'].looks}/{plans['full'].looks}; "
        f"work {work}")

    t_build = time.time()
    for arm in arms.values():
        arm.prepare(work, hh)
    shared, features, lacking = gatelib.resolve_capabilities(
        caps, {k: v.src for k, v in arms.items()})
    log(f"capabilities: shared {sorted(shared) or '-'}; features {features or '-'}"
        + (f"; lacking {lacking}" if lacking else ""))
    for arm in arms.values():
        arm.build(work, hh, features)
    identical = None
    if a.aa:
        identical = filecmp.cmp(arms["base"].binary, arms["cand"].binary, shallow=False)
        log("A/A binaries " + ("are IDENTICAL: this A/A measures noise only, no layout"
                               if identical else "differ (salted extract paths)"))

    runnable, results = [], []
    for m in metrics:
        lost = gatelib.lost_capability(m, lacking) if a.release else None
        if lost:
            results.append({"metric": m.id, "mode": m.mode, "class": m.cls,
                            "threshold": m.threshold, "verdict": gatelib.ERROR,
                            "label": gatelib.ERROR,
                            "error": f"the base has {lost} and the candidate does not "
                                     "(API removed, or its probe in capabilities.toml is stale)"})
            continue
        why = gatelib.skip_reason(m, shared, lacking)
        if not why and scale_of(m) not in m.scales:
            why = f"{scale_of(m)}: runs at {'/'.join(m.scales)} scale only"
        if why:
            results.append({"metric": m.id, "mode": m.mode, "class": m.cls,
                            "threshold": m.threshold, "verdict": gatelib.SKIPPED,
                            "label": f"SKIPPED ({why})", "skip_reason": why})
        else:
            runnable.append(m)
    dcaps = cfg.caps
    check_arm_caps(arms, dcaps, [dcaps.fixture_threads] + [dcaps.for_metric(m) for m in runnable])
    try:
        for sc in ("quick", "full"):
            at = [m for m in runnable if scale_of(m) == sc]
            if at:
                build_fixtures(cfg, at, arms, work, sc)
    except subprocess.CalledProcessError as e:
        log(f"fixture build failed: {e}")
        return gatelib.EXIT_ERROR
    build_s = time.time() - t_build

    # The refusal rule again after the builds, on the foreign load (our own
    # compile and fixture builds do not count, so there is nothing to wait
    # out).
    load_pairs = meter.window(START_WINDOW_S)
    log(f"foreign load before the pairs: {load_pairs:.1f} busy cores")
    decision, msg = gatelib.load_gate(load_pairs, host["nproc"], a.force)
    if decision == "refuse":
        log(f"refusing to measure: {msg} (after the builds). Pass --force to run anyway.")
        return gatelib.EXIT_ERROR
    if msg:
        log(f"warning: {msg}")
        forced = forced or (a.force and load_pairs > host["nproc"] / 4)

    rng = random.Random(a.seed)
    t_run = time.time()
    done = {}  # metric id -> (result, sizes its samples printed)
    for m in runnable:
        t0 = time.time()
        sm = scale_of(m)
        r = None
        if m.size_from in done:
            r = gatelib.size_from_samples(m, *done[m.size_from])
        if r is None:
            sampler = make_sampler(m, arms, sm, a.cpus, inject, dcaps)
            r = gatelib.gate_metric(m, sampler, plans[sm], rng, meter, log=log)
            done[m.id] = (r, sampler.sizes)
        r["wall_s"] = time.time() - t0
        r["scale"] = sm if m.scaled else "fixed"
        r["threads"] = dcaps.for_metric(m)
        results.append(r)
        ratio = (f"{r['ratio']:.3f} [{r['ci'][0]:.3f}, {r['ci'][1]:.3f}]"
                 if "ratio" in r else "-")
        log(f"{m.id:32} {ratio:26} T={m.threshold:.2f}  {r['label']}  ({r['wall_s']:.0f} s)")
    order = {m.id: i for i, m in enumerate(cfg.metrics.values())}
    results.sort(key=lambda r: order.get(r["metric"], 0))
    gatelib.apply_allows(results, allows)
    code = gatelib.exit_code(results)

    print()
    print(gatelib.table(results))
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = {
        "tool": "scripts/bench/ab/ab.py",
        "when": stamp,
        "base": {"rev": base, "commit": arms["base"].commit, "tree": arms["base"].tree,
                 "key": arms["base"].key, "salt": arms["base"].salt},
        "cand": {"rev": cand, "commit": arms["cand"].commit, "tree": arms["cand"].tree,
                 "key": arms["cand"].key, "salt": arms["cand"].salt},
        "harness": hh,
        "release": a.release,
        "metrics_filter": a.metrics,
        "gate_tool_clean": gate_tool_clean(),
        "scale": scale,
        "looks": {sc: list(p.looks) for sc, p in plans.items()},
        "first_look_margin": plans["quick"].first_margin,
        "capabilities": {"shared": sorted(shared), "features": features, "lacking": lacking},
        "host": {**host, "foreign_before_pairs": load_pairs, "load_end": os.getloadavg()},
        "forced": forced,
        "aa": a.aa,
        "aa_binaries_identical": identical,
        "cpus": a.cpus,
        "duckdb_caps": dcaps.as_dict(),
        "injection": {"metric": inject[0], "spec": inject[1]} if inject else None,
        "build_s": build_s,
        "run_s": time.time() - t_run,
        "exit": code,
        "results": results,
    }
    run_dir = os.path.join(work, "runs", f"{stamp}-{arms['base'].tree}-{arms['cand'].tree}")
    os.makedirs(run_dir, exist_ok=True)
    paths = [os.path.join(run_dir, "results.json")] + ([a.json] if a.json else [])
    for p in paths:
        with open(p, "w") as f:
            json.dump(out, f, indent=1)
    print(f"\nbuild + fixtures {build_s:.0f} s, pairs {out['run_s']:.0f} s"
          + ("; FORCED (loaded host)" if forced else "")
          + (f"; INJECTED {inject[0]}={inject[1]}" if inject else ""))
    print(f"results: {paths[0]}")
    if a.advance_baseline:
        advance_baseline(out, code)
    print(f"exit {code}")
    return code


if __name__ == "__main__":
    sys.exit(main())
