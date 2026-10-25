"""The perf gate's decision logic (ab_ratio_gates_PLAN §2, §3.3, §3.4, §6),
kept free of builds and processes so `tests/` can drive it with scripted
samples. `ab.py` wires it to real binaries.

Stdlib only.
"""

import fnmatch
import glob as globmod
import math
import os
import re
import statistics
import tomllib

# Verdicts. ALLOWED replaces a FAIL / INCONCLUSIVE / FLAKY / LOAD-DRIFT
# named by --allow; WRONG RESULT and ERROR can never be allowed.
PASS = "PASS"
FAIL = "FAIL"
INCONCLUSIVE = "INCONCLUSIVE"
FLAKY = "FLAKY"
LOAD_DRIFT = "LOAD-DRIFT"
WRONG = "WRONG RESULT"
ERROR = "ERROR"
SKIPPED = "SKIPPED"
ALLOWED = "ALLOWED"

ALLOWABLE = {FAIL, INCONCLUSIVE, FLAKY, LOAD_DRIFT}

# Exit codes (plan §3.2): WRONG RESULT overrides every other code.
EXIT_PASS, EXIT_FAIL, EXIT_UNSURE, EXIT_ERROR, EXIT_WRONG = 0, 1, 2, 3, 4

# The harness exits with this code when a result check fails (the fixed
# assert of §3.4 item 1); any other non-zero exit is a harness error.
CHECK_FAILED_EXIT = 86
# ... and with this one when a query, core call or the CLI errored, so the
# result could not be checked: an ERROR (exit 3), not a WRONG RESULT.
QUERY_FAILED_EXIT = 87


class ConfigError(Exception):
    pass


class WrongResult(Exception):
    """A result check failed in an arm, or the arms' digests differ."""


class HarnessError(Exception):
    """A sample could not be taken (crash, bad output)."""


# --------------------------------------------------------------- config

class Metric:
    def __init__(self, mid, d, classes):
        self.id = mid
        self.mode = d.get("mode", mid)
        self.cls = d.get("class")
        if self.cls not in classes:
            raise ConfigError(f"metric {mid}: unknown class {self.cls!r}")
        self.threshold = float(d.get("threshold", classes[self.cls]))
        self.check = d.get("check", "")
        if not isinstance(self.check, str) or not self.check.strip():
            # critique M4: a gate metric without a result check is not a
            # gate metric.
            raise ConfigError(f"metric {mid}: no `check` declared (every gate metric needs one)")
        self.fixtures = list(d.get("fixtures", []))
        self.needs = list(d.get("needs", []))
        self.digest_exclude = set(d.get("digest_exclude", []))
        self.layout_floor = d.get("layout_floor")
        self.scaled = bool(d.get("scaled", False))
        # The scales the metric runs at (default both): a metric whose op is
        # too short to measure at quick scale runs at full scale only.
        self.scales = list(d.get("scales", ["quick", "full"]))
        if not set(self.scales) <= {"quick", "full"} or not self.scales:
            raise ConfigError(f"metric {mid}: scales must be a subset of quick, full")
        self.note = d.get("note", "")

    @property
    def is_size(self):
        return self.cls == "size"


class Fixture:
    def __init__(self, fid, d):
        self.id = fid
        self.env = d["env"]
        self.build = d.get("build")  # harness subcommand, or None (cli, ipc)
        self.scaled = bool(d.get("scaled", False))
        self.shared = bool(d.get("shared", False))  # one file for both arms


class Config:
    def __init__(self, data):
        self.settings = data.get("settings", {})
        self.classes = {k: float(v) for k, v in data.get("classes", {}).items()}
        if not self.classes:
            raise ConfigError("metrics.toml: no [classes]")
        self.fixtures = {k: Fixture(k, v) for k, v in data.get("fixture", {}).items()}
        self.metrics = {}
        for mid, d in data.get("metric", {}).items():
            m = Metric(mid, d, self.classes)
            for f in m.fixtures:
                if f not in self.fixtures:
                    raise ConfigError(f"metric {mid}: unknown fixture {f!r}")
            self.metrics[mid] = m

    def plan(self, scale):
        """The pre-fixed sequential plan for `scale` (see `Plan`)."""
        looks = self.settings.get(f"{scale}_looks", DEFAULT_LOOKS)
        return Plan(looks, float(self.settings.get("first_look_margin", 0.0)))

    def select(self, patterns):
        if not patterns:
            return list(self.metrics.values())
        pats = [p for p in patterns.split(",") if p]
        out = [m for m in self.metrics.values() if any(fnmatch.fnmatch(m.id, p) for p in pats)]
        if not out:
            raise ConfigError(f"--metrics {patterns!r} matches no metric")
        return out


def load_config(path):
    with open(path, "rb") as f:
        return Config(tomllib.load(f))


# ---------------------------------------------------------- capabilities

class Capability:
    def __init__(self, name, d):
        self.name = name
        self.feature = d.get("feature", "")
        self.probe_glob = d["probe_glob"]
        self.probe_regex = re.compile(d["probe_regex"])
        self.note = d.get("note", "")


def load_capabilities(path):
    with open(path, "rb") as f:
        data = tomllib.load(f)
    return {k: Capability(k, v) for k, v in data.items()}


def probe(tree, cap):
    """Does the extracted source `tree` provide `cap`? (critique C1: a
    source probe on the code that will be compiled, not git ancestry.)"""
    for path in sorted(globmod.glob(os.path.join(tree, cap.probe_glob), recursive=True)):
        if not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as f:
            if cap.probe_regex.search(f.read()):
                return True
    return False


def resolve_capabilities(caps, trees):
    """`trees` = {arm: extracted tree}. Returns (shared capability names,
    the cargo feature set both arms build with, {capability: [arms that
    lack it]}). Both arms get the *intersection* (critique C2)."""
    have = {arm: {n for n, c in caps.items() if probe(t, c)} for arm, t in trees.items()}
    shared = set.intersection(*have.values()) if have else set()
    lacking = {
        n: sorted(arm for arm in trees if n not in have[arm]) for n in caps if n not in shared
    }
    features = sorted(caps[n].feature for n in shared if caps[n].feature)
    return shared, features, lacking


def skip_reason(metric, shared, lacking, arm_names=None):
    names = arm_names or {"base": "base", "cand": "candidate"}
    for need in metric.needs:
        if need not in shared:
            who = ", ".join(names.get(a, a) for a in lacking.get(need, ["?"]))
            return f"{who} lacks {need}"
    return None


# ------------------------------------------------------------ statistics

def binom_cdf_half(k, n):
    """P(Bin(n, 1/2) <= k)."""
    if k < 0:
        return 0.0
    return sum(math.comb(n, i) for i in range(0, min(k, n) + 1)) / 2 ** n


def order_stat_k(n, alpha=0.05):
    """The largest k with 2 P(B <= k-1) <= alpha: the exact
    distribution-free CI for the median is [r(k), r(n+1-k)] (1-based),
    coverage 1 - 2 P(B <= k-1). 0 if no such interval exists."""
    k = 0
    while k + 1 <= (n + 1) // 2 and 2 * binom_cdf_half(k, n) <= alpha:
        k += 1
    return k


def order_stat_ci(values, alpha=0.05):
    """(lo, hi, k, coverage) of the median of `values` (critique S1). Below
    the smallest n with a two-sided 95 % interval (n <= 5) the interval is
    the range [r(1), r(n)], k = 1: each bound is an exact one-sided bound
    at level 1 - 0.5^n (96.9 % at n = 5), two-sided coverage 1 - 2 x 0.5^n."""
    n = len(values)
    if n == 0:
        return (-math.inf, math.inf, 0, 0.0)
    s = sorted(values)
    k = max(order_stat_k(n, alpha), 1)
    return (s[k - 1], s[n - k], k, 1 - 2 * binom_cdf_half(k - 1, n))


# The sequential plan (owner direction 2026-10-25: cheap by default; only
# what looks slower or is undecided gets more pairs). Cumulative pair counts
# at which the CI is looked at, fixed in advance (never "until it passes").
# 6 is the smallest N with an exact two-sided 95 % interval (the range
# [r(1), r(6)], 96.9 %): a metric whose 6 ratios are all <= T passes at
# once, one whose 6 ratios are all > T is a FAIL candidate (confirmed by an
# independent rerun). Simulated error rates (scripts/bench/ab/sim.py,
# log-normal pair noise, sigma 1.5-15 %): false PASS at a true ratio = T
# 2.7 % (distribution-free: it depends only on the signs of r - T), false
# PASS at a true 1.10 against T = 1.05 <= 0.3 %; false FAIL at a true 1.00
# 0 in 4 000 runs per cell; ~7-9 pairs per metric on a quiet host.
DEFAULT_LOOKS = (6, 11, 21)


class Plan:
    """Looks at cumulative pair counts `looks` (default 6, 11, 21). At each
    look: PASS if the median's exact 95 % CI has upper bound <= T (at the
    first look: <= T - `first_margin`, a margin for a systematic A/A
    offset; 0 by default), FAIL if its lower bound > T, else extend to the
    next look; after the last look, INCONCLUSIVE."""

    def __init__(self, looks=DEFAULT_LOOKS, first_margin=0.0):
        self.looks = tuple(int(x) for x in looks)
        if not self.looks or any(b <= a for a, b in zip(self.looks, self.looks[1:])):
            raise ConfigError(f"looks must be increasing pair counts, got {looks!r}")
        if self.looks[0] < 6:
            raise ConfigError("the first look needs at least 6 pairs (the smallest exact 95 % CI)")
        self.first_margin = float(first_margin)

    @property
    def cap(self):
        return self.looks[-1]


def verdict(lo, hi, threshold):
    if hi <= threshold:
        return PASS
    if lo > threshold:
        return FAIL
    return INCONCLUSIVE


def balanced_orders(n, rng):
    """n booleans (True = the base runs first in that pair): exactly
    floor(n/2) or ceil(n/2) True, shuffled (plan §2)."""
    first = n // 2 + (rng.randrange(2) if n % 2 else 0)
    orders = [True] * first + [False] * (n - first)
    rng.shuffle(orders)
    return orders


# ------------------------------------------------------------------ load

def load_gate(load1, nproc, force):
    """('refuse' | 'warn' | 'ok', message) for the 1-min load at start
    (§6.4 as tightened by critique T2: refuse above nproc/4, warn above
    nproc/8)."""
    refuse, warn = nproc / 4, nproc / 8
    if load1 > refuse:
        msg = (f"1-min load {load1:.1f} > nproc/4 = {refuse:g}: 1.05 metrics are "
               "unreachable at this noise level")
        return ("warn" if force else "refuse"), msg + (" (--force: results marked FORCED)" if force else "")
    if load1 > warn:
        return "warn", (f"1-min load {load1:.1f} > nproc/8 = {warn:g}: expect "
                        "INCONCLUSIVE on 1.05 metrics")
    return "ok", ""


def drifted(l0, l1, factor=2.0, floor=1.0):
    """Did the 1-min load change more than `factor`x across a run? Loads
    are floored at `floor` so an idle host's 0.1 -> 0.3 is not a drift."""
    a, b = max(l0, floor), max(l1, floor)
    return max(a, b) / min(a, b) > factor


# ------------------------------------------------------------- measuring

def compare_digests(metric, b, c):
    """Raise WrongResult unless the two digests agree on every field not
    excluded by the metric (§3.4 item 2)."""
    keys = (set(b) | set(c)) - metric.digest_exclude
    diff = sorted(k for k in keys if b.get(k) != c.get(k))
    if diff:
        raise WrongResult(
            "arm digests differ: " + "; ".join(f"{k}: base {b.get(k)} cand {c.get(k)}" for k in diff)
        )


def take_pairs(metric, sampler, n, rng, load_fn):
    """n interleaved, counterbalanced pairs. `sampler(arm)` -> (value,
    digest dict); raises WrongResult / HarnessError."""
    pairs = []
    for b_first in balanced_orders(n, rng):
        got = {}
        for arm in (("base", "cand") if b_first else ("cand", "base")):
            got[arm] = sampler(arm)
        compare_digests(metric, got["base"][1], got["cand"][1])
        b, c = got["base"][0], got["cand"][0]
        if b <= 0:
            raise HarnessError(f"non-positive base sample {b}")
        pairs.append({"b": b, "c": c, "ratio": c / b, "b_first": b_first, "load": load_fn()})
    return pairs


def summarize(pairs, threshold, margin=0.0):
    ratios = [p["ratio"] for p in pairs]
    lo, hi, k, cov = order_stat_ci(ratios)
    v = verdict(lo, hi, threshold)
    if v == PASS and hi > threshold - margin:
        v = INCONCLUSIVE
    return {
        "n_pairs": len(pairs),
        "base_median": statistics.median(p["b"] for p in pairs),
        "cand_median": statistics.median(p["c"] for p in pairs),
        "ratio": statistics.median(ratios),
        "ci": [lo, hi],
        "ci_k": k,
        "ci_coverage": cov,
        "verdict": v,
    }


def measure_ratio(metric, sampler, plan, rng, load_fn):
    """One independent run: a warm-up sample per arm (discarded, digests
    still compared), then pairs up to each look of the plan (pooled),
    stopping at the first decided look. Returns a run dict."""
    l0 = load_fn()
    warm = {arm: sampler(arm) for arm in ("base", "cand")}
    compare_digests(metric, warm["base"][1], warm["cand"][1])
    pairs = []
    for i, total in enumerate(plan.looks):
        pairs += take_pairs(metric, sampler, total - len(pairs), rng, load_fn)
        s = summarize(pairs, metric.threshold, plan.first_margin if i == 0 else 0.0)
        if s["verdict"] != INCONCLUSIVE:
            break
    s.update(looks=i + 1, load_start=l0, load_end=load_fn(),
             pairs=pairs, digest=warm["cand"][1])
    return s


def measure_size(metric, sampler, rng, load_fn, samples=3):
    """Sizes: `samples` interleaved measurements per arm; the gate ratio is
    max(C) / min(B) (critique T3), exact when both arms are deterministic."""
    l0 = load_fn()
    bs, cs, digest = [], [], None
    for b_first in balanced_orders(samples, rng):
        got = {}
        for arm in (("base", "cand") if b_first else ("cand", "base")):
            got[arm] = sampler(arm)
        compare_digests(metric, got["base"][1], got["cand"][1])
        bs.append(got["base"][0])
        cs.append(got["cand"][0])
        digest = got["cand"][1]
    ratio = max(cs) / min(bs)
    return {
        "n_pairs": samples,
        "base_median": statistics.median(bs),
        "cand_median": statistics.median(cs),
        "base_values": bs,
        "cand_values": cs,
        "deterministic": len(set(bs)) == 1 and len(set(cs)) == 1,
        "ratio": ratio,
        "ci": [ratio, ratio],
        "ci_k": 0,
        "ci_coverage": None,
        "verdict": PASS if ratio <= metric.threshold else FAIL,
        "looks": 1,
        "load_start": l0,
        "load_end": load_fn(),
        "digest": digest,
    }


def measure_once(metric, sampler, plan, rng, load_fn):
    if metric.is_size:
        return measure_size(metric, sampler, rng, load_fn)
    return measure_ratio(metric, sampler, plan, rng, load_fn)


def measure_with_load_rule(metric, sampler, plan, rng, load_fn, log=None):
    """§6.4: a run whose 1-min load drifts > 2x is rerun once; a second
    drift keeps the result marked LOAD-DRIFT, which never passes."""
    run = measure_once(metric, sampler, plan, rng, load_fn)
    if not drifted(run["load_start"], run["load_end"]):
        return run
    if log:
        log(f"  {metric.id}: load drifted {run['load_start']:.1f} -> {run['load_end']:.1f}, rerunning once")
    first = run
    run = measure_once(metric, sampler, plan, rng, load_fn)
    run["drift_rerun_of"] = _strip(first)
    if drifted(run["load_start"], run["load_end"]):
        run["verdict_unadjusted"] = run["verdict"]
        run["verdict"] = LOAD_DRIFT
    return run


def _strip(run):
    return {k: v for k, v in run.items() if k != "pairs"}


def gate_metric(metric, sampler, plan, rng, load_fn, log=None):
    """Measure one metric with every rule: load drift, extension, and
    confirm-on-FAIL (critique S4: a FAIL is re-measured by an independent,
    unpooled run; FAIL only if that run FAILs on its own data, else FLAKY).
    Returns the result dict (no exception for wrong results or errors)."""
    res = {"metric": metric.id, "mode": metric.mode, "class": metric.cls,
           "threshold": metric.threshold, "check": metric.check}
    try:
        run = measure_with_load_rule(metric, sampler, plan, rng, load_fn, log)
        res.update(run)
        if run["verdict"] == FAIL:
            if log:
                log(f"  {metric.id}: FAIL ({run['ratio']:.3f} [{run['ci'][0]:.3f}, "
                    f"{run['ci'][1]:.3f}]); confirming with an independent run")
            conf = measure_with_load_rule(metric, sampler, plan, rng, load_fn, log)
            res["confirm"] = conf
            if conf["verdict"] != FAIL:
                res["verdict"] = FLAKY
                res["label"] = f"FLAKY (FAIL, then {conf['verdict']})"
            elif (metric.layout_floor is not None and not metric.is_size
                  and run["ratio"] <= metric.threshold + float(metric.layout_floor)):
                res["label"] = "FAIL (<= layout floor)"
    except WrongResult as e:
        _wrong(res, str(e))
    except HarnessError as e:
        res.update(verdict=ERROR, error=str(e))
    res.setdefault("label", res["verdict"])
    return res


def _wrong(res, msg):
    for k in ("ratio", "ci", "base_median", "cand_median", "pairs"):
        res.pop(k, None)
    res.update(verdict=WRONG, label=WRONG, error=msg)


def apply_allows(results, allows):
    """--allow METRIC=reason: FAIL / INCONCLUSIVE / FLAKY / LOAD-DRIFT ->
    ALLOWED (counted as pass, reason kept). Never WRONG RESULT or ERROR."""
    for r in results:
        reason = allows.get(r["metric"])
        if reason is None:
            continue
        if r["verdict"] in ALLOWABLE:
            r["verdict_unallowed"] = r["verdict"]
            r["verdict"] = ALLOWED
            r["label"] = f"ALLOWED ({r['label']})"
            r["allowed_reason"] = reason
        elif r["verdict"] in (WRONG, ERROR):
            r["allow_refused"] = f"--allow cannot cover {r['verdict']}"
    return results


def parse_allows(items, known):
    out = {}
    for item in items or []:
        if "=" not in item:
            raise ConfigError(f"--allow {item!r}: expected METRIC=reason")
        m, reason = item.split("=", 1)
        if m not in known:
            raise ConfigError(f"--allow {m!r}: no such metric")
        if not reason.strip():
            raise ConfigError(f"--allow {m}: the reason must not be empty")
        out[m] = reason.strip()
    return out


def exit_code(results):
    vs = {r["verdict"] for r in results}
    if WRONG in vs:
        return EXIT_WRONG
    if ERROR in vs:
        return EXIT_ERROR
    if FAIL in vs:
        return EXIT_FAIL
    if vs & {INCONCLUSIVE, FLAKY, LOAD_DRIFT}:
        return EXIT_UNSURE
    return EXIT_PASS


# ------------------------------------------------------------- the table

def fmt_value(v, size):
    if v is None:
        return "-"
    if size:
        return f"{v / 1048576:.2f} MiB"
    return f"{v:.3f}" if v < 100 else f"{v:.1f}"


def table(results):
    rows = [("metric", "base", "cand", "ratio [CI]", "T", "verdict", "n", "load", "wall")]
    for r in results:
        size = r.get("class") == "size"
        if "ratio" in r:
            ci = r.get("ci") or [None, None]
            rc = f"{r['ratio']:.3f}" if size else f"{r['ratio']:.3f} [{ci[0]:.3f}, {ci[1]:.3f}]"
        else:
            rc = "-"
        load = (f"{r['load_start']:.1f}-{r['load_end']:.1f}"
                if "load_start" in r and "load_end" in r else "-")
        rows.append((
            r["metric"],
            fmt_value(r.get("base_median"), size),
            fmt_value(r.get("cand_median"), size),
            rc,
            f"{r['threshold']:.2f}",
            r.get("label", r["verdict"]),
            str(r.get("n_pairs", "-")),
            load,
            f"{r['wall_s']:.0f}s" if "wall_s" in r else "-",
        ))
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    lines = ["  ".join(c.ljust(w) for c, w in zip(row, widths)).rstrip() for row in rows]
    lines.insert(1, "  ".join("-" * w for w in widths))
    extra = []
    for r in results:
        if r.get("allowed_reason"):
            extra.append(f"  {r['metric']}: allowed: {r['allowed_reason']}")
        if r.get("allow_refused"):
            extra.append(f"  {r['metric']}: {r['allow_refused']}")
        if r.get("error"):
            extra.append(f"  {r['metric']}: {r['verdict']}: {r['error']}")
        if r.get("skip_reason"):
            extra.append(f"  {r['metric']}: skipped: {r['skip_reason']}")
    return "\n".join(lines + extra)
