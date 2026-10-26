"""Hermetic tests of the perf gate's decision logic (ab_ratio_gates_PLAN §9
items 1-4, 6, 12, 13): statistics, pair order, load rules, capabilities,
exit codes and --allow, confirm-on-FAIL, result checks. No builds, no
processes: samples are scripted."""

import os
import random
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..")))
import gatelib as g  # noqa: E402

CLASSES = {"hot": 1.05, "io": 1.10, "size": 1.02}


def metric(mid="m", cls="hot", **kw):
    d = {"class": cls, "check": "rows + digest"}
    d.update(kw)
    return g.Metric(mid, d, CLASSES)


class Script:
    """A sampler whose base arm always takes 100 and whose candidate arm
    takes 100 x the next scripted ratio (warm-up samples included)."""

    def __init__(self, cand_ratios, digests=None):
        self.cand = list(cand_ratios)
        self.digests = digests or {}
        self.calls = {"base": 0, "cand": 0}

    def __call__(self, arm):
        self.calls[arm] += 1
        d = self.digests.get(arm, {"rows": "1"})
        if arm == "base":
            return 100.0, d
        return 100.0 * self.cand.pop(0), d


def flat_load():
    return 1.0


class OrderStatCI(unittest.TestCase):
    def check(self, n, k, coverage):
        vals = list(range(1, n + 1))
        random.Random(n).shuffle(vals)
        lo, hi, kk, cov = g.order_stat_ci(vals)
        self.assertEqual(kk, k)
        self.assertEqual((lo, hi), (k, n + 1 - k))
        self.assertAlmostEqual(cov, coverage, places=3)

    def test_n11(self):
        self.check(11, 2, 0.988)

    def test_n21(self):
        self.check(21, 6, 0.973)

    def test_n22(self):
        self.check(22, 6, 0.983)

    def test_n33(self):
        self.check(33, 11, 0.965)

    def test_n6_is_the_range(self):
        self.check(6, 1, 1 - 2 / 64)

    def test_below_6_the_range_is_used(self):
        lo, hi, k, cov = g.order_stat_ci([1.0, 1.2, 0.9, 1.1, 1.0])
        self.assertEqual((lo, hi, k), (0.9, 1.2, 1))
        self.assertAlmostEqual(cov, 1 - 2 / 32)

    def test_deterministic(self):
        r = [1.01, 0.99, 1.03, 0.98, 1.0, 1.02, 0.97, 1.04, 1.0, 0.995, 1.005]
        self.assertEqual(g.order_stat_ci(r), g.order_stat_ci(list(reversed(r))))


class Verdicts(unittest.TestCase):
    def test_upper_at_threshold_passes(self):
        self.assertEqual(g.verdict(0.95, 1.05, 1.05), g.PASS)

    def test_lower_above_threshold_fails(self):
        self.assertEqual(g.verdict(1.0501, 1.2, 1.05), g.FAIL)

    def test_lower_at_threshold_is_inconclusive(self):
        self.assertEqual(g.verdict(1.05, 1.2, 1.05), g.INCONCLUSIVE)

    def test_straddle_is_inconclusive(self):
        self.assertEqual(g.verdict(0.99, 1.07, 1.05), g.INCONCLUSIVE)


PLAN = g.Plan()  # looks 6, 11, 21


class Extension(unittest.TestCase):
    def test_default_plan(self):
        self.assertEqual(PLAN.looks, (6, 11, 21))
        for bad in ((5, 11), (11, 6), ()):
            with self.assertRaises(g.ConfigError):
                g.Plan(bad)

    def test_clear_pass_stops_at_the_first_look(self):
        s = Script([1.0] * 100)
        r = g.measure_ratio(metric(), s, PLAN, random.Random(1), flat_load)
        self.assertEqual((r["verdict"], r["n_pairs"], r["looks"]), (g.PASS, 6, 1))
        self.assertEqual(s.calls, {"base": 7, "cand": 7})  # + 1 warm-up each
        self.assertEqual(r["ci_k"], 1)  # the range of 6: exact 96.9 %
        self.assertAlmostEqual(r["ci_coverage"], 1 - 2 / 64)

    def test_first_look_needs_every_ratio_under_t(self):
        # 5 of 6 well under T, one at 1.051: not decided at 6, decided at 11.
        s = Script([1.0] + [0.98, 0.99, 1.0, 1.0, 1.01, 1.051] + [1.0] * 5)
        r = g.measure_ratio(metric(), s, PLAN, random.Random(1), flat_load)
        self.assertEqual((r["verdict"], r["n_pairs"], r["looks"]), (g.PASS, 11, 2))

    def test_first_look_margin(self):
        s = Script([1.0] + [1.04] * 6 + [1.0] * 5)
        r = g.measure_ratio(metric(), s, g.Plan(first_margin=0.02), random.Random(1), flat_load)
        self.assertEqual((r["verdict"], r["n_pairs"]), (g.PASS, 11))

    def test_straddle_extends_to_the_cap_then_inconclusive(self):
        s = Script([0.9, 1.2] * 60)
        r = g.measure_ratio(metric(), s, PLAN, random.Random(1), flat_load)
        self.assertEqual((r["verdict"], r["n_pairs"], r["looks"]), (g.INCONCLUSIVE, 21, 3))

    def test_extension_pools_and_can_decide(self):
        # 6 that straddle, then 5 at 1.0: the pooled 11 give [r(2), r(10)] = [1.0, 1.0].
        s = Script([1.0] + [0.9, 1.2, 1.0, 1.0, 1.0, 1.0] + [1.0] * 5)
        r = g.measure_ratio(metric(), s, PLAN, random.Random(1), flat_load)
        self.assertEqual((r["verdict"], r["n_pairs"], r["looks"]), (g.PASS, 11, 2))

    def test_all_over_threshold_fails_at_the_first_look(self):
        s = Script([1.2] * 7)
        r = g.measure_ratio(metric(), s, PLAN, random.Random(1), flat_load)
        self.assertEqual((r["verdict"], r["n_pairs"]), (g.FAIL, 6))


class PairOrder(unittest.TestCase):
    def test_balanced(self):
        rng = random.Random(7)
        seen_odd = set()
        for n in range(1, 40):
            for _ in range(5):
                o = g.balanced_orders(n, rng)
                self.assertEqual(len(o), n)
                self.assertIn(sum(o), (n // 2, (n + 1) // 2))
                if n % 2:
                    seen_odd.add(sum(o) - n // 2)
        self.assertEqual(seen_odd, {0, 1})  # either arm may get the extra first

    def test_pairs_follow_the_order(self):
        s = Script([1.0] * 50)
        pairs = g.take_pairs(metric(), s, 21, random.Random(3), flat_load)
        self.assertIn(sum(p["b_first"] for p in pairs), (10, 11))


class LoadRules(unittest.TestCase):
    def test_refuse_warn_ok(self):
        self.assertEqual(g.load_gate(9.0, 32, False)[0], "refuse")
        self.assertEqual(g.load_gate(9.0, 32, True)[0], "warn")
        self.assertIn("FORCED", g.load_gate(9.0, 32, True)[1])
        self.assertEqual(g.load_gate(5.0, 32, False)[0], "warn")
        self.assertEqual(g.load_gate(3.0, 32, False)[0], "ok")
        self.assertEqual(g.load_gate(8.0, 32, False)[0], "warn")  # = nproc/4: not above

    def test_drift(self):
        self.assertTrue(g.drifted(2.0, 4.5))
        self.assertTrue(g.drifted(9.0, 3.0))
        self.assertFalse(g.drifted(2.0, 3.9))
        self.assertFalse(g.drifted(0.1, 0.9))  # floored at 1.0

    def loads(self, seq):
        it = iter(seq)
        last = [1.0]

        def f():
            try:
                last[0] = next(it)
            except StopIteration:
                pass
            return last[0]
        return f

    # A load_fn call returns the foreign cores since the previous call
    # (ab.HostLoad): each run = one reset call before its warm-up, then one
    # call per pair; start/end = the medians of the first and last 3 pairs.
    def test_drift_twice_is_load_drift_never_pass(self):
        run = [0.0] + [1.0, 1.0, 1.0, 5.0, 5.0, 5.0]
        load = self.loads(run + run)
        s = Script([1.0] * 100)
        r = g.measure_with_load_rule(metric(), s, PLAN, random.Random(1), load)
        self.assertEqual(r["verdict"], g.LOAD_DRIFT)
        self.assertEqual(r["verdict_unadjusted"], g.PASS)
        self.assertEqual((r["load_start"], r["load_end"]), (1.0, 5.0))
        self.assertIn("drift_rerun_of", r)

    def test_drift_once_reruns_and_keeps_the_rerun(self):
        load = self.loads([0.0] + [1.0, 1.0, 1.0, 5.0, 5.0, 5.0] + [0.0] + [5.0] * 6)
        s = Script([1.0] * 100)
        r = g.measure_with_load_rule(metric(), s, PLAN, random.Random(1), load)
        self.assertEqual(r["verdict"], g.PASS)
        self.assertIn("drift_rerun_of", r)

    def test_below_the_floor_is_no_drift(self):
        # 0.3 -> 1.9 foreign cores: under the nproc/16 = 2 floor.
        load = self.loads([9.0] + [0.3, 0.3, 0.3, 1.9, 1.9, 1.9])
        r = g.measure_with_load_rule(metric(), Script([1.0] * 100), PLAN, random.Random(1), load)
        self.assertEqual(r["verdict"], g.PASS)
        self.assertNotIn("drift_rerun_of", r)
        # ... but a floor of 1 core sees it.
        load = self.loads(([9.0] + [0.3, 0.3, 0.3, 1.9, 1.9, 1.9]) * 2)
        plan = g.Plan(drift_floor=0.5)
        r = g.measure_with_load_rule(metric(), Script([1.0] * 100), plan, random.Random(1), load)
        self.assertEqual(r["verdict"], g.LOAD_DRIFT)

    def test_one_slow_pair_is_not_a_drift(self):
        # A medians-of-3 rule: one busy pair at either end does not trip it.
        load = self.loads([0.0] + [1.0, 9.0, 1.0, 1.0, 1.0, 9.0])
        r = g.measure_with_load_rule(metric(), Script([1.0] * 100), PLAN, random.Random(1), load)
        self.assertEqual(r["verdict"], g.PASS)

    def test_pairs_carry_the_meter_detail(self):
        class Meter:
            last = None

            def __call__(self):
                self.last = {"foreign": 1.5, "gate": 1.0}
                return 1.5
        pairs = g.take_pairs(metric(), Script([1.0] * 10), 3, random.Random(1), Meter())
        self.assertEqual(pairs[0]["host"], {"foreign": 1.5, "gate": 1.0})
        self.assertEqual(pairs[0]["load"], 1.5)

    def test_load_ends(self):
        self.assertEqual(g.load_ends([1, 2, 3, 4, 5, 6, 7]), (2, 6))
        self.assertEqual(g.load_ends([4.0]), (4.0, 4.0))
        self.assertEqual(g.load_ends([]), (0.0, 0.0))


class DuckdbCaps(unittest.TestCase):
    """perf_gate_footprint_PLAN D1, D2: the SLURM_* env and its check."""

    def test_env(self):
        self.assertEqual(g.duckdb_env(4, 12000),
                         {"SLURM_CPUS_ON_NODE": "4", "SLURM_MEM_PER_NODE": "12000"})

    def test_parse_size(self):
        self.assertAlmostEqual(g.parse_size("8.9 GiB"), 8.9 * 1024 ** 3)
        self.assertEqual(g.parse_size("512.0 MiB"), 512 * 1024 ** 2)
        self.assertEqual(g.parse_size("10 bytes"), 10)
        with self.assertRaises(ValueError):
            g.parse_size("lots")

    def test_check_caps(self):
        # 80 % of 12 000 MB = 9.6e9 B = 8.94 GiB, rendered "8.9 GiB".
        self.assertIsNone(g.check_caps("threads 4\nmemory_limit 8.9 GiB\n", 4, 12000))
        self.assertIn("threads", g.check_caps("threads 32\nmemory_limit 8.9 GiB\n", 4, 12000))
        self.assertIn("memory_limit", g.check_caps("threads 4\nmemory_limit 56.5 GiB\n", 4, 12000))
        self.assertIn("memory_limit", g.check_caps("threads 4\n", 4, 12000))
        self.assertIsNotNone(g.check_caps("", 4, 12000))

    def test_metric_threads_override(self):
        cfg = g.Config({"classes": CLASSES, "settings": {"duckdb_threads": 4},
                        "metric": {"a": {"class": "hot", "check": "c"},
                                   "b": {"class": "hot", "check": "c", "threads": 1}}})
        self.assertEqual([cfg.caps.for_metric(m) for m in cfg.metrics.values()], [4, 1])
        for bad in (0, "2", 1.5):
            with self.assertRaises(g.ConfigError):
                metric(threads=bad)

    def test_defaults_and_the_real_file(self):
        caps = g.Config({"classes": CLASSES}).caps
        self.assertEqual(caps.as_dict(), {"threads": 4, "fixture_threads": 8, "memory_mb": 12000})
        cfg = g.load_config(os.path.join(HERE, "..", "metrics.toml"))
        self.assertEqual(cfg.caps.threads, 4)
        self.assertEqual(cfg.caps.for_metric(cfg.metrics["rd_long_scan_17pct"]), 1)


class ReleaseSuite(unittest.TestCase):
    """perf_gate_footprint_PLAN D4: release_scale per metric, sizes from
    the maintenance samples."""

    def test_release_scale_default_and_validation(self):
        self.assertEqual(metric().release_scale, "quick")
        self.assertEqual(metric(scales=["full"]).release_scale, "full")
        self.assertEqual(metric(release_scale="full").release_scale, "full")
        with self.assertRaises(g.ConfigError):
            metric(scales=["full"], release_scale="quick")

    def cfg(self, **sz):
        d = {"class": "size", "check": "c", "fixtures": ["f"], "release_scale": "full"}
        d.update(sz)
        return g.Config({"classes": CLASSES, "fixture": {"f": {"env": "X", "build": "b"}},
                         "metric": {"mt": {"class": "io", "check": "c", "fixtures": ["f"],
                                           "release_scale": "full"},
                                    "sz": d}})

    def test_size_from_validation(self):
        self.assertEqual(self.cfg(size_from="mt").metrics["sz"].size_from, "mt")
        for bad in ({"size_from": "nope"}, {"size_from": "sz"},
                    {"size_from": "mt", "release_scale": "quick"},
                    {"size_from": "mt", "fixtures": []}):
            with self.assertRaises(g.ConfigError, msg=bad):
                self.cfg(**bad)

    def test_size_from_samples(self):
        m = self.cfg(size_from="mt").metrics["sz"]
        src = {"metric": "mt", "verdict": g.PASS, "load_start": 1.0, "load_end": 1.2,
               "digest": {"rows": "5"}}
        r = g.size_from_samples(m, src, {"base": [100] * 7, "cand": [101] * 7})
        self.assertEqual((r["verdict"], r["n_pairs"], r["size_from"]), (g.PASS, 7, "mt"))
        self.assertAlmostEqual(r["ratio"], 1.01)
        self.assertTrue(r["deterministic"])
        r = g.size_from_samples(m, src, {"base": [100, 100], "cand": [100, 103]})
        self.assertEqual(r["verdict"], g.FAIL)  # max(C) / min(B)
        self.assertIsNone(g.size_from_samples(m, src, {"base": [], "cand": []}))
        self.assertIsNone(g.size_from_samples(m, {**src, "verdict": g.WRONG},
                                              {"base": [1], "cand": [1]}))

    def test_the_real_release_set(self):
        cfg = g.load_config(os.path.join(HERE, "..", "metrics.toml"))
        full = sorted(m.id for m in cfg.metrics.values() if m.release_scale == "full")
        self.assertEqual(full, sorted([
            "rd_dense", "rd_long_scan_17pct", "rd_model_load_long", "rd_scalars_loader",
            "mt_compact_history_heavy", "mt_extract_head", "imp_ipc_dense",
            "wr_replace_scalar_cycle_ckpt", "wr_replace_array_cycle_ckpt",
            "wr_replace_long_cycle_ckpt", "sz_compacted_bytes", "sz_extract_bytes"]))
        self.assertTrue(all("quick" in m.scales for m in cfg.metrics.values()
                            if m.id.startswith("rd_axes_")))
        # A size metric runs after the metric it is gated from.
        order = list(cfg.metrics)
        for m in cfg.metrics.values():
            if m.size_from:
                self.assertLess(order.index(m.size_from), order.index(m.id))


class Capabilities(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ab-caps-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.caps = {
            "lookup_const": g.Capability("lookup_const", {
                "feature": "lookup_const",
                "probe_glob": "crates/core/src/**/*.rs",
                "probe_regex": r"pub const ENTITY_NAME_LOOKUP_SQL\b"}),
            "skip_unchanged": g.Capability("skip_unchanged", {
                "feature": "",
                "probe_glob": "crates/core/src/**/*.rs",
                "probe_regex": r"pub values_unchanged\b"}),
        }

    def tree(self, name, files):
        root = os.path.join(self.tmp, name)
        for rel, text in files.items():
            p = os.path.join(root, rel)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, "w") as f:
                f.write(text)
        return root

    def test_probe_present_absent_nested(self):
        t = self.tree("a", {"crates/core/src/api/x.rs": "pub const ENTITY_NAME_LOOKUP_SQL: &str = \"\";"})
        self.assertTrue(g.probe(t, self.caps["lookup_const"]))
        self.assertFalse(g.probe(t, self.caps["skip_unchanged"]))
        # A mention elsewhere (a test, a comment outside the glob) does not count.
        u = self.tree("b", {"crates/core/tests/t.rs": "pub const ENTITY_NAME_LOOKUP_SQL: &str = \"\";"})
        self.assertFalse(g.probe(u, self.caps["lookup_const"]))

    def test_intersection_both_arms(self):
        new = self.tree("new", {"crates/core/src/lib.rs":
                                "pub const ENTITY_NAME_LOOKUP_SQL: &str = \"\";\npub values_unchanged: usize,"})
        old = self.tree("old", {"crates/core/src/lib.rs": "pub values_unchanged: usize,"})
        shared, features, lacking = g.resolve_capabilities(self.caps, {"base": old, "cand": new})
        self.assertEqual(shared, {"skip_unchanged"})
        self.assertEqual(features, [])  # both arms build WITHOUT lookup_const
        self.assertEqual(lacking, {"lookup_const": ["base"]})
        m = metric("lk", needs=["lookup_const"])
        self.assertEqual(g.skip_reason(m, shared, lacking), "base lacks lookup_const")
        self.assertIsNone(g.skip_reason(metric("w", needs=["skip_unchanged"]), shared, lacking))

    def test_worktree_without_the_symbol_gets_no_feature(self):
        # A branch cut before the API (candidate) against a newer base.
        base = self.tree("base", {"crates/core/src/lib.rs": "pub const ENTITY_NAME_LOOKUP_SQL: &str = \"\";"})
        cand = self.tree("wt", {"crates/core/src/lib.rs": "// nothing"})
        shared, features, lacking = g.resolve_capabilities(self.caps, {"base": base, "cand": cand})
        self.assertNotIn("lookup_const", features)
        self.assertEqual(lacking["lookup_const"], ["cand"])
        # A release run turns this into an ERROR (ab.py), never a silent skip.
        self.assertEqual(g.lost_capability(metric("lk", needs=["lookup_const"]), lacking),
                         "lookup_const")

    def test_lost_capability_only_when_the_base_has_it(self):
        self.assertIsNone(g.lost_capability(metric("lk", needs=["lookup_const"]),
                                            {"lookup_const": ["base"]}))
        self.assertIsNone(g.lost_capability(metric("lk", needs=["lookup_const"]),
                                            {"lookup_const": ["base", "cand"]}))
        self.assertIsNone(g.lost_capability(metric("w"), {"lookup_const": ["cand"]}))

    def test_both_have_it(self):
        src = {"crates/core/src/lib.rs": "pub const ENTITY_NAME_LOOKUP_SQL: &str = \"\";"}
        shared, features, _ = g.resolve_capabilities(
            self.caps, {"base": self.tree("x", src), "cand": self.tree("y", src)})
        self.assertEqual(features, ["lookup_const"])


class ExitCodesAndAllow(unittest.TestCase):
    def res(self, *verdicts):
        return [{"metric": f"m{i}", "verdict": v, "label": v, "threshold": 1.05}
                for i, v in enumerate(verdicts)]

    def test_codes(self):
        self.assertEqual(g.exit_code(self.res(g.PASS, g.SKIPPED)), 0)
        self.assertEqual(g.exit_code(self.res(g.PASS, g.FAIL, g.INCONCLUSIVE)), 1)
        self.assertEqual(g.exit_code(self.res(g.PASS, g.INCONCLUSIVE)), 2)
        self.assertEqual(g.exit_code(self.res(g.FLAKY)), 2)
        self.assertEqual(g.exit_code(self.res(g.LOAD_DRIFT)), 2)
        self.assertEqual(g.exit_code(self.res(g.FAIL, g.ERROR)), 3)
        self.assertEqual(g.exit_code(self.res(g.FAIL, g.ERROR, g.WRONG)), 4)

    def test_allowed_counts_as_pass_and_keeps_its_reason(self):
        r = self.res(g.PASS, g.FAIL)
        g.apply_allows(r, {"m1": "accepted: new index costs 4 % on writes"})
        self.assertEqual(r[1]["verdict"], g.ALLOWED)
        self.assertEqual(r[1]["verdict_unallowed"], g.FAIL)
        self.assertEqual(r[1]["allowed_reason"], "accepted: new index costs 4 % on writes")
        self.assertEqual(g.exit_code(r), 0)

    def test_allow_never_covers_wrong_result_or_error(self):
        r = self.res(g.WRONG, g.ERROR)
        g.apply_allows(r, {"m0": "x", "m1": "y"})
        self.assertEqual([x["verdict"] for x in r], [g.WRONG, g.ERROR])
        self.assertIn("cannot cover", r[0]["allow_refused"])
        self.assertEqual(g.exit_code(r), 4)

    def test_parse_allows(self):
        known = {"a": None}
        self.assertEqual(g.parse_allows(["a=why not"], known), {"a": "why not"})
        for bad in (["a"], ["b=x"], ["a= "]):
            with self.assertRaises(g.ConfigError):
                g.parse_allows(bad, known)


class ConfirmOnFail(unittest.TestCase):
    def run_gate(self, first, second):
        # warm-up + 6 pairs per decided run; the confirmation is independent.
        s = Script([first] * 7 + [second] * 7)
        return g.gate_metric(metric(), s, PLAN, random.Random(1), flat_load), s

    def test_fail_then_pass_is_flaky(self):
        r, _ = self.run_gate(1.3, 1.0)
        self.assertEqual(r["verdict"], g.FLAKY)
        self.assertEqual(r["label"], "FLAKY (FAIL, then PASS)")
        self.assertEqual(g.exit_code([r]), 2)

    def test_fail_then_fail_is_fail(self):
        r, _ = self.run_gate(1.3, 1.25)
        self.assertEqual(r["verdict"], g.FAIL)
        self.assertEqual(g.exit_code([r]), 1)

    def test_runs_are_not_pooled(self):
        r, s = self.run_gate(1.3, 1.0)
        self.assertEqual(r["n_pairs"], 6)
        self.assertEqual(r["confirm"]["n_pairs"], 6)
        self.assertAlmostEqual(r["ratio"], 1.3)  # the first run's own data
        self.assertAlmostEqual(r["confirm"]["ratio"], 1.0)
        self.assertEqual(s.calls["cand"], 14)

    def test_pass_is_not_confirmed(self):
        r, s = self.run_gate(1.0, 1.0)
        self.assertEqual(r["verdict"], g.PASS)
        self.assertNotIn("confirm", r)

    def test_layout_floor_label(self):
        s = Script([1.06] * 24)
        m = metric(layout_floor=0.02)
        r = g.gate_metric(m, s, PLAN, random.Random(1), flat_load)
        self.assertEqual((r["verdict"], r["label"]), (g.FAIL, "FAIL (<= layout floor)"))


class SimulatedErrorRates(unittest.TestCase):
    """The plan's documented error rates, through the gate's own code
    (sim.py; fixed seed, so deterministic)."""

    def test_boundary_and_null(self):
        import sim
        p, _ = sim.simulate(1.05, 0.03, 1.05, PLAN, 1500, 7)
        self.assertLess(p.get(g.PASS, 0), 0.04)  # ~2.7 %, distribution-free
        p, _ = sim.simulate(1.05, 0.03, 1.00, PLAN, 1500, 7)
        self.assertEqual(p.get(g.FAIL, 0), 0)
        self.assertGreater(p.get(g.PASS, 0), 0.99)
        p, _ = sim.simulate(1.05, 0.03, 1.10, PLAN, 1500, 7)
        self.assertLess(p.get(g.PASS, 0), 0.005)
        self.assertGreater(p.get(g.FAIL, 0), 0.99)


class ResultChecks(unittest.TestCase):
    def test_metric_without_check_is_refused(self):
        with self.assertRaises(g.ConfigError):
            g.Metric("m", {"class": "hot"}, CLASSES)
        with self.assertRaises(g.ConfigError):
            g.Metric("m", {"class": "hot", "check": "  "}, CLASSES)
        with self.assertRaises(g.ConfigError):
            g.Config({"classes": CLASSES, "metric": {"m": {"class": "hot"}}})

    def test_config_loads_the_real_metrics_file(self):
        cfg = g.load_config(os.path.join(HERE, "..", "metrics.toml"))
        self.assertTrue(cfg.metrics)
        for m in cfg.metrics.values():
            self.assertTrue(m.check.strip(), m.id)
        caps = g.load_capabilities(os.path.join(HERE, "..", "capabilities.toml"))
        for m in cfg.metrics.values():
            for need in m.needs:
                self.assertIn(need, caps, m.id)

    def test_digest_mismatch_is_wrong_result_without_ratio(self):
        s = Script([1.0] * 30, digests={"base": {"rows": "10", "hash": "aa"},
                                        "cand": {"rows": "10", "hash": "ab"}})
        r = g.gate_metric(metric(), s, PLAN, random.Random(1), flat_load)
        self.assertEqual(r["verdict"], g.WRONG)
        self.assertNotIn("ratio", r)
        self.assertIn("hash", r["error"])
        self.assertEqual(g.exit_code([r]), 4)

    def test_excluded_digest_field_may_differ(self):
        s = Script([1.0] * 30, digests={"base": {"rows": "10", "phys": "1"},
                                        "cand": {"rows": "10", "phys": "2"}})
        r = g.gate_metric(metric(digest_exclude=["phys"]), s, PLAN, random.Random(1), flat_load)
        self.assertEqual(r["verdict"], g.PASS)

    def test_harness_check_failure_is_wrong_result(self):
        def sampler(arm):
            if arm == "cand":
                raise g.WrongResult("cand: CHECK FAILED: read-back: 11 rows, expected 10")
            return 100.0, {}
        r = g.gate_metric(metric(), sampler, PLAN, random.Random(1), flat_load)
        self.assertEqual(r["verdict"], g.WRONG)
        self.assertNotIn("ratio", r)

    def test_size_metric(self):
        s = Script([1.0] * 3 + [1.01] * 0)
        r = g.gate_metric(metric(cls="size"), s, PLAN, random.Random(1), flat_load)
        self.assertEqual((r["verdict"], r["n_pairs"], r["deterministic"]), (g.PASS, 3, True))
        s = Script([1.03, 1.0, 1.0] + [1.05] * 3)
        r = g.gate_metric(metric(cls="size"), s, PLAN, random.Random(1), flat_load)
        self.assertEqual(r["verdict"], g.FAIL)  # max(C)/min(B), then confirmed


if __name__ == "__main__":
    unittest.main()
