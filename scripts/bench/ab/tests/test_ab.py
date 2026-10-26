"""The runner's process boundary and start-up refusals (ab_ratio_gates_PLAN
§9 items 6, 9, 13): sample parsing, harness check failures, the injection
guard. No builds."""

import io
import os
import subprocess
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..")))
import ab  # noqa: E402
import gatelib as g  # noqa: E402

M = g.Metric("wr_x", {"class": "io", "check": "c", "mode": "wr_x"}, {"io": 1.1})


def proc(rc, out="", err=""):
    return subprocess.CompletedProcess(["h"], rc, stdout=out, stderr=err)


class ParseSample(unittest.TestCase):
    def test_value_and_digest(self):
        v, d = ab.parse_sample("base", M, proc(0, "noise\ndigest a=1 b=x\n12.5\n"))
        self.assertEqual((v, d), (12.5, {"a": "1", "b": "x"}))

    def test_check_failure_exit_is_wrong_result(self):
        with self.assertRaises(g.WrongResult) as cm:
            ab.parse_sample("cand", M, proc(g.CHECK_FAILED_EXIT, "", "CHECK FAILED: rows 11 != 10\n"))
        self.assertIn("rows 11", str(cm.exception))

    def test_query_failure_is_an_error_not_a_wrong_result(self):
        with self.assertRaises(g.HarnessError) as cm:
            ab.parse_sample("base", M, proc(g.QUERY_FAILED_EXIT, "",
                                            "QUERY FAILED: SELECT ...: INTERNAL Error: x\n"))
        self.assertIn("INTERNAL Error", str(cm.exception))

    def test_crash_is_harness_error(self):
        with self.assertRaises(g.HarnessError):
            ab.parse_sample("cand", M, proc(101, "", "thread 'main' panicked\n"))

    def test_missing_digest_is_harness_error(self):
        with self.assertRaises(g.HarnessError):
            ab.parse_sample("base", M, proc(0, "12.5\n"))

    def test_malformed_digest_is_harness_error(self):
        with self.assertRaises(g.HarnessError):
            ab.parse_sample("base", M, proc(0, "digest a=1 oops\n12.5\n"))

    def test_garbage_value_is_harness_error(self):
        with self.assertRaises(g.HarnessError):
            ab.parse_sample("base", M, proc(0, "digest a=1\nnot a number\n"))


class StartupRefusals(unittest.TestCase):
    def run_main(self, argv, env=None):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.dict(os.environ, env or {}), redirect_stdout(out), redirect_stderr(err):
            code = ab.main(argv)
        return code, out.getvalue() + err.getvalue()

    def test_inject_env_refuses_a_gate_run(self):
        code, text = self.run_main(["HEAD~1", "HEAD"], {"NODAL_AB_INJECT": "wr_x:1.15"})
        self.assertEqual(code, g.EXIT_ERROR)
        self.assertIn("NODAL_AB_INJECT", text)

    def test_inject_flag_needs_self_test(self):
        code, text = self.run_main(["HEAD~1", "HEAD", "--inject", "wr_editor_save=1.15"])
        self.assertEqual(code, g.EXIT_ERROR)
        self.assertIn("--self-test", text)

    def test_high_load_refuses_without_force(self):
        with mock.patch.object(ab, "host_info", return_value={
                "nproc": 32, "cpu": "x", "kernel": "k", "governor": "performance",
                "boost": "1", "load_start": (20.0, 20.0, 20.0)}):
            code, text = self.run_main(["HEAD~1", "HEAD"])
        self.assertEqual(code, g.EXIT_ERROR)
        self.assertIn("refusing to start", text)

    def test_unknown_allow_metric(self):
        code, text = self.run_main(["HEAD~1", "HEAD", "--allow", "nope=x"])
        self.assertEqual(code, g.EXIT_ERROR)

    def test_an_unexpected_exception_is_an_error_not_a_fail(self):
        with mock.patch.object(ab, "gate", side_effect=ZeroDivisionError("x")):
            code, text = self.run_main(["HEAD~1", "HEAD"])
        self.assertEqual(code, g.EXIT_ERROR)
        self.assertIn("ZeroDivisionError", text)

    def test_list(self):
        code, text = self.run_main(["--list"])
        self.assertEqual(code, 0)
        self.assertIn("check:", text)


class CapsCheck(unittest.TestCase):
    """The per-arm start-up check of the DuckDB caps (perf_gate_footprint_PLAN D1)."""

    def arms(self):
        a = mock.Mock(binary="/bin/h")
        a.name = "base"
        return {"base": a}

    def run_check(self, stdout, rc=0):
        seen = []

        def fake_run(cmd, **kw):
            seen.append(kw["env"])
            return proc(rc, stdout)
        caps = g.Caps(4, 8, 12000)
        with mock.patch.object(ab.subprocess, "run", side_effect=fake_run), \
                redirect_stdout(io.StringIO()):
            ab.check_arm_caps(self.arms(), caps, [4, 1, 4])
        return seen

    def test_each_thread_value_is_checked_with_its_env(self):
        def fake_run(cmd, **kw):
            t = kw["env"]["SLURM_CPUS_ON_NODE"]
            self.assertEqual(kw["env"]["SLURM_MEM_PER_NODE"], "12000")
            return proc(0, f"threads {t}\nmemory_limit 8.9 GiB\n")
        with mock.patch.object(ab.subprocess, "run", side_effect=fake_run) as run, \
                redirect_stdout(io.StringIO()):
            ab.check_arm_caps(self.arms(), g.Caps(4, 8, 12000), [4, 1, 4])
        self.assertEqual(run.call_count, 2)

    def test_uncapped_arm_refuses(self):
        with self.assertRaises(g.ConfigError) as cm:
            self.run_check("threads 32\nmemory_limit 56.5 GiB\n")
        self.assertIn("did not take effect", str(cm.exception))

    def test_failing_binary_refuses(self):
        with self.assertRaises(g.ConfigError):
            self.run_check("", rc=101)

    def test_sampler_env_carries_the_metric_cap(self):
        arm = mock.Mock(binary="/bin/h", env={"NODAL_AB_SYNTH": "/f.db"})
        m = g.Metric("rd_x", {"class": "hot", "check": "c", "threads": 1}, {"hot": 1.05})
        got = {}

        def fake_run(cmd, **kw):
            got.update(kw["env"])
            return proc(0, "digest a=1\n1.0\n")
        with mock.patch.object(ab.subprocess, "run", side_effect=fake_run):
            ab.make_sampler(m, {"base": arm}, "quick", None, None, g.Caps(4, 8, 12000))("base")
        self.assertEqual((got["SLURM_CPUS_ON_NODE"], got["SLURM_MEM_PER_NODE"]), ("1", "12000"))
        self.assertEqual(got["NODAL_AB_SYNTH"], "/f.db")


class Baseline(unittest.TestCase):
    def setUp(self):
        import tempfile
        fd, self.path = tempfile.mkstemp(prefix="ab-baseline-")
        os.close(fd)
        with open(self.path, "w") as f:
            f.write("1111111111111111111111111111111111111111\n")
        self.addCleanup(os.remove, self.path)
        p = mock.patch.object(ab, "BASELINE", self.path)
        p.start()
        self.addCleanup(p.stop)

    def out(self, **kw):
        o = {"base": {"rev": "1" * 40, "commit": "1" * 40},
             "cand": {"rev": "HEAD", "commit": "2" * 40}, "forced": False, "aa": False,
             "injection": None, "release": True, "scale": "full", "metrics_filter": None,
             "gate_tool_clean": True, "results": []}
        o.update(kw)
        return o

    def advance(self, out, code):
        buf = io.StringIO()
        with redirect_stdout(buf):
            ab.advance_baseline(out, code)
        return ab.read_baseline(), buf.getvalue()

    def test_read(self):
        self.assertEqual(ab.read_baseline(), "1" * 40)

    def test_advances_after_a_passing_committed_run(self):
        rev, text = self.advance(self.out(), 0)
        self.assertEqual(rev, "2" * 40)
        self.assertIn("advanced", text)

    def test_never_after_a_failure_a_worktree_or_a_forced_run(self):
        for out, code in ((self.out(), 1), (self.out(), 2),
                          (self.out(cand={"rev": "WORKTREE", "commit": "3" * 40}), 0),
                          (self.out(forced=True), 0), (self.out(aa=True), 0),
                          (self.out(injection={"metric": "x", "spec": "wrong"}), 0),
                          (self.out(release=False), 0), (self.out(scale="quick"), 0),
                          (self.out(metrics_filter="rd_point*"), 0),
                          (self.out(base={"rev": "x", "commit": "4" * 40}), 0),
                          (self.out(gate_tool_clean=False), 0)):
            rev, text = self.advance(out, code)
            self.assertEqual(rev, "1" * 40)
            self.assertIn("not advanced", text)

    def test_an_allowed_regression_advances_and_is_listed_for_the_record(self):
        res = [{"metric": "wr_x", "verdict": g.ALLOWED, "verdict_unallowed": g.FAIL,
                "allowed_reason": "accepted: new index"}]
        rev, text = self.advance(self.out(results=res), 0)
        self.assertEqual(rev, "2" * 40)
        self.assertIn("accepted: new index", text)


if __name__ == "__main__":
    unittest.main()
