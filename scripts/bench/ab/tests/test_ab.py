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

    def test_crash_is_harness_error(self):
        with self.assertRaises(g.HarnessError):
            ab.parse_sample("cand", M, proc(101, "", "thread 'main' panicked\n"))

    def test_missing_digest_is_harness_error(self):
        with self.assertRaises(g.HarnessError):
            ab.parse_sample("base", M, proc(0, "12.5\n"))

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

    def test_list(self):
        code, text = self.run_main(["--list"])
        self.assertEqual(code, 0)
        self.assertIn("check:", text)


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
        o = {"cand": {"rev": "HEAD", "commit": "2" * 40}, "forced": False, "aa": False,
             "injection": None}
        o.update(kw)
        return o

    def advance(self, out, code):
        buf = io.StringIO()
        with redirect_stdout(buf):
            ab.advance_baseline(out, code, None)
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
                          (self.out(injection={"metric": "x", "spec": "wrong"}), 0)):
            rev, text = self.advance(out, code)
            self.assertEqual(rev, "1" * 40)
            self.assertIn("not advanced", text)


if __name__ == "__main__":
    unittest.main()
