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


if __name__ == "__main__":
    unittest.main()
