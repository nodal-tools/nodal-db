"""Cache key and lock seeding of scripts/lib/revbuild.py
(ab_ratio_gates_PLAN §9 items 5 and 11; critique M1-M3). Hermetic: a
throwaway git repository, no cargo."""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "..", "lib")))
import revbuild  # noqa: E402

LOCK = b"""version = 4

[[package]]
name = "duckdb"
version = "1.20000.0"

[[package]]
name = "tempfile"
version = "3.27.0"
"""


def write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data if isinstance(data, bytes) else data.encode())


class Repo:
    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix="revbuild-test-")
        self.run("init", "-q", "-b", "main")
        self.run("config", "user.email", "t@example.invalid")
        self.run("config", "user.name", "t")
        write(self.p(".gitignore"), "target/\n*.log\n")
        write(self.p("Cargo.toml"), "[workspace]\nmembers = [\"crates/*\"]\n")
        write(self.p("Cargo.lock"), LOCK)
        write(self.p("crates/core/src/lib.rs"), "pub fn f() {}\n")
        write(self.p("schema/a.sql"), "CREATE TABLE t (x INT);\n")
        write(self.p("README.md"), "readme\n")
        self.run("add", "-A")
        self.run("commit", "-q", "-m", "init")

    def p(self, rel):
        return os.path.join(self.dir, rel)

    def run(self, *args):
        subprocess.run(["git", "-C", self.dir, *args], check=True, capture_output=True)

    def close(self):
        shutil.rmtree(self.dir, ignore_errors=True)


class TreeIdTest(unittest.TestCase):
    def setUp(self):
        self.repo = Repo()
        self.addCleanup(self.repo.close)

    def wt(self):
        return revbuild.tree_id(self.repo.dir, "WORKTREE")

    def test_clean_worktree_is_stable(self):
        self.assertEqual(self.wt(), self.wt())
        self.assertTrue(self.wt().startswith("wt-"))

    def test_commit_tree_id_is_the_commit(self):
        head = revbuild.commit_id(self.repo.dir, "HEAD")
        self.assertEqual(revbuild.tree_id(self.repo.dir, "HEAD"), head)
        self.assertEqual(len(head), 12)

    def test_untracked_file_under_crates_changes_it(self):
        before = self.wt()
        write(self.repo.p("crates/core/src/new_mod.rs"), "pub fn g() {}\n")
        after = self.wt()
        self.assertNotEqual(before, after)
        # ... and its content too, not just its name.
        write(self.repo.p("crates/core/src/new_mod.rs"), "pub fn h() {}\n")
        self.assertNotEqual(after, self.wt())

    def test_root_manifest_and_lock_change_it(self):
        base = self.wt()
        write(self.repo.p("Cargo.toml"), "[workspace]\nmembers = [\"crates/*\"]\n# bump\n")
        self.assertNotEqual(base, self.wt())
        write(self.repo.p("Cargo.toml"), "[workspace]\nmembers = [\"crates/*\"]\n")
        self.assertEqual(base, self.wt())
        write(self.repo.p("Cargo.lock"), LOCK.replace(b"1.20000.0", b"1.20001.0"))
        self.assertNotEqual(base, self.wt())

    def test_tracked_edit_and_schema_change_it(self):
        base = self.wt()
        write(self.repo.p("schema/a.sql"), "CREATE TABLE t (x BIGINT);\n")
        self.assertNotEqual(base, self.wt())

    def test_diff_drivers_do_not_hide_an_edit(self):
        # A textconv filter (or an external diff) in the user's git config
        # must not reach the key: two different edits, two different ids.
        write(self.repo.p(".git/info/attributes"), "*.rs diff=blind\n")
        self.repo.run("config", "diff.blind.textconv", "sh -c 'echo same' --")
        self.repo.run("config", "diff.external", "true")
        write(self.repo.p("crates/core/src/lib.rs"), "pub fn f() { 1; }\n")
        one = self.wt()
        write(self.repo.p("crates/core/src/lib.rs"), "pub fn f() { 2; }\n")
        self.assertNotEqual(one, self.wt())

    def test_ignored_and_outside_files_do_not(self):
        base = self.wt()
        write(self.repo.p("crates/core/target/release/junk"), "x")
        write(self.repo.p("crates/core/build.log"), "x")
        write(self.repo.p("README.md"), "changed outside the core paths\n")
        write(self.repo.p("notes.txt"), "untracked outside")
        self.assertEqual(base, self.wt())

    def test_worktree_extract_copies_exactly_the_hashed_files(self):
        write(self.repo.p("crates/core/src/new_mod.rs"), "pub fn g() {}\n")
        write(self.repo.p("crates/core/target/junk"), "x")
        os.remove(self.repo.p("schema/a.sql"))  # tracked, deleted
        dest = tempfile.mkdtemp(prefix="revbuild-extract-")
        self.addCleanup(shutil.rmtree, dest, True)
        revbuild.extract(self.repo.dir, "WORKTREE", dest)
        got = sorted(
            os.path.relpath(os.path.join(r, f), dest)
            for r, _, fs in os.walk(dest) for f in fs
        )
        self.assertEqual(got, ["Cargo.lock", "Cargo.toml", "crates/core/src/lib.rs",
                               "crates/core/src/new_mod.rs"])

    def test_commit_extract(self):
        dest = tempfile.mkdtemp(prefix="revbuild-extract-")
        self.addCleanup(shutil.rmtree, dest, True)
        revbuild.extract(self.repo.dir, "HEAD", dest)
        self.assertTrue(os.path.exists(os.path.join(dest, "schema", "a.sql")))


class BuildKeyTest(unittest.TestCase):
    ARGS = ("abc123", "deadbeef", ["lookup_const"], "rustc 1.90.0\nhost: x\n", LOCK)

    def key(self, **kw):
        tree, tool, feats, rustc, lock = self.ARGS
        args = dict(tree=tree, tool_hash=tool, features=feats, rustc=rustc,
                    lock_bytes=lock, salt="", env="")
        args.update(kw)
        return revbuild.build_key(**args)

    def test_stable(self):
        self.assertEqual(self.key(), self.key())

    def test_feature_set_changes_it(self):
        self.assertNotEqual(self.key(), self.key(features=[]))
        self.assertNotEqual(self.key(features=[]), self.key(features=None))
        self.assertEqual(self.key(features=["a", "b"]), self.key(features=["b", "a"]))

    def test_rustc_changes_it(self):
        self.assertNotEqual(self.key(), self.key(rustc="rustc 1.91.0\nhost: x\n"))

    def test_lock_changes_it(self):
        self.assertNotEqual(self.key(), self.key(lock_bytes=LOCK + b"\n# x\n"))

    def test_build_env_changes_it(self):
        self.assertNotEqual(self.key(), self.key(env="RUSTFLAGS=-Ctarget-cpu=native"))
        env = revbuild.build_env({"RUSTFLAGS": "-Cx", "CARGO_PROFILE_RELEASE_LTO": "fat",
                                  "CC": "clang", "HOME": "/h", "CARGO_TARGET_DIR": "/t"})
        self.assertEqual(env, "CARGO_PROFILE_RELEASE_LTO=fat\0CC=clang\0RUSTFLAGS=-Cx")
        self.assertEqual(revbuild.build_env({"HOME": "/h"}), "")

    def test_aa_salts_give_two_keys_for_one_commit(self):
        a, b = self.key(salt="aa1a"), self.key(salt="aa1b")
        self.assertNotEqual(a, b)
        self.assertNotEqual(a, self.key())

    def test_tree_and_tool_change_it(self):
        self.assertNotEqual(self.key(), self.key(tree="abc124"))
        self.assertNotEqual(self.key(), self.key(tool_hash="deadbeee"))


class LockSeedTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="revbuild-lock-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.tool_src = os.path.join(self.tmp, "toolsrc")
        write(os.path.join(self.tool_src, "Cargo.toml"), "[package]\nname='t'\n[workspace]\n")
        write(os.path.join(self.tool_src, "src", "main.rs"), "fn main() {}\n")
        # A stale lock and build output next to the tool source never travel.
        write(os.path.join(self.tool_src, "Cargo.lock"), b"stale")
        write(os.path.join(self.tool_src, "target", "x"), b"x")
        self.tree = os.path.join(self.tmp, "tree")
        write(os.path.join(self.tree, "Cargo.lock"), LOCK)

    def test_tool_lock_is_the_root_lock(self):
        tool = revbuild.install_tool(self.tree, self.tool_src, "scripts/tool")
        with open(os.path.join(tool, "Cargo.lock"), "rb") as f:
            self.assertEqual(f.read(), LOCK)
        self.assertFalse(os.path.exists(os.path.join(tool, "target")))
        self.assertEqual(revbuild.lock_versions(LOCK),
                         {("duckdb", "1.20000.0"), ("tempfile", "3.27.0")})

    def test_no_root_lock_no_seed(self):
        os.remove(os.path.join(self.tree, "Cargo.lock"))
        tool = revbuild.install_tool(self.tree, self.tool_src, "scripts/tool")
        self.assertFalse(os.path.exists(os.path.join(tool, "Cargo.lock")))

    def test_changed_root_lock_changes_the_key(self):
        keys = []
        for lock in (LOCK, LOCK.replace(b"3.27.0", b"3.28.0")):
            write(os.path.join(self.tree, "Cargo.lock"), lock)
            tool = revbuild.install_tool(self.tree, self.tool_src, "scripts/tool")
            with open(os.path.join(tool, "Cargo.lock"), "rb") as f:
                keys.append(revbuild.build_key("t", "h", [], "rustc", f.read()))
        self.assertNotEqual(keys[0], keys[1])

    def test_tool_hash_ignores_lock_and_target(self):
        h = revbuild.dir_hash(self.tool_src)
        write(os.path.join(self.tool_src, "Cargo.lock"), b"other")
        write(os.path.join(self.tool_src, "target", "y"), b"y")
        self.assertEqual(h, revbuild.dir_hash(self.tool_src))
        write(os.path.join(self.tool_src, "src", "main.rs"), "fn main() { }\n")
        self.assertNotEqual(h, revbuild.dir_hash(self.tool_src))


if __name__ == "__main__":
    unittest.main()
