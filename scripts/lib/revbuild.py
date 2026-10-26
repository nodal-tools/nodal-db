"""Build a tool crate against the core of a git revision (shared by
`scripts/check/write_equivalence/check.py` and the perf gate
`scripts/bench/ab/ab.py`).

A *tool crate* is a standalone Cargo workspace (its own `[workspace]`)
that depends on `crates/nodal-db-core` by a relative path. To build it
against revision REV, the revision's source is extracted into a directory,
the tool directory of the *current* checkout is copied into that tree at
the same relative place (so every arm builds the same tool source), and
the tool is built there in release mode.

The cache key of a built binary covers everything that goes into it
(ab_ratio_gates_PLAN §3.2, critique M1-M3):

- the **tree id**: the commit id, or for the literal `WORKTREE` the HEAD
  commit plus a hash of `git diff HEAD` over the core paths *and* of the
  content of every untracked, non-ignored file under them. The WORKTREE
  extract copies exactly the files that are hashed (tracked plus
  untracked non-ignored), so what is built is what is keyed;
- the **tool hash** (the tool directory's files, without `target/` and
  `Cargo.lock`);
- the **cargo feature set**;
- the output of **`rustc -Vv`**;
- the **resolved tool lock**: the tool crate's `Cargo.lock` is seeded
  from the revision's root `Cargo.lock` before anything resolves, so the
  tool builds with the dependency versions the revision pins, and the
  lock as resolved (seeded entries plus tool-only ones) is hashed;
- an optional **salt** (the A/A calibration builds one commit twice:
  two salts give two extract directories, two binaries, two keys);
- the **build environment** that changes code generation without touching
  any file (`RUSTFLAGS` and friends, `CARGO_PROFILE_*`, the C/C++ compiler
  and flags the bundled DuckDB builds with), when any is set.
  (Cargo config files outside the tree, e.g. `~/.cargo/config.toml`, are
  not keyed: clear `--work` after changing one.)

Stdlib only.
"""

import hashlib
import os
import shutil
import subprocess
import time

# The revision paths a tool build depends on: the root manifest and lock,
# the crates (the core and its path dependencies) and the schema DDL the
# core embeds.
CORE_PATHS = ("Cargo.toml", "Cargo.lock", "crates", "schema")

# Never copied into an extract or hashed as tool source.
SKIP_DIRS = ("target", "__pycache__")


def git(repo, *args, text=True):
    out = subprocess.run(
        ["git", "-C", repo, *args], check=True, capture_output=True, text=text
    ).stdout
    return out.strip() if text else out


def repo_root(start):
    return git(start, "rev-parse", "--show-toplevel")


def commit_id(repo, rev, short=12):
    """The commit id of `rev` (`short` hex digits; 0 = full)."""
    full = git(repo, "rev-parse", rev + "^{commit}")
    return full[:short] if short else full


def worktree_files(repo, paths=CORE_PATHS):
    """Relative paths of the working tree's files under `paths`: tracked
    files that still exist plus untracked files that are not ignored
    (sorted, `/`-separated)."""
    tracked = git(repo, "ls-files", "-z", "--cached", "--", *paths, text=False)
    others = git(
        repo, "ls-files", "-z", "--others", "--exclude-standard", "--", *paths, text=False
    )
    names = set()
    for blob in (tracked, others):
        for raw in blob.split(b"\0"):
            if raw:
                names.add(raw.decode())
    return sorted(n for n in names if os.path.isfile(os.path.join(repo, n)))


def untracked_files(repo, paths=CORE_PATHS):
    blob = git(repo, "ls-files", "-z", "--others", "--exclude-standard", "--", *paths, text=False)
    return sorted(raw.decode() for raw in blob.split(b"\0") if raw)


def tree_id(repo, rev, paths=CORE_PATHS):
    """A cache key for the revision's source under `paths`: the commit id,
    or for `WORKTREE` the HEAD commit plus a hash of the uncommitted
    changes (tracked diff and untracked, non-ignored files)."""
    if rev != "WORKTREE":
        return commit_id(repo, rev)
    h = hashlib.sha1()
    # No external diff driver or textconv filter: the key must see the raw
    # bytes, whatever the user's git configuration.
    h.update(git(repo, "diff", "--no-ext-diff", "--no-textconv", "HEAD", "--binary", "--",
                 *paths, text=False))
    for name in untracked_files(repo, paths):
        h.update(b"\0untracked\0" + name.encode() + b"\0")
        with open(os.path.join(repo, name), "rb") as f:
            h.update(f.read())
    return "wt-" + commit_id(repo, "HEAD") + "-" + h.hexdigest()[:12]


def dir_hash(path, skip_files=("Cargo.lock",)):
    """A hash of a directory's files (names and content), without
    `target/`, `__pycache__/` and `skip_files`."""
    h = hashlib.sha1()
    for root, dirs, files in os.walk(path):
        dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)
        for name in sorted(files):
            if name in skip_files or name.endswith(".pyc"):
                continue
            p = os.path.join(root, name)
            h.update(os.path.relpath(p, path).encode() + b"\0")
            with open(p, "rb") as f:
                h.update(f.read() + b"\0")
    return h.hexdigest()[:8]


def extract(repo, rev, dest, paths=CORE_PATHS):
    """Write the revision's source into `dest` (replaced): `git archive`
    of a commit, or the working tree's `worktree_files` for WORKTREE."""
    shutil.rmtree(dest, ignore_errors=True)
    os.makedirs(dest)
    if rev == "WORKTREE":
        for name in worktree_files(repo, paths):
            out = os.path.join(dest, name)
            os.makedirs(os.path.dirname(out), exist_ok=True)
            shutil.copy2(os.path.join(repo, name), out)
    else:
        archive = git(repo, "archive", commit_id(repo, rev, 0), text=False)
        subprocess.run(["tar", "-x", "-C", dest], input=archive, check=True)


def install_tool(tree, tool_dir, tool_rel):
    """Copy the tool directory into the extracted `tree` at `tool_rel`
    (replacing the revision's own copy, if any) and seed its `Cargo.lock`
    from the tree's root `Cargo.lock` (critique M2). Returns the tool path
    inside the tree."""
    tool = os.path.join(tree, tool_rel)
    shutil.rmtree(tool, ignore_errors=True)
    shutil.copytree(
        tool_dir, tool, ignore=shutil.ignore_patterns(*SKIP_DIRS, "Cargo.lock", "*.pyc")
    )
    seed_lock(tree, tool)
    return tool


def seed_lock(tree, tool):
    """Copy the tree's root `Cargo.lock` to the tool crate (no-op when the
    revision has none)."""
    root_lock = os.path.join(tree, "Cargo.lock")
    if os.path.exists(root_lock):
        shutil.copy2(root_lock, os.path.join(tool, "Cargo.lock"))


def resolve_lock(tool, env=None):
    """Let cargo resolve the tool's dependency graph against its seeded
    lock (adding tool-only entries, dropping unused ones; seeded versions
    stay) and return the resolved `Cargo.lock` bytes."""
    subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--quiet"],
        cwd=tool, check=True, stdout=subprocess.DEVNULL, env=env,
    )
    with open(os.path.join(tool, "Cargo.lock"), "rb") as f:
        return f.read()


def rustc_vv(cwd=None):
    return subprocess.run(
        ["rustc", "-Vv"], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout


def lock_versions(lock_bytes):
    """{(name, version)} of a Cargo.lock (for tests and diagnostics)."""
    out = set()
    name = None
    for line in lock_bytes.decode().splitlines():
        if line.startswith("name = "):
            name = line.split('"')[1]
        elif line.startswith("version = ") and name is not None:
            out.add((name, line.split('"')[1]))
            name = None
    return out


# Environment variables that change the built binary without changing a
# file (exact names, and prefixes ending in `_`).
BUILD_ENV = ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_RUSTFLAGS",
             "CARGO_BUILD_TARGET", "RUSTC", "CARGO_PROFILE_", "CC", "CXX", "CFLAGS",
             "CXXFLAGS", "LDFLAGS", "DUCKDB_")


def build_env(environ=None):
    """The `BUILD_ENV` part of `environ` (default: os.environ) as one
    string; empty when none is set."""
    environ = os.environ if environ is None else environ
    keep = sorted(
        (k, v) for k, v in environ.items()
        if any(k.startswith(p) if p.endswith("_") else k == p for p in BUILD_ENV)
    )
    return "\0".join(f"{k}={v}" for k, v in keep)


def build_key(tree, tool_hash, features, rustc, lock_bytes, salt="", env=None):
    """The binary cache key (module docs). `features` is None for the
    crate's default features, else the exact list; `env` is the
    `build_env()` string (default: from os.environ)."""
    h = hashlib.sha1()
    feat = "<default>" if features is None else ",".join(sorted(features))
    for part in (feat, rustc, salt):
        h.update(part.encode() + b"\0")
    h.update(lock_bytes)
    env = build_env() if env is None else env
    if env:  # unset: the key of a plain build stays what it was
        h.update(b"\0env\0" + env.encode())
    return f"{tree}-w{tool_hash}-{h.hexdigest()[:10]}"


BUILD_JOBS = "8"


def cargo_env(target_dir, environ=None, sccache=None):
    """The environment of a tool build (perf_gate_footprint_PLAN D7): at
    most BUILD_JOBS jobs (cargo passes it to the cc crate, so it also caps
    the bundled DuckDB's C++ compile, ~1-1.5 GB per job), and the sccache
    compiler cache when it is installed (as scripts/dev/agent-env.sh sets
    it up), so a per-arm DuckDB compile takes ~20 s instead of minutes. A
    value already in the caller's environment wins. Applied to the build
    only: the cache key (`build_env`) reads the caller's environment, so a
    compiler wrapper added here does not change keys."""
    env = dict(os.environ if environ is None else environ)
    env["CARGO_TARGET_DIR"] = target_dir
    env.setdefault("CARGO_BUILD_JOBS", BUILD_JOBS)
    if sccache is None:
        sccache = shutil.which("sccache")
    if sccache and "RUSTC_WRAPPER" not in env and "CC" not in env and "CXX" not in env:
        env["RUSTC_WRAPPER"] = sccache
        env["CC"] = f"{sccache} cc"
        env["CXX"] = f"{sccache} c++"
        # DuckDB's sources are unpacked into each target dir: list the
        # folders as base dirs so cache keys match across them. The server
        # reads the list once, when it starts, so it is the same list as
        # agent-env.sh's (whoever starts the server, every job hits).
        env.setdefault("SCCACHE_BASEDIRS", ":".join(sccache_basedirs(target_dir)))
    return env


def sccache_basedirs(target_dir):
    """agent-env.sh's base-dir list (the repo's target, target-agent-1..8,
    target-ab/target), plus `target_dir` when it is elsewhere."""
    target_dir = os.path.abspath(target_dir)
    probe = target_dir
    while not os.path.isdir(probe) and os.path.dirname(probe) != probe:
        probe = os.path.dirname(probe)
    try:
        root = repo_root(probe)
    except (subprocess.CalledProcessError, OSError):
        return [target_dir]
    dirs = [os.path.join(root, "target")]
    dirs += [os.path.join(root, f"target-agent-{i}") for i in range(1, 9)]
    dirs.append(os.path.join(root, "target-ab", "target"))
    if target_dir not in dirs:
        dirs.append(target_dir)
    return dirs


def cargo_build(tool, target_dir, bin_name, features=None, package=None, quiet=True):
    """`cargo build --release` of the crate (or workspace `package`) at
    `tool` into `target_dir` (environment: `cargo_env`); returns the built
    binary's path."""
    cmd = ["cargo", "build", "--release"]
    if quiet:
        cmd.append("--quiet")
    if package:
        cmd += ["-p", package]
    if features is not None:
        cmd += ["--no-default-features"]
        if features:
            cmd += ["--features", ",".join(sorted(features))]
    subprocess.run(cmd, cwd=tool, check=True, env=cargo_env(target_dir))
    return os.path.join(target_dir, "release", bin_name)


def build_tool(repo, rev, tool_dir, tool_rel, work, bin_name, features=None,
               salt="", use_cache=True, log=print):
    """Extract `rev`, install the tool, resolve its lock, build (or reuse a
    cached binary). Returns (binary path, key, extracted tree)."""
    tree = tree_id(repo, rev)
    tool_hash = dir_hash(tool_dir)
    src = os.path.join(work, "src", f"{tree}-w{tool_hash}" + (f"-s{salt}" if salt else ""))
    extract(repo, rev, src)
    tool = install_tool(src, tool_dir, tool_rel)
    lock = resolve_lock(tool)
    key = build_key(tree, tool_hash, features, rustc_vv(tool), lock, salt)
    binary = os.path.join(work, "bin", f"{bin_name}-{key}")
    if use_cache and os.path.exists(binary):
        log(f"{rev} ({key}): cached binary")
        return binary, key, src
    t0 = time.time()
    log(f"{rev} ({key}): building ...")
    built = cargo_build(tool, os.path.join(work, "target"), bin_name, features)
    os.makedirs(os.path.dirname(binary), exist_ok=True)
    shutil.copy2(built, binary + ".tmp")
    os.replace(binary + ".tmp", binary)
    log(f"{rev} ({key}): built in {time.time() - t0:.0f} s")
    return binary, key, src
