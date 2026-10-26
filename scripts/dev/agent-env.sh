# Source this before building or testing in an agent session or a worktree:
#   source scripts/dev/agent-env.sh [SLOT]
# It bounds one job to a share of the machine and makes the bundled DuckDB
# C++ compile hit a shared compiler cache instead of recompiling (~4.5 min
# cold → ~20 s cached).
#
# SLOT (1-8, default 1) picks the build folder target-agent-SLOT under the
# repo root. Use one slot per concurrent job: sharing one target dir between
# worktrees can reuse a stale workspace crate (cargo does not key path
# crates by their location). The slots are fixed so the cache's base-dir
# list (read once, when the sccache server starts) always covers them.

# The MAIN checkout's root, also from inside a linked worktree, so every job
# shares the same slot folders and the cache's base-dir list matches.
_common="$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null)"
if [ -n "$_common" ]; then _root="$(dirname "$_common")"; else _root="$(pwd)"; fi
_slot="${1:-1}"
case "$_slot" in [1-8]) ;; *) echo "agent-env: SLOT must be 1-8" >&2; return 1 2>/dev/null || exit 1;; esac

# The DuckDB and test-thread caps of cargo test / run / bench come from the
# repo's .cargo/config.toml [env] (CONTRIBUTING "Machine budget"), for
# everyone; this script adds the build side.
# Budget: ~8 cores per job. Cargo passes this to the cc crate (NUM_JOBS), so
# it also caps DuckDB's C++ compile, whose peak is roughly 1-1.5 GB per job.
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-8}"

if command -v sccache >/dev/null 2>&1; then
  export RUSTC_WRAPPER=sccache
  export CC="sccache cc"
  export CXX="sccache c++"
  # DuckDB's sources are unpacked into each target dir, so absolute paths
  # differ per folder; list the folders as base dirs so the cache keys match.
  # The sccache server reads this when it starts; the list is the same for
  # every slot, so one server serves all jobs.
  _dirs="$_root/target"
  for i in 1 2 3 4 5 6 7 8; do _dirs="$_dirs:$_root/target-agent-$i"; done
  export SCCACHE_BASEDIRS="$_dirs:$_root/target-ab/target"
fi

export CARGO_TARGET_DIR="$_root/target-agent-$_slot"
unset _root _common _slot _dirs i
