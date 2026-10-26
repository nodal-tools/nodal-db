# Contributing

Stub — grows with the project. What is binding already:

## Spec workflow
All lasting work products (plans, findings, critiques, handoffs) go in
`specs/` under the conventions in `specs/README.md`.

## Dev setup
- **Python 3.11+** (the DuckDB 2.0 Python client requires it; the skin's
  wheels are abi3-py311) and **DuckDB 2.0** (`duckdb==2.0.0` in
  `dev-requirements.txt` / `docs-requirements.txt`, matching the engine the
  core bundles). `just venv` and `just docs-venv` create the two venvs.
- Lint with ruff before committing.
- **SQL lambdas: use `lambda x: ...` syntax only.** The single-arrow form
  `x -> ...` is disabled by default in DuckDB 2.0 and removed in 2.1.

## DuckDB upgrades
A DuckDB bump is a matched bump: the bundled engine (`duckdb` in the
root `Cargo.toml`) and the Python pins (`dev-requirements.txt`,
`docs-requirements.txt`) move together; when the bump changes the arrow
version duckdb-rs re-exports, so do the `arrow` and `arrow-pyarrow` pins
(and through them `pyo3`; see the comments in `Cargo.toml`). The engine
behaviours the code relies on (storage format, `array_id` parity, index
access paths, index key order, the caching-operators workaround, axis
rendering in read paths) are listed with their guards in
[`docs/dev/engine_rules.md`](docs/dev/engine_rules.md); read it before
changing the schema, core SQL, indexes, sequences, storage or compaction,
and re-check it after a bump. After one:
- Run `just perf-gate PRE_BUMP_MAIN WORKTREE` before merging (a bump is
  the canonical perf-sensitive change; both arms compile their own
  DuckDB), then `just full`. Besides the slow tests and the release perf
  gate, it runs the index-scan guards (`array_id_index_scan`,
  `entity_name_index_scan`, `point_read_index_scan`) on a release-built
  engine. They check optimizer behaviour, not a documented contract, and
  fail loudly when it changes; their failure messages say what to
  re-evaluate.
- If `duckdb_env_caps` fails, the new DuckDB no longer sizes its thread
  pool and memory limit from `SLURM_CPUS_ON_NODE` / `SLURM_MEM_PER_NODE`:
  the `cargo test` caps and the perf gate's caps (which refuses to start
  then) need another mechanism ("Machine budget" below).
- If `duckdb_dict_cache_guard`'s tripwire fails ("the DuckDB dict-cache
  bug appears fixed"), remove the `debug_enable_caching_operators`
  workaround as described in engine_rules.md §5. If every core open
  fails on that setting, the new DuckDB renamed or dropped it: check
  whether the bug is fixed before deciding what replaces it. The
  dict-cache data guard in `point_read_index_scan` fails with it (its
  positive control no longer raises): re-evaluate engine_rules.md §6
  then.
- If the release notes mention Unicode or utf8proc updates (case
  mapping), compact the dev files after the bump. The `lower(name)`
  indexes (`ix_entity_lname`, the `uq_*_name` unique indexes over
  `cur_name`) persist keys computed by the engine that built them, so a
  changed case mapping could make an index lookup miss a row a scan
  would find; compaction rebuilds every index from the DDL. A changed
  case mapping also fails `duckdb_lower_guard` (engine_rules.md §8),
  which pins where DuckDB's `lower()` and the Rust name fold differ; it
  fails the same way after a Rust toolchain bump that changes
  `to_lowercase`.

## Testing rules (binding once code exists)
Adopted from flextool-engine's hard-won lessons:

1. **Tests build their databases from JSON/schema fixtures — never read a
   checked-in binary DB.** Checked-in `.duckdb`/`.sqlite` files lag the
   schema and silently under-cover. Provide a `json_to_db`-style fixture
   builder from day one.
2. **Fixture files are append-only for agents.** Provide an explicit
   extension mechanism (an `extend_*` hook or separate overlay files) so
   agents add cases without rewriting curated fixtures.
3. **Parity tests gate rewrites.** When a component is reimplemented
   (e.g. a faster path replacing a reference path), keep the reference as an
   oracle and gate the new path with equality/tolerance parity tests.

## Machine budget
DuckDB sizes every instance to the machine by default: as many threads as
cores and a memory limit of 80 % of RAM, per instance. A test binary runs
many tests at once, each with its own instance, so an unbounded `cargo
test` puts hundreds of runnable threads on every core in bursts. The repo
bounds this in `.cargo/config.toml [env]`, for every `cargo test`, `cargo
run` and `cargo bench` started in it:

| variable | value | effect |
|---|---|---|
| `SLURM_CPUS_ON_NODE` | 4 | DuckDB threads per instance (DuckDB reads it when it creates an instance) |
| `SLURM_MEM_PER_NODE` | 8000 | DuckDB memory_limit = 80 % of 8 000 MB per instance |
| `RUST_TEST_THREADS` | 8 | at most 8 tests at once per test binary |

A value in your environment wins: `SLURM_CPUS_ON_NODE=8 cargo test`,
`RUST_TEST_THREADS=32 cargo test`; an empty `SLURM_*` value
(`SLURM_CPUS_ON_NODE= SLURM_MEM_PER_NODE=`) runs DuckDB uncapped. Keep in
mind that a `cargo run -p nodal-db-cli -- import …` and the criterion
benches (`just bench`) run at 4 DuckDB threads too; set the variables
empty to measure the engine as shipped. 4 rather than 1 keeps DuckDB's
parallel operators running in tests (the dict-cache bug and row-order
nondeterminism live there). `tests/duckdb_env_caps.rs` fails if a DuckDB
release stops reading the variables. `just full` runs its release-build
guard step (the index-scan guards and the dict-cache tripwire) uncapped,
so the engine's production thread count is checked too, builds at 8 cargo
jobs unless `CARGO_BUILD_JOBS` is set, and runs inside a systemd user
scope with `MemoryMax=14G` and no swap (`NODAL_FULL_MEMORY_MAX=20G` or
`=off` to change it; without `systemd-run --user` it runs unbounded and
says so). Agents additionally source `scripts/dev/agent-env.sh SLOT` (8
build jobs, own target dir, shared compiler cache).

## Write-path equivalence check
A change to the write path that is meant to leave the stored data alone
(a refactor, a performance change) is checked against the code before it
with `scripts/check/write_equivalence/check.py`, the randomized
counterpart of the write-path golden (`tests/write_path_golden.rs`):

```
python3 scripts/check/write_equivalence/check.py OLD_REV [NEW_REV|WORKTREE] \
    [--seeds 1,2,3] [--commits 100] [--big]
```

It builds the workload crate next to it against the core of each
revision (release, one shared target directory, binaries cached per
commit and workload), runs the same seeded random API workload with both (segments of
every tier and value kind under several class and alternative spellings,
structural creates, value deletes with misses and duplicates, erasure,
direct `history` writes) and compares every commit's report or error and
every row of every table, apart from the commit timestamps and the
`db_identity` row. It exits non-zero on the first difference and prints
it. The first build compiles the bundled DuckDB (minutes); a run of three
seeds at the default 100 commits takes a few minutes more. `--big` adds
rounds that cross the write path's chunk and round thresholds (slower).
The workload uses API present since `73e70d6`.

Two modes compare a core that skips unchanged writes with one that
versions them: `--fresh-values` makes every written cell differ from its
stored value and makes no call that could change nothing, so the outputs
must still be byte-identical (the changed path is untouched);
`--logical` runs a repeat-heavy workload and compares only the logical
state (current values after every call, as-of values at every call
boundary, reports without commit ids and counts), and requires the new
arm to have no more history rows and no audit finding the old arm lacks.
An as-of row may be a content-less stub in one arm only (the arms version
different spans): an old-arm stub matches any content, a new-arm stub
only when the old arm shows at most one distinct content across that
stub's whole span and a stub somewhere in it. See `check.py`.

## Performance gates
Performance is judged by **paired A/B
ratios on one host**, never against absolute milliseconds recorded on
another machine. The millisecond numbers in `benches/RESULTS.md` and the
criterion benches (`just bench`) are information only. The gate builds the code before your change
(BASE) and with it (CAND), runs both on the same fixtures, interleaved, and
compares their times.

**When to run it.** Before merging a branch that touches the write path,
the read macros, the schema or its indexes, SQL in the core, compaction or
extract, or bumps DuckDB (the canonical perf-sensitive change: BASE =
pre-bump main):

```
just perf-gate                      # BASE = merge-base with main, CAND = working tree, --quick
just perf-gate BASE CAND --quick    # any two revisions (CAND may be WORKTREE)
just perf-gate --metrics 'wr_*'     # a subset (comma-separated globs; --list shows all)
```

The first run compiles the bundled DuckDB once (minutes; at most 8 cargo
jobs, and ~20 s per arm when `sccache` is installed); arms, fixtures and
binaries are cached under `target-ab/` (gitignored). `--full` uses the
criterion gate's fixture sizes instead of a tenth of them.

**The release gate** (`just full`) runs `ab.py --release
--advance-baseline`: BASE is the commit in `scripts/bench/ab/BASELINE` (the
last passing release run), CAND is HEAD (a clean tree; with uncommitted
changes, the working tree, and nothing advances). Each metric runs at its
`release_scale` (`metrics.toml`): quick for most, full for the ones whose
cost depends on file size (the dense, model-load, scalars-loader and long
scan reads, the CHECKPOINT cycles, the import, compaction and extract and
their sizes); the axis-rendering metrics run quick only, which detects
their regression class at 1.3-5.6x. The size metrics are gated from the
file sizes the compaction and extract samples already produce. When every
metric passes (exit 0; an `--allow`ed metric counts, and its reason goes
into the block), BASELINE moves to HEAD: commit it together with the run's
block in `crates/nodal-db-core/benches/RESULTS.md` ("A/B gate runs"), so
the chain of release-to-release ratios stays visible. Only a complete run
advances it: the release suite (or `--full`), no `--metrics` subset, BASE
= BASELINE, and no uncommitted change in the gate itself
(`scripts/bench/ab/`, `scripts/lib/`). In a
release run, a metric whose API the baseline has and HEAD lacks is an
ERROR rather than SKIPPED (an API removal, or a stale probe in
`capabilities.toml`). Each pre-merge gate tolerates
a few percent; the release gate is what catches several of them adding up.

**How a metric is judged.** Each metric (`scripts/bench/ab/metrics.toml`)
is one harness mode, run in its own process per sample. After a warm-up
sample per arm, pairs of samples run in counterbalanced order, and the
statistic is the median of the per-pair ratios CAND / BASE with its exact
order-statistic 95 % interval. A metric stops at the first pre-fixed look
(6, 11, 21 pairs) that decides it: PASS when the interval's upper bound is
at most the metric's threshold, FAIL when its lower bound is above it.
Thresholds: 1.05 for in-process reads and lookups, 1.10 for anything with
a commit, an open or a CHECKPOINT, 1.02 for file sizes. A speed-up is
reported but never gates. A FAIL is re-measured by an independent run and
stays a FAIL only if that run fails too. Every sample also checks its
result (row counts, read-back of written keys) and prints a digest of the
logical result; the two arms' digests must be equal.

**Verdicts and exit status** (the table and `target-ab/runs/*/results.json`):

| exit | verdict | meaning |
|---|---|---|
| 0 | PASS / ALLOWED / SKIPPED | no regression above the thresholds |
| 1 | FAIL | a confirmed regression |
| 2 | INCONCLUSIVE / FLAKY / LOAD-DRIFT | undecided after 21 pairs; a FAIL the rerun did not confirm; the host load changed > 2x twice. Look, rerun on a quieter host |
| 3 | ERROR | a build or harness error, or a refused start |
| 4 | WRONG RESULT | a result check failed or the arms' results differ; overrides everything |

`SKIPPED (base lacks X)`: the metric needs an API one of the revisions does
not have (`scripts/bench/ab/capabilities.toml`, detected in the source).

**Accepting a regression.** `--allow METRIC="reason"` turns that metric's
FAIL (or INCONCLUSIVE, FLAKY, LOAD-DRIFT) into ALLOWED with the reason in
the table and `results.json`; copy the reason into the PR. `--allow` never
covers WRONG RESULT.

**The gate's footprint.** Every sample runs with DuckDB capped at 4
threads (1 for `rd_long_scan_17pct`, whose few-ms scan is bimodal when
threaded) and a memory_limit of 8.9 GiB, every fixture build at 8 threads
(`[settings]` in `metrics.toml`): the runner sets `SLURM_CPUS_ON_NODE` /
`SLURM_MEM_PER_NODE`, which DuckDB reads when it creates an instance, so
the caps reach both arms, old baselines, raw connections and the arm's
CLI alike. Before the fixtures, each arm's binary must report exactly
these caps, else the run stops (exit 3). Both arms always share the caps,
so ratios stay comparable; absolute milliseconds differ from an uncapped
run. A sample then averages 1-2.4 cores, ~5 at a burst.

**The host.** The load rules count only the *foreign* load: the busy
cores of every other process (busy CPU from `/proc/stat` minus the gate's
own and its samples' CPU), so the gate's own samples and builds never
count against it. The gate refuses to start when other processes keep
more than nproc/4 cores busy over 5 s and warns above nproc/8; `--force`
runs anyway and marks the results FORCED. The check repeats after the
builds, with no waiting. A metric's run whose foreign load (median of its
first 3 pairs vs its last 3) changes more than 2x is rerun once, then
LOAD-DRIFT; loads below nproc/16 cores count as nproc/16, so a desktop's
own small bursts are not a drift. Under load the per-pair
noise rises from ~2 % to 8-15 %, and 1.05 metrics then end INCONCLUSIVE.
Per pair the results record the foreign and gate cores, CPU pressure (PSI)
and the 1-minute load average (information only), and the run the CPU
governor and boost flag; it warns when the governor is not `performance`
(it changes nothing). `--cpus LIST` runs both arms under `taskset` (DuckDB
does not see the affinity mask, but its thread count is the cap anyway).

**A/A calibration.** `just perf-gate HEAD HEAD --aa` builds one revision
twice with different salts (two binaries, two fixture sets) and must PASS
every metric; `--aa-salt K` draws another pair. Its ratios are the host's
noise and code-layout floor. Run it when the host, the toolchain or the
bundled DuckDB changes, or when a FAIL looks spurious; not on every gate
run.

**Adding a metric.** Add a harness mode (`scripts/bench/ab/harness/`,
public API only, setup untimed) that checks its result and returns a
`Sample` with a digest of the logical result, then a `metrics.toml` entry
with its class, fixtures and `check` (the runner refuses a metric without
one). If the mode needs an API a baseline may lack, add a capability with
a source probe. `just perf-gate-test` runs the tool's own tests;
`ab.py --self-test --inject METRIC=1.15|wrong|wrongvalue` checks that the
whole pipeline catches a slowdown or a wrong result.

## PR checklist
To be defined with the first release process.

## Docs
The documentation site is built with [Zensical](https://zensical.org) from
`docs/` (config: `zensical.toml`) and published to GitHub Pages by
`.github/workflows/docs.yml` on every push to `main`; PRs build it in
strict mode. Docs tooling lives in its own venv: `just docs-venv` once, then
`just docs` (regenerate + live preview) or `just docs-build`.

The schema diagrams and view tables on the Schema page are **generated**:
`scripts/docs/gen_schema.py` builds `schema/*.sql` into DuckDB and writes
`docs/_generated/*.md` (never edit those by hand). After any schema change,
run `just docs-gen` and commit the result — CI's `--check` step fails on
stale output.
