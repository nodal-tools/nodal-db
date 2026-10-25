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
access paths, index key order, the caching-operators workaround) are listed with their guards in
[`docs/dev/engine_rules.md`](docs/dev/engine_rules.md); read it before
changing the schema, core SQL, indexes, sequences, storage or compaction,
and re-check it after a bump. After one:
- Run `just perf-gate PRE_BUMP_MAIN WORKTREE` before merging (a bump is
  the canonical perf-sensitive change; both arms compile their own
  DuckDB), then `just full`. Besides the slow tests and the release perf
  gate, it runs the index-scan guards (`array_id_index_scan`,
  `entity_name_index_scan`) on a release-built engine. They check
  optimizer behaviour, not a documented contract, and fail loudly when it
  changes; their failure messages say what to re-evaluate.
- If `duckdb_dict_cache_guard`'s tripwire fails ("the DuckDB dict-cache
  bug appears fixed"), remove the `debug_enable_caching_operators`
  workaround as described in engine_rules.md §5. If every core open
  fails on that setting, the new DuckDB renamed or dropped it: check
  whether the bug is fixed before deciding what replaces it.
- If the release notes mention Unicode or utf8proc updates (case
  mapping), compact the dev files after the bump. The `lower(name)`
  indexes (`ix_entity_lname`, the `uq_*_name` unique indexes over
  `cur_name`) persist keys computed by the engine that built them, so a
  changed case mapping could make an index lookup miss a row a scan
  would find; compaction rebuilds every index from the DDL.

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

The first run compiles the bundled DuckDB once (minutes); arms, fixtures
and binaries are cached under `target-ab/` (gitignored). `--full` uses the
criterion gate's fixture sizes instead of a tenth of them.

**The release gate** (`just full`) runs `ab.py --release --full
--advance-baseline`: BASE is the commit in `scripts/bench/ab/BASELINE` (the
last passing release run), CAND is HEAD (a clean tree; with uncommitted
changes, the working tree, and nothing advances). When every metric
passes (exit 0; an `--allow`ed metric counts, and its reason goes into the
block), BASELINE moves to HEAD: commit it together with the run's block in
`crates/nodal-db-core/benches/RESULTS.md` ("A/B gate runs"), so the chain
of release-to-release ratios stays visible. Only a complete run advances
it: full scale, no `--metrics` subset, BASE = BASELINE, and no uncommitted
change in the gate itself (`scripts/bench/ab/`, `scripts/lib/`). In a
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

**The host.** The gate refuses to start when the 1-minute load average is
above nproc/4 and warns above nproc/8; `--force` runs anyway and marks the
results FORCED. Under load the per-pair noise rises from ~2 % to 8-15 %,
and 1.05 metrics then end INCONCLUSIVE. It records the CPU governor and
boost flag and warns when the governor is not `performance` (it changes
nothing). `--cpus LIST` runs both arms under `taskset`, but DuckDB sizes
its thread pool from the machine, not the affinity mask (32 threads even
on one pinned CPU), so pinning oversubscribes; use it only on a host you
cannot quiet.

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
