# nodal-db developer task runner.
#
# Tiered testing (see README "Testing tiers"):
#   just            quick gate  — every push: workspace tests (slow ones skipped) + schema smoke
#   just t <args>   targeted    — pass cargo-test args, e.g. `just t -p nodal-db-core canon`
#   just full       release gate — everything incl. #[ignore] slow tests + the A/B perf
#                   gate against the last passing release run (scripts/bench/ab/BASELINE)
#
# DB inspection commands live in the README ("Inspecting a database").

py := ".venv/bin/python"

# Show the recipe list.
default:
    @just --list

# Quick gate (every-push tier): workspace tests with slow #[ignore] tests skipped,
# the DuckDB schema smoke suite, then the perf-gate tool's own unit tests
# (stdlib, seconds, no builds). Needs the .venv (see `just venv`).
test:
    cargo test --workspace
    {{py}} schema/smoke_test.py
    python3 -m unittest discover -s scripts/bench/ab/tests

# Targeted run — forwards any args to `cargo test`.
# e.g. `just t -p nodal-db-core canon`  /  `just t --test api_write_parameter`
t *ARGS:
    cargo test {{ARGS}}

# Everything, including the #[ignore] slow tests, the schema smoke suite, the
# criterion benches compiled (not run: they record, they do not gate), the
# index-scan guards, and last the A/B perf gate at full scale against
# scripts/bench/ab/BASELINE, which advances to HEAD when every metric passes
# (commit it with the run's block in benches/RESULTS.md).
# Release gate (pre-release only): slow tests + smoke + A/B perf gate vs BASELINE.
full:
    cargo test --workspace -- --include-ignored --skip helper_
    {{py}} schema/smoke_test.py
    python3 -m unittest discover -s scripts/bench/ab/tests
    cargo bench --no-run
    # The index-scan guards (array_id; the point-read macros and their
    # dict-cache bug guards; entity lower(name) + axis_hash behind the
    # OFFSET 0 fence) in a release build: debug-built DuckDB compresses
    # join keys and needs an optimizer override in those tests, so only a
    # release run checks the engine as shipped.
    cargo test --release -p nodal-db-core --test array_id_index_scan --test entity_name_index_scan --test point_read_index_scan
    python3 scripts/bench/ab/ab.py --release --advance-baseline

# Pre-merge, expected for perf-sensitive branches (CONTRIBUTING "Performance
# gates"): BASE (default: merge-base with main) vs CAND (default: the working
# tree), paired ratios on this host. e.g. `just perf-gate`, `just perf-gate
# 88c0c5b HEAD --metrics 'wr_*'`, `just perf-gate HEAD HEAD --aa` (A/A).
# Arguments are passed through as given ("$@"), so a quoted
# `--allow 'METRIC=a reason with spaces'` or `--metrics 'rd_*'` stays one word.
# A/B perf gate (exit 0 pass, 1 fail, 2 inconclusive, 3 error, 4 wrong result).
[positional-arguments]
perf-gate *ARGS:
    python3 scripts/bench/ab/ab.py "$@"

# The perf-gate tool's unit tests alone (stdlib, no builds).
perf-gate-test:
    python3 -m unittest discover -s scripts/bench/ab/tests

# Builds and runs a few metrics with injected slowdowns and wrong results
# (minutes; wants a quiet host): the whole pipeline must catch them.
# The perf gate's integration self-test (A/A, injection, wrong result, old baseline).
[positional-arguments]
perf-gate-selftest *ARGS:
    python3 scripts/bench/ab/selftest.py "$@"

# DuckDB schema smoke suite alone (245 checks).
smoke:
    {{py}} schema/smoke_test.py

# Criterion benches, recording only (informational; long; results in benches/RESULTS.md).
bench:
    cargo bench

# Opt-in, NOT part of `test`, `full` or `bench`: bulk ingest + read paths through
# the core (~30 min; `--quick` ~1-2 min), then the raw-DuckDB layout comparison.
# Results: scripts/bench/RESULTS_results_path.md. e.g. `just bench-results --quick`
# Results-scale benchmark (ingest + reads through the core, then raw-DuckDB layout).
bench-results *ARGS:
    cargo bench -p nodal-db-cli --bench results_path -- all {{ARGS}}
    {{py}} scripts/bench/raw_duckdb_layout.py {{ARGS}}

# Runs under any Python with duckdb + numpy + pyarrow, e.g. a throwaway venv
# with another DuckDB release: `just bench-raw-layout /tmp/duck2/bin/python --quick`
# Raw-DuckDB layout comparison alone (dense lists vs lean long table).
bench-raw-layout python=py *ARGS:
    {{python}} scripts/bench/raw_duckdb_layout.py {{ARGS}}

# Opt-in, ~15 s; results: scripts/bench/RESULTS_results_path.md §7.1. Optional
# cross-check on a core-written file: `just bench-long-size --core-db <file>`.
# Other shapes (default rank1): `just bench-long-size --shape sparse2d` (one
# 4.56 M-cell rank-2 value, ~1 min) or `--shape rank3`.
# Long-tier size breakdown (value_long from schema/*.sql, one part removed per variant).
bench-long-size *ARGS:
    {{py}} scripts/bench/long_tier_size_breakdown.py {{ARGS}}

# Input files: `NODAL_RESULTS_DIR=/tmp/nr NODAL_RESULTS_KEEP=1 cargo bench -p nodal-db-cli
# --bench results_path -- dense10k long`, then `--db /tmp/nr/ipc_dense_10p_r0.db`.
# Surrogate-key value_array variant + commit-after-replace probe (any duckdb Python;
# superseded by the array_id schema: needs a 0.3-schema input file).
bench-sk-variant python=py *ARGS:
    {{python}} scripts/bench/surrogate_key_variant.py {{ARGS}}

# Fast feedback loop — format check, lint, typecheck; no tests.
check:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo check --workspace

# Create/refresh the Python venv with the smoke-suite + conformance-oracle deps.
venv:
    python3 -m venv .venv
    .venv/bin/pip install -r dev-requirements.txt

# --- Documentation site (Zensical; published to GitHub Pages by .github/workflows/docs.yml)

docs_py := ".venv-docs/bin/python"

# Kept separate from .venv so the test venv stays lean.
# Create/refresh the docs venv (.venv-docs: zensical + pinned duckdb).
docs-venv:
    python3 -m venv .venv-docs
    .venv-docs/bin/pip install -r docs-requirements.txt

# Regenerate the schema diagrams/view tables (docs/_generated/) from schema/*.sql.
docs-gen:
    {{docs_py}} scripts/docs/gen_schema.py

# Fail if the committed generated schema docs are stale (what CI runs).
docs-check:
    {{docs_py}} scripts/docs/gen_schema.py --check

# Regenerate, then build the site into site/ (strict: warnings fail, as in CI).
docs-build: docs-gen
    .venv-docs/bin/zensical build --clean --strict

# Regenerate, then serve the site locally with live reload (http://localhost:8000).
docs: docs-gen
    .venv-docs/bin/zensical serve
