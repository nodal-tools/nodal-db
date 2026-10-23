# nodal-db developer task runner.
#
# Tiered testing (see README "Testing tiers"):
#   just            quick gate  — every push: workspace tests (slow ones skipped) + schema smoke
#   just t <args>   targeted    — pass cargo-test args, e.g. `just t -p nodal-db-core canon`
#   just full       release gate — everything incl. #[ignore] slow tests + benches
#
# DB inspection commands live in the README ("Inspecting a database").

py := ".venv/bin/python"

# Show the recipe list.
default:
    @just --list

# Quick gate (every-push tier): workspace tests with slow #[ignore] tests skipped,
# then the DuckDB schema smoke suite. Needs the .venv (see `just venv`).
test:
    cargo test --workspace
    {{py}} schema/smoke_test.py

# Targeted run — forwards any args to `cargo test`.
# e.g. `just t -p nodal-db-core canon`  /  `just t --test api_write_parameter`
t *ARGS:
    cargo test {{ARGS}}

# Release gate (pre-release only): everything, including the #[ignore] slow tests,
# the schema smoke suite, and the benchmark gate (long — builds a ~150 MB DB).
full:
    cargo test --workspace -- --include-ignored
    {{py}} schema/smoke_test.py
    cargo bench
    # The array_id index-scan guard in a release build: debug-built DuckDB
    # compresses join keys and needs an optimizer override in that test, so
    # only a release run checks the engine as shipped.
    cargo test --release -p nodal-db-core --test array_id_index_scan

# DuckDB schema smoke suite alone (212 checks).
smoke:
    {{py}} schema/smoke_test.py

# Benchmark gate alone (long; results in benches/RESULTS.md).
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
