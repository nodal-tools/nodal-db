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

# DuckDB schema smoke suite alone (195 checks).
smoke:
    {{py}} schema/smoke_test.py

# Benchmark gate alone (long; results in benches/RESULTS.md).
bench:
    cargo bench

# Fast feedback loop — format check, lint, typecheck; no tests.
check:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo check --workspace

# Create/refresh the Python venv with the smoke-suite + conformance-oracle deps.
venv:
    python3 -m venv .venv
    .venv/bin/pip install -r dev-requirements.txt
