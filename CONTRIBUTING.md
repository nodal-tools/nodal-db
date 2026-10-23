# Contributing

Stub — grows with the project. What is binding already:

## Spec workflow
All lasting work products (plans, findings, critiques, handoffs) go in
`specs/` under the conventions in `specs/README.md`.

## Dev setup
To be defined when the first code lands (expected: Python 3.11+, DuckDB;
lint with ruff before committing).

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
