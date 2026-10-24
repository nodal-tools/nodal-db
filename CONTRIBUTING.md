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
