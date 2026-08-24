# nodal-db

A planning-scale parameter database: a DuckDB schema plus an all-Rust core
(canonicalizing writer library, `nodal-db` CLI) for storing model inputs,
scenarios, and aggregated outputs. See `CONTRIBUTING.md` for the spec workflow
and `spec/` for the normative contract (canonical encoding, reconstruction,
the Arrow-IPC bulk format).

## Testing tiers

Managed with [`just`](https://github.com/casey/just); recipes in `justfile`.

| Command | Tier | What runs |
|---|---|---|
| `just t <args>` | targeted | forwards to `cargo test`, e.g. `just t -p nodal-db-core canon` |
| `just` (or `just test`) | quick gate | `cargo test --workspace` (slow `#[ignore]` tests skipped) + schema smoke |
| `just full` | release gate | the above **with** `--include-ignored` slow tests + `cargo bench` |

The quick gate is what you run on every change; `just full` is for pre-release.
Slow tests are marked `#[ignore = "slow: pre-release tier"]` so they drop out
of the quick gate automatically — tag any new slow test the same way. First run
compiles bundled DuckDB (~16 min, cached thereafter). `just venv` creates the
Python venv the smoke suite and conformance oracles need.

## Inspecting a database

DB files are written by the bundled **DuckDB 1.5.5** engine. A reader must be
1.5.x — an older DuckDB (the machine's standalone CLI may lag) will refuse to
open a 1.5.5 file with a storage-version error.

First make one to look at (or point the commands at any `.duckdb` file):

```bash
cargo run -p nodal-db-cli -- init /tmp/demo.duckdb
# ... then `nodal-db import` some data, or inspect a smoke/test DB.
```

### Easiest: the project venv (guaranteed version-matched, no install)

The `.venv` DuckDB is pinned to 1.5.5, so it always matches the file. Open
**read-only** so you never contend with a writer for the single-writer lock:

```bash
# One-off query:
.venv/bin/python -c "import duckdb; \
  print(duckdb.connect('/tmp/demo.duckdb', read_only=True).sql('SHOW TABLES'))"

# Interactive shell:
.venv/bin/python -q
>>> import duckdb; c = duckdb.connect('/tmp/demo.duckdb', read_only=True)
>>> c.sql(\"SELECT * FROM read_parameter('node', 'demand', 'Base')\")
```

### DuckDB CLI + web UI (needs a 1.5.x CLI)

The standalone CLI gives an interactive shell and a local web table browser.
Update it to 1.5.x first (the version manager's `latest` may be stale):

```bash
curl https://install.duckdb.org | sh      # installs/updates the latest CLI
~/.duckdb/cli/latest/duckdb --version      # confirm >= 1.5.x

duckdb -readonly /tmp/demo.duckdb          # interactive SQL, read-only
duckdb -ui /tmp/demo.duckdb                # opens the web UI in a browser
```

Inside an already-open CLI you can also launch the browser UI with
`CALL start_ui();`.

### Useful starter queries

The schema exposes read macros and audit views rather than raw tables — prefer
these:

```sql
SHOW TABLES;                                    -- everything (36 tables, 66 views, macros)
SELECT * FROM read_parameter('<class>', '<parameter>', '<alternative>');  -- values, any tier
SELECT * FROM value_rows;                        -- current values as long rows
SELECT * FROM value_cells;                       -- array/long cells expanded
SELECT * FROM audit_health;                      -- integrity + maintenance health, one row
SELECT * FROM "commit" ORDER BY id;              -- the write/version history (user, message, time)
```

Note: `cargo test` builds throwaway DBs in tempdirs (gone after the run) — use
`nodal-db init` / `import` to make a persistent file to browse.
