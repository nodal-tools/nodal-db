# nodal-db

A planning-scale parameter database: a DuckDB schema plus an all-Rust core
(canonicalizing writer library, `nodal-db` CLI) for storing model inputs,
scenarios, and aggregated outputs. Documentation:
<https://nodal-tools.github.io/nodal-db/> (sources in `docs/`); see
`CONTRIBUTING.md` for development setup.

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
>>> c.sql(\"SELECT * FROM read_parameter('node', 'demand') WHERE alternative = 'Base'\")
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
SELECT * FROM read_parameter('<class>', '<parameter>');  -- values, any tier, all alternatives
SELECT * FROM value_rows;                        -- current values as long rows
SELECT * FROM value_cells;                       -- array/long cells expanded
SELECT * FROM audit_health;                      -- integrity + maintenance health, one row
SELECT * FROM "commit" ORDER BY id;              -- the write/version history (user, message, time)
```

Note: `cargo test` builds throwaway DBs in tempdirs (gone after the run) — use
`nodal-db init` / `import` to make a persistent file to browse.

## License

Copyright (c) 2026 Nodal-Tools Oy.

nodal-db is licensed under the [European Union Public Licence v1.2 (EUPL-1.2)](https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12); see [`LICENSE`](LICENSE).

### What this means in practice

This section explains how we, the copyright holders, interpret the license for this project. Where it gives you more freedom than the license text alone, it is an additional permission from us as the licensor, and you may rely on it. It never takes away any right the license gives you.

**You may freely:**

- Use the schema and tools for any purpose, including commercial use.
- Use them unmodified in internal systems and in hosted or SaaS products, without any obligation to publish your own code. If you give others access to nodal-db's functionality, keep the copyright and license notices intact and point to where its source is available (a link to this repository is enough).
- Build your own applications, queries, reports and integrations on top of the schema. These are your own works and are not covered by this license.

**Your data is yours.** Data stored in databases that use this schema is not a derivative work and is not covered by this license. A database file necessarily contains the schema definitions; you may share, publish or sell such database files on terms of your choice, and the schema they contain creates no license obligations for you or for the recipients.

**If you modify nodal-db's files** (schema definitions, migrations or tool source code) and distribute them, or make them available to others over a network (for example as part of a hosted service), you must publish those modified files under the EUPL-1.2 (or, where the EUPL's compatibility clause applies, under a compatible licence listed in its Appendix).

Files you write yourself (such as your own additional tables, extensions or application code) are not covered, provided they do not include modified copies of nodal-db's files. Unmodified copies of nodal-db's files that you include remain under the EUPL-1.2 with their notices intact.

### Questions

If you are unsure whether your use case creates obligations, please open an issue. We are happy to clarify.
