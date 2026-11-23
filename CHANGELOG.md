# Changelog

All notable changes to nodal-db are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the package
version follows [Semantic Versioning](https://semver.org/). Each GitHub
release copies its section of this file as its release notes.

## [0.1.0] - unreleased

The first public release, for test users.

### Versions

| Version | 0.1.0 |
|---|---|
| Package (the Rust crates, the `nodal-db` CLI, the `nodal-db` Python package) | 0.1.0 |
| Schema version (the table layout of the files it writes) | 7 |
| Read-surface contract (`SELECT * FROM nodal_read_surface()`) | 1.7 (`contract_major` 1, `revision` 7) |
| Bulk import format (`nodal:format_version`) | 1.0 |

See [Versions and compatibility](https://nodal-tools.github.io/nodal-db/versions/).

**Compatibility policy through 0.x.** The 0.x releases are for test users.
A release that changes the schema version cannot open files of the
previous one: re-create them (`nodal-db init`, then import the data
again). There are no migrations before 1.0.0. These notes say whenever
the schema version, the read-surface `contract_major` or the bulk format
major changes.

### Platforms

- Python wheels on PyPI (`pip install nodal-db`) for Linux x86_64
  (manylinux_2_28, glibc 2.28 or newer) and macOS arm64 (11.0 or newer),
  CPython 3.11 or newer (one abi3 wheel per platform), plus the sdist.
- `nodal-db` CLI binaries for the same two platforms, attached to the
  GitHub release.
- The Rust crates are not on crates.io yet (planned from 0.2).
- The Spine importer (`python/nodal_spine_import`) is not packaged: run
  it from a checkout of the repository.

### Features

- **A database format in DuckDB files**: classes (with multi-dimensional
  classes over other classes), entities, parameters with declared value
  types, value lists, alternatives and scenarios with ranked alternatives,
  entity activation and group membership.
- **Typed value storage, readable with plain SQL**: scalars, dense arrays
  and long (row-per-cell) values over datetime, integer and label
  dimensions; the core picks the storage tier. Any DuckDB 2.0 client
  reads the data through stable views and table macros (the read
  surface), such as `read_parameter`, `value_rows` and `value_cells`.
- **One canonicalizer**: every writer (Rust, the CLI, Python) goes through
  the same pipeline, so the same data always produces the same stored
  bytes and content hashes.
- **Append-only history** with as-of reads at any commit; long values are
  diffed per cell, and an unchanged write makes no new version (a call
  that changes nothing makes no commit).
- **One atomic save** (`api::commit` in Rust, `Database.commit` in Python)
  carrying value writes, structural changes (creates, renames, scenario
  rankings, activations, memberships), value and object deletes, purges
  and metadata, with machine-readable warnings and per-item outcomes.
- **Metadata**: sources, licences, roles, key-value pairs and properties,
  linked to objects and to individual values.
- **Templates**: load a vocabulary (classes, parameters, value lists) from
  YAML or JSON, plan and apply updates to a populated database, export it
  back.
- **Maintenance**: purge (versioned close-out of a scope), hard erasure of
  one alternative with a trace, history-free extract into a new file,
  compaction, and read-surface refresh.
- **Bulk import** of Arrow-IPC files (`nodal-db import`), atomic in one
  commit.
- **CLI exit codes**: 0 success, 1 error, 2 usage error (bad
  arguments), 75 busy (retry later); `maintain refresh-views --check`
  also exits 3 (stale) or 4 (newer than this nodal-db).
