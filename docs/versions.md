# Versions and compatibility

A nodal-db release carries four version numbers. Each one answers a
different question:

| Version | Where to see it | What it tells you |
|---|---|---|
| **Package version** (semver, `0.1.0`) | `nodal-db --version`, `nodal_db.__version__`, the crates' `Cargo.toml` | Which release of the software you run. The Rust crates, the `nodal-db` CLI and the `nodal-db` Python package share one version. |
| **Schema version** (an integer, `7`) | `SELECT value FROM database_property_current WHERE key = 'schema_version'` | The layout of the tables in a database file. |
| **Read-surface contract** (`contract_major`, `revision`) | `SELECT * FROM nodal_read_surface()` | The version of the public views and macros a file carries. |
| **Bulk format version** (`1.0`) | the `nodal:format_version` key of an import file | The Arrow-IPC import format of `nodal-db import`. |

A file also records the build that created it in the `created_by`
property (`SELECT value FROM database_property_current WHERE key =
'created_by'`), for example `nodal-db-core 0.1.0 (git 1a2b3c4d5e6f)` for a
release build, or `nodal-db-core 0.1.0 (dev)` for a build from a source
checkout.
Include it in bug reports.

## Schema version

The schema version is a plain integer that rises by one with every
change to the table layout. A nodal-db build works on files of its own
schema version only: a write, a compaction, an extract or an erase on a
file of another schema version is refused before anything is changed,
with an error (`SchemaMismatch`, Python `nodal_db.SchemaMismatch`) that
names the file's version and the build's. Files of the development
versions before 0.1.0 carry a label such as `0.6-metadata`; they are
refused the same way.

## Read-surface contract

The views and table macros live inside each file. `contract_major`
increases with every breaking change to a stable object, `revision` with
every change at all. A newer nodal-db brings an older file's surface up
to date on its next write. The rules are in
[The read surface](dev/read_surface.md#versions-and-old-files).

## Bulk format version

The import format is versioned `MAJOR.MINOR`. A reader accepts every
minor version of its major and refuses another major. See
[Format version](dev/arrow_import_format.md#format-version).

## Through 0.x: test releases

The 0.x releases are for test users. During 0.x:

- **A new schema version means re-creating files.** There are no
  migrations: create a new file with the new release (`nodal-db init`)
  and import the data again (`nodal-db import`, the Spine import or your
  own writer). The release notes (`CHANGELOG.md` in the repository,
  copied to each GitHub release) say when the schema version changes, and
  changes are batched so that this happens rarely.
- **The read surface can break** with a `contract_major` bump, stated in
  the release notes.
- **The bulk format stays 1.x** unless the release notes say otherwise.

At 0.1.0 the file format, the canonical encoding
([Canonical encoding](dev/canonical_encoding.md)), the read surface and
the bulk format are decided and documented on purpose; changing them
before 1.0.0 is possible but costs every test user a re-create.

## From 1.0.0

From 1.0.0 the format is frozen in the usual sense:

- a change of the schema version comes with a migration that upgrades
  existing files;
- a breaking change to the read surface (`contract_major`) or the bulk
  format (a new major) needs a new major release of the package.
