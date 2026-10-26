# The read surface

Reading a nodal-db database is reading SQL: the views and table macros
of [`schema/070`–`130_*.sql`](https://github.com/nodal-tools/nodal-db/tree/main/schema)
are the public read interface, for any language and any DuckDB client.
This page states which of them are public, the rules every reader
follows, which read to use for what, the column conventions, and how the
surface is versioned. The base tables are not part of it: their layout
may change with any schema version.

To see which version of the read surface a file carries:

```sql
SELECT * FROM nodal_read_surface();
-- contract_major | revision | ddl_hash
```

A missing `nodal_read_surface()` means the file predates versioning
(see [Versions and old files](#versions-and-old-files)).

## What is public

Every view and macro has exactly one class. The class is written in the
DDL as a tag on the line directly above its `CREATE`
(`-- @surface: stable`), so a class change is reviewed in the same diff
as the object.

| Class | What it is | What may change without a contract-major bump |
|---|---|---|
| **stable** | The API: a documented name, parameters, columns (name, type, order) and semantics. | New columns appended at the end; new objects; speed-ups and bug fixes that keep the documented semantics. |
| **mirror** | `<table>_current` and `<table>_asof(c)`: one base table's real (non-generated) columns under one validity predicate. | Columns follow the table, so they change only with a schema version. Select them by name. The mirror names are stable. |
| **advisory** | The `audit_*` reports. A non-empty result is a finding, one row per finding (`audit_health` and `audit_erasure` are inventories). | Their columns may change in any revision: display or count them, never decode them by position. |
| **internal** | Helpers the stable objects are built from (`axis_param_*`, `axis_parametric`, `axis_position_stored*`, `value_array_cells`, `entity_activation_own`, `deact_edge`). | Anything, including removal. Not for outside readers. |

Read through stable and mirror objects, never through internal ones and
never through the base tables. The [reference](#reference) below lists
every stable object with its columns and types, the mirrors and the
reports; internal objects are not listed.

**Surface names are reserved.** Every name in `070`–`130` (and every
retired one, listed in `schema/read_surface_retired.txt`) belongs to
nodal-db. A view of yours with such a name is replaced when nodal-db
refreshes the file; a table of yours with such a name blocks the refresh
(your writes still go through, with a warning). Give your own objects
other names, for example a prefix of your tool's.

## Rules for every reader

- **Hold connections per operation: open, query, close.** DuckDB's file
  lock covers readers too: an idle read-only connection (a notebook, a
  GUI tab) blocks every writer until it closes. To notice new commits,
  poll `dataset_version` (or `SELECT max(id) FROM "commit"`) with a short
  read. See [connection discipline](../usage.md#connection-discipline).
- **Row order is unspecified** for every object: add an `ORDER BY`. The
  order of a stable object's **existing** columns is guaranteed, but new
  columns may be appended at the end in any revision: tolerate extra
  trailing columns. `INSERT INTO t SELECT * FROM read_parameter(…)` into
  a fixed-width table, a `UNION ALL` against an older snapshot or an
  equality test on an Arrow schema break when a column is appended;
  select the columns you need by name. A view of yours written as
  `SELECT *` over a stable object widens silently when it gains a column.
  Select mirror and advisory columns by name.
- **Do not create temporary objects with surface names.** A
  `CREATE TEMP VIEW` or `CREATE TEMP MACRO` named like a surface object
  shadows it everywhere on that connection, also inside every other view
  and macro of the file and when qualified as `main.`.
- **Never call a table macro through an attached database.**
  `other.read_parameter(…)` silently reads the *default* database's
  tables. Make the other file the default (`USE other`, then switch
  back) or open it on its own connection; never store a view that calls
  another file's macro. Views of an attached file are fine: they bind to
  their own file.
- **DuckDB 2.0 dict-cache bug.** On your own connection, run
  `SET debug_enable_caching_operators = false` before filtering
  `value_rows`, `value_cells` or `resolved_value_rows` on array values
  (details under [DuckDB version](../usage.md#duckdb-version)).
  `read_parameter`, `read_value`, `read_values` and `value_rows_for` are
  immune on any connection. The core and the Python package set it for
  you.
- **DuckDB 2.0 or newer** is needed to open the file.
- **Names are case-insensitive** in every name argument (class,
  parameter, entity, alternative, scenario, display mode). An unknown
  name returns zero rows, never an error.

## Which read to use

| You need | Use | Why not the others |
|---|---|---|
| Every value of one parameter, rendered | `read_parameter(class, parameter)` | It reads only that parameter; `value_rows` pays for every axis in use in the file. |
| One parameter, scalars only, large | `read_parameter_scalar(class, parameter)` | No tier union, no axis work. |
| One parameter, arrays as lists (engine hot path) | `read_parameter_dense(class, parameter)` | No unnest: render the indices from your cached dictionaries. |
| One value, or a few, by name | `read_value`, `read_values` | An index fetch: milliseconds instead of a read of every array. Up to **50 value hits** per call (a hit is an existing array value for one (entity, alternative) pair; 17 entities in 3 alternatives is 51, a full scan). |
| The same by ids | `value_rows_for(parameter_definition_id, entity_ids, alternative_ids)` | |
| Everything current, by ids | `value_rows` (rendered) or `value_cells` (positions only) | For bulk export, not for point reads. |
| One scenario's winners | `resolved_value_rows(scenario)` | It still reads `value_rows` (a full scan; the dict-cache rule applies). |
| Competing values, candidates | `scenario_value_conflict`, `scenario_value_candidate`, `scenario_value_resolved` | |
| Past state | `<table>_asof(c)`, `value_array_asof_for(c, …)` | They fail loudly instead of answering when an erasure removed content of that commit. |
| Every position of every axis | `axis_position` | For audits and exports only: it expands every axis of the file, so never join it in a read path. |

## Column conventions

These hold for every stable object with value columns (`value_cells`,
`value_rows`, `value_rows_for`, `read_parameter`, `read_value`,
`read_values`, `resolved_value_rows`):

- `tier` is one of `scalar`, `long`, `array`.
- `value_kind` is one of `float`, `int`, `bool`, `string`, `date_time`,
  `duration`. Exactly the `v_*` column(s) of the kind are non-NULL:
  `v_float`, `v_int`, `v_bool`, `v_str`, `v_datetime`, and for a
  duration both `v_dur_months` and `v_dur_micros`.
- `idx1_*` … `idx4_*` are the indices of dimensions 1 to 4, NULL beyond
  the value's rank and for kinds the axis does not have: `idxN_text` for
  label and time-pattern axes, `idxN_ts` for datetime and period axes,
  `idxN_end_ts` for period axes (the period is `[idxN_ts, idxN_end_ts)`),
  `idxN_num` for integer axes, `idxN_coord` for float axes. `idxN_ordinal`
  is the semantic sort key of the dimension (NULL for scalars).
- Two column orders exist for the same index columns, and both are kept:
  `value_rows` and `value_rows_for` group them per dimension
  (`idx1_text, idx1_ts, idx1_end_ts, idx1_num, idx1_ordinal, idx1_coord,
  idx2_…`); `read_parameter`, `read_value` and `read_values` group them
  per kind (`idx1_text … idx4_text, idx1_ts … idx4_ts, …`), with the
  period ends `idx1_end_ts … idx4_end_ts` after the value columns.
- Scalar rows have positions `iN_pos = 0` (not NULL) and NULL axis ids.
- Ids (`entity_id`, `parameter_definition_id`, …) are stable within one
  file but neither dense, ordered nor portable between files.
  `value_array_current.array_id` is a storage handle, never an identity.
- Types: the generated reference lists each column's DuckDB type. The
  stable objects use `BIGINT`, `INTEGER`, `SMALLINT`, `DOUBLE`,
  `BOOLEAN`, `VARCHAR`, `TIMESTAMP` and the lists `DOUBLE[]`, `BIGINT[]`.

## Versions and old files

The read surface lives **inside each database file**: `init` writes the
views and macros of the nodal-db that creates the file, and the file
keeps them. `nodal_read_surface()` says which:

- `contract_major` is `0` until the encoding freeze and `1` from then
  on. It changes only for a breaking change: a stable object, column or
  parameter removed or renamed, a stable column's type or position
  changed, a documented semantics changed (case sensitivity, the NULL
  conventions, the alternative default), or a mirror removed or renamed.
- `revision` increases with every change to the surface DDL, breaking or
  not: appended columns, new objects, faster bodies, bug fixes, any
  change to an advisory or internal object.
- `ddl_hash` identifies the exact DDL text (comments aside).

What nodal-db does with a file whose surface differs from its own:

- **An older file is upgraded by the next write.** Before a write,
  nodal-db replaces the file's views and macros with its own (about
  0.2 s, once), in its own transaction. Data and history are not
  touched and no commit is recorded. Views of your own are kept; a table
  of yours with a reserved name blocks the upgrade, and the write report
  says so (`read_surface_warning`). `nodal-db maintain refresh-views
  FILE` (Python `db.refresh_read_surface()`) upgrades a file you only
  read; `--check` only reports (exit code 0 current, 1 older, 2 newer).
- **Reads through nodal-db work on an older file** and warn once per
  file (Python: `nodal_db.ReadSurfaceStaleWarning`; CLI: on stderr).
  nodal-db's own reads (`read_parameter`, `read_values` in Rust and
  Python) use temporary copies of this build's objects they need, which
  costs about 15 ms per read; other reads (Python `db.query`, Rust
  `conn::with_read`) use the file's own objects, which within one
  contract major are compatible, and a read that needs an object the
  file lacks is retried through temporary copies (`db.query`,
  `conn::with_read_retrying`) or fails with `ReadSurfaceStale`
  (`conn::with_read`). A file of another contract major is always read
  through this build's objects. Do not `ATTACH` another nodal-db file
  inside such a read: the temporary copies would shadow its views too.
- **A newer file is never downgraded** by an older nodal-db's writes;
  its reads use the file's newer objects within the same major.
  Compaction is the exception: it rebuilds the file with its own build's
  objects and warns; the newer nodal-db upgrades the file again on its
  next write.
- **Your own DuckDB connection** gets none of this: check
  `SELECT * FROM nodal_read_surface()` (an error means the file predates
  versioning) and run `nodal-db maintain refresh-views FILE` or any
  write with a current nodal-db.

The committed listing `schema/read_surface_contract.tsv` holds every
column of every stable, mirror and advisory object; the generator
(`just docs-gen`) refuses to rewrite it for a breaking change without the
matching bump, and a test fails when the DDL changed without a revision
bump. How to change a surface object is in
[Engine rules §9](engine_rules.md#9-changing-a-read-surface-object).

## Reference

--8<-- "_generated/read_surface.md"
