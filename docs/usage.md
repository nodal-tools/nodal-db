# Using nodal-db

The first part of this page walks through creating a database, writing
data and reading it back. The second part covers the rules that tools
integrating with nodal-db must follow.

The examples share one small data set, taken from energy-system
modelling (nodal-db's first use case) but standing for any kind of
model: a `node` class with an hourly
`demand` series, a `unit` class with a scalar `capacity`, a `Base`
alternative and a `high_demand` alternative stacked on top of it in a
scenario called `stress`. The outputs shown were produced by running
these examples.

## Getting started

### Build

Versioned releases, when available, are listed on the
[Releases page](https://github.com/nodal-tools/nodal-db/releases). To
build from a checkout of the repository, you need a Rust toolchain and,
for the Python parts, Python 3.11 or later.

```bash
git clone https://github.com/nodal-tools/nodal-db.git
cd nodal-db
cargo build --release -p nodal-db-cli    # produces target/release/nodal-db
```

The first build compiles the bundled DuckDB engine and takes a while
(around 15 minutes); later builds are incremental.

To build the Python package (`import nodal_db`) into a virtual
environment:

```bash
python3 -m venv .venv
.venv/bin/pip install maturin pyarrow duckdb
cd crates/nodal-db-py
VIRTUAL_ENV=../../.venv ../../.venv/bin/maturin develop
```

The examples below call the CLI as `nodal-db`; put
`target/release/nodal-db` on your `PATH`, or call it with its full path.

### Create a database

=== "CLI"

    ```bash
    nodal-db init demo.duckdb --alternative Base
    ```

    ```text
    initialized demo.duckdb
    ```

=== "Python"

    ```python
    import nodal_db

    db = nodal_db.init("demo.duckdb", default_alternative="Base")
    ```

=== "Rust"

    ```rust
    use nodal_db_core::schema_init;

    schema_init::init_schema_from_sources_with_default(
        db_path, schema_init::EMBEDDED_SCHEMA, Some("Base"),
    )?;
    ```

`init` refuses to overwrite an existing database. The `--alternative`
option creates an initial alternative; without it, the database starts
with no alternatives and one is created on the first write that names it.

To start from a template that declares the model's classes and
parameters, pass it at creation time (the examples below start without
one, so the template load does not shift their commit numbers):

```bash
nodal-db init demo.duckdb --alternative Base --template model.yaml
```

A minimal template:

```yaml
template:
  name: mini
  version: "1"
  role: input

classes:
  - name: node
    parameters:
      - name: demand
        types:
          - {kind: float}
          - {kind: indexed, dims: [{name: time, kind: datetime}]}
  - name: unit
    parameters:
      - name: capacity
        types: [{kind: float}]
  - name: unit__node
    dimensions: [unit, node]
```

Without a template, classes, parameters and entities are created
automatically on first write and each creation is reported as a warning.

### Write data

Values are passed as Arrow data in **logical long** form. Column 0 holds
entity names, the following columns are the index dimensions in order,
and the last column holds the value. A two-column table
`(entity, value)` is a scalar. The Arrow type of each column sets its
kind: `timestamp[us]` for datetime indexes, `int64` for integer indexes,
`string` (utf8) for labels; `float64`, `int64`, `bool`, `string` or
`timestamp[us]` for values. Other types are rejected rather than converted:
cast nanosecond timestamps (the pandas and polars default) to microseconds,
and `large_string` or string-view columns to `string`. (The
`nodal-db import` file format also accepts `timestamp[s]` and
`timestamp[ms]` and converts them to microseconds exactly; see
[Arrow-IPC import format](dev/arrow_import_format.md#timestamps).) You never choose the storage tier;
the core does.

In the running example, the CLI tab loads the `Base` data and the Python
tab adds the `high_demand` variant; each tab shows an interface that can
do either.

=== "CLI"

    `nodal-db import` reads an Arrow-IPC bulk file. Any Arrow library can
    write one: one IPC stream per (class, parameter), concatenated in one
    file, with the target in the schema metadata. This Python script
    writes a two-segment file:

    ```python
    import pyarrow as pa
    from datetime import datetime, timedelta

    def segment(sink, cls, param, batch, n_segments):
        meta = {
            "spinal:format": "spinal-db-bulk",
            "spinal:format_version": "1.0",
            "spinal:segment_count": str(n_segments),
            "spinal:class": cls,
            "spinal:parameter": param,
            "spinal:form": "long",
        }
        schema = batch.schema.with_metadata(meta)
        with pa.ipc.new_stream(sink, schema) as w:   # one complete stream per segment
            w.write_batch(batch.cast(schema))

    t0 = datetime(2030, 1, 1)
    hours = [t0 + timedelta(hours=h) for h in range(4)]

    demand = pa.record_batch({
        "entity": ["north"] * 4 + ["south"] * 4,
        "time":   pa.array(hours * 2, pa.timestamp("us")),
        "value":  pa.array([10.0, 12.0, 11.5, 9.0, 5.0, 5.5, 6.0, 5.2]),
    })
    capacity = pa.record_batch({
        "entity": ["coal_plant", "gas_plant"],
        "value":  pa.array([400.0, 250.0]),
    })

    with open("base.arrows", "wb") as f:
        segment(f, "node", "demand", demand, 2)
        segment(f, "unit", "capacity", capacity, 2)
    ```

    !!! note
        The metadata keys keep the `spinal:` prefix and the format name
        `spinal-db-bulk` from the project's earlier name. They are part
        of the frozen v1.0 format.

    Import it as one commit:

    ```bash
    nodal-db import base.arrows --db demo.duckdb --alternative Base \
        --user alice --message "initial data"
    ```

    ```text
    imported base.arrows: commit 2, 2 segment(s), 4 value(s) written, 0 unchanged, 4 row(s) written, axes 1 created / 1 reused, tiers [array, scalar]
    warning: auto-created entity_class "node" (id 1)
    warning: auto-created parameter_definition "demand" (id 1)
    warning: auto-created entity "north" (id 1)
    ...
    ```

    The full format, including the `dense` form for large series, is
    described in [Arrow-IPC import format](dev/arrow_import_format.md).

=== "Python"

    ```python
    import pyarrow as pa
    import nodal_db
    from datetime import datetime, timedelta

    db = nodal_db.open("demo.duckdb")

    t0 = datetime(2030, 1, 1)
    batch = pa.record_batch({
        "entity": ["north"] * 4,
        "time":   pa.array([t0 + timedelta(hours=h) for h in range(4)],
                           pa.timestamp("us")),
        "value":  pa.array([14.0, 16.0, 15.0, 12.0]),
    })
    report = db.write_parameter("node", "demand", "high_demand", batch,
                                user="alice", message="high-demand variant")
    ```

    The report is a plain dict:

    ```text
    {'commit_id': 3, 'tiers': ['array'], 'values_written': 1, ...,
     'warnings': [{'kind': 'auto_created', 'object_type': 'alternative',
                   'name': 'high_demand', 'id': 2}]}
    ```

    Several writes, deletions and structural changes can go into one
    commit with `db.commit_changes(segments=..., structural=...,
    deletes=..., object_deletes=..., user=...)`.

=== "Rust"

    ```rust
    use std::sync::Arc;
    use nodal_db_core::api;
    use nodal_db_core::duckdb::arrow::array::{Float64Array, StringArray};
    use nodal_db_core::duckdb::arrow::datatypes::{DataType, Field, Schema};
    use nodal_db_core::duckdb::arrow::record_batch::RecordBatch;
    use nodal_db_core::types::WriteOptions;

    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec!["coal_plant", "gas_plant"])),
        Arc::new(Float64Array::from(vec![400.0, 250.0])),
    ])?;

    let report = api::write_parameter(
        db_path, "unit", "capacity", "Base", batch,
        &[],                      // label-dimension ordinal domains
        WriteOptions::default(),
        "alice", "initial capacities",
    )?;
    ```

    Use the Arrow types re-exported as `nodal_db_core::duckdb::arrow` so
    that your code links against the same Arrow version as the core.
    `api::commit_changes` combines writes, deletions and structural
    changes in one commit.

### Writing a value that has not changed

A value equal to the one already stored is not written again: no new
version, no history row. The report counts it in `values_unchanged`
instead of `values_written`. Equal means the same tier, the same
parameter type, the same axes and the same stored bits in every cell,
so `-0.0` and `0.0` (which the core stores as one value) are equal,
while `1.0` and the next larger float are not.

When nothing in a call changes anything (every value unchanged, every
delete finds nothing, every structural change is already in place),
the call makes **no commit**. The report then has `commit_id` 0, the
head commit does not move, and the commit message is not recorded:

```text
imported base.arrows: no changes (nothing committed), 2 segment(s), 4 value(s) unchanged
```

```python
report = db.write_parameter("node", "demand", "high_demand", batch,
                            user="alice", message="same data again")
report["commit_id"], report["values_unchanged"]   # (0, 1)
```

Use `head_commit` when you need the commit the data is current at. A
call that changes at least one value commits as usual, and only the
changed values get the new commit. `--force-replace` (or
`force_replace`) only decides what the replaced versions keep; it does
not make the core rewrite unchanged values. A call may not write a
value and delete the same value; the core refuses it before writing
anything. Likewise a call may not write a value and delete an object
that the value belongs to: its entity (or an entity it is an element
of), its parameter, its alternative or its class. The core refuses
such a call whatever the values and whatever the `cascade` option.
Write the values and delete the objects in separate calls.

A script that has to send such a batch as one call can make the delete
win instead: pass `covered_writes="delete_wins"` to `commit_changes` in
Python, or set `covered_writes: CoveredWrites::DeleteWins` in the
`WriteOptions` in Rust. The core then drops every write that a delete in
the call covers before anything is written (a dropped write creates no
entity, parameter or alternative either), runs the deletes as usual and
writes the rest. A covered write is dropped only when a delete that
covers it takes effect: a value delete, or an object delete that
actually deletes its object (or whose object does not exist). If none
does, for example a delete with `cascade` off that the object's other
dependents block, the core refuses the whole call and commits nothing,
because dropping the write would lose it while the object stays. Each
dropped write is listed in the report as a
`dropped_by_delete` warning, with the segment's index and the index of
one covering delete that took effect (`by` names the list, `deletes` or
`object_deletes`; a value delete is named first if there is one):

```python
report = db.commit_changes(
    segments=[{"class": "node", "parameter": "demand",
               "alternative": "Base", "batch": batch}],
    object_deletes=[{"kind": "entity", "class": "node", "name": "north",
                     "cascade": True}],
    user="alice", covered_writes="delete_wins")
report["warnings"]
# [{'kind': 'dropped_by_delete', 'segment_index': 0, 'by': 'object_deletes',
#   'key_index': 0, 'object_type': 'value', 'name': 'node:demand/Base/north',
#   'id': 0}]
```

Names are compared the way the database resolves them, and an object
delete covers everything its cascade would reach (an entity's
containers, a class's entities and parameters), whatever its `cascade`
option. A dropped write is not a change: if the deletes in the call find
nothing to delete either, the call makes no commit.

Under either setting, a call may not create an object with a structural
change and delete that object (or the class it belongs to, or an entity
it is built from) with an object delete; the core refuses it. Create and
delete in separate calls. The core checks this twice, and each check has
an edge worth knowing:

- Before anything runs, it compares names. A create and a delete of the
  same name are refused even when the object already exists and the
  create would change nothing.
- After the structural changes ran, it compares the objects the delete
  would reach with the objects this call actually created (an existing
  object that a create left as it was does not count). The delete
  reaches everything its cascade would, whatever its `cascade` option:
  creating an entity built from `north` and deleting `north` with
  `cascade` off is refused, although that delete alone would only be
  blocked.

### Build a scenario

A scenario is an ordered list of alternatives; later entries take
precedence.

=== "Python"

    ```python
    db.commit_structural([
        {"op": "create_scenario", "name": "stress"},
        {"op": "set_scenario_alternatives", "scenario": "stress",
         "alternatives": ["Base", "high_demand"]},
    ], user="alice", message="stress scenario")
    ```

=== "Rust"

    `api::commit_structural` (or the `structural` argument of
    `api::commit_changes`) takes a list of `types::StructuralChange`
    values, such as `CreateScenario` and `SetScenarioAlternatives`.

The CLI has no scenario commands; use Python or Rust, or a template.

### Read with SQL

Reads use SQL views and table macros. Any DuckDB client of version 2.0
or newer works (see [DuckDB version](#duckdb-version)). Open
the file **read-only** and close the connection when done:

```python
import duckdb

con = duckdb.connect("demo.duckdb", read_only=True)
try:
    print(con.sql("""
        SELECT entity, alternative, tier, idx1_ts, v_float
        FROM read_parameter('node', 'demand')
        ORDER BY entity, alternative, idx1_ts"""))
finally:
    con.close()
```

```text
┌─────────┬─────────────┬─────────┬─────────────────────┬─────────┐
│ entity  │ alternative │  tier   │       idx1_ts       │ v_float │
├─────────┼─────────────┼─────────┼─────────────────────┼─────────┤
│ north   │ Base        │ array   │ 2030-01-01 00:00:00 │    10.0 │
│ north   │ Base        │ array   │ 2030-01-01 01:00:00 │    12.0 │
│ ...     │             │         │                     │         │
│ north   │ high_demand │ array   │ 2030-01-01 00:00:00 │    14.0 │
│ ...     │             │         │                     │         │
│ south   │ Base        │ array   │ 2030-01-01 03:00:00 │     5.2 │
└─────────┴─────────────┴─────────┴─────────────────────┴─────────┘
```

`read_parameter(class, parameter)` returns every current value of one
parameter, across all alternatives, in one long shape: `idx1`…`idx4`
columns per index kind (`_text`, `_ts`, `_num`, `_ordinal`, `_coord`)
and `v_*` columns per value kind (`v_float`, `v_int`, `v_bool`, `v_str`,
`v_datetime`, …), then the period ends `idx1_end_ts`…`idx4_end_ts`
(a period index is `[idxN_ts, idxN_end_ts)`). New columns may be
appended at the end in later versions: select columns by name.
[The read surface](dev/read_surface.md) documents every column. The index columns are numbered by position; the
declared dimension names are in `parameter_type_dimension`. If you
filter array values on your own DuckDB connection, see
[DuckDB 2.0: filtered reads of long arrays](#duckdb-version) first.

Commonly used parts of the read surface:

| Query | Returns |
|---|---|
| `read_parameter(class, parameter)` | current values of one parameter, all alternatives |
| `read_parameter_scalar(class, parameter)` | scalar values only; faster for large scalar parameters |
| `read_parameter_dense(class, parameter)` | dense-array values as lists with axis ids, for bulk reads |
| `read_value(class, parameter, entity[, alternative])` | one value, in `read_parameter`'s shape; all alternatives if `alternative` is omitted |
| `read_values(class, parameter, entities[, alternatives])` | a few values (lists of names), in `read_parameter`'s shape |
| `value_rows_for(parameter_definition_id, entity_ids, alternative_ids)` | the same by ids, in `value_rows`' shape |
| `resolved_value_rows(scenario)` | the winning value per (entity, parameter) in a scenario |
| `scenario_value_conflict` | values that compete within a scenario, before resolution |
| `scenario_contents(scenario)` | entities, activation and group membership in a scenario |
| `value_rows`, `value_cells` | all current values as long rows |
| `<table>_asof(commit)` | a table as of an earlier commit, e.g. `value_scalar_asof(3)` |
| `value_array_asof_for(commit, parameter_definition_id, entity_id, alternative_id)` | one array value as of an earlier commit (see [Read history](#read-history)) |
| `"commit"` | the commit log: id, timestamp, user, message |
| `audit_health`, `audit_*` | integrity, completeness and maintenance reports |
| `nodal_read_surface()` | the version of the views and macros the file carries (see [Old files](#old-files-refresh-the-read-surface)) |

Scenario resolution:

```sql
SELECT entity, parameter, alternative, rank, idx1_ts, v_float
FROM resolved_value_rows('stress')
WHERE parameter = 'demand'
ORDER BY entity, idx1_ts;
```

```text
┌─────────┬───────────┬─────────────┬───────┬─────────────────────┬─────────┐
│ entity  │ parameter │ alternative │ rank  │       idx1_ts       │ v_float │
├─────────┼───────────┼─────────────┼───────┼─────────────────────┼─────────┤
│ north   │ demand    │ high_demand │     2 │ 2030-01-01 00:00:00 │    14.0 │
│ ...     │           │             │       │                     │         │
│ south   │ demand    │ Base        │     1 │ 2030-01-01 00:00:00 │     5.0 │
│ ...     │           │             │       │                     │         │
└─────────┴───────────┴─────────────┴───────┴─────────────────────┴─────────┘
```

`north` takes its demand from `high_demand` (rank 2); `south` has no
value there and falls back to `Base`.

The Python package also offers reads that manage the connection for you:

```python
db = nodal_db.open("demo.duckdb")
batch = db.read_parameter("unit", "capacity", "Base")    # pyarrow.RecordBatch
table = db.query("SELECT * FROM audit_health").read_all()  # pyarrow.Table
# a few values (the read_values macro): one entity in every alternative,
# then two entities in one alternative
coal = db.read_values("unit", "capacity", ["coal"])
pair = db.read_values("unit", "capacity", ["coal", "wind"], ["Base"])
```

`db.read_values(class, parameter, entities, alternatives=None)` follows
the rules of [Reading one value or a few](#reading-one-value-or-a-few);
Rust code calls `api::read_values` the same way.

For interactive browsing, the DuckDB CLI works too, provided it is
version 2.0 or newer: `duckdb -readonly demo.duckdb`, or `duckdb -ui
demo.duckdb` for the browser UI.

#### Reading one value or a few

`read_parameter` reads a whole parameter: a filter written outside it
(`WHERE entity = 'north'`) is applied after every array of the parameter
has been read. To read one entity, or a few, use `read_value` or
`read_values`. They return the same columns and rows as the filtered
`read_parameter`, but read only the arrays they need, so a point read
costs milliseconds instead of a read of every array of the parameter:

```sql
-- one value
SELECT idx1_ts, v_float FROM read_value('node', 'demand', 'north', 'Base');
-- one entity in every alternative (the alternative argument omitted)
SELECT alternative, idx1_ts, v_float FROM read_value('node', 'demand', 'north');
-- a few entities in two alternatives
SELECT * FROM read_values('node', 'demand', ['north', 'south'], ['Base', 'high_demand']);
```

- Names match case-insensitively, like the class and parameter names.
  Omitting the alternatives (or passing `NULL`) means every current
  alternative; an empty list means none.
- Keep the reads small: up to **50 value hits** per call, where a hit is
  an array value that exists for one of the (entity, alternative)
  pairs, so 17 entities in 3 alternatives is already 51. Above that the
  read falls back to scanning every array of the table, which costs as
  much as `read_parameter`; for a whole parameter use `read_parameter`
  (or `read_parameter_dense` in engine code).
- `value_rows_for(parameter_definition_id, entity_ids, alternative_ids)`
  is the same read by ids, with the columns of `value_rows`.
- Row order is unspecified, as for `read_parameter`: add an `ORDER BY`.
- They are not affected by the DuckDB 2.0 error on filtered array reads
  (see [the warning under DuckDB version](#duckdb-version)), so a filtered
  read of one or a few series, such as one series in one time window,
  works on any connection, whatever its settings.


### Read history

After the coal plant's capacity is updated from 400 to 450 in commit 5,
the old row is still there, closed out:

```sql
SELECT entity_id, v_float, version, valid_from_commit, valid_to_commit
FROM value_scalar ORDER BY entity_id, version;
```

```text
┌───────────┬─────────┬─────────┬───────────────────┬─────────────────┐
│ entity_id │ v_float │ version │ valid_from_commit │ valid_to_commit │
├───────────┼─────────┼─────────┼───────────────────┼─────────────────┤
│         3 │   400.0 │       1 │                 2 │               5 │
│         3 │   450.0 │       2 │                 5 │            NULL │
│         4 │   250.0 │       1 │                 2 │            NULL │
└───────────┴─────────┴─────────┴───────────────────┴─────────────────┘
```

and an as-of read returns the earlier state:

```sql
SELECT entity_id, v_float FROM value_scalar_asof(3);   -- 400.0 and 250.0
```

For one array value, `value_array_asof_for(commit, parameter_definition_id,
entity_id, alternative_id)` returns the rows of `value_array_asof(commit)`
for that key. It fetches a value that has not changed since `commit` by
index, in milliseconds; a value that changed after `commit` still scans
the stored history of every array, as `value_array_asof` does.

### Erase an alternative

Erasure physically removes all data of one alternative and leaves an
`erasure_marker` (see [Features](features.md#erasure-with-markers)). It
is intended for output databases, such as replacing the results of a
superseded run. Preview first:

=== "CLI"

    ```bash
    nodal-db erase-alternative demo.duckdb high_demand --preview
    nodal-db erase-alternative demo.duckdb high_demand --note "superseded" --user alice
    ```

    ```text
    would erase alternative 'high_demand' (id 2, identity current — will be closed):
      value_array                       1 rows
      scenario_alternative              1 rows
      total 2 rows, ~64 payload bytes (advisory)
      note: scenario 'stress' (id 1) loses a rank row
    ```

    `--expect-present NAME` makes the erase refuse unless the named
    replacement alternative exists.

=== "Python"

    ```python
    db.preview_erase_alternative("high_demand")
    db.erase_alternative("high_demand", user="alice", note="superseded")
    ```

=== "Rust"

    `erasure::preview_erase_alternative` and `erasure::erase_alternative`
    (or `erasure::erase_alternative_and_compact`).

After the erase, as-of reads at earlier commits fail:

```text
duckdb.InvalidInputException: Invalid Input Error: as-of 3 is unreliable:
content erased at a later commit — see erasure_marker / audit_erasure
```

`SELECT * FROM audit_erasure` shows what was erased, by whom and when.
The file shrinks only after compaction.

### Extract a database without its history

A [history-free extract](features.md#history-free-extract) writes a new
file with the current content of a database and none of its superseded
versions. The source file is only read.

=== "CLI"

    ```bash
    nodal-db extract demo.duckdb demo-slim.duckdb --note "for sharing"
    nodal-db extract demo.duckdb demo-at-3.duckdb --as-of 3
    ```

    On a tiny database the file barely shrinks: a fixed per-file overhead
    dominates. The size gain grows with the amount of superseded history.

    ```text
    extracted the current state (commit 4) of demo.duckdb into demo-slim.duckdb
      source identity   ad485e98-1793-4189-b965-06d2f02f3c10
      extract identity  e4a3d2be-3de7-4675-b234-6046029c2829
      horizon commit 4, extract commit 5, horizon marker 1
      size 10.8 MiB -> 10.8 MiB; 5 rows left out, 0 unreferenced axes dropped
    note: the extract has a NEW database identity; as-of reads before commit 5 fail in it (the source keeps the full history)
    ```

=== "Python"

    ```python
    report = db.extract("demo-slim.duckdb", user="alice", note="for sharing")
    slim = nodal_db.open("demo-slim.duckdb")
    ```

    `as_of=3` extracts the state right after commit 3. The returned
    dict has the identities, the horizon and extract commits, the rows
    left out per table and the file sizes.

=== "Rust"

    `extract::extract(src, dest, &ExtractOpts { as_of, user, note })`,
    returning an `ExtractReport`.

To compact a database in place instead (keeping all history), use
`nodal-db maintain compact demo.duckdb` or `db.compact()`.

`--if-needed` (Python: `db.compact(if_needed=True)`) compacts only when
it is due: when the file has grown to three times its size after the
last compaction (`--growth`), or when 50 commits have landed since then
(`--max-commits`; the default grows with the file size above about
70 MiB, to about 100 at 400 MiB and at most 200; `0` in the CLI or
`None` in Python disables it). The commit count matters
even when the file does not grow: every commit makes each later open of
the file slightly slower until the next compaction. The size and commit
of the last compaction are kept in a `demo.duckdb.compaction-baseline`
file next to the database; keep it with the database or delete it, never
edit it.

```text
$ nodal-db maintain compact demo.duckdb --if-needed
demo.duckdb: no compaction needed (10.8 MiB; below the growth and commit thresholds)
```

A compaction usually makes the file smaller, but it does not have to: a
file that was compacted recently and has had few commits since is already
densely packed. With `--if-needed`, if the rebuilt file is not smaller,
nodal-db keeps the original file, prints a warning, and still records the
size and commit, so it does not try again until the next threshold. The
Python report then has `kept_original: True` and the rebuilt size in
`bytes_rebuilt`. A compaction due to the commit count is the exception:
it keeps the original only when the rebuilt file is larger, because it
pays off through faster opens even when the file stays the same size.
A plain `nodal-db maintain compact` (or `db.compact()`) always replaces
the file, because you asked for it, and prints a warning
when the result is not smaller. That is rare; the warning is there so
that you notice it.

A database written before 2026-10-24 gets the smaller long-tier
index (in a benchmark, about 30 instead of about 80 bytes per long
cell) on its next compaction, automatic or `nodal-db maintain compact`,
or on an extract. Nothing else changes: the old index enforces exactly
the same rule.

On a very large database with a long history, a compaction takes minutes
(about six minutes for a 1.4 GB file with 228 million long-table rows).
It holds the database lock for that time, so run it when nobody needs to
write.

!!! note "Identity after swapping files"
    An extract is a new database with its own identity. If you replace
    your working file with its extract (extract, check the result, then
    move the extract to the old path), the database at that path now has
    a different identity than before. Anything that refers to the old
    identity, such as an output database that records which input
    database it was produced from, refers to the old file. Keep that
    file as the archive: the extract's horizon marker
    (`SELECT continuation_db FROM erasure_marker WHERE kind = 'horizon'`)
    names it, and its full history stays readable there.

### Import a Spine database

The `nodal_spine_import` package in `python/` imports a Spine Toolbox
database into a new nodal-db file. It needs `spinedb_api` and the
`nodal_db` package installed, and `python/` on the Python path:

```python
from nodal_spine_import import SpineImporter, Profile

class MyProfile(Profile):
    template_name = "my_model"

report = SpineImporter("model.sqlite", "model.duckdb", MyProfile(),
                       user="alice", version="7").run()
print(report.summary())
```

```text
import model.sqlite -> model.duckdb [my_model @ 7]
counts: activation_rows=1, alternatives=2, classes=3, entities=4, ...
drift rows: 0
residue items: 0
```

`version` is the source schema version to record; a profile can instead
read it from the source by overriding `version_of`. The report lists
everything that could not be imported.

**The import creates a new file, all or nothing.** The importer builds
the database in a temporary directory next to the target and moves it
to the target path only when every write has succeeded. If the import
fails, nothing is left at the target path and the temporary files are
removed. If the target path already exists, the import refuses to
start and leaves the existing file as it is.

**Duplicate keys.** Spine stores a map as two parallel lists, so a map
can repeat a key, for example `{"a": 1, "b": 2, "a": 3}`. A nodal-db
value holds one value per index, so the importer keeps the **last**
entry for a repeated key and imports the rest of the value normally.
This is the entry a Python `dict` built from the map keeps. As in a
`dict`, the key keeps the place where it first appears, which sets the
label order: `{"b": 1, "a": 2, "b": 3}` imports as `b = 3`, `a = 2`,
with `b` ordered before `a`. The same rule applies at every level of a
nested map. A repeated key in an
outer map keeps the last entry's whole sub-map; the earlier sub-maps
are not merged into it. A time stamp repeated in a time series is
treated the same way.

Each repeated key is a problem in the source data, so the importer
reports it as a warning: in `report.duplicate_keys`, in
`report.summary()`, and through Python's `logging` (logger
`nodal_spine_import`). The warning names the class, entity, parameter
and alternative, the path of keys down to the map level, every
conflicting entry with its position in that map, and which entry was
kept:

```text
duplicate keys: 1 (last entry kept)
  duplicate key: class 'hub', entity 'h1', parameter 'settings', alternative 'Base': key 'a' appears 2 times at the top level of the value (entry 0: 1.0; entry 2: 3.0); kept the last, entry 2. A map holds one value per key: fix the source so the key appears once.
```

The last entry is a choice, not necessarily what the author of the
source meant. Fix the source so that each key appears once, then
import again.

**Names with leading or trailing whitespace.** A nodal-db name must be
non-empty, have no leading or trailing whitespace, and contain no
control characters. Spine allows names such as `"coal "`, so the
importer removes the leading and trailing whitespace from the name of
every class, parameter, entity, alternative, scenario and value list,
and imports it under the trimmed name. Every reference uses the trimmed
name too: the dimensions of a class, the elements of an entity, the
alternatives of a scenario, activation rows, parameter values, and a
parameter's value list.

Only names are trimmed. Map keys (index labels), string values and value
list members are data and are imported exactly as they are.

Each trimmed name is reported as a warning, in the same places as a
duplicate key: in `report.trimmed_names`, in `report.summary()`, and
through the `nodal_spine_import` logger. The warning names the kind of
name, its class (for a parameter or an entity), the name as written in
the source, and the name it was imported as:

```text
trimmed names: 1 (imported without leading/trailing whitespace)
  trimmed name: entity 'g1 ' in class 'gadget' has leading or trailing whitespace; imported as 'g1'. Names in nodal-db are trimmed: fix the source name to stop this warning.
```

Trimming does not fix every name. The import stops with an error, and
leaves no database behind, when:

- trimming would give two names of the same kind the same name, for
  example the entities `"g1"` and `"g1 "` in one class, or the
  alternatives `"high"` and `" HIGH"` (names are compared without
  regard to case). The error lists the colliding names as written in
  the source;
- a name is empty once trimmed, for example `"  "`;
- a name contains a control character, such as a tab or a line break,
  anywhere but at its start or end.

## For integrators

Tools that read or write nodal-db databases must respect a few
boundaries. The full contract is in
[Building against nodal-db](dev/building_against_nodal-db.md); the
reasoning is in [Architecture](dev/architecture.md).

### Writing: two entry points

All writes go through the Rust core, reached in one of two ways:

1. **The core API**, directly from Rust (`nodal-db-core`) or through the
   Python package, which contains no logic of its own.
2. **`nodal-db import`** with an Arrow-IPC bulk file, from any language.

Do not write to the base tables with SQL, and do not reimplement the
canonical encoding, tier decision or history protocol in a client. These
are deterministic functions that must give identical results for every
writer. The conformance vectors in `conformance/` define them; any
implementation that touches canonical data must reproduce them exactly.

### Reading: SQL is the interface

Read through the views and table macros (`schema/070_*.sql` to
`schema/130_*.sql`), not the base tables. The base-table layout is not a
stable interface; the read surface is. [The read surface](dev/read_surface.md)
is its contract: what is public, which read to use, the column
conventions and a reference of every column. The [schema page](schema.md)
shows the tables for reference.

### Old files: refresh the read surface

The views and macros live inside each database file, as the nodal-db
that created the file wrote them. A newer nodal-db brings its own:

- **The next write upgrades the file** (about 0.2 s, once): nodal-db
  replaces the file's views and macros with its own before the write.
  Data and history are not touched and no commit is recorded. Your own
  views are kept. A table of yours that uses a name of nodal-db's views
  blocks the upgrade; the write still goes through and its report says
  so (`read_surface_warning`).
- **Reading without writing works**, but warns once per file and is a
  little slower: nodal-db reads the file through temporary copies of its
  own views where it needs them.
- **Upgrade a file you only read** with the command, or from Python:

```sh
nodal-db maintain refresh-views demo.duckdb --check   # report only; exit 0 current, 1 older, 2 newer
nodal-db maintain refresh-views demo.duckdb
```

```python
db.read_surface_status()["status"]   # "current", "older", "missing", "newer", ...
db.refresh_read_surface()             # no-op when current
```

On your own DuckDB connection, `SELECT * FROM nodal_read_surface()`
shows the file's version (`contract_major`, `revision`, `ddl_hash`); an
error means the file predates versioning and should be refreshed. A
file written by a *newer* nodal-db is never downgraded by an older one,
except by compaction, which says so in its warning. Names of nodal-db's
views and macros are reserved: give your own objects other names.

!!! warning "Do not call table macros through an attached database"
    When one connection reads two nodal-db files (`ATTACH 'other.duckdb'
    AS other`), views work as expected: `other.value_scalar_current`
    reads `other.duckdb`. Table macros do not. DuckDB resolves the
    tables inside a macro in the connection's *default* database, so
    `other.value_scalar_asof(5)` or `other.read_parameter('unit',
    'capacity')` silently returns rows from the default database, with
    no error. To use a macro of another file, make it the default first
    (`USE other`, then switch back), or open the file on its own
    connection. Do not store a view that calls another database's macro.

### Connection discipline

!!! warning "Hold connections per operation, never long-lived"
    DuckDB's file lock applies to readers too: while any process holds a
    connection to the file, even a read-only one, a writer cannot open it
    to commit. A notebook or GUI that keeps a connection open blocks
    every write until it closes.

    Open, query, close. Do not keep a connection open between queries.
    The Python `nodal_db.Database` handle follows this rule internally;
    it stores only the path.

To detect new commits, poll the newest commit id with a short read:

```sql
SELECT max(id) FROM "commit";
```

To write only if nobody else has committed since you read, pass the
commit id you read as `expect_commit`. The core checks it inside the
write transaction, so no other commit can slip in between the check and
the write. If the database has moved on, nothing is written and the call
fails with `Stale`:

=== "Python"

    ```python
    head = db.head_commit()
    # ... read, compute ...
    try:
        db.write_parameter("unit", "capacity", "Base", batch, user="alice",
                           expect_commit=head)
    except nodal_db.Stale as e:
        print(f"someone committed: expected {e.expected}, found {e.found}")
    ```

    `write_parameter`, `commit_changes`, `commit_structural` and
    `delete_values` take `expect_commit`. `Stale` is a subclass of
    `ApiError`.

=== "Rust"

    ```rust
    let head = api::head_commit(db_path)?;
    let opts = WriteOptions { expect_commit: Some(head), ..Default::default() };
    match api::commit_changes(db_path, segments, vec![], vec![], vec![],
                              "alice", "", opts) {
        Err(DbError::Stale { expected, found }) => { /* reload, re-apply */ }
        other => { other?; }
    }
    ```

    Every `api` write entry point honors `WriteOptions::expect_commit`.
    `history::write_value` ignores it; inside a raw
    `conn::with_write_txn_retrying_opts`, pass `TxnOptions`.

The check is an exact match: a head *below* the expected id (the file was
replaced or restored from a backup) is also `Stale`. Compaction keeps
commit ids, so it never makes a write stale.

Expect contention. A writer that cannot get the lock fails with a
defined error (`DbError::Busy` in Rust, `nodal_db.Busy` in Python) after
a bounded retry; a reader may briefly fail to open while a commit is in
progress and should retry.

A database file made by another nodal-db schema version is reported as
`SchemaMismatch` (`DbError::SchemaMismatch { found, expected, .. }` in
Rust, `nodal_db.SchemaMismatch` with `found` and `expected` attributes
in Python, a subclass of `ApiError`): a write that fails on such a file,
and compaction, extract and erase, which refuse it up front. `found` is
the file's `schema_version`, `expected` this build's. It is never
retried. Re-create the database with this build (init, then import), or
use a nodal-db build that matches the file.

### DuckDB version

!!! warning "Readers need DuckDB 2.0 or newer"
    Database files are written by the DuckDB engine bundled in the core,
    currently 2.0.0, in DuckDB's native 2.0 storage format, and the schema
    is verified on the 2.0 line. A 1.x DuckDB, such as an outdated
    standalone CLI or an older `duckdb` Python package, refuses to open
    the file with a storage-version error. The DuckDB 2.0 Python client
    needs Python 3.11 or later. Check with
    `duckdb --version` or `python -c "import duckdb; print(duckdb.__version__)"`.

!!! warning "DuckDB 2.0: filtered reads of long arrays can fail"
    On DuckDB 2.0.x, a query that filters `value_rows`, `value_cells`
    or `resolved_value_rows` (which reads `value_rows`) on array values,
    such as a time window
    (`WHERE idx1_ts BETWEEN …`) or a threshold (`WHERE v_float > …`),
    can fail with `INTERNAL Error: dict-surviving cache: …` on a
    database with several hundred long arrays (for example 8760 hourly
    values each). This is a DuckDB bug, not damaged data. The error also
    invalidates the database in your process: close every connection to
    the file, then connect again (a new connection opened while an old one
    is still open fails with "database has been invalidated").

    The core and the Python package (`db.query`, `db.read_parameter`)
    avoid it already. On your own DuckDB connection, turn off the
    setting that triggers it, either when you open the file or with
    `SET`:

    ```python
    con = duckdb.connect("demo.duckdb", read_only=True,
                         config={"debug_enable_caching_operators": "false"})
    ```

    ```sql
    SET debug_enable_caching_operators = false;
    ```

    If the file is already open in the same process, `config=` fails
    with a "different configuration" error: use `SET` on that connection
    instead. For the DuckDB CLI, run the `SET` statement first. Unfiltered reads,
    aggregates over a whole parameter and `read_parameter_dense` are
    not affected. Neither are `read_parameter`, `read_value` and
    `read_values`: filter those and no setting is needed; for one or a
    few series (one series in one time window) `read_value` and
    `read_values` are also much faster (see
    [Reading one value or a few](#reading-one-value-or-a-few)).

### Arrow versions

The core and the Python package are built against one pinned Arrow
version. Rust code should use the Arrow types re-exported from
`nodal_db_core::duckdb::arrow`. Python code can pass any object that
implements the Arrow PyCapsule interface; `pyarrow` 16 or later is
required at runtime.
