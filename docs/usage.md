# Using nodal-db

The first part of this page walks through creating a database, writing
data and reading it back. The second part covers the rules that tools
integrating with nodal-db must follow.

The examples share one small data set: a `node` class with an hourly
`demand` series, a `unit` class with a scalar `capacity`, a `Base`
alternative and a `high_demand` alternative stacked on top of it in a
scenario called `stress`. The outputs shown were produced by running
these examples.

## Getting started

### Build

nodal-db has no binary releases yet. Build it from a checkout of the
repository. You need a Rust toolchain; for the Python parts, Python 3.9
or later.

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
    imported base.arrows: commit 2, 2 segment(s), 4 value(s), 4 row(s) written, axes 1 created / 1 reused, tiers [array, scalar]
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

Reads use SQL views and table macros. Any DuckDB client of version 1.5
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
`v_datetime`, …). The index columns are numbered by position; the
declared dimension names are in `parameter_type_dimension`.

Commonly used parts of the read surface:

| Query | Returns |
|---|---|
| `read_parameter(class, parameter)` | current values of one parameter, all alternatives |
| `read_parameter_scalar(class, parameter)` | scalar values only; faster for large scalar parameters |
| `read_parameter_dense(class, parameter)` | array-tier values as lists with axis ids, for bulk reads |
| `resolved_value_rows(scenario)` | the winning value per (entity, parameter) in a scenario |
| `scenario_value_conflict` | values that compete within a scenario, before resolution |
| `scenario_contents(scenario)` | entities, activation and group membership in a scenario |
| `value_rows`, `value_cells` | all current values as long rows |
| `<table>_asof(commit)` | a table as of an earlier commit, e.g. `value_scalar_asof(3)` |
| `"commit"` | the commit log: id, timestamp, user, message |
| `audit_health`, `audit_*` | integrity, completeness and maintenance reports |

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
```

For interactive browsing, the DuckDB CLI works too, provided it is
version 1.5 or newer: `duckdb -readonly demo.duckdb`, or `duckdb -ui
demo.duckdb` for the browser UI.

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
`schema/120_*.sql`), not the base tables. The base-table layout is not a
stable interface; the read surface is. The [schema page](schema.md) shows
the tables for reference.

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

Expect contention. A writer that cannot get the lock fails with a
defined error (`DbError::Busy` in Rust, `nodal_db.Busy` in Python) after
a bounded retry; a reader may briefly fail to open while a commit is in
progress and should retry.

### DuckDB version

!!! warning "Readers need DuckDB 1.5 or newer"
    Database files are written by the DuckDB engine bundled in the core,
    currently 1.5.5, and the schema is verified on the 1.5 line. An older
    DuckDB, such as an outdated standalone CLI, refuses to open the file
    with a storage-version error. Check with
    `duckdb --version` or `python -c "import duckdb; print(duckdb.__version__)"`.

### Arrow versions

The core and the Python package are built against one pinned Arrow
version. Rust code should use the Arrow types re-exported from
`nodal_db_core::duckdb::arrow`. Python code can pass any object that
implements the Arrow PyCapsule interface; `pyarrow` 16 or later is
required at runtime.
