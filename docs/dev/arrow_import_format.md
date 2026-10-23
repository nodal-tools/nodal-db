# Arrow-IPC import format

`nodal-db import` reads parameter values from a file in the Arrow-IPC
bulk format. This page describes that format for tool authors who
produce such files from Julia, R, Rust, Python or any other language
with an Arrow library. It is the write path for every client that does
not link against the Rust core.

The importer does no interpretation of its own. It checks the file,
makes a few purely structural conversions (dictionary decoding,
timestamp unit upcasting, dense-to-long expansion) and passes the data
to the same core that every other write goes through. Canonical
encoding, axis derivation and the
[storage tier](../features.md#storage-tiers) are decided there, not by
the file.

## Format version

This page describes **format version 1.0**.

Every stream in a file declares its version in the
`spinal:format_version` metadata key as `MAJOR.MINOR`. The importer
applies this rule:

| File declares | Importer behaviour |
|---|---|
| a different major version | rejected before anything is written (E3) |
| the same major, any minor (also a newer one) | import is attempted |

The compatibility promise that makes the second row safe:

- A **minor** version only adds constructs that an older importer
  cannot misread: a new Arrow type, a new `spinal:form` value, or a new
  optional metadata key whose absence changes nothing. A file that uses
  such a construct fails on an older importer with a specific error; it
  is never imported with a different meaning.
- Anything that changes the meaning of a construct that is already
  valid is a **major** change. Examples: a new required key, a change in
  how a column type maps to an index kind, a key that overrides a
  column's kind, or a calendar or timezone annotation that changes what
  stored timestamps mean.

!!! warning "Write `1.0`"
    Producers should write `spinal:format_version = "1.0"`. The value
    must be decimal `MAJOR.MINOR` with no leading zeros (`1.0`, not
    `01.0` or `1`); anything else is E3.

!!! note "The `spinal:` prefix"
    The metadata keys use the prefix `spinal:` and the format name
    `spinal-db-bulk`, from the project's earlier name. They are part of
    the 1.0 format and will not change within major version 1.

## Container

An import file is **one or more complete Arrow IPC streams written
back-to-back** into one file. Each stream is called a *segment*.

- Use the Arrow IPC **stream** format (`pyarrow.ipc.new_stream`;
  in Julia, `Arrow.write` to an open IO handle without `file=true`;
  `write_ipc_stream` in R; `StreamWriter` in arrow-rs). The IPC **file** format (Feather v2, starting with the
  magic bytes `ARROW1`) is rejected (E1).
- Each segment has its own schema, carries its own metadata, and
  targets exactly one `(class, parameter, alternative)`.
- Each segment must end with the 8-byte end-of-stream marker
  (`ff ff ff ff 00 00 00 00`). Stock writers write it when the writer is
  closed or finished. A segment that ends without it is rejected as a
  truncated file (E10). Streams in the legacy pre-0.15 Arrow framing
  (4-byte end marker) are also rejected.
- The next segment starts at the byte directly after the previous
  segment's end marker. No padding or separators; trailing bytes that do
  not start a valid stream are an error (E6).
- The conventional file extension is `.arrows`; the importer does not
  check it.

A segment may contain any number of record batches; they are treated as
one table. Rows of one entity may be spread across batches. A segment
with zero rows in total is an error (E6): nothing is written and nothing
is auto-created from it.

Producing the container needs no special library support: open one
file, run your Arrow library's stream writer once per segment on the
same file handle, and close each writer before starting the next. You
can also write each segment to its own file and concatenate the files
byte-for-byte (`cat a.arrows b.arrows > all.arrows`).

!!! warning "Never append to a multi-segment file"
    Append APIs such as Julia's `Arrow.append` extend the stream they
    find in the file. Called on a file that already holds two or more
    segments, they corrupt it without reporting an error. Append only to
    a single-segment file, then concatenate the finished files.

Write the file to a temporary path and rename it into place after the
last writer has closed, so that a crash never leaves a half-written file
under the final name.

### Encodings

| Encoding | Accepted |
|---|---|
| Dictionary-encoded top-level columns | yes, decoded to the plain type |
| Dictionary-encoded elements inside a dense-form list | **no** (E6) |
| Buffer compression `ZSTD`, `LZ4_FRAME` | yes |
| Run-end encoding | **no** (E6) |

## Segment metadata

Schema-level metadata on each segment (string keys and values):

| Key | Required | Value |
|---|---|---|
| `spinal:format` | yes | exactly `spinal-db-bulk` (else E2) |
| `spinal:format_version` | yes | `1.0` (see [Format version](#format-version)) |
| `spinal:segment_count` | yes | total number of segments in the file, as a positive decimal integer; the same on every segment |
| `spinal:class` | yes | target entity class name |
| `spinal:parameter` | yes | target parameter name |
| `spinal:form` | yes | `long` or `dense` |
| `spinal:alternative` | no | target alternative name (see below) |

A missing `spinal:segment_count`, `spinal:class`, `spinal:parameter`
or `spinal:form`, or a `spinal:form` other than `long`/`dense`, is E4.
A missing `spinal:format` is E2 and a missing `spinal:format_version`
is E3. `spinal:segment_count` lets the importer detect a file that was cut
off cleanly between two segments: if the segments disagree on the count,
or the file holds a different number of segments than declared, the
import fails with E10.

Unknown keys in the `spinal:` namespace, in schema or field metadata,
produce a warning and are otherwise ignored (E5). Keys outside the namespace are ignored silently,
so other tools may add their own metadata.

### Targets

Classes, parameters, entities and alternatives are referenced **by
name**. Name matching is case-insensitive (`Flow` and `flow` are the
same parameter).

The alternative of a segment is taken from `spinal:alternative` if
present, otherwise from the `--alternative` command-line option. If
neither is given, the import fails (E4). If both are given, they must
match case-insensitively (E4 otherwise); one never silently overrides
the other.

Two segments in one file must not target the same
`(class, parameter, alternative)`, compared case-insensitively (E7).

Missing referents are **created automatically** and reported as
warnings: the entity class, the parameter definition, the alternative
and each entity. Two exceptions:

- An entity of a multi-dimensional class (such as `unit__node`) is not
  auto-created, because its identity is its member tuple, which the file
  does not carry. Create such entities first (for example with a
  template); otherwise the import fails (E6).
- When the parameter definition already exists with declared types, the
  segment must match one of them: the same rank, the same index kind at
  each position and the same value kind. A segment of a new shape is
  rejected (E6); an import never widens a declared type.

## Long form

`spinal:form = long` carries values as one row per index tuple. The
columns are identified **by position**:

| Position | Name (recommended) | Arrow type | Content |
|---|---|---|---|
| 0 | `entity` | `utf8` | entity name |
| 1 … rank | the dimension name | see [Index kinds](#index-kinds) | index coordinate |
| last | `value` | see [Value kinds](#value-kinds) | the value |

Rank is the number of columns minus two, from 0 to 4. A rank-0 segment
(a scalar parameter) has the two columns `(entity, value)` and one row
per entity.

The column names of the dimension columns become the dimension names
when the parameter definition is auto-created. For an existing
definition they are not checked; dimensions match by position.

The names `entity` and `value` are recommended rather than required,
but if a column is named `entity` or `value` (case-insensitively), it
must be in its slot: `entity` first, `value` last. Otherwise the import
fails (E6). This catches swapped columns in a rank-0 segment with a
string value, where both columns are `utf8`. If neither name appears,
the importer prints a warning and reads the columns by position.

Rows need not be sorted; the core sorts during canonicalization.
Grouping rows by entity is good for locality.

### Nulls and absence

- A null in the `entity` column or in any dimension column is an error
  (E6).
- A null in the `value` column means **the cell is absent**: the index
  tuple is dropped, exactly as if the row had not been written.
- An entity all of whose value cells are null is an error (E6).
- At rank 0, each entity must appear in exactly one row (E6 otherwise).
- Two rows with the same entity and index tuple are an error (E6).
- Entity names must be non-empty, and one segment must use one spelling
  per entity: `north` and `North` in the same segment are an error (E6),
  because names resolve case-insensitively.

Each entity's rows form its complete value for the target parameter
and alternative. The import replaces the entity's current value as a
whole; it does not merge with cells already in the database. Entities
not present in the segment are not touched.

## Dense form

`spinal:form = dense` carries **one row per entity**, with each
dimension's coordinates as a list and the values as one flat list. It
avoids repeating the coordinates on every row for regular data such as
a large set of hourly series.

| Position | Name (recommended) | Arrow type | Content |
|---|---|---|---|
| 0 | `entity` | `utf8` | entity name |
| 1 … rank | the dimension name | `list<T>` (not `large_list`), `T` per [Index kinds](#index-kinds), not dictionary-encoded | that dimension's coordinates |
| last | `value` | `list<float64>` | the values, flattened row-major |

Rank must be 1 to 4; for scalars use the long form.

Rules for each row:

- **Row-major layout.** The value list is laid out in declared
  dimension order with the **last dimension varying fastest**, and its
  length must equal the product of the dimension list lengths (E8).
- The coordinates within each dimension list must be distinct (E8).
- No dimension list may be empty (E8).
- No nulls anywhere: entity, dimension lists, list entries, value list
  or value entries (E6). Dense data is a complete grid; for data with
  missing cells, use the long form.
- Each entity appears in at most one row per segment, compared
  case-insensitively (E6).
- Only `list<float64>` values are supported in 1.0. Integer, boolean,
  string and datetime values must use the long form.

A rank-2 example: dimension `case` = `["a", "b"]`, dimension `step` =
`[1, 2, 3]`, value list `[1, 2, 3, 4, 5, 6]` means

| case | step | value |
|---|---|---|
| a | 1 | 1 |
| a | 2 | 2 |
| a | 3 | 3 |
| b | 1 | 4 |
| b | 2 | 5 |
| b | 3 | 6 |

!!! warning "Column-major arrays"
    Julia, R and Fortran store arrays column-major (first index fastest),
    and NumPy arrays may be Fortran-ordered. Flattening such an array
    directly (`vec(A)` in Julia, `as.vector` in R) produces the wrong
    layout at rank 2 and above. Transpose or permute the dimensions so
    that the last dimension varies fastest before flattening.

The importer expands each dense row into long rows and hands them to the
core like any long segment. The dense form is only a transport
optimization: the storage tier is decided by the core from the data,
and the coordinate order in the lists only pairs coordinates with
values. Reads return values in canonical order, which may differ from
the order in the file.

## Index kinds

The Arrow type of a dimension column (in dense form, the list's element
type) sets the index kind:

| Arrow type | Index kind |
|---|---|
| `timestamp[s]`, `timestamp[ms]`, `timestamp[us]`, with or without a timezone | datetime |
| `int64` | integer |
| `utf8` (`string`) | label |

Any other type is rejected (E6), including `int32`, `large_utf8` and
`string_view`. Cast before writing.

Label dimensions additionally need a domain, see
[Label domains](#label-domains).

Period and time-pattern dimensions have no representation in format 1.0
and cannot be imported yet.

## Value kinds

The Arrow type of the value column sets the value kind (long form):

| Arrow type | Value kind |
|---|---|
| `float64` | float |
| `int64` | int |
| `bool` | bool |
| `utf8` (`string`) | string |
| `timestamp[s]`, `timestamp[ms]`, `timestamp[us]`, with or without a timezone | datetime |

Any other type is rejected (E6), including `float32`, `int32`,
`large_utf8`, `string_view`, durations and intervals. Duration values
cannot be imported yet.

Float values:

- **NaN** is a legal value. It does not mean "missing"; for a missing
  cell, use a null (long form) or leave the row out. In the canonical
  encoding every NaN bit pattern counts as the same value.
- **±Inf** are legal values.
- **−0.0** is accepted. The canonical encoding treats it as equal to
  `+0.0`; the float itself is stored as written.

### Timestamps

!!! warning "Timestamp units: seconds, milliseconds or microseconds only"
    The database stores timestamps with microsecond resolution.
    `timestamp[s]` and `timestamp[ms]` are converted to microseconds
    exactly. **`timestamp[ns]` is rejected** (E6), because converting it
    would drop precision. pandas and polars default to nanoseconds: cast
    first, for example `series.astype("datetime64[us]")` in pandas or
    `pa.array(..., pa.timestamp("us"))` in pyarrow. Julia's
    `Dates.DateTime` is written as `timestamp[ms]` by Arrow.jl and is
    accepted as is.

Timestamps are stored without a timezone. Write timezone-naive columns.
A column with a timezone is accepted as UTC instants: the stored value
is the UTC time, the timezone is dropped, and the import prints a
`timezone-normalized` warning for the column. Second and millisecond
timestamps must still fit the 64-bit microsecond range after
conversion (roughly ±290 000 years around 1970).

!!! warning "String columns: `utf8` only"
    Entity, label and string value columns must be `utf8`
    (`pa.string()`). `large_utf8` (`pa.large_string()`) and string-view
    columns are rejected (E6). Some libraries, polars for example,
    produce these by default; cast to `utf8` before writing. Dictionary-encoded `utf8` is fine.

## Label domains

Every label dimension must carry its **ordered domain**: the list of
labels in their meaningful order. It is stored as Arrow **field
metadata** on the dimension's field:

- key: `spinal:label_domain`
- value: a JSON array of strings, for example `["low", "mid", "high"]`

Rules:

- Required on every label dimension, in both forms. A label dimension
  without it is an error (E6). In the dense form, put it on the outer
  `list<utf8>` field.
- Every label that appears in the data must be in the domain (E6
  otherwise). The domain may contain labels that do not appear.
- Invalid JSON is an error (E6).

The domain sets the label order used for storage and sorting. In the
dense form the dimension list says which labels the entity has; the
domain says how all labels are ordered. The domain is read from each
file as written; it is not checked against the domains of earlier
imports of the same parameter.

## Commits and atomicity

One `nodal-db import` call is **one transaction and one commit**,
covering every segment in the file. Either everything is written, or
the database is left unchanged. A failure in the last row of the last
segment rolls back the whole file, so a failed import can be fixed and
re-run without cleanup.

- `--db` must name an existing database created with `nodal-db init`.
- The commit's author is `--user`, or by default the first non-empty
  of the environment variables `USER`, `USERNAME`, `LOGNAME`. If none is
  set, the import fails before reading the file (a usage error without
  an `E` class).
- `--message` sets the commit message (default empty).
- Re-importing the same data writes a new version of each value; it is
  not skipped as a duplicate.
- With `--force-replace`, the superseded versions are not kept in
  history: the old rows remain as stubs without content. This applies to
  all segments in the file.
- `--class` and `--parameter` are assertions, not filters: if given,
  every segment must target that class or parameter, or the import
  fails (E4).

Metadata checks for all segments (E1 to E4, E7, E10) run before the
database is opened. While the import writes, it holds DuckDB's
exclusive write lock on the file; no other process can open the
database until it finishes. For very large files this can take minutes.

The transaction boundary is the producer's choice. Importing once at the
end of a run makes the run all-or-nothing. Importing after each step
(for example each rolling window of a simulation) commits each step
durably, so completed steps survive a later crash.

## Errors

Errors print to standard error as `Error: E<n>: <message>` and exit
with code 1; the database is unchanged. Warnings print to standard
output and do not affect the exit code (0). The class (`E1` to `E10`)
is stable; the message text may change between releases.

| Class | Meaning | Typical causes |
|---|---|---|
| E1 | not an Arrow IPC stream | empty file; IPC file / Feather format (`ARROW1` magic); not Arrow at all |
| E2 | wrong format identifier | `spinal:format` missing or not `spinal-db-bulk` |
| E3 | version problem | `spinal:format_version` missing, malformed, a major version other than 1, or different in different segments |
| E4 | metadata or target problem | `spinal:segment_count`, `spinal:class`, `spinal:parameter` or `spinal:form` missing; unknown `spinal:form`; no alternative; `spinal:alternative` conflicts with `--alternative`; `--class`/`--parameter` assertion does not match |
| E5 | warning only | unknown `spinal:` metadata key, ignored |
| E6 | malformed payload or rejected by the core | unsupported Arrow type (`timestamp[ns]`, `large_utf8`, `float32`, …); run-end encoding; rank above 4; null where not allowed; zero-row segment; `entity`/`value` column out of place; label domain missing, invalid or not covering the data; duplicate index tuple or dense entity; rank-0 entity with several rows; empty entity name or two spellings of one entity; type does not match the existing definition; entity of a multi-dimensional class does not exist; `--db` is not an initialized nodal-db database; trailing bytes after a segment; a stream cut off in the middle of a message |
| E7 | duplicate target | two segments target the same `(class, parameter, alternative)` |
| E8 | dense shape error | value list length is not the product of the dimension list lengths; duplicate coordinate in a dimension list; empty dimension list |
| E9 | database busy | the write lock could not be obtained, or a write conflict persisted, within the retry budget; retry later |
| E10 | file truncated or mis-assembled | a segment lacks the end-of-stream marker; legacy 4-byte end marker; segments disagree on `spinal:segment_count`; actual segment count differs from the declared one; `spinal:segment_count` not a positive integer |

Some real messages:

```text
Error: E1: file.arrow is in the Arrow IPC *file* (Feather / random-access) format; this importer accepts only the IPC *stream* format (spec §2.1)
Error: E6: segment 1 (node:cap): column "t" is timestamp[ns]; nanosecond truncation is lossy and never silent — cast to microseconds producer-side (pandas: .astype("datetime64[us]")) (§5.2.1)
Error: E6: segment 1 (node:cap): label dimension "s" has no spinal:label_domain field metadata (required on every label dimension, §7.2)
Error: E8: segment 1 (node:cap): dense row 0 (entity "a"): payload length 2 != product of axis lengths 3 (§6.2 row-major law)
Error: E10: segment 2 is not terminated by the end-of-stream marker — truncation suspected (producer crash before close?)
```

## Worked example

This example writes a two-segment file with pyarrow, imports it into a
new database and reads it back with DuckDB. Segment 1 is a rank-2 long
segment: `flow` of a node per scenario label and hour, with one absent
cell. Segment 2 is a rank-1 dense segment: an hourly `price` series for
two nodes.

```python
import json
from datetime import datetime, timedelta
import pyarrow as pa

SEGMENTS = 2  # total segments in this file; stamped on every segment

def write_segment(sink, cls, param, form, fields, columns):
    meta = {
        "spinal:format": "spinal-db-bulk",
        "spinal:format_version": "1.0",
        "spinal:segment_count": str(SEGMENTS),
        "spinal:class": cls,
        "spinal:parameter": param,
        "spinal:form": form,
    }
    schema = pa.schema(fields, metadata=meta)
    batch = pa.record_batch(columns, schema=schema)
    with pa.ipc.new_stream(sink, schema) as writer:   # one complete stream
        writer.write_batch(batch)                     # close() writes the end marker

t0 = datetime(2030, 1, 1)
hours = [t0 + timedelta(hours=h) for h in range(3)]

with open("results.arrows", "wb") as f:
    # Segment 1: long form, rank 2 (label "scenario" x datetime "time").
    write_segment(
        f, "node", "flow", "long",
        [
            pa.field("entity", pa.string(), nullable=False),
            pa.field("scenario", pa.string(), nullable=False,
                     metadata={"spinal:label_domain": json.dumps(["low", "mid", "high"])}),
            pa.field("time", pa.timestamp("us"), nullable=False),
            pa.field("value", pa.float64()),
        ],
        [
            ["north"] * 4,
            ["low", "low", "high", "high"],
            pa.array([hours[0], hours[1], hours[0], hours[1]], pa.timestamp("us")),
            [1.5, 2.0, 3.25, None],          # null value = cell absent
        ],
    )
    # Segment 2: dense form, rank 1 (one row per entity).
    write_segment(
        f, "node", "price", "dense",
        [
            pa.field("entity", pa.string(), nullable=False),
            pa.field("time", pa.list_(pa.timestamp("us")), nullable=False),
            pa.field("value", pa.list_(pa.float64()), nullable=False),
        ],
        [
            ["north", "south"],
            [hours, hours],
            [[31.7, 30.2, 29.9], [28.0, 27.5, 27.1]],
        ],
    )
```

Create a database and import the file:

```bash
nodal-db init model.duckdb
nodal-db import results.arrows --db model.duckdb --alternative Base \
    --user alice --message "run 1 results"
```

```text
initialized model.duckdb
imported results.arrows: commit 2, 2 segment(s), 3 value(s), 5 row(s) written, axes 3 created / 1 reused, tiers [long, array]
warning: auto-created entity_class "node" (id 1)
warning: auto-created parameter_definition "flow" (id 1)
warning: auto-created alternative "Base" (id 1)
warning: auto-created entity "north" (id 1)
warning: auto-created parameter_definition "price" (id 2)
warning: auto-created entity "south" (id 2)
```

Three values were written (one `flow` value, two `price` values) in one
commit. The core stored `flow` on the long tier, because the grid is
not complete (no `high` value at 01:00), and `price` on the array tier.

Read back, read-only, closing the connection afterwards:

```python
import duckdb

con = duckdb.connect("model.duckdb", read_only=True)
try:
    print(con.sql("""
        SELECT entity, alternative, tier, idx1_text, idx2_ts, v_float
        FROM read_parameter('node', 'flow')
        ORDER BY entity, idx1_ordinal, idx2_ts"""))
    print(con.sql("""
        SELECT entity, tier, idx1_ts, v_float
        FROM read_parameter('node', 'price')
        ORDER BY entity, idx1_ts"""))
finally:
    con.close()
```

```text
┌─────────┬─────────────┬─────────┬───────────┬─────────────────────┬─────────┐
│ entity  │ alternative │  tier   │ idx1_text │       idx2_ts       │ v_float │
│ varchar │   varchar   │ varchar │  varchar  │      timestamp      │ double  │
├─────────┼─────────────┼─────────┼───────────┼─────────────────────┼─────────┤
│ north   │ Base        │ long    │ low       │ 2030-01-01 00:00:00 │     1.5 │
│ north   │ Base        │ long    │ low       │ 2030-01-01 01:00:00 │     2.0 │
│ north   │ Base        │ long    │ high      │ 2030-01-01 00:00:00 │    3.25 │
└─────────┴─────────────┴─────────┴───────────┴─────────────────────┴─────────┘

┌─────────┬─────────┬─────────────────────┬─────────┐
│ entity  │  tier   │       idx1_ts       │ v_float │
│ varchar │ varchar │      timestamp      │ double  │
├─────────┼─────────┼─────────────────────┼─────────┤
│ north   │ array   │ 2030-01-01 00:00:00 │    31.7 │
│ north   │ array   │ 2030-01-01 01:00:00 │    30.2 │
│ north   │ array   │ 2030-01-01 02:00:00 │    29.9 │
│ south   │ array   │ 2030-01-01 00:00:00 │    28.0 │
│ south   │ array   │ 2030-01-01 01:00:00 │    27.5 │
│ south   │ array   │ 2030-01-01 02:00:00 │    27.1 │
└─────────┴─────────┴─────────────────────┴─────────┘
```

The null cell was dropped, and the labels sort by their domain order
(`low` before `high`), not alphabetically.

## Producer checklist

- [ ] IPC **stream** format, one stream per `(class, parameter,
      alternative)`, each writer closed so the end marker is written.
- [ ] All six required metadata keys on every segment, with the same
      `spinal:segment_count` everywhere.
- [ ] Strings as `utf8`, not `large_utf8` or string view.
- [ ] Timestamps in `s`, `ms` or `us`, never `ns`; preferably
      timezone-naive.
- [ ] Integers as `int64` (R's `integer` and many defaults are 32-bit),
      floats as `float64`.
- [ ] `spinal:label_domain` on every label dimension.
- [ ] Dense values flattened row-major (last dimension fastest).
- [ ] Missing cells as null values (long form) or omitted rows, not as
      NaN.
