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

`init` creates a new file: in the CLI, Python and Rust alike it refuses
a path that already exists (any file, an empty one included) or that has
a `.wal` file next to it. The `--alternative` option creates an initial
alternative; without it, the database starts with no alternatives and one
is created on the first write that names it.

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
            "nodal:format": "nodal-db-bulk",
            "nodal:format_version": "1.0",
            "nodal:segment_count": str(n_segments),
            "nodal:class": cls,
            "nodal:parameter": param,
            "nodal:form": "long",
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

    Several writes, deletions, structural changes and purges can go into
    one commit with `db.commit_changes(segments=..., structural=...,
    deletes=..., object_deletes=..., purges=..., user=...)`.

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

#### Values of several types for one parameter

A parameter can declare several value types, for example a `float` and
a `string` constant, or a constant and an indexed series. Each value is
held to the declared types when it is written: a batch must match
exactly one of them (value kind, number of index dimensions, their kinds
and names), and a write never adds a type to the declaration. A batch
that matches none is refused with nothing written, as
`DbError::UndeclaredValueType` in Rust and `nodal_db.UndeclaredValueType`
in Python (an `ApiError` subclass). It names the target
(`segment_index`, `class`/`class_name`, `parameter`, `alternative`), the
first `entity` of the refused batch and its `entity_count`, the batch's
`value_kind` and `dimensions` (`(name, kind)` pairs, empty for a
constant) and the `declared` types.

Values of different types for one parameter and alternative can go in
one commit: give the segment one batch per type. The core groups a
segment's batches by their column types and dimension names; batches
with the same types and names are one table, as always, and each group
is checked against the declared types on its own. So a `t` batch and a
`time` batch are two groups, each matched by its own dimension name. An
entity can appear in one group only, since it has one value. If the
parameter has no declared type yet, the first group declares it, and a
group of another type is then refused like any undeclared type.

Label domains given by position (the `domains` argument) apply to the
dimension that the first group with a label dimension at that position
has. A group whose label dimension at that position has another name
does not get that domain; give it its own in the column's
`nodal:label_domain` field metadata. In a segment of one group nothing
changes: the `domains` argument wins over field metadata at the same
position.

```python
db.ensure_entity_class("node", user="alice")
db.ensure_parameter_definition(
    "node", "level", types=[{"kind": "float"}, {"kind": "string"}],
    user="alice")
north = pa.record_batch({"entity": ["north"], "value": [12.0]})
south = pa.record_batch({"entity": ["south"], "value": ["high"]})
db.commit_changes(
    segments=[{"class": "node", "parameter": "level", "alternative": "Base",
               "batch": [north, south]}],          # a list: one batch per type
    user="alice")
```

In Rust, put both batches in `Segment::batches`. Writing a value of
another type for an entity that already has one (a string where a float
was) replaces it, as a write of the same type would.

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
anything, as `DbError::WriteAndDeleteSameKey` in Rust /
`nodal_db.WriteAndDeleteSameKey` in Python (both subclasses of
`ApiError`), naming the colliding `(class, parameter, alternative,
entity)` key by its `key_index` in the call's delete list plus
`class`/`class_name`, `parameter`, `alternative` and `entity`.
Likewise a call may not write a value and delete an object
that the value belongs to: its entity (or an entity it is an element
of), its parameter, its alternative or its class. The core refuses
such a call whatever the values and whatever the `cascade` option, as
`DbError::CoveredWrite` / `nodal_db.CoveredWrite` (also `ApiError`
subclasses), naming the covering object delete by its `key_index`,
`kind`, `target` and `class`/`class_name`. Both exceptions carry a
`resolved` attribute: the covered write's resolved `(entity_id,
parameter_definition_id, alternative_id)` when the in-transaction,
resolved-id check is what caught it, `None` when the up-front
name check already did. Write the values and delete the objects in
separate calls.

```python
try:
    db.commit_changes(
        segments=[{"class": "node", "parameter": "demand",
                   "alternative": "Base", "batch": batch}],
        deletes=[("node", "demand", "Base", "north")],
        user="alice")
except nodal_db.WriteAndDeleteSameKey as e:
    print(e.key_index, e.class_name, e.parameter, e.alternative, e.entity)
```

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

### Names, labels and Unicode

Some characters can be written in more than one way in Unicode. `é`, for
example, is either the single character U+00E9 or `e` followed by the
combining accent U+0301. The two look the same and Unicode defines them
as the same text, but their bytes differ. macOS file names, some
editors and text copied from some PDFs use the decomposed form.

nodal-db treats the two kinds of text differently:

- **Names and index labels are stored in Unicode Normalization Form C
  (NFC).** This covers the names of classes, entities, parameters,
  alternatives, scenarios, value lists, units and class sets, the
  dimension names (a batch's dimension column names), and every index
  label (map keys and label domains). The core converts each one to
  NFC before it compares, looks up or stores it, so two spellings that
  differ only in this way are one name or one label. You read back the
  NFC form, which can differ in bytes from what you wrote, in the way
  `-0.0` is read back as `0.0`. Names are also compared without regard
  to case (see **Case** below).
- **String values are stored exactly as written.** This covers string
  values, value-list members, descriptions and commit messages. Two
  string values whose bytes differ are different values, even when they
  look the same.

The normalization uses the tables of Unicode 16.0.0, whatever Unicode
version your platform has. When a write changes any names or labels, the
report carries one `text_normalized` warning per segment and kind, with
the number of changed spellings and the first one, as written and as
stored:

```text
{'kind': 'text_normalized', 'what': 'label', 'count': 1,
 'written': 'é', 'stored': 'é', 'object_type': 'segment',
 'name': 'node:mode_cost/Base', 'id': 0}
```

Lookups (reads through the core, deletes, renames) convert their
arguments too, so either spelling finds the object. Some consequences:

- A label domain that lists both spellings of one label repeats it, and
  is refused; so is a value that has two rows whose index tuples differ
  only in this way (a duplicate index tuple). The error message shows
  both spellings with their code points, because they look identical.
- Two entity names in one batch that differ only in this way are one
  entity.
- **Direct SQL** sees the stored NFC text. A query whose literal is in
  another form misses it; normalize the literal with DuckDB's
  `nfc_normalize('...')`, or in Python with
  `nodal_db.normalize_identifier(s)`, which is the core's own function.
  Use the latter in code that compares names the way the database does
  (a search box, an importer's duplicate check): Python's `unicodedata`
  follows the interpreter's Unicode version (15.0 on Python 3.12), which
  disagrees with 16.0.0 on a few recently added characters.

**Case.** Two names are the same name when their NFC forms are equal
after DuckDB's `lower()`, the Unicode 15.1 simple lowercase mapping
applied letter by letter. The core's own checks use an exact copy of it,
so the core and the database never disagree about which names collide.
The mapping has no special rules, which gives a few results that differ
from everyday language rules:

- Greek final sigma is an ordinary letter: `ΟΔΟΣ` and `οδοσ` are one
  name, but `οδος` (with the final form `ς`) is another.
- `ß` is not `ss`: `Straße` and `STRASSE` are two names.
- Turkish dotted capital `İ` lowers to plain `i`: `İx`, `Ix` and `ix` are
  one name.
- The case pairs added in Unicode 16 are case-sensitive.

In Python, `nodal_db.fold_name(nodal_db.normalize_identifier(s))` is the
core's identity of name `s`; use it, not `str.lower` or `str.casefold`
(they follow the interpreter's Unicode version and apply the final-sigma
rule), when you must predict whether two names collide. In SQL,
`lower(name) = lower('...')` on NFC text is the same rule exactly.

When a write refers to an existing object by a spelling that differs from
the stored one only in case, the write goes to that object (names are
case-insensitive), and the report says so with one `name_folded` warning
per group of the call and kind of object. `spellings` lists each such
reference once: the spelling as written, as stored, and the id of the
object it resolved to (one spelling that resolved to two objects, such as
entities of two classes, is listed twice). `count` is the length of the
list, and `written`, `stored` and `id` repeat its first entry. `what` is
the group (`segment`, `delete`, `object_delete` or `structural`) and
`object_type` the kind of object:

```text
{'kind': 'name_folded', 'what': 'segment', 'count': 1,
 'written': 'north', 'stored': 'North', 'object_type': 'entity',
 'name': 'node:demand/Base', 'id': 7,
 'spellings': [{'written': 'north', 'stored': 'North', 'id': 7}]}
```

`nodal-db import` and the Spine importer summarize these warnings by kind
of object, counting each (object, written spelling) once, so a case
variant referenced from several segments counts once.

It is not an error and not a write by itself. A client that writes back
the spellings it read never sees one; if it does, it is about to change
an object it may not have meant (for example, a user typed `NORTH` for a
new entity while `North` exists). The reference that creates an object, a
rename's new name and the verbs (`ensure_*`, which return ids) are never
reported.

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

#### Refinement links

A refinement link says that two entities are the same real thing at two
resolutions, for example a power plant of an energy-system model and the
generator that represents it in a grid model. The link is an ordinary
entity of a 2-D class whose element 1 is the coarse entity and element 2
the fine one, and the class is marked as a **refinement class**. Then an
explicit deactivation of the coarse entity (its own `entity_alternative`
row) also switches off the fine one, in every scenario, unless the fine
entity has its own row: `entity_activation_resolved` reports it as
`active_source = 'propagated'`, and `realizes_edge` lists the
(coarse, fine) pairs.

A class is a refinement class only when it is marked; its name has no
meaning. Mark or unmark one with:

=== "Python"

    ```python
    db.set_refinement_class("plant_generator", True, user="alice")
    ```

=== "Rust"

    `verbs::set_refinement_class(&db, "plant_generator", true, "alice")`.

The class must have exactly two dimensions. The marks are stored in the
core property `refinement_classes`, a JSON array of class ids
(`[12,40]`), so they follow class renames; deleting or purging a marked
class removes its id in the same commit. Templates do not carry the mark
yet: a template can declare the class, and a program or the API marks it
after the load (a template export does not include it either).

### Renaming objects and editing descriptions

Five structural changes rename an object or revise its description:
`RenameAlternative`, `RenameScenario`, `RenameEntityClass`,
`RenameParameter` and `RenameEntity` (Python ops `rename_alternative`,
`rename_scenario`, `rename_entity_class`, `rename_parameter`,
`rename_entity`). Each takes the object's `current` name, a `new_name`
and an optional `description`; `RenameParameter` and `RenameEntity` also
take the `class` the parameter or entity belongs to. Entities have rules
of their own as well: see [Renaming entities](#renaming-entities).

```python
db.commit_structural([
    {"op": "rename_entity_class", "current": "node", "new_name": "bus"},
    {"op": "rename_parameter", "class": "bus", "current": "demand",
     "new_name": "load", "description": "Hourly load, MW"},
], user="alice")
```

A rename keeps the object's id and closes the current row with a
same-id successor (`version + 1`), so history is continuous: values,
entities, dimensions, defaults and type declarations refer to the id
and follow the rename with no rewrite, an N-D class that uses a renamed
class as a dimension shows the new name, and `<table>_asof(commit)`
shows the old name at earlier commits. Class names are unique in the
database; parameter names are unique within their class.

- **What counts as a rename.** Any difference from the stored spelling
  after NFC is a rename, case-only and Unicode-form-only changes
  included (`"High" -> "HIGH"`), reported with the effect `Renamed`.
  Renaming to a name that is already a *different* live object (by the
  case-insensitive name identity) is refused; renaming an object to its
  own case-restyled spelling never collides with itself. A caller that
  wants case-only renames treated as nothing to do checks the stored
  spelling itself and skips the call.
- **Descriptions.** `new_name` equal to the stored spelling with a
  `description` revises only the description; the effect is `Updated`.
  A missing `description` (`None`) keeps the stored one, and `""` clears
  it: a stored description is never the empty string, anywhere in the
  database (creates and templates follow the same rule). The same name
  with no description change, or `""` on an object with no description,
  is `NoOp`; a call of only no-ops makes no commit (`commit_id` 0).
  `CreateEntityClass` and `CreateEntity` (Python `create_entity_class`,
  `create_entity`) also take a `description`, applied only when the
  class or entity is created: a create of an existing one is a `NoOp`
  that keeps the stored description (when one call creates the same
  entity twice, the first description wins). `RenameEntity` revises an
  entity's description.
- **Several changes in one call** apply in order. Address an object by
  the name it has at that point of the call: after `node -> bus`, a
  later item, segment or delete uses `bus`. A reference by the old name
  is refused (it would otherwise create a new, empty object or delete
  nothing), unless an earlier item of the call created a new object
  under that name. A rename may not take a name that an earlier rename
  of the same call freed, so swapping two names (`a -> t`, `b -> a`,
  `t -> b`) is refused: rename both to temporary names in one commit
  and to their final names in the next. A chain (`a -> b`, then
  `b -> c`) is one rename. A rename and an object delete of the same
  object in one call are refused; do them in separate calls. When a
  client stages creates and renames together, it should send the renames
  first: a create of an existing name does nothing, so a create sent
  before the rename of that name lands on the object being renamed.
- **Template-stamped databases.** On a database that a template stamped
  (the `template_name` database property), renaming a class or parameter is
  refused by default with `TemplateStampedRename`
  (`DbError::TemplateStampedRename`, `nodal_db.TemplateStampedRename`,
  an `ApiError` subclass). nodal-db cannot tell which classes and
  parameters the template declares, so the refusal covers all of them.
  The error lists every refused item (`change_index`, kind, class,
  `current`, `new_name`) so a client can ask its user once; to go ahead,
  resend the allowed items with `allow_on_template: true`
  (`"allow_on_template": True` in Python). Case-only renames,
  description edits and objects created in the same call need no flag.
  After such a rename, applying a *changed* template that still declares
  the old name stops with a message naming the rename (rename it back,
  or have the template rename it with a `rename_class` /
  `rename_parameter` hint) instead of creating a second, empty object
  under the old name; re-applying the same template does nothing. (So a
  rename keeps the template's certified content hash, while a purge or
  object delete of vocabulary withdraws it: the same template still
  counts as applied after a rename, and re-applying it would otherwise
  only re-create the old name.) A
  changed template also writes its own descriptions back over edited
  ones. A history-free extract forgets the old names, after which a
  changed template creates the old name anew.
- **Template documents.** The template-owned parameter groups and audit
  rules (the `presentation` and `audit_rules` database properties) name
  classes and parameters; a rename updates them in the same commit, and so
  does an object delete or a purge that closes them (see [Purge](#purge)).
- **Outside the database.** Names that other programs store (Arrow import
  files, results templates, saved SQL such as
  `read_parameter('node', 'demand')`) are not updated: after a rename a
  read by the old name returns no rows, and a write by it creates a new
  object. Update those references together with the rename.

### Renaming entities

`RenameEntity` (Python op `rename_entity`) renames one entity of a class
and/or revises its description, with the rules above:

```python
db.commit_structural([
    {"op": "rename_entity", "class": "node", "current": "n1",
     "new_name": "bus", "description": "Main bus"},
], user="alice")
```

The entity keeps its id, so its values, activations, memberships,
location, metadata links and the N-D entities that use it as an element
all follow the rename; `entity_asof(commit)` shows the old name at
earlier commits. In addition:

- **Uniqueness.** Entity names are unique within their class, compared
  case-insensitively. When the class is a member of a class set, the new
  name must also be free in every other member class of the set: an
  element of a set-typed dimension is found by its name in all member
  classes, so a shared name would make it ambiguous. Both are refused.
  For the same reason, a name freed by a rename earlier in the call is
  taken neither in the entity's own class nor in another class of a set
  the class belongs to (rename in two commits).
- **The old name in the same call.** A segment, a value delete, an
  object delete (by name or by element tuple), an activation, a
  membership, a `CreateEntity` element or a metadata item that uses the
  old name is refused, naming the new one. Without this a 0-D segment
  label spelled with the old name would create a new, empty entity.
  `CreateEntity` of the old name creates a new entity (a new id); after
  that, the old name refers to the new entity.
- **Deletes and purges.** A renamed entity cannot be object-deleted in
  the same call (by either name, through its class, or through an element
  whose delete would cascade to it); deleting an N-D entity that uses a
  renamed element is allowed. A purge of the entity's class keeps the
  renamed entity (it is part of the call) and closes its values that the
  call does not write.
- **Many renames.** A long run of consecutive `RenameEntity` items (20 or
  more) is applied as one batch, so renaming thousands of entities in one
  call takes seconds rather than minutes. The result is the same as
  applying them one by one.
- **Templates.** Entity renames need no `allow_on_template` flag; see
  [N-D entities](#n-d-entities-elements-names-and-duplicates) for what a
  later template apply does with a renamed seed entity.
- **A Spine re-import.** Importing the same Spine database again after a
  rename does not follow it: a renamed 0-D entity is created again under
  its old name, and an N-D entity whose element was renamed fails to
  import ("already exists with elements [u1, bus], not [u1, n1]"). Rename
  in the source as well, or purge and re-import. A re-import does not
  update descriptions.

#### N-D entities: elements, names and duplicates

An N-D entity's identity is its element tuple; its name is a separate,
stored string.

- **Names do not follow elements.** `CreateEntity` without a `name`
  derives one from the element names joined with `__` (`u1__n1`), once,
  at create time. Renaming an element does not rename the N-D entities
  that use it: after `n1 -> bus`, `u1__n1` keeps its name and its
  elements are `(u1, bus)`.
- **Never parse an N-D entity's name** to find its elements: after a
  rename the parsed name is wrong, and if a new `n1` is created later it
  points at the wrong entity without any error. Read the elements from
  the `entity_byname` view (`class`, `entity`, `entity_id`,
  `dimension_count`, `element_names`, `element_classes`; empty lists for
  a 0-D entity).
- **One entity per element tuple.** Two live entities of one class may
  not hold the same element tuple. `CreateEntity`, `ensure_entity` and
  the Spine importer follow this table:

  | Create | Result |
  |---|---|
  | explicit name that exists, same elements | `NoOp` |
  | explicit name that exists, other elements | refused ("already exists with elements …") |
  | new explicit name, elements free | created |
  | new explicit name, elements held | refused: "elements [u1, bus] already exist as \"u1__n1\"; duplicate element tuples are not allowed" |
  | no name, elements held by an entity with the derived name | `NoOp` |
  | no name, elements held under another name | refused: "… already exist as \"link\"; address it by that name" |
  | no name, elements free, derived name taken by another tuple | refused: "the derived name \"u1__n1\" is taken by an entity with elements [u1, bus]; give an explicit name" |
  | no name, elements free, derived name free | created |

  Elements are resolved first, so an unknown element is reported before
  anything else. Deleting or purging an N-D entity and creating its
  tuple again in one call is refused, also under a new name (deletes and
  purges run after the structural changes, so the tuple is still held):
  to give an N-D entity a new name, use `RenameEntity`; to replace it,
  use two calls. Uniqueness is enforced by nodal-db's writer, not by an
  index; files written before the rule (or by direct SQL) may hold
  duplicates. The advisory `audit_entity_tuple_dup` view lists them
  (class, element names, the holders' ids and names). They are left as
  they are and every other operation on them works, but a new create of
  such a tuple is refused (listing all holders), and template export
  refuses until they are resolved.
- **Templates and renamed seeds.** A template apply does not recognize
  renames. It creates what the template declares where it can and skips
  what it cannot, with a note in the plan and apply report: a 0-D seed
  that was renamed away is created again, empty, under its declared
  name; an N-D seed whose name is taken by an entity over other elements,
  or whose elements an entity already holds under another name, is
  skipped, and so are the seeds and seed memberships that depend on a
  skipped seed. An apply with skips is not stamped converged
  (`template_content_hash` is withheld), so every later apply of the same
  template reports the skips again until they are resolved (by renaming
  back, deleting the clashing entity, or a template that drops the seed).
  Template validation refuses two seeds of a class with the same
  elements. Migration `run:` programs see renamed seed entities under
  their current names.

### Purge

A purge closes out everything in a named scope in one commit: all values
of a class, every entity, every alternative but some, every class. It is
the counterpart of Spine Toolbox's "purge before writing". **Most work
does not need it**: a write replaces the values it names, an object
delete removes one object with its dependents, and an import into a new
file starts empty. Use a purge when a source is the whole truth for a
scope and anything it no longer states must go.

=== "Python"

    ```python
    db.preview_purge([{"scope": "values", "class": "node"}])
    db.purge([{"scope": "values", "class": "node"}], user="alice")

    # Purge and re-import in one commit: a true replace.
    db.commit_changes(segments=[...], structural=[...],
                      purges=[{"scope": "entities"}], user="alice")
    ```

=== "CLI"

    ```bash
    nodal-db purge demo.duckdb --scope values --class node --preview
    nodal-db purge demo.duckdb --scope classes --except node --user alice
    ```

    `--allow-outside-scope` and `--allow-on-template` are the two opt-ins;
    `template apply --recreate-removed` re-creates removed vocabulary.

=== "Rust"

    `api::purge` and `api::preview_purge`, or the `purges` channel of
    `api::commit_change_set` (`ChangeSet { purges, .. }`) with
    `types::PurgeRequest` and `types::PurgeScope`.

**Scopes.** Each request names one scope and its filters:

| Scope | Closes |
|---|---|
| `values` (`class`, `parameter` (needs `class`), `alternative`) | values of every tier and their value metadata |
| `entities` (`class`) | entities with everything keyed on them: values, activations, memberships, locations, units, metadata, annotations, and the N-D entities that use them as elements |
| `activations` (`class`, `alternative`) | activation rows |
| `memberships` (`group_class`) | membership rows of the class's groups |
| `alternatives` (`except`) | every alternative but those listed, with their values, activations, scoped memberships and scenario rankings |
| `scenarios` | every scenario and its ranking |
| `classes` (`except`) | every class but those listed, with their entities, parameters, dimensions, display modes, class-set membership, annotations, and the classes that use them as a dimension |
| `parameters` (`class`) | parameter definitions with their types, defaults, values, units, value metadata and annotations |
| `value_lists` (`except`) | every list but those listed, with its members and annotations |

Filter and `except` names are looked up as written (case-insensitively,
in NFC) after the call's structural changes and value writes (so a class
a segment of the same call creates is known, with nothing to close yet);
an unknown or empty name is refused, and so is a name that a rename
earlier in the same call replaced. A filter or flag the scope does not
take is refused too (Python and the CLI), since ignoring it would widen
the purge. Purges run last in the call, under its commit; the report's
`purges` list has one outcome per request (`closed_out`, `objects`,
`spared`, `unbound`). A purge that closes nothing and a call that also
writes nothing make no commit.

**True replace.** A purge closes everything in its scope *except
exactly what the same call writes*, at each scope's own level: the value
keys it writes (unchanged ones included), the activations, memberships
and scenario rankings it sets, and the objects it names or creates.
Sparing goes upward: a spared value keeps its entity, parameter and
alternative; a spared entity keeps its class and its elements; a spared
parameter keeps its class and the value list it binds. A spared object
keeps only its own row and identity (an N-D entity's elements, a
parameter's types and defaults, a list's members); everything else under
it closes unless the call states it too. So `entities` plus a
re-import keeps the re-imported entities and closes their unstated
values, activations and memberships. A re-import cannot move an N-D
entity's element tuple to a new name: the old holder is still live when
the create runs, so the create is refused (see [N-D entities: elements, names and
duplicates](#n-d-entities-elements-names-and-duplicates)); use `RenameEntity`. Three limits:

- A spared entity keeps its attachments: its location, its parameter
  units, its metadata links and its annotations. A call cannot re-state
  all of them, so closing them would lose them for good. They close only
  with an entity the purge closes.
- A spared class, parameter or value list keeps its display modes,
  class-set membership and annotations: nothing in a call re-states
  them, and a template does not carry them.
- The replace stops at identity: a spared parameter definition keeps its
  old types, description and value-list binding, because a create of an
  existing definition changes nothing.

Rows the call itself writes are never closed, and closed values follow
their definition's retention (`retain_content`, or the call's
`retain_override`; `force_replace` keeps no content, as for a value
delete). Structural rows and parameter defaults are always
kept in history.

**Reach outside the scope.** A request whose cascade would close objects
that no request of the call has in scope is refused, with nothing
written, and the error lists them per kind and class. The refusal comes
only after the call's writes have run (and been rolled back), so preview
first. The objects it lists are N-D entities of
other classes that use a purged entity as an element, classes that keep
a purged class as a dimension (also through a class set), and, for
`value_lists`, the parameters outside every scope that still bind a
purged list. Add those objects to the call's scopes, or set
`"allow_outside_scope": True` on the request. For a bound list the flag
removes the binding: the definition gets a new version without its list,
its values stay as they are, and as-of reads before the purge still show
the binding. Only the value-list audit stops checking it, and an audit
rule whose selector lost its list is dropped. A later re-import does not
re-bind the definition.

**Template-stamped databases.** On a database a template stamped, a
`classes`, `parameters` or `value_lists` request that would close
vocabulary that existed before the call is refused with
`TemplateStampedPurge` (`DbError::TemplateStampedPurge`,
`nodal_db.TemplateStampedPurge`, an `ApiError` subclass), listing every
such request; resend it with `"allow_on_template": True`. Data scopes
need no flag, also for entities the template seeded, and re-creating
seeds is the template's (or the import's) job. An object delete of a
declared class or parameter, and `drop_value_list`, need no
`allow_on_template` (they name what they remove); a purge does, since
its scope covers vocabulary it does not name.

A purge, object delete or `drop_value_list` that closes vocabulary
withdraws the template's certified content hash, so the next apply of
**the same template** no longer skips as a no-op, and neither the same
nor a changed template re-creates what was removed: the apply stops with
"class `X` was removed locally in commit N: re-create it with
`recreate_removed`, or drop it from the template". Apply with
`recreate_removed=True` (Python), `ApplyOptions::recreate_removed` (Rust)
or `template apply --recreate-removed` (CLI) to re-create the removed
items with new ids; the plan notes each, and `template_plan` /
`template plan` take the same option to preview it. **This is a
behaviour change for object deletes and `drop_value_list`** of declared
items: an apply used to re-create them silently. Removals made by the template's
own migration steps and `run:` programs never block. For a `run:`
program, "its commits" means every commit since the template's last
commit up to the program's progress commit, so a removal some other
writer made in that window (for example while an interrupted apply was
waiting to be resumed) counts as the template's and does not block. After a
history-free extract the removal history is gone, and a changed template
re-creates such items silently again.

**Template documents.** A purge or object delete that closes classes or
parameters removes their names from the template's parameter groups and
audit rules in the same commit: a closed parameter's group entry, a rule
whose class or selector is gone, and a closed parameter from a rule's
requirements (a rule left with none goes too). When no rule is left the
audit rules document is removed.

**History and size.** A purge closes rows; it never removes them. As-of
reads before the purge see everything, no erasure marker is written, and
the file does not shrink: it grows by the closed rows' new versions until
[compaction](features.md#compaction). A purge is one transaction that
holds the database's write lock throughout: purging about 105 million
long-tier cells took about ten minutes and peaked at 3.4 GB of memory,
within the 6.4 GB DuckDB limit of an 8 GB budget (scalars and arrays are
far cheaper). Preview a large purge first: when a request's scope holds
more than 50 million long-tier cells plus array elements
(`purge::PURGE_LARGE_CELLS`, half that measured point), the preview and
the purge add a `purge_large` warning (`WarningKind::PurgeLarge`, with
`cells` and `threshold`). The purge still runs. Purging and re-importing on
every run without unchanged-value skipping grows the history by the
whole scope each time. To discard data for good, use
[Erase an alternative](#erase-an-alternative); to start over without the
history, [extract a database without its history](#extract-a-database-without-its-history)
or import into a new file.

**Preview.** `preview_purge` reports, per request, the objects and rows
(per table) it would close, its reach outside the scope and whether it
closes template vocabulary. A row an earlier request of the call covers
counts in that earlier request only, as in the commit. It writes
nothing, so nothing is spared: its counts are upper bounds of a call
that also writes.

### Metadata and provenance

nodal-db records where data comes from and under which terms, in three
kinds of records and two kinds of links:

- **Sources** (a publication, a dataset, a measurement campaign) with a
  short unique name and descriptive fields (title, path or URL,
  edition, publisher, issued and retrieved dates, citation,
  description).
- **Licenses**, with a unique name, an `spdx` flag for names that are
  SPDX identifiers, and an optional ODRL policy (JSON-LD, stored as
  given). A source lists the licenses it is published under.
- **Pairs**: free `name = value` annotations (keywords, contributors,
  notes). A pair lives once and anything can link to it.
- **Object links** hang a source, a license or a pair on the dataset, an
  entity, a class, a parameter, an alternative, a scenario, a run, a
  value list, a unit, or on a source or license itself (a source derived
  from another, a license's own contributors).
- **Value links** hang a source or a pair on one value. A value reaches
  licenses through its sources.

Every link may carry a **role** (`primary_source`, `author`, ...). Roles
are created on purpose (`PutRole` / `put_role`); a link naming an unknown
role is refused, so a typo never becomes a role. A fresh file has no
roles. The recommended names, with the term each stands for:

| Role | Meaning |
|---|---|
| `author`, `publisher`, `maintainer`, `contributor` | Frictionless / DataCite contributor roles |
| `primary_source` | prov:hadPrimarySource |
| `adjustment` | a source that adjusted the primary one |
| `derived_from`, `edition_of`, `part_of` | relations between sources (prov:wasDerivedFrom, ...) |
| `declared` | what the data owner declared |

One target under two roles is two links; "no role" is one more.

All of it goes through the metadata channel of a save (`metadata=` in
Python, `ChangeSet::metadata` in Rust), in the same atomic commit as the
values. Items are `"op"`-keyed dicts:

=== "Python"

    ```python
    db.commit_changes(metadata=[
        {"op": "put_source", "source": "IEA WEO 2024",
         "fields": {"title": "World Energy Outlook 2024", "issued": "2024-10-16"}},
        {"op": "put_license", "license": "CC-BY-4.0", "fields": {"spdx": True}},
        {"op": "set_source_licenses", "source": "IEA WEO 2024", "licenses": ["CC-BY-4.0"]},
        {"op": "put_role", "role": "primary_source"},
        {"op": "link", "subject": {"dataset": True},
         "target": {"source": "IEA WEO 2024"}, "role": "primary_source"},
        {"op": "link_value",
         "value": {"class": "node", "parameter": "demand",
                   "alternative": "Base", "entity": "north"},
         "target": {"source": "IEA WEO 2024"}},
        {"op": "set_property", "key": "title", "value": "Nordic grid model"},
    ], user="alice")
    ```

=== "Rust"

    ```rust
    use nodal_db_core::api::{commit_change_set, ChangeSet};
    use nodal_db_core::types::{LinkSpec, MetadataChange as M, Ref, RoleSel, Target, ValueAddr};

    let set = ChangeSet {
        metadata: vec![
            M::PutSource { source: Ref::Name("IEA WEO 2024".into()), rename: None,
                           fields: Default::default() },
            M::LinkValue {
                value: ValueAddr { class: "node".into(), parameter: "demand".into(),
                                   alternative: "Base".into(), entity: "north".into() },
                link: LinkSpec { target: Target::Source(Ref::Name("IEA WEO 2024".into())),
                                 role: RoleSel::NoRole },
            },
        ],
        ..Default::default()
    };
    commit_change_set(&db, set, "alice", "provenance", Default::default())?;
    ```

A `put_*` of an unknown name creates the record; of a known one it
revises it to exactly the given fields (same id, a new version; nothing
when equal). Names of sources, licenses, roles, pair keys and property
keys are identifiers (case-insensitive, NFC, no edge whitespace); pair
values, titles, paths, citations and policies are stored verbatim. A
pair whose key differs from an existing key only in case is stored under
the existing spelling (with a `name_folded` warning); `rename_pair_key`
re-spells or renames every pair of a key at once.

#### New value versions: keep, set or clear

Value links belong to a **version** of the value. When a save gives a
value that has links a new version, it must say what the new version
keeps; there is no default:

- **keep**: the new version keeps the old version's links;
- **clear**: the new version has none;
- **set**: the new version has exactly the listed links.

Say it once for the whole save, per `(class, parameter, alternative)`
target, or per value; the value wins over the target, the target over
the save:

=== "Python"

    ```python
    db.write_parameter("node", "demand", "Base", batch, user="alice",
                       value_metadata="keep")
    db.commit_changes(segments=[...], user="alice", metadata=[
        {"op": "value_metadata", "scope": "save", "action": "clear"},
        {"op": "value_metadata",
         "scope": {"class": "node", "parameter": "demand", "alternative": "Base"},
         "action": {"set": [{"source": "IEA WEO 2024"}]}},
    ])
    ```

=== "CLI"

    ```bash
    nodal-db import update.arrows --db demo.duckdb --value-metadata keep
    ```

A save that changes such a value without a statement is refused as a
whole (`ValueMetadataUnstated`, Python `nodal_db.ValueMetadataUnstated`,
with `count` and up to ten `sample` names) and writes nothing. Values
without links, new values, unchanged writes and deleted values need no
statement. A save- or target-level `set` also annotates the new values
of the save ("everything in this import is from IEA"); a value-level
`set` or `clear` also edits the links of an unchanged value. Next to a
keep, `link_value` / `unlink_value` add or remove single links; next to
a value-level `set` they are refused (a set is the exact list).
Template migrations that only re-spell stored strings keep the links.
`value_metadata_link_asof(c)` and `value_metadata_asof_named(c)` show
each version's links.

#### Deleting metadata

`delete_source`, `delete_license` and `delete_pair` refuse while links
remain (`restricted`, with blockers, nothing closed), judged on the state
after the save: links the same save removes do not block, links a keep
reopens do. With `"cascade": True` the links close with the record. A
source's license list and its own links close with it; a license is
also restricted by the sources published under it. A role is deleted
only when nothing uses it (rename it instead). One save may name an
object in one revise, rename or delete item only; links may name an
object the same save creates, but no other item may revise, rename or
delete it (create it in its final form, or change it in a later save).

Links never block deleting the object they hang on: deleting an entity,
class, parameter, alternative or scenario closes its links (and the
links of its values) with it. Erasing an alternative removes its links
physically; sources, licenses and pairs stay, with their history.

#### Database properties

Single-valued facts about the database live in `database_property`, one
current value per key:

- **core** keys the core writes; a save cannot set them:
    - `schema_version` (see [Versions and compatibility](versions.md)),
      `db_identity` (the file's identity UUID) and `created_by` (the
      nodal-db build that created the file, e.g.
      `nodal-db-core 0.1.0 (git 1a2b3c4d5e6f)` for a release build,
      `(dev)` for a local one);
    - the template stamp (`template_name`, `template_version`,
      `template_content_hash`, `template_migration_step`,
      `template_applied_commit`);
    - `refinement_classes` (see [Refinement links](#refinement-links));
    - `presentation` and `audit_rules`: JSON documents the core and the
      editor share (parameter groups; conditional requirements). Their
      shape is internal to nodal-db and may change in 0.x; each carries a
      `"version"` member (now `1`) that changes when the shape does;
    - every key that starts with `nodal:` (in any case), a namespace
      reserved for future core keys;
- **tool** keys `<tool>:<name>` a tool owns (e.g. `flextool:frozen`).
  Every key with a `:` after its first character is a tool key, except
  the reserved `nodal:` keys, so a CURIE-style key such as `dct:title`
  is owned by a tool named `dct`; use the plain documented keys below for
  the dataset's own facts. A tool uses its own name as the prefix (as
  FlexTool does with `flextool:`);
- **user** keys, everything else. Documented keys, with their crosswalk:

| Key | Meaning | DCAT / Dublin Core / Frictionless |
|---|---|---|
| `identifier` | persistent identifier (DOI) | dct:identifier / `id` |
| `base_iri` | IRI root of the dataset's terms; survives an extract | void:uriSpace |
| `title`, `description` | the dataset's title and description | dct:title, dct:description / `title`, `description` |
| `version` | the dataset's own version label | dcat:version / `version` |
| `created` | creation date | dct:created / `created` |
| `homepage` | landing page | dcat:landingPage / `homepage` |
| `language` | language tag | dct:language |
| `semantic_context` | JSON-LD context of the IRIs the dataset uses | |
| `conforms_to` | the profile the dataset claims | dct:conformsTo |

Set or clear one with `set_property` (`"value": None` clears); read them
with `db.database_property(key)`, `db.database_properties()`, or the
`database_properties` view. Keywords, themes and contributors, which
have several values, are pairs linked to the dataset.

**Reads.** `value_metadata_named`, `object_metadata_named`,
`dataset_metadata`, `source_licenses_named`, `value_licenses` ("every
value under license X") and `database_properties` name everything; the
`*_current` / `*_asof(c)` mirrors of the eight tables show the rows.

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

Do not create a macro named like a DuckDB built-in function (`lower`,
`max`, `nextval`, `range`, …) in the file's `main` schema. DuckDB would
use it instead of the built-in in every later query on the file,
including the name lookups and unique indexes nodal-db relies on, so
nodal-db refuses to open such a file (`ShadowedBuiltin`, naming the
macro and the `DROP MACRO` statement that removes it) until you drop
it. Anything written to the file while the macro existed may have left
duplicate names that the name indexes no longer catch, so compact the
file after the drop (`nodal-db maintain compact FILE`): compaction
rebuilds the indexes and fails loudly on such a duplicate. Put your own
macros under another name or in another schema (`CREATE SCHEMA my;
CREATE MACRO my.lower(x) AS …`).

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

### Opening files from others

A database file carries its own SQL: the views and macros of its read
surface. Whoever made the file could have replaced them, so a file you
were sent runs its author's SQL whenever it is read. nodal-db therefore
opens every connection with DuckDB's file, network and extension access
turned off: no reads or writes of other files (`read_csv`, `read_text`,
`COPY … TO`, `glob`, `ATTACH`), no HTTP, no extension install or
autoload, no persistent secrets. The configuration is locked, so no SQL
on the connection can turn this back on. The database file itself, its
`.wal` and DuckDB's temporary spill files work as usual; compaction and
extract may also attach the one source file they copy.

This applies to `db.query` too, so your own SQL there cannot read or
write other files. Run the query in nodal-db and do the file I/O on
your own DuckDB connection or in pyarrow:

```python
table = db.query("SELECT * FROM read_parameter('node', 'demand')").read_all()
pyarrow.parquet.write_table(table, "demand.parquet")
```

For a file you trust, `trusted=True` opens that one query's connection
with DuckDB's full access:

```python
db.query("SELECT * FROM read_csv('targets.csv')", trusted=True)
```

!!! warning "`trusted=True` trusts the file, not only your query"
    The file's own views and macros run on the same connection, so with
    `trusted=True` they get full access too: they can read any file you
    can read and make network requests. Use it only for files you made
    or trust. Rust code passes `conn::Access::Trusted` to
    `conn::with_read_retrying_as` for the same effect.

Your own DuckDB connection to a file someone sent you is not locked
down: open it with `config={"enable_external_access": False,
"autoload_known_extensions": False, "autoinstall_known_extensions":
False}` (Python) when you only read it.

The CLI also caps DuckDB's memory and threads for every database a
command opens, so a huge or crafted file cannot take the whole machine:
by default at most 50% of RAM (DuckDB's own default is 80%) and at most
8 threads. Set them with the global options `--memory-limit SIZE` (`4GiB`,
`25%`) and `--threads N`, e.g. `nodal-db --memory-limit 8GiB maintain
compact big.duckdb`. Under SLURM (`SLURM_MEM_PER_NODE`,
`SLURM_CPUS_ON_NODE` set) DuckDB sizes itself from the allocation
instead. DuckDB spills large sorts and joins to disk, so a lower limit
makes a large operation slower, not impossible.

Other inputs are bounded too: the importer refuses an Arrow-IPC file
larger than 16 GiB or one whose compressed buffers claim more than
2 GiB each, or 16 GiB in total, once decompressed, one whose dictionary
columns and dense rows would decode to more than 16 GiB, and entity
names or labels longer than 64 KiB (all E6). These limits are the
importer's own, separate from `--memory-limit`. A template
larger than 16 MiB or with flow collections (`[…]`, `{…}`) nested more
than 64 deep is refused before it is parsed.

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
last compaction (`--growth`), or when 60 commits have landed since then
(`--max-commits`; the default grows with the file size above about
180 MiB, to 100 from about 560 MiB; `0` in the CLI or `None` in Python disables it). The commit count matters
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

**Compatibility note.** nodal-db renders time-pattern positions with `;`
for AND within a clause and `,` for OR between clauses (for example
`M1-4;WD1-5,M9-10`). This matches the time-pattern notation used by Spine
Toolbox databases. (Time-pattern values are not imported yet; the
importer reports them.)

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
everything that could not be imported. The descriptions of classes,
parameters, alternatives, scenarios and entities are imported with
them.

**Metadata.** Spine's `metadata` entries become pairs (see
[Metadata and provenance](#metadata-and-provenance)), in one final
commit: an entry's key spelled two ways (`Source`, `source`) is one key
in its first spelling (the merge is residue), entity metadata becomes
entity links, value metadata becomes value links of the imported
version (a link of a value that was not imported is residue), and
entries nothing links to are linked to the dataset. Spine `source` and
`license` entries stay pairs. A profile can route a tool's own marker
entries to tool database properties by overriding `tool_property(name)`
(return `"<tool>:<name>"`, or `None` for an ordinary entry).

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
list members are data and are imported as they are, except that nodal-db
stores names and map keys in Unicode NFC (see
[Names, labels and Unicode](#names-labels-and-unicode)). The importer does
not convert anything itself; the core does, and the import report counts
what it converted (`normalized identifiers` in `report.summary()`). Two
names of one kind that differ only in Unicode normalization (or in it
and case) stop the import, like a case collision. Two keys of one map
that differ only in this way are one label to nodal-db, so they are a
duplicate key: the last entry is kept and the warning in
`report.duplicate_keys` says the spellings differ only in Unicode
normalization.

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

Which versions a file and a build carry, and what a new release means
for existing files, is on [Versions and compatibility](versions.md).

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
in Python, a subclass of `ApiError`): every write (of any kind) and
compaction, extract and erase refuse such a file up front, before
writing anything; reads are not checked. `found` is
the file's `schema_version`, `expected` this build's. It is never
retried. Re-create the database with this build (init, then import), or
use a nodal-db build that matches the file.

Compaction first folds a leftover `<db>.wal` (left by a writer that
crashed after committing) into the file. When DuckDB cannot replay or
remove that WAL (another file's WAL, an old one of this file, an
unreadable one, or the file cannot be opened read-write), compaction
refuses with `UnreplayableWal` (`DbError::UnreplayableWal { wal, cause }`
in Rust, `nodal_db.UnreplayableWal` with `wal` and `cause` attributes in
Python). Nothing is changed and the WAL is not deleted. Inspect it, move
it aside, then compact again.

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
