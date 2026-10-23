---
hide:
  - toc
---

# Schema

nodal-db is a DuckDB schema plus a Rust core that writes into it. The
schema lives in [`schema/*.sql`](https://github.com/nodal-tools/nodal-db/tree/main/schema),
applied in file order by `schema/build.py` (the Rust core embeds the same
files). It is domain-agnostic: "node", "unit" or "generator" are not tables
but `entity_class` and `parameter_definition` *rows*, loaded from a
template.

The diagrams and view tables on this page are **generated** from that DDL
(`scripts/docs/gen_schema.py`), so they track the schema exactly; CI fails
if they go stale.

## Layers

The DDL files form layers, each building on the ones before:

| Files | Layer |
| --- | --- |
| 001_meta | The append-only `commit` log, key/value `metadata` and its link tables, and the `erasure_marker` trace. |
| 010_entities | Entity classes (N-dimensional), class-sets, entities, their elements and locations. |
| 020_alternatives | Alternatives, scenarios (rank-ordered alternatives), entity activation, and group membership. |
| 030_parameters | Units registry, value lists, parameter definitions, per-entity unit overrides, declared parameter types. |
| 040_axes | Shared, immutable axis objects (time grids, periods, labels, …) that array values index into. |
| 050_values | The three storage tiers — scalar, dense array (one flat list per value), long (one row per cell) — and the definition-level default/prefill mirrors. |
| 055–065 | Run provenance, authored display modes, and the ontology crosswalk (`semantic_annotation`). |
| 070–120 | Views and table macros only: current state, the unified read surface, scenario resolution, completeness, history (as-of), and audits. |

### Conventions that apply to every table

- **Versioned rows.** Mutable tables carry `valid_from_commit` /
  `valid_to_commit` (both `REFERENCES commit(id)`) and a `version`. A row
  is current while `valid_to_commit IS NULL`; edits close the old row and
  append a new one, so history is never rewritten. (The dense-array tier is the
  exception: `value_array` holds current rows only and superseded ones move
  to its `value_array_hist` twin.) These versioning columns and their
  commit edges are omitted from the diagrams below to keep them legible.
- **Stable ids, not primary keys.** Versioned rows share their `id` across
  versions, so `id` is not a `PRIMARY KEY` there and DuckDB cannot enforce
  foreign keys into those tables. Real primary keys exist only on tables
  whose rows are never versioned: the append-only `commit`, `axis` (and its
  per-kind child tables), `run` and `erasure_marker`, plus `value_array`,
  which holds current rows only.
- **`array_id`, the dense-array row handle.** `value_array`'s primary key
  is a surrogate `array_id`; the key triple (`entity_id`,
  `parameter_definition_id`, `alternative_id`) stays the logical identity
  and is enforced by a `UNIQUE` constraint. `value_array_hist` carries the
  same `array_id` as its first column, copied from the live row it
  superseded. An `array_id` is a physical handle, not an identity: it
  stays the same while a value is replaced in place, a delete or tier
  change followed by a new write gets a new one, and ids are never
  re-used. Ids are non-contiguous by design (the sequence steps by 2,
  which keeps id sets non-contiguous so lookups use the index); do not
  assume they are dense, ordered by time or portable between databases.
  The table-shaped views `value_array_current` and `value_array_asof`
  show the column; the logical read surface (`read_parameter*`,
  `value_cells`, `value_rows`) does not.
  To read one array's payload in plain SQL, resolve the id on the narrow
  columns first and fetch the list by it, or use `read_parameter_dense`:

  ```sql
  SELECT p.values
  FROM value_array k JOIN value_array p USING (array_id)
  WHERE k.entity_id = 42 AND k.parameter_definition_id = 7
    AND k.alternative_id = 1;
  ```

  A filter on the key triple directly on the list-carrying scan
  (`SELECT values FROM value_array WHERE entity_id = …`) reads the list
  data of the whole table instead.
- **One current row, case-insensitive names.** Hidden generated `cur_*`
  columns back unique indexes that allow exactly one current row per id
  and case-insensitive name uniqueness; they are omitted from the diagrams.

### Reading the diagrams

- **Solid edge** — a `FOREIGN KEY` declared in the DDL and enforced by
  DuckDB (always into `commit` or `axis`).
- **Dashed edge** — a reference implied by the column name (`*_id`), not
  enforced by the engine. Most of these are checked after the fact by the
  `audit_dangling_ref` view.
- Boxes without columns are tables from another group, shown for context.
- The value tables all reference the same few tables, so each gets its own
  small diagram instead of one tangled one.
- Wide tables show only their key columns; every column of every table is
  listed in the collapsible **Columns** block under each diagram.

## Tables

--8<-- "_generated/schema_diagrams.md"

## Views and table macros

Readers normally query views and macros rather than the base tables: the
`*_current` views are the default read surface, and the `*_asof(c)` macros
reconstruct any past commit. Purposes below are taken from the DDL comments.

--8<-- "_generated/schema_views.md"
