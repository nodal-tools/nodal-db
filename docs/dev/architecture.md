# Architecture

This page maps the system's structure and its invariants, with the
reasoning behind each major decision. It is a map, not the contract:
**the code and the conformance suite are authoritative**. The
executable contract is the
[`conformance/`](https://github.com/nodal-tools/nodal-db/tree/main/conformance)
directory together with the Rust test suite; module-level doc comments
in `crates/nodal-db-core/src/` carry the detail for each component. If
this page and the code disagree, the code wins and this page should be
fixed.

For what nodal-db does from a user's point of view, see
[Features](../features.md) and [Using nodal-db](../usage.md). For the
rules a client tool must follow, see
[Building against nodal-db](building_against_nodal-db.md).

## 1. System shape

One DuckDB file is the database. Around it:

- **`nodal-db-core`** (Rust library,
  [`crates/nodal-db-core`](https://github.com/nodal-tools/nodal-db/tree/main/crates/nodal-db-core))
  — the writer library and the **single canonicalizer**. Every write,
  from any client, ends in this crate.
- **`nodal-db` CLI**
  ([`crates/nodal-db-cli`](https://github.com/nodal-tools/nodal-db/tree/main/crates/nodal-db-cli))
  — `init` (schema embedded at compile time; `--template` loads a
  template right after), `import` (the
  [Arrow-IPC bulk format](arrow_import_format.md)), `erase-alternative`,
  and `template load` / `plan` / `apply` / `export`. It is the write
  boundary for clients in languages without bindings. `migrate` and
  `maintain` are reserved verbs that are not implemented; the reconcile
  runner in v1 is `template apply`.
- **Python skin** (`nodal_db`,
  [`crates/nodal-db-py`](https://github.com/nodal-tools/nodal-db/tree/main/crates/nodal-db-py))
  — a thin PyO3/maturin wrapper over the core: it marshals Arrow data in
  and out and translates errors into Python exceptions, with no logic of
  its own. Python *readers* do not need it; they query the SQL views
  with the `duckdb` package.
- **egui editor** — developed in a separate repository, so that it
  cannot reach core internals. It depends on `nodal-db-core`, reads
  through the SQL views and writes through the core API. *In
  development.*

The API rule is **Arrow in, Arrow out**: values cross the API as Arrow
arrays and plain data, never as a hierarchy of value-object classes.
This is a deliberate one-way decision; it keeps the API identical
across Rust, Python and the bulk format.

## 2. Storage layer: the DuckDB schema

[`schema/`](https://github.com/nodal-tools/nodal-db/tree/main/schema)
holds the executable DDL as numbered SQL files applied in lexical order.
There is one file set and three appliers: `schema/build.py`,
`nodal_db_core::schema_init`, and the compile-time embedded copy used by
the CLI and the Python skin (a census test asserts the embedded list
matches `schema/*.sql`).

- `001` meta (commit log, erasure markers, sequences, metadata) · `010`
  entities/classes/dimensions · `020` alternatives/scenarios · `030`
  parameter definitions · `040` axes (including parametric float and
  datetime axes, and the CRS property) · `050` value tables (three tiers
  plus the `_hist` twin of the array tier) · `055` provenance (`run`) ·
  `060` display modes · `065` semantic annotations
- `070`–`120` the read surface: `_current` views, read macros
  (`read_parameter`, `value_rows`, …), scenario and activation
  resolution, completeness, `_asof` history views, and the audit views
  (`audit_health` and others). Validation is *reporting*, not
  constraints: questionable data is surfaced by audit views rather than
  rejected at write time, so a partly finished model can always be
  stored.
- `900` bootstrap seed.

Design points:

- **Append-only history everywhere** via `valid_from_commit` /
  `valid_to_commit`. A row of the `commit` table is the validity edge:
  every change made in one commit carries that commit's id.
- **Scenario resolution is done in views.** Alternatives are stacked by
  rank, and activation propagation (membership plus `realizes`
  refinement edges) is a recursive view. Structural *resolution* lives
  in SQL; value *transforms* stay in tool code.
- **Units and CRS are expressed, never asserted**: they are carried and
  hashed with the data, never converted.

The [schema page](../schema.md) shows the generated table diagram.

The schema is verified by `schema/smoke_test.py` (198 checks), which
includes hostile-data regressions: it plants corruptions and asserts
that the right constraint or audit fires. Run it with the project-local
`.venv/bin/python`; its DuckDB version must match the engine bundled in
the core (currently 1.5.5).

## 3. Workspace map

Cargo workspace, three crates: `nodal-db-core`, `nodal-db-cli`,
`nodal-db-py`. Modules in `crates/nodal-db-core/src/`:

| Module | Responsibility |
|---|---|
| `types` | Plain, owned, FFI-friendly data types for the pipeline; the Python skin wraps them directly. |
| `encoding` | Canonical byte encoding of axes and values, and the XXH3-64 content hash. Pure, no database. |
| `canon` | The canonicalization pipeline: NULL elimination → axis derivation → row-major sort → tier. Pure over Arrow arrays. |
| `tier` | Pipeline step 4: the deterministic scalar / array / long decision and payload shaping. |
| `reconstruct` | Dense flat payload → logical long rows; the inverse of `tier`, bit-compatible with the SQL views. |
| `error` | The one `DbError` for the database-facing layer. |
| `schema_init` | Apply `schema/*.sql` in numbered order (`build.py` in Rust) and the embedded schema list. |
| `conn` | Connection discipline: open-work-close, a per-path in-process lock, bounded-backoff retry, write transactions that stamp the commit row. |
| `axis` | Content-verified axis deduplication (the hash narrows, byte equality decides). |
| `history` | The history write protocol: from/to close-out, array `_hist` pre-image, version counter, tier transitions; batched long-tier inserts. |
| `api` | The public write/read surface: `write_parameter`, `ingest_segments`, `commit_changes`, `delete_values`, `commit_structural`, `delete_objects`, `preview_object_deletes`, `read_parameter`, `db_identity`. |
| `object_delete` | Close-out of objects and their dependents, driven by one leg registry (cascade, RESTRICT report and preview cannot drift apart). |
| `structural` | The structural (non-value) write path: create/rename alternatives and scenarios, scenario ranking, activation, membership, and entity / class / parameter creation. |
| `verbs` | Idempotent single-purpose write verbs (`ensure_entity`, `set_scenario_alternatives`, …) used by template migration programs and importers; thin wrappers over the `structural` and `api` paths. |
| `erasure` | Hard erasure of one alternative's data, with an `erasure_marker` trace, in one transaction. |
| `compaction` | Physical repack: rebuild into a fresh file, full-content self-check, atomic rename. |
| `template` | The template loader: parse and validate the YAML/JSON template format, load into a fresh database, reconcile against a populated one (plan / apply with hints and `run:` migration programs), export. |

`crates/nodal-db-cli/src/`: `main` (verb dispatch), `template` (the
`template` subcommands), and `import`, the Arrow-IPC reader. `import` is
a *thin structural adapter*: it decodes the container, upcasts
timestamps, expands dense segments and makes one `ingest_segments`
call. It never canonicalizes anything itself.

The core re-exports its `duckdb` crate (and thus `duckdb::arrow`) so
downstream code compiles against exactly one Arrow version. The CLI's
IPC reader needs Arrow's `ipc` feature, which `duckdb` does not enable,
so the workspace adds a direct `arrow` dependency pinned to exactly the
version `duckdb` re-exports; Cargo unifies the two into one crate. A
`duckdb` upgrade therefore means bumping `duckdb`, `arrow` and
`arrow-pyarrow` together (see the comments in the workspace
`Cargo.toml`).

## 4. The write path and the one canonicalizer

**Invariant: never add a writer path that bypasses the canonicalization
pipeline.** The tier and the canonical encodings are deterministic
functions of the data. Two independent canonicalizers drift, and drift
breaks history diffs, axis deduplication, and byte equality between
writers. Hence there are exactly two write boundaries: the
`nodal-db-core` API in-process (directly or through the Python skin),
and `nodal-db import` for everything else. Both end in the same code.

The pipeline:

1. **NULL elimination** — NULL means absent. There is no "present but
   null" cell, so a writer that sends explicit NULLs and one that omits
   the tuples produce the same result.
2. **Axis derivation** — axes come *from the data*: the distinct present
   coordinates per dimension, sorted by kind. Regular progressions are
   always stored in parametric form (start, step, length). Label
   ordinals come from the caller's ordered domain (an in-process
   argument, or the `spinal:label_domain` field metadata in the bulk
   format).
3. **Row-major sort** in declared-dimension order.
4. **Tier decision** — rank 0 → scalar; rank ≥ 1, float-valued, full
   Cartesian grid → dense array; otherwise → long. The *library*
   decides; callers never pick a tier.

Then come content-verified axis deduplication (`axis`) and the history
write protocol (`history`), all inside one atomic, conflict-retried
commit whose `commit` row is the validity edge. Name resolution on the
value path **auto-creates missing names with a loud, machine-readable
warning**: end-user tools choose their own policy, and the library
neither guesses silently nor hard-blocks. The structural path resolves
strictly and errors on a missing referent.

Canonical encodings: axes and values serialize to a pinned byte layout
hashed with **XXH3-64, seed 0**. The hash is only an accelerator:
deduplication narrows by hash and then compares bytes, so a hash
collision can never attach values to the wrong axis. A value content
hash is implemented but not yet stored. The byte layouts are pinned by
the vectors in
[`conformance/encoding_vectors`](https://github.com/nodal-tools/nodal-db/tree/main/conformance/encoding_vectors)
and
[`conformance/value_vectors`](https://github.com/nodal-tools/nodal-db/tree/main/conformance/value_vectors).

## 5. Value model

- **Three physical tiers, one logical shape**: sparse or mixed data as
  long rows, dense float grids as a flat row-major `DOUBLE[]` (with a
  `BIGINT[]` sibling) against shared axes, and rank-0 values as
  scalars. Readers never see the tier; the views present one long
  format. See [storage tiers](../features.md#storage-tiers).
- **Axes are content-addressed and immutable**, in the spirit of CF
  coordinate variables: shared by foreign key, deduplicated by content,
  parametric where regular so reconstruction is bit-exact. The
  parametric float-axis reconstruction rule (no fused multiply-add,
  fixed operand order) is evaluated in two places, the SQL views and
  Rust, so it is pinned by a conformance fixture whose inputs give a
  different result under FMA.
- **History is a hybrid**: scalar and long tiers version rows in place
  with from/to commits; the array tier keeps only live rows and copies
  pre-images to a `_hist` twin, entirely in SQL so the engine guarantees
  atomicity. Version counters are partitioned per tier per key and
  checked by the `audit_history_tiling` view.
- **NaN** is a legal *value* (every NaN bit pattern has the same
  canonical encoding), never a legal axis coordinate.
- The rank cap is 4.

## 6. Concurrency and connection discipline

**Invariant: hold connections per operation, never long-lived.**
DuckDB's file lock is symmetric: while any read-write connection is
open, other read-write *and* read-only opens fail, and an idle read-only
handle (a GUI, a notebook) blocks a writer's read-write open just as
much, starving every commit. So everything is open → work → close:

- `conn::with_write_txn` holds the read-write lock only for the span of
  one commit; `with_read` opens a fresh read-only connection per query.
- Change detection is **commit polling**: a poller makes a short
  read-only query of the newest commit id between writer commits. There
  is no notification channel and no long-lived watcher connection.
- Contention is *defined behavior*: bounded-backoff retry, surfacing as
  `DbError::Busy` rather than a panic or hang. The write-conflict retry
  matches on the `Conflict` substring, because DuckDB's exact message
  changed between versions.
- **In-process serialization**: DuckDB's file lock excludes other
  *processes* only. Two threads opening the same file get independent
  database instances whose commits can silently merge or be lost. Every
  open-use-close therefore first takes a process-global per-path mutex.

The model is single-writer. If this proves insufficient, the intended
next step is a serving process that owns the file, not a change to the
discipline. The client-side rules are in
[Using nodal-db](../usage.md#connection-discipline).

## 7. Deletion, erasure and the change model

Value deletes, structural changes and object deletes ride the same
atomic commit as value writes (`api::commit_changes`). Object deletion
(alternative / parameter / entity / class) closes out the object row and
its dependents via one **leg registry**, so the preview counts, the
RESTRICT report and the cascade updates are generated from the same
table and cannot drift. Deletes are close-outs: history survives.

What cascades is decided by one question: does the delete punch a hole
in a live N-dimensional container? The cascade follows those edges and
the preview shows the full blast radius before anything is written.
In-place structural *reshape* (changing a parameter's dimensions while
keeping its values) is not supported; the supported route is close-out
and recreate.

**Invariant: history is append-only, with one sanctioned exception.**
Rows are closed out, never updated in place. Physical removal exists
only as `erasure::erase_alternative` (CLI: `nodal-db erase-alternative`),
which removes every row keyed on one alternative, of any validity, and
writes an `erasure_marker` row in the same transaction. The removed
rows carry no close-out edge; the marker is their trace. The `commit`
log, the alternative's identity row (closed, not removed), `run`
provenance and axes are retained. As-of reads that would silently span
removed content fail loudly on the marker. History may be removed with
a trace; it may never be rewritten. Erasing is intended for discarding
results that are about to be regenerated, not for curated input data.
See [erasure with markers](../features.md#erasure-with-markers).

## 8. Compaction

DuckDB's single-file storage never shrinks the file, so
`compaction::compact` rebuilds it: create a fresh temporary database
with the schema, attach the original read-only, copy every base table in
foreign-key order (clustered with current rows first), restore sequence
state, run a full-content self-check, fsync, and atomically rename over
the original. It is non-destructive (all history, commits and axes
survive at the query level) and crash-safe by construction: the original
is only read until the final rename. Compaction holds the per-path lock
for the whole operation and runs outside any write transaction, so a
failed compaction can never roll back a commit. `compact_if_needed`
skips the rebuild below a growth threshold, and
`erasure::erase_alternative_and_compact` chains the two operations.
Compaction is a Rust API only; there is no CLI verb yet.

## 9. Testing

Three legs, run through `just` (`just` is the quick gate, `just full`
the release gate that adds the `#[ignore]`d slow tests and benchmarks):

1. **Schema smoke suite** (`.venv/bin/python schema/smoke_test.py`, 198
   checks): builds a demo model, exercises resolution, membership,
   history and audits, then plants hostile-data corruptions and asserts
   that the right constraint or audit fires.
2. **Rust suite** (`cargo test --workspace`): unit tests, integration
   tests in
   [`crates/nodal-db-core/tests`](https://github.com/nodal-tools/nodal-db/tree/main/crates/nodal-db-core/tests),
   and end-to-end CLI tests that drive the real binary.
3. **The conformance suite is the executable contract.**
   [`conformance/`](https://github.com/nodal-tools/nodal-db/tree/main/conformance)
   holds encoding vectors, value vectors, pipeline cases,
   history-protocol cases (all six directed tier transitions),
   reconstruction fixtures, and an independent Python oracle that checks
   that a second implementation produces identical bytes.

The benchmark gate lives in `crates/nodal-db-core/benches/`, with
results recorded in
[`benches/RESULTS.md`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/benches/RESULTS.md);
it is re-run at each DuckDB upgrade. One known residual is recorded
there: a point read of a single array-tier value is slower than its
target, because DuckDB decompresses the containing segment.

CI runs `cargo test --workspace` and the schema smoke suite.

## 10. Status

- **Implemented**: the core write and read path, the tier pipeline, axis
  deduplication, the history protocol, Arrow-IPC import, CLI `init`,
  value and object deletion, structural writes and verbs, hard erasure,
  compaction, the template loader (load / plan / apply / export), the
  Python skin, conformance and benchmark gates.
- **In development**: the editor (separate repository).
- **Planned**: CLI `migrate` and `maintain` (verbs reserved); a CLI verb
  for compaction.
- **Not planned for now** (design left open, no code): a serving
  process, value inheritance, symbols, structured units, in-place
  reshape.
- **Known debt before the encoding freezes**: the encoding pins Unicode
  16.0.0 NFC normalization. This is currently held only by an exact
  `unicode-normalization = "=0.1.24"` pin, while the Python oracle uses
  CPython's own Unicode tables; the normalization tables need to be
  vendored.

## 11. Naming note

The project was renamed from spinal-db to nodal-db; crates, CLI and
Python package use `nodal-db` / `nodal_db`. The `spinal:*` Arrow
metadata keys of the [bulk format](arrow_import_format.md) were
deliberately *not* renamed: they are frozen in format v1.0, and changing
them would be a format break.
