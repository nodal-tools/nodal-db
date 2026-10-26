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
Before changing the schema, core SQL, indexes, sequences, storage or compaction, read [Engine rules](engine_rules.md).

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
  `extract` (a history-free copy into a new file), `maintain compact`,
  and `template load` / `plan` / `apply` / `export`. It is the write
  boundary for clients in languages without bindings. `migrate` is a
  reserved verb that is not implemented; the reconcile runner in v1 is
  `template apply`.
- **Python skin** (`nodal_db`,
  [`crates/nodal-db-py`](https://github.com/nodal-tools/nodal-db/tree/main/crates/nodal-db-py))
  — a thin PyO3/maturin wrapper over the core: it marshals Arrow data in
  and out and translates errors into Python exceptions, with no logic of
  its own. Python *readers* do not need it; they query the SQL views
  with the `duckdb` package.
- **egui editor** — developed in a separate repository, so that it
  cannot reach core internals. It depends on `nodal-db-core`, reads
  through the SQL views and writes through the core API. Its
  per-entity reads (one entity, a few, in one or all alternatives) go
  through `api::read_values` (the `read_values` macro); the bulk display
  of a whole parameter goes through `read_parameter`. *In
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
  datetime axes, and the CRS property) · `050` value tables (three storage tiers
  plus the `_hist` twin of the dense-array tier) · `055` provenance (`run`) ·
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

The schema is verified by `schema/smoke_test.py` (234 checks), which
includes hostile-data regressions: it plants corruptions and asserts
that the right constraint or audit fires. Run it with the project-local
`.venv/bin/python`; its DuckDB version must match the engine bundled in
the core (currently 2.0.0).

Engine facts the code relies on (DuckDB 2.0):

- **Files are created in the native 2.0 storage format** (tag `v2.0.0+`
  in `duckdb_databases()`), by every create path: `schema_init`, the CLI
  `init`, the Python skin, compaction's rebuild, and `schema/build.py`
  (gated to DuckDB >= 2.0). Readers need DuckDB 2.0 or newer. The format
  matters for speed: on a 1.5-format file every write to `value_array`
  logs the whole list column to the WAL and stalls the commit on a
  checkpoint; on a native file the same commit takes milliseconds.
  `tests/storage_format.rs` guards this. DuckDB 2.0 still writes an old
  1.5-format file in its old format, so a stale local file keeps the slow
  path until it is rebuilt: `compaction::compact` always writes a native
  file, or re-create the file.
- **A failed statement aborts the surrounding transaction** (DuckDB 2.0
  default: every error invalidates it, including catalog and binder
  errors). Probe-and-continue inside a write transaction is not allowed:
  any "try this statement, fall back on error" step must run before
  `BEGIN` or in its own transaction.
- **Bulk inserts bind one list per column.** Long cells are written as
  `INSERT … SELECT (?::BIGINT[])[u.k], …, u.v_float, … FROM (SELECT
  unnest(?::T[]) …) u`: each per-cell column is one list, and the per-key
  columns (key triple, member binding, axis ids) are short per-key lists
  indexed by a per-cell key ordinal `k`, up to 16 384 cells per
  execution. With the `unnest`s in a FROM subquery rather than in the
  outer select list, a large value inserts about twice as fast. An array
  payload is one bound `?::DOUBLE[]` (a set of arrays, one nested
  `?::DOUBLE[][]`); a materialized axis's child rows are one columnar
  statement per kind. The statement text does not
  depend on the number of cells, so it is prepared once per transaction
  (through the write connection's statement cache, capacity 64). DuckDB
  2.0 parses a statement with thousands of placeholders slowly, which is
  why one placeholder per cell is avoided. Two engine behaviours the code
  relies on: several `unnest`s in one select list zip by position and
  pad a shorter list with NULL **silently**, so every execution first
  checks that each bound list has the chunk's length, and that every key
  ordinal is in range, since an out-of-range `list[k]` also reads NULL
  silently (in every build, failing with `DbError::Internal`); and with
  the default
  `preserve_insertion_order` the rows land in list order, which keeps a
  freshly written long value's rows in canonical cell order. The write
  connection never changes that setting. A cell diff appends the cells
  it inserts after the value's other rows, so a value's current rows are
  in cell order only until its first partial rewrite; compaction's
  clustering restores the order. **Value rows have no physical-order
  contract**: every reader that needs cell order orders by positions
  (`ORDER BY idx1_ordinal, …` on `read_parameter`), and the core's own
  readers sort or key by position.
- **Point lookups are sequential scans; value writes and deletes are
  set-based.** DuckDB 2.0 never scans through a multi-column index (so the
  value tables carry no key-triple index; their one-current unique indexes
  are integrity, not access paths, and their column order is chosen for
  size: the columns that are constant within a value first, the
  highest-fan-out column last, guarded by `tests/long_index_size.rs`)
  nor through any index on a VIRTUAL
  generated column (`cur_name`, `cur_cls`): each per-key value statement
  scans the table, zone-map pruned, at a floor of roughly 0.5–1 ms. So a commit buffers
  its values per tier and writes each buffer in set-based rounds, one
  writer per tier: `history::write_scalars`, `write_longs` and
  `write_arrays`. A round is one probe per tier for the live state
  (scalar first, then long, then array, the per-key precedence) and the
  versions, the per-key close-out for a tier transition, one close-out
  `UPDATE` per retention mode (for arrays: the pre-image copy and one
  `UPDATE` in place, keyed by `array_id`), and one columnar `INSERT`;
  the keys bind as zipped lists. Rounds are bounded at 16 384 keys and,
  for long values, 16 384 cells (262 144 elements for arrays); a larger
  value is a round of its own. A round of one key binds the probes and
  close-outs as point filters, which are cheaper at that size; the long
  insert has one form for every size. A value's axes are deduplicated
  when it is buffered, in value order, so axis ids are minted exactly as
  a key-by-key writer would. A segment's entity names are resolved the
  same way: one lookup per segment (none for names an earlier segment of
  the commit resolved), one columnar `INSERT … RETURNING` for the
  missing ones. All buffers are flushed after the last segment and
  before the deletes, which run as one set-based round too
  (`history::delete_values`): the class, parameter and alternative
  names resolve once per spelling, each class's entity names in one
  lookup, then the three live probes and one close-out per tier and
  retention mode.

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
| `history` | The history write protocol: from/to close-out, array `_hist` pre-image, version counter, tier transitions; the columnar-bind bulk inserts; one set-based writer per tier and the set-based delete (rounds per commit). |
| `api` | The public write/read surface: `write_parameter`, `ingest_segments`, `commit_changes`, `delete_values`, `commit_structural`, `delete_objects`, `preview_object_deletes`, `read_parameter`, `read_values`, `db_identity`. |
| `object_delete` | Close-out of objects and their dependents, driven by one leg registry (cascade, RESTRICT report and preview cannot drift apart). |
| `structural` | The structural (non-value) write path: create/rename alternatives and scenarios, scenario ranking, activation, membership, and entity / class / parameter creation. |
| `verbs` | Idempotent single-purpose write verbs (`ensure_entity`, `set_scenario_alternatives`, …) used by template migration programs and importers; thin wrappers over the `structural` and `api` paths. |
| `erasure` | Hard erasure of one alternative's data, with an `erasure_marker` trace, in one transaction. |
| `compaction` | Physical repack: rebuild into a fresh file, full-content self-check, atomic rename. |
| `extract` | History-free extract: a new file with the content valid at one commit, the referenced axes only, and a `kind='horizon'` erasure marker; the source is only read. |
| `rebuild` | Crate-private machinery shared by `compaction` and `extract`: the per-table catalog maps (insertion order, generated columns, clustering keys, extract selection classes), the read-only source attach, the sequence restore, durability helpers. |
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
   the tuples produce the same result. Value cells are read here too:
   value floats are normalized (see below), and a timestamp (value or
   coordinate) outside the store's range, 290309 BC to 294247 AD
   (DuckDB's TIMESTAMP span), is rejected rather than stored.
2. **Axis derivation** — axes come *from the data*: the distinct present
   coordinates per dimension, sorted by kind. Regular progressions are
   always stored in parametric form (start, step, length). Label
   ordinals come from the caller's ordered domain (an in-process
   argument, or the `spinal:label_domain` field metadata in the bulk
   format).
3. **Row-major sort** in declared-dimension order.
4. **Tier decision** — rank 0 → scalar; rank ≥ 1, float-valued, full
   Cartesian grid → dense array; otherwise → long (one row per cell).
   The *library* decides; callers never pick a tier.

Then come content-verified axis deduplication (`axis`) and the history
write protocol (`history`), all inside one atomic, conflict-retried
commit whose `commit` row is the validity edge. Name resolution on the
value path **auto-creates missing names with a loud, machine-readable
warning**: end-user tools choose their own policy, and the library
neither guesses silently nor hard-blocks. The structural path resolves
strictly and errors on a missing referent.

**Unchanged writes are skipped.** Before it closes out or inserts
anything, the history protocol compares each value with the key's
current stored value, in the same probe statements it already runs
(scalar: the grouped scalar probe; long: the per-position probe that is
also the version source; array: the narrow live probe as a prefilter,
then the payload by `array_id`). The comparison is on the *stored
columns*, not on a second canonicalization or a hash: the new side is
exactly what the encoder would insert, the stored side is read raw, and
equal means the same tier, binding (`parameter_type_id`), axis ids, and
per row or cell the same `value_kind` and payload bits, with the row's
content retained. Every doubt errs toward "changed": a false "changed"
costs one extra version, a false "unchanged" would silently drop a
write. A long value is diffed cell by cell: when every current cell
shares one set of axis ids, one value kind and one binding, equal to
the new value's, only the changed and removed cells are closed out (one
UPDATE joined on the bound positions, in chunks of 16 384) and only the
changed and added cells are inserted; otherwise, or when every cell
changed, the whole value is closed by the key-level UPDATE. A one-key
diff that closes more cells than it keeps (at most 16 384 kept) closes
"every current cell but the kept ones" instead, an anti-join at the
key-level UPDATE's per-row cost. The
per-position probe streams the stored cells through Arrow into a
compact position-sorted vector (32 bytes per cell, no per-row string;
about 180 MB for a 4.6 million-cell value) and the diff is a sort-merge
with the new value's cells, which canon emits in position order. On
top, `commit_changes` and its wrappers make no commit for
a call that provably changed nothing (every item reported a known
no-op: unchanged values, deletes that found nothing, structural no-ops,
object deletes that found nothing, no auto-create); the transaction is
rolled back and the report carries `commit_id` 0, so the head and the
polling cookie do not move. The check is an allow-list, so a new
outcome or channel commits until it is classified. A write and a delete
of the same value in one call is refused up front, and so is a write
of a value that an object delete in the same call would cascade over
(the cascade closes only rows older than the commit, so it would keep
the cells the write just inserted). A caller can opt out of both
refusals with `WriteOptions::covered_writes = DeleteWins`: the deletes
are then resolved inside the transaction before the first segment and
every write they cover is dropped before it is resolved or buffered
(reported as a `DroppedByDelete` warning, which the no-op check treats
as a no-op), so the cascade never meets a row this commit wrote. The
drop stands only if a covering delete takes effect (a value delete, or
an object delete that ends `Deleted` or whose target did not exist);
otherwise, as for a RESTRICT delete that ends `Restricted`, the call is
refused and rolled back. Under either policy an object delete that
reaches an object a structural change of the same call created is
refused, since the same-commit guard would leave that object live. Direct `history`
callers in their own transaction still commit.

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

- **Three storage tiers, one logical shape**: values with no index as
  scalars (`value_scalar`); float values on a complete grid as a dense
  array, one flat row-major `DOUBLE[]` per value against shared axes
  (`value_array`); everything else (incomplete grids, non-float values)
  in long format, one row per cell (`value_long`). Both the dense array
  and the long tier go up to four dimensions. The schema also has a
  `BIGINT[]` column on `value_array` for integer grids, which the read
  views decode but the core does not write yet, so integer values use
  the long tier. Readers never see the tier; the views present one long
  format. See [storage tiers](../features.md#storage-tiers), and
  [Design rationale: value storage](design_rationale.md) for the
  alternatives considered and the performance trade-offs.
- **Axes are content-addressed and immutable**, in the spirit of CF
  coordinate variables: shared by foreign key, deduplicated by content,
  parametric where regular so reconstruction is bit-exact. The
  parametric float-axis reconstruction rule (no fused multiply-add,
  fixed operand order) is evaluated in two places, the SQL views and
  Rust, so it is pinned by a conformance fixture whose inputs give a
  different result under FMA.
- **History is a hybrid**: scalar and long tiers version rows in place
  with from/to commits; the dense-array tier keeps only live rows and copies
  pre-images to a `_hist` twin, entirely in SQL so the engine guarantees
  atomicity. Version counters are partitioned per tier per key and
  checked by the `audit_history_tiling` view.
- **Dense arrays are fetched by `array_id`.** DuckDB uses an index scan
  only for a single-column key, so `value_array` has a surrogate
  `array_id` primary key next to the `UNIQUE` key triple. The writer
  resolves a value's `array_id` on narrow columns and keys every
  statement that touches the list payload (the pre-image copy, the
  in-place update, the delete) by it; the read macros do the same with a
  self-join. `array_id` is a physical handle, never canonical content: it
  is minted only by its sequence (through the column default for a
  one-key write; a set-based round draws its ids from the sequence first,
  sorts them and assigns them in input order, so they are the ids
  key-by-key inserts would mint), never updated, copied verbatim
  into `_hist` and by compaction, and checked by
  `audit_array_id_coherence` (one id, one key triple). Its sequence steps
  by 2, which keeps id sets non-contiguous so lookups use the index.
  Compaction re-creates sequences with their full specification
  (increment included) so the stride survives.
- **Single-key indexes are used only behind a fence.** DuckDB 2.0 scans
  through an index only when the scan carries that one filter and nothing
  else that statistics cannot drop. Single-name entity lookups therefore
  query the `ix_entity_lname` expression index (`lower(name)`) alone in a
  subquery fenced by `OFFSET 0` and apply the class and current-row
  filters outside it (axis dedup does the same with `axis_hash` and the
  axis kind). That is an optimizer behaviour, not a documented contract, so
  `crates/nodal-db-core/tests/entity_name_index_scan.rs` checks the
  production statements with `EXPLAIN ANALYZE` on a fixture with several
  classes and closed-out rows, next to an unfenced negative control.
  Every other plain index was dropped; a new one needs such a guard.
- **Float normalization**: the canonicalization pipeline stores every
  `-0.0` as `+0.0` and every value NaN as the one quiet NaN
  (`0x7ff8000000000000`), using the same function the canonical encoding
  uses, so stored bits and content hashes cannot disagree. NaN is a legal
  *value* but never a legal axis coordinate.
- The rank cap is 4.

## 6. Concurrency and connection discipline

**Invariant: hold connections per operation, never long-lived.**
DuckDB's file lock is symmetric: while any read-write connection is
open, other read-write *and* read-only opens fail, and an idle read-only
handle (a GUI, a notebook) blocks a writer's read-write open just as
much, starving every commit. So everything is open → work → close:

- `conn::with_write_txn` holds the read-write lock only for the span of
  one commit; `with_read` opens a fresh read-only connection per query.
- Both refuse a path that is not an existing file before opening it (a
  read-write open would otherwise create an empty database), and a write
  whose `commit` insert fails on a file without a `commit` table reports
  "not an initialized nodal-db database" instead of DuckDB's catalog error.
- Change detection is **commit polling**: a poller makes a short
  read-only query of the newest commit id between writer commits. There
  is no notification channel and no long-lived watcher connection.
- **Optimistic concurrency is a write option, not a poll.** A writer that
  read the database at commit `c` and wants to write only if nobody
  committed since passes `WriteOptions::expect_commit = Some(c)` (the
  `api` entry points; `conn::TxnOptions` for a raw transaction). The check
  runs inside the write transaction, right after `BEGIN` and before the
  `commit` row: if the head (`COALESCE(max(id), 0)`) is not exactly `c`,
  the transaction rolls back and the call fails with `DbError::Stale
  { expected, found }`, with nothing written, no commit row and no
  sequence value used. The rule is `!=`, so a file replaced or restored
  under the caller (head below `c`) is stale too; compaction preserves
  commit ids and never is. The check and the write share one transaction
  under the exclusive file lock and the per-path mutex, so no commit can
  land between them; a separate pre-commit poll could not give that.
  `Stale` is never retried. The template apply's plan→apply guard
  (`ApplyOptions::expect_commit`) uses the same check.
- Contention is *defined behavior*: bounded-backoff retry, surfacing as
  `DbError::Busy` rather than a panic or hang. The write-conflict retry
  matches on the `Conflict` substring, because DuckDB's exact message
  depends on the operation (`Conflict on update!` for an UPDATE,
  `Conflict on tuple deletion!` for a DELETE).
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

The other writer of `erasure_marker` rows is the history-free extract
(`extract::extract`), and it removes nothing: it never writes its source.
It builds a new file whose lineage is "the source's commits up to H,
then the extract commit E", and records what that file leaves out with a
`kind='horizon'` marker (`horizon_commit` = H, `continuation_db` = the
source's identity) in E. The new file becomes visible (by a no-clobber
publish) only after that marker is written. Version counters are kept
as they are, and `audit_history_tiling` is horizon-aware per key.

## 8. Compaction

DuckDB's single-file storage never shrinks the file, so
`compaction::compact` rebuilds it: create a fresh temporary database
with the schema, attach the original read-only, copy every base table in
foreign-key order (clustered with current rows first, see below), restore sequence
state, run a full-content self-check, fsync, and atomically rename over
the original. It is non-destructive (all history, commits and axes
survive at the query level) and crash-safe by construction: the original
is only read until the final rename. Compaction holds the per-path lock
for the whole operation and runs outside any write transaction, so a
failed compaction can never roll back a commit.
`erasure::erase_alternative_and_compact` chains an erase with a compaction.
The CLI verb is `nodal-db maintain compact`; Python has
`Database.compact()`.

**When to compact: `compact_if_due`.** Two costs grow between
compactions. The file accretes dead blocks (every commit's checkpoint
strands some), and every read-write commit-checkpoint cycle adds a
roughly constant cost (≈0.9 ms) to every *later* open of the file, while
barely moving its byte size. `compaction::compact_if_due(db, policy,
head)` gates on both, cheapest first:

1. no usable baseline ⇒ compact (establishes it);
2. the byte gate: the file is at least `growth_factor` (3.0) times its
   size right after the last compaction ⇒ compact;
3. the commit-count backstop: at least `N` commits since the last
   compaction ⇒ compact. `N` is 50 for files up to ≈68 MiB and grows with
   the size to 200 (`size_scaled_max_commits`: the amortized optimum
   √(2·C/g) with the repack cost C estimated from the size), so huge
   files, whose repack takes seconds, compact less often. A head below
   the recorded commit means the baseline belongs to another file
   (restored or replaced) ⇒ compact. The backstop works around a DuckDB
   catalog growth issue
   ([duckdb/duckdb#27808](https://github.com/duckdb/duckdb/issues/27808)),
   the source of the per-open cost above.

The baseline lives in a sidecar file `<db>.compaction-baseline` (two
lines, `bytes=` and `commit=`), never in the database: a `metadata` row
would need a commit of its own. Every repack and every extract writes
both values, the commit read on the new file itself. A sidecar written
by an older build (bytes only) is adopted with the current head, not
compacted. The no-op path is two `stat`s and a sidecar read when the
caller passes the head (the editor passes the commit id of the save it
just made); with `head = None` the gate opens the database read-only
once to read it (the CLI's `--if-needed`, Python's
`compact(if_needed=True)`). A failed repack updates nothing, so a
per-save caller holds a `compaction::CompactionBackoff`, which skips the
gate for `max(5, N/5)` commits after a failure instead of retrying a
1-9 s repack on every save. `compact_if_needed` is the byte gate alone,
kept for compatibility.

**Clustering order.** The copy sorts every versioned table by
`(valid_to_commit IS NULL) DESC, valid_to_commit, <keys>,
valid_from_commit` (`rebuild::cluster_order`): the current rows form one
contiguous prefix in key order (they all tie on a NULL `valid_to_commit`),
and the closed rows follow, grouped by the commit that closed them. The
first cut sorted the closed rows by key, which interleaves the versions of
every cell. On a history-heavy long table that made compaction *grow* the
file (1.81 → 2.94 GB at 228 M rows), for two reasons. DuckDB keeps a
hidden persistent index (an ART) for every `REFERENCES` constraint, not
listed in `duckdb_indexes()`, and with one commit's row ids scattered the
two commit-FK indexes of `value_long` grew about 9×. And the commit
columns and `version` lost their run-length encoding. By close commit,
the same file compacts to 1.38 GB. The trade: a closed row no longer sits
next to the other versions of its cell, so key-filtered history reads
prune less, and the closed rows gain min/max pruning on
`valid_to_commit`. The size is pinned by
`tests/compaction.rs::compact_shrinks_history_heavy_long_table` and
measured by `benches/compaction_size.rs`.

**Size guard.** Before the swap, the rebuilt file's on-disk size (after
its checkpoint and close, so its WAL is empty) is compared with the
source's, both measured as the file plus its WAL
(`compaction::keeps_original`). The growth and no-baseline triggers
(`compact_if_due`, `compact_if_needed`) keep the original when the
rebuild is not smaller. The commit-count backstop keeps it only when the
rebuild is strictly larger: the backstop exists to reset the per-commit
open cost, which barely shows in the byte size, so an equal-size rebuild
still pays off and is swapped in (with a `warning`). A kept original
deletes the temporary file and still resets the baseline to the
original's size and the head, so the commit backstop waits another `N`
commits instead of repeating the repack on every save. The report says
`swapped: false`, `kept_original: true`, with the discarded size in
`bytes_rebuilt` and a `warning`. Keeping the original is the same path
as a failed self-check: the original is never written. An explicit
`compaction::compact` (the CLI's `maintain compact` without
`--if-needed`, Python's `compact()`) swaps anyway and sets `warning`.

A full compaction of a very large history-heavy file takes minutes
(about 6 minutes at 1.4 GB, 228 M long rows), most of it the per-row
foreign-key checks of the copy. A per-save caller must not run
`compact_if_due` on its UI thread.

The history-free extract (`extract::extract`, CLI `nodal-db extract`)
uses the same rebuild machinery (the crate-private `rebuild` module) to
build a *new* file instead of replacing the original: each base table
has a selection class (commit log, trace tables, versioned rows, the
array tier, axes), the selection SQL is generated from that map and used
both for the copy and for the self-check, only the axes the carried
values reference are copied, and the sequences are restored to the
source's state. Before it writes its own commit it compares every
table's selection with the copy and the joining views (resolution,
membership and activation, effective units) between source and copy.
The new file is published with a hard link, so an existing file is
never replaced. Table macros are never called through the attached
source: DuckDB would resolve them in the copy's catalog. Compaction
never consults the extract's selection map, so a schema change can not
make a compaction on the commit path fail on it.

## 9. Testing

Three legs, run through `just` (`just` is the quick gate, `just full`
the release gate that adds the `#[ignore]`d slow tests and benchmarks):

1. **Schema smoke suite** (`.venv/bin/python schema/smoke_test.py`, 233
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
it is re-run at each DuckDB upgrade. Dense-array point reads go through
the `array_id` index: `read_parameter_dense` and `value_cells` resolve
the key triple to `array_id` on narrow columns, then fetch the list
payload by an index scan. The rendered point reads `read_value`,
`read_values` and `value_rows_for` fold the resolved ids into one list
first, so they stay on the index for 0 to 50 value hits, including a
value that does not exist and a parameter with no arrays. Reads of more
than 50 values at once still scan the payload of the whole table, as do
point reads through `value_rows`, `read_parameter` and
`value_array_asof`, whose outer filters never reach their scans. The
index-scan choice is made at run time and depends on engine heuristics,
so `crates/nodal-db-core/tests/array_id_index_scan.rs` and
`point_read_index_scan.rs` check it with `EXPLAIN ANALYZE` on every
`cargo test`, including negative controls that show the fallback for
dense ids and for 51 keys ([engine rules](engine_rules.md) §2 and §6).

CI runs `cargo test --workspace` and the schema smoke suite.

## 10. Status

For the current state of the code, see the
[commit history](https://github.com/nodal-tools/nodal-db/commits/main),
the [Releases page](https://github.com/nodal-tools/nodal-db/releases) and
the [CI runs](https://github.com/nodal-tools/nodal-db/actions/workflows/ci.yml).
The components described on this page are implemented except for the
following:

- **In development**: the editor (separate repository).
- **Planned**: CLI `migrate` (verb reserved); pruning history in place
  (an extract is the supported way to drop history today).
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
