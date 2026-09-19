# Architecture

> **Role of this file:** the authoritative *map* of system structure and
> invariants, with the *why* for each major decision and a pointer to the
> spec that owns the detail. It deliberately does not duplicate spec
> content — when this file and a frozen spec disagree, the spec wins and
> this file has rotted; fix it.
> **Last substantial update: 2026-09-17.**

Authoritative sources, in reading order: `specs/HANDOFF.md` (project
state + work queue), `specs/schema_draft_plan.md` Part 3 (the decision
log), `specs/value_model_plan.md` (the frozen value model, "VMP" below),
`specs/rust_implementation_plan.md` + `specs/rust_impl_session_decisions.md`
(implementation architecture and its running decision record). Note the
split: **`specs/`** is local working memory (gitignored), while
**`spec/`** holds the committed *normative contracts* — 
`spec/canonical_encoding.md`, `spec/reconstruction.md`, and
`spec/arrow_ipc_bulk_format.md` (signed off as v1.0).

## 1. System shape

One DuckDB file is the database. Around it:

- **`nodal-db-core`** (Rust library) — the writer library and the **single
  canonicalizer**. All writes in the whole ecosystem funnel through it.
  *Implemented.*
- **`nodal-db` CLI** (`crates/nodal-db-cli`) — `init` (embedded schema,
  `--template` for init-with-template), `import` (the Arrow-IPC bulk
  format), and `template load` / `plan` / `apply` / `export` (the
  template-IR loader, `specs/template_ir_plan.md`), the declared write
  boundary for every non-Rust client (Julia, Python pipelines).
  `migrate` / `maintain` remain frozen not-implemented stubs reserving
  the verb surface (the v1 reconcile runner is `template apply`; the
  full-chain `migrate` verb is a later door, plan §4.4). *Implemented.*
- **egui editor** — lives in the separate `../nodal-editor` repo
  (two-repo layout, `specs/language_and_architecture_research.md`
  §3.2/§3.3: the editor must not be able to reach core internals). It
  depends on released `nodal-db-core`, reads via the SQL views, writes
  via the core API. *In progress, tracked in that repo's
  `specs/HANDOFF.md`.*
- **PyO3/maturin Python skin** — thin wrapper over the core for
  FlexTool/engine interop. *Planned, not started.* Python *readers* need
  no bindings at all: they query the views directly with the `duckdb`
  package (reads are invisible to the core).

The API rule is **Arrow-in/Arrow-out** — no value-object class hierarchy,
ever (a one-way door; `specs/language_and_architecture_research.md` §1.5).
Rust + egui was decided 2026-08-16/17; the decision and its rationale are
in `specs/schema_draft_plan.md` Part 3.

## 2. Storage layer: the DuckDB schema

`schema/` holds the executable DDL as numbered SQL files applied in
lexical order (by `schema/build.py`, by `core::schema_init`, and by the
CLI's compile-time-embedded copy — three appliers, one file set):

- `001` meta (commit log, sequences, dataset metadata) · `010` entities/
  classes/dimensions · `020` alternatives/scenarios · `030` parameter
  definitions · `040` axes (incl. parametric float/datetime axes, CRS
  seam) · `050` value tables (three tiers + `_hist` twin) · `055`
  provenance (`run`) · `060` display modes · `065` semantic annotations
- `070`–`120` the read surface: `_current` views, read macros
  (`read_parameter`, `value_rows`, …), scenario/activation resolution,
  completeness, `_asof` history views, and the audit views
  (`audit_health` et al. — validation is *reporting*, not constraints,
  per the validation philosophy in `schema_draft_plan.md` Part 2)
- `900` bootstrap seed.

Design highlights and where they are owned:
- **Append-only history everywhere** via `valid_from_commit` /
  `valid_to_commit`; the `commit` row is the validity edge (VMP Part 5;
  benchmarked in `specs/versioning_from_to_date_research.md`).
- **Scenario resolution is view-side** (rank-stacked alternatives), and
  activation propagation (membership + `realizes` refinement edges) is a
  recursive view fixpoint — structural *resolution* in SQL, value
  *transforms* stay in tool code (`specs/multifidelity_entity_linking_FINDINGS.md`).
- **Units/CRS are express-never-assert**: carried, hashed, never
  converted (`schema_draft_plan.md` Part 2).
- The DDL review map (build order, rationale, deviations, measured read
  performance) is `specs/ddl_sketch.md`.

Verified by `schema/smoke_test.py`: **157/157 checks**, including
hostile-data regressions from the DDL critique. Run with the
project-local `.venv/bin/python` (DuckDB 1.5.5 — must match the bundled
engine that writes the files).

## 3. Workspace map (as it exists today)

Cargo workspace, two crates. `crates/nodal-db-core/src/`:

| Module | Responsibility (one line) |
|---|---|
| `types` | Plain, FFI-friendly owned data types for the pipeline (future PyO3 skin wraps them 1:1). |
| `encoding` | Canonical byte encoding of axes and values + XXH3-64 content hash; pure, no DB (`spec/canonical_encoding.md`). |
| `canon` | The canonicalization pipeline: NULL elimination → axis derivation → row-major sort → tier; pure over Arrow arrays. |
| `tier` | Step 4: the deterministic scalar/array/long decision + payload shaping. |
| `reconstruct` | Dense flat payload → logical long rows; the inverse of `tier`, bit-compatible with the SQL views (`spec/reconstruction.md`). |
| `error` | The one `DbError` for the DB-facing layer. |
| `schema_init` | Apply `schema/*.sql` in numbered order (build.py in Rust). |
| `conn` | Connection discipline: open-work-close, per-path in-process lock, bounded-backoff retry, write transactions stamping the commit row. |
| `axis` | Content-verified axis dedup persistence (hash narrows, byte-equality decides). |
| `history` | The history write protocol: from/to close-out, array `_hist` pre-image, version counter, tier transitions; batched long-tier inserts. |
| `api` | The public surface: `write_parameter`, `ingest_segments`, `commit_changes`, `delete_values`, `delete_objects`, `preview_object_deletes`, `read_parameter`. |
| `object_delete` | Structural close-out of objects + dependents, driven by one leg registry (cascade, RESTRICT report, and preview can never drift). |
| `compaction` | **WIP, uncommitted.** Physical repack: rebuild into a fresh file, full-content self-check, atomic rename (`specs/compaction_DESIGN.md`). |

`crates/nodal-db-cli/src/`: `main` (verb dispatch, embedded schema for
`init`), `import` (Arrow-IPC bulk-format reader — a *thin structural
adapter* that decodes, upcasts timestamps, expands dense segments, and
makes ONE `ingest_segments` call; it never re-canonicalizes).

The crate re-exports its `duckdb` (and thus `duckdb::arrow`) so
downstream code compiles against exactly one Arrow version (risk R2,
`rust_implementation_plan.md` Part 2).

## 4. The write path and the one canonicalizer

**Invariant (CLAUDE.md #2): never add a writer path that bypasses the
canonicalization pipeline.** The tier and the canonical encodings are
deterministic functions of the data; the project's founding lesson (from
the toolbox codec work, `specs/initial_ideas.md` §1) is that two
independent canonicalizers *drift* — and drift breaks history diffs,
axis dedup, and cross-writer byte equality. Hence exactly two write
boundaries: the `nodal-db-core` API in-process, and `nodal-db import`
(Arrow-IPC) for everything else. Both end in the same code.

The pipeline (normative: VMP Part 3, "The canonicalization pipeline"):

1. **NULL elimination** — NULL ≡ absent; there is no "present but null"
   cell, so explicit-NULL and omitted-tuple writers converge.
2. **Axis derivation** — axes come *from the data*: distinct present
   coords per dimension, sorted by kind; regular progressions MUST
   canonicalize to parametric form (parametric-first); label ordinals
   come from the caller's ordered domain (in-process arg and/or Arrow
   field metadata `spinal:label_domain`).
3. **Row-major sort** in declared-dimension order.
4. **Tier decision** — rank 0 → scalar; rank ≥ 1, float-valued, full
   Cartesian grid → dense array; otherwise → long. The *library* decides;
   callers never pick a tier (VMP Part 6).

Then: content-verified axis dedup (`axis`) and the history write
protocol (`history`), all inside ONE atomic, conflict-retried commit
whose `commit` row is the validity edge. Name resolution **auto-creates
with a loud, machine-readable warning** (decision B — end-user tools
choose the policy, the library never silently guesses *or* hard-blocks).

Canonical encodings: axes and values serialize to a pinned byte layout
hashed with **XXH3-64, seed 0** (decision C). The hash is an accelerator
only — dedup narrows by hash, then compares bytes, because collisions
must not mis-attach values (`axis` module doc). The value content-hash
exists but is deliberately dormant (no stored column yet). Full detail:
`spec/canonical_encoding.md`; conformance vectors under `conformance/`.

## 5. Value model at architecture level

(Own spec: VMP Parts 1–5. This is the summary; do not extend it here.)

- **Three physical tiers**, one logical shape: sparse/mixed data as long
  rows, dense float grids as flat row-major `DOUBLE[]` (+ `BIGINT[]`
  sibling) against shared axes, rank-0 as scalars. Readers never see the
  tier — the canonical read shape is one long format via the views.
- **Axes are content-addressed and immutable** (the CF coordinate-variable
  idea): shared by FK, deduped by content, parametric where regular so
  reconstruction is bit-exact. The parametric float-axis reconstruction
  rule (no FMA, fixed operand order) is pinned by a genuinely
  FMA-diverging conformance fixture — two implementations (SQL view,
  Rust) evaluate it, so it is exactly the two-canonicalizers hazard in
  miniature (`specs/float_axis_crs_int_plan.md` §9.2).
- **History is a hybrid**: from/to-commit versioning in place for scalar
  and long tiers; the array tier keeps only live rows and copies
  pre-images to a `_hist` twin entirely in SQL (engine-guaranteed
  atomicity). Version counters are partitioned per tier per key and
  audited (`audit_history_tiling`).
- **NaN** is a legal *value* (canonicalized to one bit pattern), never a
  legal axis coordinate.
- Rank cap is 4 (a documented consequence, not a config).

## 6. Concurrency and connection discipline

**Invariant (CLAUDE.md #1): readers hold connections per-operation,
never long-lived.** DuckDB's file lock is symmetric: while any RW
connection is open, other RW *and RO* opens fail — and an idle RO handle
(a GUI, a notebook) equally blocks a writer's RW acquisition, starving
every commit. So everything is open → work → close (normative: VMP
Part 6, "Connection discipline"):

- `conn::with_write_txn` holds the RW lock only for the span of one
  commit; `with_read` is a fresh RO connection per query.
- Change detection is **commit-cookie polling**: pollers do a
  per-operation RO read of the commit log between writer commits — no
  notification channel, no long-lived watcher handle.
- Contention is *defined behavior*: bounded-backoff retry surfacing as a
  first-class `DbError::Busy`, never a stack trace. The write-conflict
  retry matcher keys on the `Conflict` substring (the exact wording
  changed between DuckDB 1.4.4 and 1.5.5).
- **In-process serialization**: DuckDB's file lock excludes other
  *processes* only; two threads opening the same file get independent
  database instances whose commits silently merge or vanish (api
  critique F1 — a real corruption class, found and fixed). Every
  open-use-close therefore first takes a process-global per-path mutex.

The single-writer model is v1; a serving process is the recorded v1.x
escape hatch if the discipline proves insufficient (VMP Part 8
decision 1).

## 7. Deletion and the change model

Value deletes and object deletes ride the same atomic commit as writes
(`api::commit_changes`). Object deletion (alternative / parameter /
entity / class) closes out the object row and its dependents via one
**leg registry**, so the preview counts, the RESTRICT report, and the
cascade UPDATEs are generated from the same table and cannot drift.
Deletes are close-outs, not erasure — history survives.

The change-model rethink that briefly paused this work is **resolved**
(`specs/versioned_change_classification_FINDINGS.md`,
`specs/object_delete_change_model_reeval.md`): the dividing line is not
"structural vs data" but "does the delete punch a hole in a live N-D
container", and cascade recurses through those two edges with a full
blast-radius preview. Deliberately deferred, as separate future work:
in-place structural *reshape* (the migration-script case) and a
hard-erasure/true-delete door (flagged on the encoding-freeze checklist).

## 8. Compaction (work in progress, uncommitted)

DuckDB's single-file block manager never shrinks the file, so
`compaction::compact` rebuilds it: init a fresh temp, attach the
original read-only, copy every base table in FK order, restore sequence
state, run a full-content self-check, fsync, atomic rename. It is
non-destructive (all history, commits, axes survive at the query level)
and crash-safe by construction (the original is only read until the
final rename). Design + critique resolutions: `specs/compaction_DESIGN.md`.

## 9. Testing architecture

Three legs, run via `just` tiers (`just` = quick gate, `just full` =
release gate with `#[ignore]`d slow tests + benches):

1. **Schema smoke suite** (`.venv/bin/python schema/smoke_test.py`,
   157/157): builds a demo model, exercises resolution / membership /
   history / audits, then plants hostile-data corruptions and asserts
   the right constraint or audit fires.
2. **Rust suite** (`cargo test --workspace`): unit + integration tests
   in `crates/nodal-db-core/tests/` and end-to-end CLI tests driving the
   real binary.
3. **The conformance suite is the executable contract** (VMP Part 9):
   `conformance/` holds encoding vectors, value vectors, pipeline cases,
   history-protocol cases (all 6 directed tier transitions CI-pinned),
   reconstruction fixtures, and an *independent Python oracle* — the
   machine check that a second implementation produces identical bytes.
   The pre-freeze **benchmark gate** lives in
   `crates/nodal-db-core/benches/` with results recorded in
   `benches/RESULTS.md`; re-run at each DuckDB bump (the point-array
   read residual, `specs/ddl_sketch.md` §10, is still open on 1.5.5).

CI is minimal by design: `cargo test --workspace` + schema smoke.

## 10. Status ledger

- **Implemented**: core write/read path, tier pipeline, axis dedup,
  history protocol (batched long-tier writes), Arrow-IPC import + CLI
  `init`, value/object deletion, de-Base DB init, conformance +
  benchmark gates, template-IR loader v1 (`template` module: parse /
  validate / load / reconcile+hints+`run:` runner / export;
  `specs/template_ir_plan.md` phase 2).
- **In progress**: compaction (uncommitted working tree:
  `compaction.rs`, `tests/compaction.rs`, small `conn.rs` visibility
  change); editor (separate `nodal-editor` repo).
- **Planned**: PyO3 skin; CLI `migrate`/`maintain` (verbs reserved).
- **Deferred doors** (design held open, no code): serving process,
  native Julia writer (conformance-gated), value inheritance, symbols,
  structured units, in-place reshape, hard erasure
  (`schema_draft_plan.md` Part 3 open items; HANDOFF §8.4).
- **Live pre-freeze debt**: NFC frozen-table vendoring — the encoding
  pins Unicode 16.0.0 NFC, currently held only by an exact
  `unicode-normalization = "=0.1.24"` pin while the Python oracle uses
  CPython's own tables (`rust_impl_session_decisions.md`, "Deferred
  pre-freeze debt"). Must close before the encoding freezes.

## 11. Naming note (spinal → nodal, 2026-09)

The project renamed spinal-db → nodal-db (`f6e00a1`): crates, CLI, and
README use `nodal-db`. Deliberately *not* renamed: the `spinal:*` Arrow
metadata keys — they are frozen in the signed-off bulk-format v1.0
(`spec/arrow_ipc_bulk_format.md`) and changing them is a format break.
The `specs/` design record still says `spinal_db` throughout; read it
as historical, don't churn it.
