//! nodal-db-core — the writer library and the single canonicalizer.
//!
//! Phase 0 scaffold: module stubs only. The canonical byte encoding and the
//! 64-bit content hash live here as an `encoding` module (folded in per
//! HANDOFF §6.6, overriding the plan's separate-crate proposal). Real
//! implementation lands in Phases 1-3.

/// Re-export of the one `duckdb` (and thus the one `duckdb::arrow`) this
/// library is built against, so downstream crates (the CLI, tests) can name
/// the exact same types the public API uses — the compile-time anchor of the
/// R2 one-arrow-version rule.
pub use duckdb;

/// Minimum bundled DuckDB version the schema semantics are verified on.
/// Mirrors `schema/build.py`'s `MINIMUM_DUCKDB`. Asserted at connection open
/// (see the Phase-0 spike) and, later, by `conn`.
///
/// 2.0 because the project writes DuckDB's native 2.0 storage format
/// (`storage_version` tag `v2.0.0+`; readers need DuckDB >= 2.0) and the
/// schema is verified only on 2.0. Native files are what make an array
/// replace commit cheap: the 1.5 format logs the whole list column to the
/// WAL on every write. The tests in `tests/storage_format.rs` guard that.
pub const MINIMUM_DUCKDB: (u32, u32) = (2, 0);

/// Plain FFI-friendly data types for the pipeline (Phase 2).
pub mod types;

/// Unicode text normalization: the core's one NFC function (plan T5).
pub mod text;

/// Canonical byte encoding + 64-bit content hash (Phase 1) + value encoding
/// (Phase 2, spec §9 / OPEN-7).
pub mod encoding;

/// The canonicalization pipeline: NULL elimination, axis derivation, sort,
/// tier (Phase 2, VMP Part 3).
pub mod canon;

/// The tier decision — pipeline step 4 (Phase 2, VMP Part 3).
pub mod tier;

/// Array-tier reconstruction: dense flat `DOUBLE[]` -> logical long rows, the
/// inverse of `tier` (Phase 2, VMP Part 3; `specs/reconstruction.md`).
pub mod reconstruct;

/// One error type for the DB-facing layer (Phase 2b).
pub mod error;

/// Apply `schema/*.sql` in numbered order (Phase 2b, `build.py` in Rust).
pub mod schema_init;

/// Connection discipline: open-write-close / open-read-close (Phase 2b, VMP Part 6).
pub mod conn;

/// Content-verified axis dedup persistence (Phase 2b, VMP Part 4).
pub mod axis;

/// The history write protocol: from/to close-out, array `_hist` pre-image,
/// version counter, retention, tier transitions (Phase 3a, VMP Part 5).
pub mod history;

/// The public Arrow-in write path: `write_parameter` / `read_parameter`
/// (Phase 3b, VMP Parts 3-6). Ties name-resolution (auto-create, decision B),
/// declared-type lookup, canon, tier, axis dedup, and history into one atomic,
/// retried commit.
pub mod api;

/// Object deletion: structural close-out of an object row (+ dependents) inside
/// the same atomic commit as value writes/deletes (object_delete plan §2-§4).
/// A single leg registry drives the cascade, the RESTRICT report, and the
/// read-only preview.
pub mod object_delete;

/// Physical repack (Sense A): rebuild the DB file into a fresh, densely-packed
/// one that reclaims dead blocks while preserving ALL logical content, then
/// atomically swap it in (`specs/compaction_DESIGN.md`). The rebuild copy is
/// clustered current-first (erasure_compaction_plan §4.1, always-on).
pub mod compaction;

/// The shared whole-file rebuild machinery (catalog maps, real-column
/// discovery, RO source ATTACH, sequence restore, durability helpers) used by
/// [`compaction`] and the history-free extract.
pub(crate) mod rebuild;

/// History-free extract: a new database file holding exactly the content
/// valid at one commit of a source, with a `kind='horizon'` erasure marker as
/// the trace of the omitted history; the source is only read and becomes the
/// archive.
pub mod extract;

/// Hard erasure: physical removal with a trace — erase-by-alternative plus the
/// `erasure_marker` write, one atomic transaction; the sanctioned exception to
/// append-only history (`specs/erasure_compaction_plan.md`).
pub mod erasure;

/// The G8 migration verbs: the core write surface `run:` migration programs
/// and the Spine importer use for vocabulary, scenario-rank, activation, and
/// template-identity writes (`specs/flextool_port_finale_plan.md` §1, D-F4).
pub mod verbs;

/// The sanctioned structural (non-value) write path — the fourth
/// `commit_changes` channel: create/rename alternatives + scenarios, declarative
/// scenario ranking (A-min), per-edge activation + membership, and the D-S9
/// entity/class/parameter creates (`specs/structural_write_api_plan.md`). Shared
/// appliers so no table gets a second writer (invariant 2).
pub mod structural;

/// The template IR loader: parse/validate the YAML-1.2/JSON vocabulary IR,
/// load into a fresh DB, reconcile against a populated one (three-layer
/// migration model, v1 mechanics), export back out
/// (`specs/template_ir_plan.md`; HANDOFF §8.4 phase 2).
pub mod template;
