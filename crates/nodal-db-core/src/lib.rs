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

/// Minimum bundled DuckDB version the schema semantics were verified on.
/// Mirrors `schema/build.py`'s `MINIMUM_DUCKDB`. Asserted at connection open
/// (see the Phase-0 spike) and, later, by `conn`.
pub const MINIMUM_DUCKDB: (u32, u32) = (1, 4);

/// Plain FFI-friendly data types for the pipeline (Phase 2).
pub mod types;

/// Canonical byte encoding + 64-bit content hash (Phase 1) + value encoding
/// (Phase 2, spec §9 / OPEN-7).
pub mod encoding;

/// The canonicalization pipeline: NULL elimination, axis derivation, sort,
/// tier (Phase 2, VMP Part 3).
pub mod canon;

/// The tier decision — pipeline step 4 (Phase 2, VMP Part 3).
pub mod tier;

/// Array-tier reconstruction: dense flat `DOUBLE[]` -> logical long rows, the
/// inverse of `tier` (Phase 2, VMP Part 3; `spec/reconstruction.md`).
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
