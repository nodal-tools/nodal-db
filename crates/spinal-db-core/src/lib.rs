//! spinal-db-core — the writer library and the single canonicalizer.
//!
//! Phase 0 scaffold: module stubs only. The canonical byte encoding and the
//! 64-bit content hash live here as an `encoding` module (folded in per
//! HANDOFF §6.6, overriding the plan's separate-crate proposal). Real
//! implementation lands in Phases 1-3.

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
