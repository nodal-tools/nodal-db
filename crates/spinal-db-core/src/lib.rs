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

/// Canonical byte encoding + 64-bit content hash (Phase 1).
pub mod encoding {}
