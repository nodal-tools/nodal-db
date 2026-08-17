//! One error type for the DB-facing layer (`conn`, `axis`, `schema_init`).
//!
//! `thiserror` per the plan (Part 3 `error`). The pure `canon`/`encoding`
//! modules keep their own errors; those fold in here via `#[from]` so a
//! write path returns a single `DbError`.

use crate::encoding::EncodeError;
use crate::MINIMUM_DUCKDB;

/// Errors raised by connection handling, schema application, and axis dedup.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// The RW (or RO) lock could not be acquired within the bounded-backoff
    /// budget: another connection holds the file. CLAUDE.md invariant 1 — an
    /// idle RO handle starves every commit — makes this a first-class,
    /// user-visible state, never a stack trace (VMP Part 6).
    #[error("database busy: could not acquire the lock after {attempts} attempts \
             (another read-only or read-write connection holds the file)")]
    Busy { attempts: u32 },

    /// The bundled DuckDB is older than the line the schema semantics were
    /// verified on (mirrors `build.py`'s version gate; risk R1).
    #[error("bundled DuckDB {found} < required {}.{} (schema verified on 1.4.x)",
            MINIMUM_DUCKDB.0, MINIMUM_DUCKDB.1)]
    VersionTooOld { found: String },

    /// The schema directory could not be read (Q4: dir-read for now).
    #[error("schema directory unusable ({path}): {source}")]
    SchemaDir {
        path: String,
        source: std::io::Error,
    },

    /// An `axis` row carried a `kind` the reconstruction does not know.
    #[error("axis id {id}: unknown kind {kind:?} in DB row")]
    UnknownAxisKind { id: i64, kind: String },

    #[error(transparent)]
    Duckdb(#[from] duckdb::Error),

    #[error(transparent)]
    Encode(#[from] EncodeError),
}
