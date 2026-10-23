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
    #[error(
        "database busy: could not acquire the lock after {attempts} attempts \
             (another read-only or read-write connection holds the file)"
    )]
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

    /// A materialized axis reached the write boundary with an empty or
    /// mismatched child list (F1 guard, Phase-2b critique): its declared
    /// `length` must equal the number of coordinate/position child rows, and
    /// must be non-zero. A release-mode check so a malformed axis can never be
    /// persisted (the debug-only `encode_axis` assert is not enough).
    #[error("malformed materialized axis ({kind}): length {length} != child count {children}")]
    MalformedAxis {
        kind: &'static str,
        length: i32,
        children: usize,
    },

    /// The history write protocol exhausted its bounded conflict-retry budget:
    /// a concurrent writer kept winning the write-write race on the same key
    /// (DuckDB `TransactionException: Conflict …`). Surfaced to the user, never
    /// a stack trace (VMP Part 5 retry semantics).
    #[error("write conflict: transaction still lost the race after {attempts} retries")]
    Conflict { attempts: u32 },

    /// The public write API (`api::write_parameter` / `api::ingest_segments`)
    /// was handed a batch it cannot interpret — wrong column count, a
    /// non-string entity column, an unsupported Arrow type for a dimension or
    /// the value column, etc. A caller-facing contract error, never a stack
    /// trace. The message carries its own context (segment ordinal and
    /// class:parameter where known) — no entry-point prefix is added here, so
    /// wrapped errors are never double-prefixed (CLI critique F5).
    #[error("{0}")]
    Api(String),

    #[error(transparent)]
    Duckdb(#[from] duckdb::Error),

    #[error(transparent)]
    Encode(#[from] EncodeError),

    /// A canonicalization-pipeline error surfaced through the write path.
    #[error(transparent)]
    Canon(#[from] crate::canon::CanonError),

    /// An Arrow compute/project error while slicing the write batch per entity.
    #[error(transparent)]
    Arrow(#[from] duckdb::arrow::error::ArrowError),
}
