//! One error type for the DB-facing layer (`conn`, `axis`, `schema_init`).
//!
//! `thiserror` per the plan (Part 3 `error`). The pure `canon`/`encoding`
//! modules keep their own errors; those fold in here via `#[from]` so a
//! write path returns a single `DbError`.

use crate::encoding::EncodeError;
use crate::types::{ObjectKind, StructuralKind};
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
    #[error("bundled DuckDB {found} < required {}.{} (schema verified on 2.0.x)",
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
    ///
    /// The same guard refuses an axis whose text is not what canon produces
    /// (`problem` says which, plan T6): a label or tag that is not in NFC,
    /// or a label axis that holds one label twice. Canon never builds
    /// either; a path that bypassed it would store labels whose bytes
    /// depend on write order.
    #[error("{}", malformed_axis_message(kind, *length, *children, problem.as_deref()))]
    MalformedAxis {
        kind: &'static str,
        length: i32,
        children: usize,
        problem: Option<String>,
    },

    /// The history write protocol exhausted its bounded conflict-retry budget:
    /// a concurrent writer kept winning the write-write race on the same key
    /// (DuckDB `TransactionException: Conflict …`). Surfaced to the user, never
    /// a stack trace (VMP Part 5 retry semantics).
    #[error("write conflict: transaction still lost the race after {attempts} retries")]
    Conflict { attempts: u32 },

    /// The caller's base-version precondition failed: the write was made
    /// with `expect_commit = Some(expected)` (`WriteOptions` /
    /// `conn::TxnOptions`, or the template's `ApplyOptions`), but the
    /// database's head commit is `found`. Checked inside the write
    /// transaction, before the commit row, so nothing was written. Its own
    /// variant so callers can match it structurally: it is neither a
    /// `Conflict` (never retried) nor an `Api` contract error.
    #[error(
        "stale: the database is at commit {found}, expected {expected} \
         — reload and re-apply"
    )]
    Stale { expected: i64, found: i64 },

    /// The database file's `schema_version` is `found`, not this build's
    /// `expected` ([`crate::compaction::SCHEMA_VERSION`]): the file was made
    /// by another nodal-db schema version. Raised by a write that fails on
    /// such a file (the engine error is kept in `message`) and by the
    /// compaction, extract and erase pre-checks. Its own variant so callers
    /// can match it structurally instead of the text; never retried. The
    /// remedy is to re-create the database with this build (init + import)
    /// or to use a build matching the file. `message` is the full text,
    /// worded per operation.
    #[error("{message}")]
    SchemaMismatch {
        found: String,
        expected: String,
        message: String,
    },

    /// A read through the core needed a view or macro that the database
    /// file's read surface lacks, because the file's surface is older than
    /// this build's (`found`: its `nodal_read_surface()` version, `None` when
    /// the file predates versioning; `expected`: this build's). Raised instead
    /// of DuckDB's raw "does not exist" Catalog error by `conn::with_read`;
    /// the core's own named reads and `conn::with_read_retrying` read through
    /// temporary copies of this build's objects instead. The remedy is
    /// `nodal-db maintain refresh-views FILE` (`read_surface::refresh_read_surface`)
    /// or any write with this build, which refreshes the file. Its own
    /// variant, mirroring `SchemaMismatch`; never retried.
    #[error("{message}")]
    ReadSurfaceStale {
        found: Option<String>,
        expected: String,
        message: String,
    },

    /// Compaction refused a database whose leftover `<db>.wal` (a writer
    /// crashed after committing) could not be folded into the file: DuckDB
    /// could not replay it (a foreign WAL, an older generation of this
    /// file's WAL, an unreadable header) or the read-write open and
    /// `CHECKPOINT` of the original failed (a read-only file, a WAL held
    /// open on Windows). Nothing was changed and the WAL is never deleted:
    /// a person decides whether its content matters. Not retryable as is;
    /// move the WAL aside, then compact. `cause` is DuckDB's or the core's
    /// reason.
    #[error("{} could not be replayed or removed: {cause}; move it aside to compact", wal.display())]
    UnreplayableWal {
        wal: std::path::PathBuf,
        cause: String,
    },

    /// The database file defines a macro (`kind`: "macro" or "table
    /// macro") named like a DuckDB built-in function in its `main` schema
    /// (`schema`, always "main" today), where DuckDB resolves it before the
    /// built-in on every connection: e.g. a `CREATE MACRO lower(x) AS …`
    /// run with direct SQL would silently replace the name fold (engine
    /// rule 8). Raised by every core open of such a file, before any core
    /// SQL runs, so nothing was read or written now. Not retryable as is:
    /// drop the macro with direct SQL (the message gives the statement).
    /// Anything another client wrote while the macro existed may have left
    /// duplicate names or name-index keys that do not match their rows
    /// (the macro computed them; the drop does not recompute them);
    /// compaction rebuilds every index and fails loudly on a duplicate, so
    /// compact the file after the drop. Its own variant so callers can
    /// match it structurally.
    #[error(
        "the database file defines a {kind} `{name}` in its `{schema}` schema, which \
         shadows DuckDB's built-in function of that name for every query on the file \
         (nodal-db's name folding included); nodal-db refuses the file until it is \
         dropped ({}) or moved to another schema. Anything written to the file while \
         it existed may have left duplicate names or name-index keys that do not \
         match their rows: after the drop, compact the file (nodal-db maintain \
         compact), which fails loudly on such a duplicate",
        drop_macro_sql(kind, schema, name)
    )]
    ShadowedBuiltin {
        name: String,
        schema: String,
        kind: String,
    },

    /// The public write API (`api::write_parameter` / `api::ingest_segments`)
    /// was handed a batch it cannot interpret — wrong column count, a
    /// non-string entity column, an unsupported Arrow type for a dimension or
    /// the value column, etc. A caller-facing contract error, never a stack
    /// trace. The message carries its own context (segment ordinal and
    /// class:parameter where known) — no entry-point prefix is added here, so
    /// wrapped errors are never double-prefixed (CLI critique F5).
    #[error("{0}")]
    Api(String),

    /// D10 (skip_unchanged_writes_PLAN, the P2 review): a value write in a
    /// call also carries an object delete whose cascade covers it — the
    /// cascade closes only rows older than the commit, so under the long
    /// cell diff it would close the unchanged old cells and keep the
    /// changed ones the write just inserted, a partial value under a
    /// closed object. Raised by both the up-front pure-name check
    /// (`api::reject_write_and_object_delete`, `resolved: None`) and its
    /// in-transaction repeat on resolved ids
    /// (`object_delete::written_value_covered`, `resolved` names the
    /// covered write's `(entity_id, parameter_definition_id,
    /// alternative_id)`). Its own variant (editor request R3,
    /// `nodal-editor/specs/core_requests_2026-10-27.md`) so a caller can
    /// match the refusal structurally and name the covering object delete,
    /// instead of matching `message`, which stays exactly what either
    /// check has always produced. Boxed (clippy::result_large_err): the
    /// details would otherwise bloat every `Result<_, DbError>` in the
    /// crate, not just this refusal's own callers.
    #[error("{}", .0.message)]
    CoveredWrite(Box<CoveredWriteDetails>),

    /// D9 (critique C1): a call writes a value and also deletes it by the
    /// same key. Deletes run after the writes, so the delete would close
    /// the row the write just inserted when the write changed the value,
    /// but succeed (as a no-op) when it did not — the outcome would depend
    /// on the content, so the call is refused up front whatever it is.
    /// Raised by both the up-front pure-name check
    /// (`api::reject_write_and_delete`, `resolved: None`) and the
    /// resolved-id repeat inside `apply_deletes` (`resolved` names the
    /// same key's `(entity_id, parameter_definition_id, alternative_id)`).
    /// A parallel typed variant to [`DbError::CoveredWrite`] (editor
    /// request R3): the ask was specifically about D10, but the two
    /// refusals are structurally twins, so this one gets the same
    /// treatment rather than staying a bare `Api` string. `message` is
    /// unchanged by this — it never carried ids, and still does not.
    /// Boxed for the same reason as `CoveredWrite`.
    #[error("{}", .0.message)]
    WriteAndDeleteSameKey(Box<WriteAndDeleteSameKeyDetails>),

    /// A `commit_changes` call renamed classes or parameters of a
    /// template-stamped database (metadata `template_name` present)
    /// without `allow_on_template` on the item (class_param_rename_PLAN
    /// OD7). The core cannot tell which classes and parameters the template
    /// declares, so every rename that changes a class or parameter's folded
    /// name (case-only renames and description-only revisions are exempt),
    /// of an object that existed before the call, is refused by default: a
    /// renamed declared object blocks the next apply of a changed template
    /// until it is renamed back or the template ships a rename hint.
    /// `renames` lists every such item of the call, so a client can ask its
    /// user once and resend the allowed items with `allow_on_template:
    /// true`. Checked after the call's structural changes ran, before any
    /// value write; the transaction rolls back, so nothing was written.
    /// Boxed like [`DbError::CoveredWrite`].
    #[error("{}", .0.message)]
    TemplateStampedRename(Box<TemplateStampedRenameDetails>),

    /// A writer-internal invariant failed before any data reached DuckDB —
    /// e.g. the columnar-insert length check (a bound list shorter or longer
    /// than its chunk would be NULL-padded silently by the zip-unnest). Never
    /// a caller error: it signals a core bug, and the transaction is rolled
    /// back like any other failure.
    #[error("internal writer invariant violated: {0}")]
    Internal(String),

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

/// [`DbError::CoveredWrite`] (D10)'s payload, boxed out of the enum so a
/// large struct variant does not grow every `Result<_, DbError>` in the
/// crate (clippy::result_large_err).
#[derive(Debug)]
pub struct CoveredWriteDetails {
    /// The covering object-delete key's 0-based index in the call.
    pub key_index: usize,
    /// The covering object delete's kind (entity, parameter, alternative,
    /// class, scenario).
    pub kind: ObjectKind,
    /// The covering object delete's target, as written: a name, or
    /// `(a, b, …)` for an element tuple.
    pub target: String,
    /// The covering object delete's class (`None` for `Alternative`/`Class`,
    /// which are addressed by a globally unique name).
    pub class: Option<String>,
    /// The covered write's resolved ids, when the in-transaction check
    /// found them; `None` from the up-front name check, which cannot see
    /// them.
    pub resolved: Option<(i64, i64, i64)>,
    pub message: String,
}

/// [`DbError::WriteAndDeleteSameKey`] (D9)'s payload, boxed for the same
/// reason as [`CoveredWriteDetails`].
#[derive(Debug)]
pub struct WriteAndDeleteSameKeyDetails {
    /// The delete key's 0-based index in the call's delete list.
    pub key_index: usize,
    pub class: String,
    pub parameter: String,
    pub alternative: String,
    pub entity: String,
    /// The resolved ids, when the resolved-id check found them; `None`
    /// from the up-front name check.
    pub resolved: Option<(i64, i64, i64)>,
    pub message: String,
}

/// [`DbError::TemplateStampedRename`]'s payload.
#[derive(Debug)]
pub struct TemplateStampedRenameDetails {
    /// The database's stamped `template_name`.
    pub template_name: String,
    /// The database's stamped `template_version`, if any.
    pub template_version: Option<String>,
    /// Every refused rename of the call, in call order.
    pub renames: Vec<TemplateStampedRenameItem>,
    pub message: String,
}

/// One rename [`DbError::TemplateStampedRename`] refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TemplateStampedRenameItem {
    /// The item's position in the call's `structural` vec.
    pub change_index: usize,
    /// `EntityClass` or `Parameter`.
    pub kind: StructuralKind,
    /// The parameter's class as written (`None` for a class rename).
    pub class: Option<String>,
    /// The item's `current`, as written (after NFC).
    pub current: String,
    pub new_name: String,
}

/// The statement that drops a [`DbError::ShadowedBuiltin`] macro, valid for
/// any name: quoted, with `"` doubled, and schema-qualified only (the user's
/// connection may name the file's catalog differently).
fn drop_macro_sql(kind: &str, schema: &str, name: &str) -> String {
    let table = if kind == "table macro" { " TABLE" } else { "" };
    let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let schema = if schema == "main" {
        "main".to_string()
    } else {
        q(schema)
    };
    format!("DROP MACRO{table} {schema}.{}", q(name))
}

fn malformed_axis_message(
    kind: &str,
    length: i32,
    children: usize,
    problem: Option<&str>,
) -> String {
    match problem {
        Some(problem) => format!("malformed axis ({kind}): {problem}"),
        None => {
            format!(
                "malformed materialized axis ({kind}): length {length} != child count {children}"
            )
        }
    }
}
