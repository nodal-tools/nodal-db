//! Database properties: the single-valued `database_property` table, the one
//! writer of its rows (one writer per table).
//!
//! A key's `owner` is a function of its spelling, CHECKed by the DDL
//! (`schema/001_meta.sql`), so the DDL is the one classifier and
//! [`owner_of`] mirrors it:
//!   - **core** keys ([`CORE_KEYS`]): the schema version, the DB identity,
//!     `created_by`, the template stamp, the `presentation` document,
//!     `audit_rules` and `refinement_classes`, and every key of the reserved
//!     namespace [`CORE_NAMESPACE`] (`nodal:`). Only the core writes them
//!     (`set_core`, `clear_core`, `insert_core`); the metadata channel
//!     refuses them. A new core key goes into `nodal:`, which needs no DDL
//!     change (the bare names are the closed list of 0.1.0);
//!   - **tool** keys: `<tool>:<name>`, both parts non-empty, any tool name
//!     but `nodal`;
//!   - **user** keys: everything else ([`WELL_KNOWN_USER_KEYS`] are
//!     documented, not validated).
//!
//! Every write is close-out-then-insert under the key's version counter; the
//! unique index on the folded key makes a second current row impossible.

use duckdb::{params, Connection, OptionalExt};

use crate::error::DbError;
use crate::types::fold;

/// The core-owned keys, equal to the DDL's CHECK list (pinned by a test).
pub const CORE_KEYS: &[&str] = &[
    "schema_version",
    "db_identity",
    "created_by",
    "template_name",
    "template_version",
    "template_content_hash",
    "template_migration_step",
    "template_applied_commit",
    "presentation",
    "audit_rules",
    "refinement_classes",
];

/// The namespace reserved for future core keys: any key starting with
/// `nodal:` (any case) is core-owned, so a new core key needs no DDL change,
/// and no tool may be called `nodal`.
pub const CORE_NAMESPACE: &str = "nodal:";

/// Documented user keys with a conventional meaning (not validated): a
/// persistent identifier (DOI), an IRI root that survives an extract (unlike
/// `db_identity`), the dataset's own title, description and version label,
/// its creation date, homepage and language, the JSON-LD context of its
/// IRIs and the profile it conforms to.
pub const WELL_KNOWN_USER_KEYS: &[&str] = &[
    "identifier",
    "base_iri",
    "title",
    "description",
    "version",
    "created",
    "homepage",
    "language",
    "semantic_context",
    "conforms_to",
];

/// Who owns a property key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Owner {
    Core,
    User,
    Tool,
}

impl Owner {
    /// The stored `owner` tag.
    pub fn tag(self) -> &'static str {
        match self {
            Owner::Core => "core",
            Owner::User => "user",
            Owner::Tool => "tool",
        }
    }
}

/// The owner of `key`, as the DDL's CHECKs classify it.
pub fn owner_of(key: &str) -> Owner {
    let folded = fold(key);
    if CORE_KEYS.contains(&folded.as_str()) || folded.starts_with(CORE_NAMESPACE) {
        Owner::Core
    } else if key.find(':').is_some_and(|i| i > 0) {
        Owner::Tool
    } else {
        Owner::User
    }
}

/// One key's current value.
pub fn read(conn: &Connection, key: &str) -> Result<Option<String>, DbError> {
    conn.query_row(
        "SELECT value FROM database_property WHERE cur_key = lower(?)",
        params![key],
        |r| r.get::<_, String>(0),
    )
    .optional()
    .map_err(|e| older_layout(conn, e))
}

/// Every current property as `(key, value, owner)`, ordered by folded key.
pub fn read_all(conn: &Connection) -> Result<Vec<(String, String, String)>, DbError> {
    let rows = (|| {
        let mut stmt = conn.prepare(
            "SELECT key, value, owner FROM database_property \
             WHERE valid_to_commit IS NULL ORDER BY lower(key)",
        )?;
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok::<_, duckdb::Error>(rows)
    })();
    rows.map_err(|e| older_layout(conn, e))
}

/// Refuse a file of another schema version before a read that touches its
/// tables (`SchemaMismatch` naming the file's own version, from either
/// layout): the read paths that would otherwise fail on a missing table
/// with a raw catalog error (template plan, purge preview, property reads).
pub(crate) fn require_current(conn: &Connection) -> Result<(), DbError> {
    match schema_version_any(conn)? {
        Some(found) if !crate::compaction::is_current_schema_version(&found) => {
            let expected = crate::compaction::SCHEMA_VERSION;
            Err(DbError::SchemaMismatch {
                message: format!(
                    "read refused: database schema version {found:?} is not this nodal-db's \
                     ({expected:?}); re-create the database (init + import) with this \
                     nodal-db, or use a nodal-db build matching its schema version"
                ),
                found,
                expected: expected.to_string(),
            })
        }
        _ => Ok(()),
    }
}

/// A read of `database_property` that failed because the file predates it
/// (a pre-0.6 file): [`DbError::SchemaMismatch`] naming the file's own
/// version, never the raw "table does not exist". Any other error passes.
fn older_layout(conn: &Connection, e: duckdb::Error) -> DbError {
    if !crate::read_surface::is_missing_object_error(&e) {
        return e.into();
    }
    match schema_version_any(conn) {
        Ok(Some(found)) if !crate::compaction::is_current_schema_version(&found) => {
            let expected = crate::compaction::SCHEMA_VERSION;
            DbError::SchemaMismatch {
                message: format!(
                    "read refused: database schema version {found:?} is not this nodal-db's \
                     ({expected:?}); re-create the database (init + import) with this \
                     nodal-db, or use a nodal-db build matching its schema version"
                ),
                found,
                expected: expected.to_string(),
            }
        }
        _ => e.into(),
    }
}

/// What a property write did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PropertyEffect {
    Created,
    Updated,
    Cleared,
    NoOp,
}

/// Set (or with `None`, clear) `key` at commit `c`: no-op when equal, else
/// close-out-then-insert under the same key with `version + 1`. The owner is
/// derived from the key; callers gate who may write which owner.
fn put(
    conn: &Connection,
    c: i64,
    key: &str,
    value: Option<&str>,
) -> Result<PropertyEffect, DbError> {
    let existing: Option<(String, i64)> = conn
        .query_row(
            "SELECT value, version FROM database_property WHERE cur_key = lower(?)",
            params![key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let owner = owner_of(key).tag();
    match (existing, value) {
        (None, None) => Ok(PropertyEffect::NoOp),
        (Some((v, _)), Some(new)) if v == new => Ok(PropertyEffect::NoOp),
        (Some(_), None) => {
            close(conn, c, key)?;
            Ok(PropertyEffect::Cleared)
        }
        (Some((_, version)), Some(new)) => {
            close(conn, c, key)?;
            conn.execute(
                "INSERT INTO database_property (key, value, owner, valid_from_commit, version) \
                 VALUES (?, ?, ?, ?, ?)",
                params![key, new, owner, c, version + 1],
            )?;
            Ok(PropertyEffect::Updated)
        }
        (None, Some(new)) => {
            // A key cleared earlier continues its version counter.
            let version: i64 = conn.query_row(
                "SELECT coalesce(max(version), 0) FROM database_property WHERE lower(key) = lower(?)",
                params![key],
                |r| r.get(0),
            )?;
            conn.execute(
                "INSERT INTO database_property (key, value, owner, valid_from_commit, version) \
                 VALUES (?, ?, ?, ?, ?)",
                params![key, new, owner, c, version + 1],
            )?;
            Ok(PropertyEffect::Created)
        }
    }
}

fn close(conn: &Connection, c: i64, key: &str) -> Result<usize, DbError> {
    Ok(conn.execute(
        "UPDATE database_property SET valid_to_commit = ? WHERE cur_key = lower(?)",
        params![c, key],
    )?)
}

fn require_core(key: &str) -> Result<(), DbError> {
    if owner_of(key) != Owner::Core {
        return Err(DbError::Internal(format!(
            "property::set_core on the non-core key {key:?}"
        )));
    }
    Ok(())
}

/// Set a core key (the template stamp, `presentation`, `audit_rules`).
pub(crate) fn set_core(conn: &Connection, c: i64, key: &str, value: &str) -> Result<(), DbError> {
    require_core(key)?;
    put(conn, c, key, Some(value)).map(|_| ())
}

/// Clear a core key (no-op when absent).
pub(crate) fn clear_core(conn: &Connection, c: i64, key: &str) -> Result<(), DbError> {
    require_core(key)?;
    put(conn, c, key, None).map(|_| ())
}

/// Insert a core key's first row verbatim (the extract's own identity at E,
/// after the bootstrap rows were wiped).
pub(crate) fn insert_core(
    conn: &Connection,
    c: i64,
    key: &str,
    value: &str,
) -> Result<(), DbError> {
    require_core(key)?;
    conn.execute(
        "INSERT INTO database_property (key, value, owner, valid_from_commit) \
         VALUES (?, ?, 'core', ?)",
        params![key, value, c],
    )?;
    Ok(())
}

/// Set or clear a user or tool key (the metadata channel's `SetProperty`).
/// A core key is refused; a tool key must be `<tool>:<name>` with both parts
/// non-empty (the DDL CHECK is the coarse backstop).
pub(crate) fn set_user(
    conn: &Connection,
    c: i64,
    key: &str,
    value: Option<&str>,
) -> Result<PropertyEffect, DbError> {
    validate_user_key(key)?;
    put(conn, c, key, value)
}

/// The pure checks of a user or tool key (run in prepare, before any SQL).
pub(crate) fn validate_user_key(key: &str) -> Result<(), DbError> {
    if key.is_empty() || key.trim() != key {
        return Err(DbError::Api(format!(
            "property key {key:?}: must be non-empty, without leading or trailing whitespace"
        )));
    }
    if key.starts_with(':') {
        return Err(DbError::Api(format!(
            "property key {key:?}: a leading ':' is not allowed (tool keys are '<tool>:<name>')"
        )));
    }
    if owner_of(key) == Owner::Core {
        return Err(DbError::Api(format!(
            "property key {key:?} is core-owned: only the core writes it"
        )));
    }
    if let Some((tool, name)) = key.split_once(':') {
        let ok = |s: &str| !s.is_empty() && s.trim() == s;
        if !ok(tool) || !ok(name) {
            return Err(DbError::Api(format!(
                "tool property key {key:?}: both parts of '<tool>:<name>' must be non-empty, \
                 without edge whitespace"
            )));
        }
    }
    Ok(())
}

/// The current `schema_version` rows of the file (or of the attached catalog
/// `db`), from whichever layout the file has: `database_property` (0.6 on),
/// else the pre-0.6 `metadata` table, so an older file is named by its own
/// version (`SchemaMismatch { found: "0.4-array-id" }`), never a raw catalog
/// error. Empty when neither table exists (not a nodal-db file) or the row
/// is missing. As of commit `at` when given.
///
/// No catalog listing (a `duckdb_tables()` probe costs ~10 ms on a fresh
/// connection, on every write cycle): the 0.6 table is read directly and the
/// old one only after a missing-table error. Inside an explicit transaction
/// that error aborts the transaction, so the fallback query fails too; the
/// write cycle's gate therefore reads the 0.6 table only and resolves an
/// older layout after its rollback (`conn::run_txn_once`).
pub fn schema_versions_any(
    conn: &Connection,
    db: Option<&str>,
    at: Option<i64>,
) -> Result<Vec<String>, DbError> {
    let prefix = db.map(|d| format!("{d}.")).unwrap_or_default();
    let live = match at {
        None => "valid_to_commit IS NULL".to_string(),
        Some(n) => format!(
            "valid_from_commit <= {n} AND (valid_to_commit IS NULL OR valid_to_commit > {n})"
        ),
    };
    let read = |sql: String| -> Result<Vec<String>, duckdb::Error> {
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    };
    match read(format!(
        "SELECT value FROM {prefix}database_property \
         WHERE lower(key) = 'schema_version' AND {live}"
    )) {
        Ok(v) => Ok(v),
        Err(e) if crate::read_surface::is_missing_object_error(&e) => match read(format!(
            "SELECT value FROM {prefix}metadata WHERE lower(name) = 'schema_version' AND {live}"
        )) {
            Ok(v) => Ok(v),
            Err(e) if crate::read_surface::is_missing_object_error(&e) => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        },
        Err(e) => Err(e.into()),
    }
}

/// The file's current schema version from either layout
/// ([`schema_versions_any`]); `None` when there is none.
pub fn schema_version_any(conn: &Connection) -> Result<Option<String>, DbError> {
    Ok(schema_versions_any(conn, None, None)?.into_iter().next())
}
