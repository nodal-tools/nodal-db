//! Apply `schema/*.sql` in numbered order into a connection — the `build.py`
//! algorithm, in Rust (rust_implementation_plan.md Part 3 `schema_init`).
//!
//! Used by every DB-backed test fixture and (later) the CLI `init` verb. The
//! schema directory is resolved from `CARGO_MANIFEST_DIR` for now; a
//! `--schema-dir` override / `include_str!` embedding is a later decision
//! (plan Q4) and deliberately not blocked on here — a directory read is fine.

use std::fs;
use std::path::{Path, PathBuf};

use duckdb::Connection;

use crate::error::DbError;

/// The repo's `schema/` directory, resolved relative to this crate:
/// `crates/nodal-db-core` → repo root → `schema`.
pub fn schema_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("schema")
}

/// Numbered `.sql` files in lexical order (mirrors `build.py`'s `sql_files()`
/// glob `[0-9]*.sql`): only files whose name starts with an ASCII digit, so
/// `build.py`/`smoke_test.py` and helpers are skipped.
fn numbered_sql_files(dir: &Path) -> Result<Vec<PathBuf>, DbError> {
    let read = fs::read_dir(dir).map_err(|source| DbError::SchemaDir {
        path: dir.display().to_string(),
        source,
    })?;
    let mut files: Vec<PathBuf> = read
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension().map(|x| x == "sql").unwrap_or(false)
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false))
                    .unwrap_or(false)
        })
        .collect();
    files.sort();
    Ok(files)
}

/// Apply every numbered schema file (in order) from `dir` into `conn`. Each
/// file is one `execute_batch` (the file may hold many statements); this runs
/// under DuckDB autocommit, which is correct only for a fresh init — the value
/// write path takes an explicit transaction (`conn::with_write_txn`).
pub fn apply_schema_from(conn: &Connection, dir: &Path) -> Result<(), DbError> {
    for f in numbered_sql_files(dir)? {
        let sql = fs::read_to_string(&f).map_err(|source| DbError::SchemaDir {
            path: f.display().to_string(),
            source,
        })?;
        conn.execute_batch(&sql)?;
    }
    Ok(())
}

/// Seed one default alternative after the schema is applied (decision D3). This
/// is an OBJECT seed — the same raw-INSERT shape the old `900_bootstrap.sql`
/// used for `Base`, now parameterized and explicit — so it does NOT go through
/// (or bypass) the value canonicalization pipeline (CLAUDE.md invariant 2). It
/// attaches the row to the bootstrap commit (the sole `commit` row of a fresh
/// DB, `max(id)`), matching the pre-D3 `currval('seq_commit')` behavior.
fn seed_default_alternative(conn: &Connection, name: &str) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO alternative (id, name, valid_from_commit) \
         VALUES (nextval('seq_alternative'), ?, (SELECT max(id) FROM commit))",
        [name],
    )?;
    Ok(())
}

/// Build a fresh database at `db_path` and apply the full schema (from the
/// crate-resolved `schema_dir()`). Opens read-write, applies, closes — the
/// `init` primitive tests and the CLI share. Seeds NO alternative (decision D3;
/// back-compat shim over `init_schema_with_default(.., None)`).
pub fn init_schema(db_path: &Path) -> Result<(), DbError> {
    init_schema_with_default(db_path, None)
}

/// Build a fresh database at `db_path` applying the numbered schema files from
/// an explicit directory (the CLI `init --schema-dir` dev override, plan Q4).
/// Seeds NO alternative (decision D3; shim over `_with_default(.., None)`).
pub fn init_schema_from_dir(db_path: &Path, dir: &Path) -> Result<(), DbError> {
    init_schema_from_dir_with_default(db_path, dir, None)
}

/// Build a fresh database at `db_path` from in-memory `(file name, sql)`
/// sources, applied in lexical name order — the compile-time-embedded schema
/// path of the CLI `init` verb (plan Q4: `include_str!` embedding). Seeds NO
/// alternative (decision D3; shim over `_with_default(.., None)`).
pub fn init_schema_from_sources(db_path: &Path, sources: &[(&str, &str)]) -> Result<(), DbError> {
    init_schema_from_sources_with_default(db_path, sources, None)
}

/// As [`init_schema`], but optionally seed one default alternative (decision
/// D3): `Some(name)` ⇒ exactly one alternative row exists in the fresh DB;
/// `None` ⇒ zero (the value write path auto-creates on first use).
pub fn init_schema_with_default(
    db_path: &Path,
    default_alternative: Option<&str>,
) -> Result<(), DbError> {
    init_schema_from_dir_with_default(db_path, &schema_dir(), default_alternative)
}

/// As [`init_schema_from_dir`], with the optional D3 default-alternative seed.
pub fn init_schema_from_dir_with_default(
    db_path: &Path,
    dir: &Path,
    default_alternative: Option<&str>,
) -> Result<(), DbError> {
    // Same in-process per-path serialization as every other open (conn F1).
    let lock = crate::conn::path_lock(db_path);
    let _serial = lock.lock().unwrap_or_else(|p| p.into_inner());
    let conn = Connection::open(db_path)?;
    apply_schema_from(&conn, dir)?;
    if let Some(name) = default_alternative {
        seed_default_alternative(&conn, name)?;
    }
    conn.close().map_err(|(_, e)| DbError::Duckdb(e))?;
    Ok(())
}

/// As [`init_schema_from_sources`], with the optional D3 default-alternative seed.
pub fn init_schema_from_sources_with_default(
    db_path: &Path,
    sources: &[(&str, &str)],
    default_alternative: Option<&str>,
) -> Result<(), DbError> {
    let lock = crate::conn::path_lock(db_path);
    let _serial = lock.lock().unwrap_or_else(|p| p.into_inner());
    let conn = Connection::open(db_path)?;
    let mut sorted: Vec<&(&str, &str)> = sources.iter().collect();
    sorted.sort_by_key(|(name, _)| *name);
    for (_, sql) in sorted {
        conn.execute_batch(sql)?;
    }
    if let Some(name) = default_alternative {
        seed_default_alternative(&conn, name)?;
    }
    conn.close().map_err(|(_, e)| DbError::Duckdb(e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alt_count(db: &Path) -> i64 {
        let conn = Connection::open(db).expect("open");
        let n = conn
            .query_row("SELECT count(*) FROM alternative", [], |r| r.get::<_, i64>(0))
            .expect("count");
        conn.close().expect("close");
        n
    }

    /// D3: a fresh DB seeds NO alternative unless one is explicitly requested.
    #[test]
    fn fresh_db_seeds_no_alternative_by_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let none_db = dir.path().join("none.duckdb");
        init_schema(&none_db).expect("init none");
        assert_eq!(alt_count(&none_db), 0, "default init seeds zero alternatives");

        let base_db = dir.path().join("base.duckdb");
        init_schema_with_default(&base_db, Some("Base")).expect("init base");
        assert_eq!(alt_count(&base_db), 1, "Some(name) seeds exactly one alternative");
    }
}
