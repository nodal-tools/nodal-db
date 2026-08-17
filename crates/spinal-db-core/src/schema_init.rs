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
/// `crates/spinal-db-core` → repo root → `schema`.
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

/// Build a fresh database at `db_path` and apply the full schema (from the
/// crate-resolved `schema_dir()`). Opens read-write, applies, closes — the
/// `init` primitive tests and the CLI share.
pub fn init_schema(db_path: &Path) -> Result<(), DbError> {
    // Same in-process per-path serialization as every other open (conn F1).
    let lock = crate::conn::path_lock(db_path);
    let _serial = lock.lock().unwrap_or_else(|p| p.into_inner());
    let conn = Connection::open(db_path)?;
    apply_schema_from(&conn, &schema_dir())?;
    conn.close().map_err(|(_, e)| DbError::Duckdb(e))?;
    Ok(())
}
