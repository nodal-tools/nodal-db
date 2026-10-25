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

/// The schema embedded at compile time (plan Q4 decision: `include_str!`
/// embedding, so a deployed artifact — the CLI binary, the Python wheel —
/// needs no schema directory). File names are kept for lexical ordering, the
/// same order `schema/build.py` and [`apply_schema_from`] apply.
///
/// ONE list, here in the core, consumed by every embedder (the CLI `init`
/// verb, the Python skin's `nodal_db.init`) — the founding drift lesson
/// applied to init: two hand-maintained copies of this list WILL diverge on
/// the next schema file. Renaming a schema file is a compile error here,
/// loudly; ADDING one is caught by `embedded_schema_matches_directory_census`
/// below (a bare `include_str!` list cannot self-detect an omission).
pub static EMBEDDED_SCHEMA: &[(&str, &str)] = &[
    ("001_meta.sql", include_str!("../../../schema/001_meta.sql")),
    (
        "010_entities.sql",
        include_str!("../../../schema/010_entities.sql"),
    ),
    (
        "020_alternatives.sql",
        include_str!("../../../schema/020_alternatives.sql"),
    ),
    (
        "030_parameters.sql",
        include_str!("../../../schema/030_parameters.sql"),
    ),
    ("040_axes.sql", include_str!("../../../schema/040_axes.sql")),
    (
        "050_values.sql",
        include_str!("../../../schema/050_values.sql"),
    ),
    (
        "055_provenance.sql",
        include_str!("../../../schema/055_provenance.sql"),
    ),
    (
        "060_display.sql",
        include_str!("../../../schema/060_display.sql"),
    ),
    (
        "065_semantic.sql",
        include_str!("../../../schema/065_semantic.sql"),
    ),
    (
        "070_views_current.sql",
        include_str!("../../../schema/070_views_current.sql"),
    ),
    (
        "080_views_read.sql",
        include_str!("../../../schema/080_views_read.sql"),
    ),
    (
        "090_views_resolution.sql",
        include_str!("../../../schema/090_views_resolution.sql"),
    ),
    (
        "100_views_completeness.sql",
        include_str!("../../../schema/100_views_completeness.sql"),
    ),
    (
        "110_views_history.sql",
        include_str!("../../../schema/110_views_history.sql"),
    ),
    (
        "120_views_audit.sql",
        include_str!("../../../schema/120_views_audit.sql"),
    ),
    (
        "900_bootstrap.sql",
        include_str!("../../../schema/900_bootstrap.sql"),
    ),
];

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
                    .map(|n| {
                        n.chars()
                            .next()
                            .map(|c| c.is_ascii_digit())
                            .unwrap_or(false)
                    })
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
    let conn = Connection::open_with_flags(
        db_path,
        crate::conn::core_config(duckdb::AccessMode::ReadWrite)?,
    )?;
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
    let conn = Connection::open_with_flags(
        db_path,
        crate::conn::core_config(duckdb::AccessMode::ReadWrite)?,
    )?;
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
            .query_row("SELECT count(*) FROM alternative", [], |r| {
                r.get::<_, i64>(0)
            })
            .expect("count");
        conn.close().expect("close");
        n
    }

    /// The embedded schema census: [`EMBEDDED_SCHEMA`]'s file set must equal
    /// the `schema/` directory's numbered `*.sql` files exactly (same names,
    /// same lexical order, identical content). A schema file added to the
    /// directory but not to the list would otherwise ship silently absent
    /// from every embedded-init artifact (CLI binary, Python wheel).
    #[test]
    fn embedded_schema_matches_directory_census() {
        let files = numbered_sql_files(&schema_dir()).expect("list schema files");
        let dir_names: Vec<String> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        let embedded_names: Vec<String> =
            EMBEDDED_SCHEMA.iter().map(|(n, _)| n.to_string()).collect();
        assert_eq!(
            embedded_names, dir_names,
            "EMBEDDED_SCHEMA is out of sync with schema/*.sql — update the \
             include_str! list in schema_init.rs"
        );
        for (path, (name, embedded_sql)) in files.iter().zip(EMBEDDED_SCHEMA) {
            let on_disk = fs::read_to_string(path).expect("read schema file");
            assert_eq!(
                &on_disk, embedded_sql,
                "embedded {name} differs from the on-disk file (stale build?)"
            );
        }
    }

    /// D3: a fresh DB seeds NO alternative unless one is explicitly requested.
    #[test]
    fn fresh_db_seeds_no_alternative_by_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let none_db = dir.path().join("none.duckdb");
        init_schema(&none_db).expect("init none");
        assert_eq!(
            alt_count(&none_db),
            0,
            "default init seeds zero alternatives"
        );

        let base_db = dir.path().join("base.duckdb");
        init_schema_with_default(&base_db, Some("Base")).expect("init base");
        assert_eq!(
            alt_count(&base_db),
            1,
            "Some(name) seeds exactly one alternative"
        );
    }

    /// RFC-4122 textual shape: 8-4-4-4-12 hex groups.
    fn assert_uuid_shape(id: &str) {
        assert_eq!(id.len(), 36, "not a UUID: {id}");
        for (i, c) in id.chars().enumerate() {
            if matches!(i, 8 | 13 | 18 | 23) {
                assert_eq!(c, '-', "not a UUID: {id}");
            } else {
                assert!(c.is_ascii_hexdigit(), "not a UUID: {id}");
            }
        }
    }

    /// DB-identity (schema_interop.md §9): every init path mints ONE fresh,
    /// well-formed UUID per database — distinct across databases, stable
    /// across reopens (each `api::db_identity` call is a fresh connection).
    #[test]
    fn db_identity_minted_unique_and_stable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let a = dir.path().join("a.duckdb");
        let b = dir.path().join("b.duckdb");
        init_schema(&a).expect("init a");
        init_schema(&b).expect("init b");

        let id_a = crate::api::db_identity(&a).expect("identity a");
        let id_b = crate::api::db_identity(&b).expect("identity b");
        assert_uuid_shape(&id_a);
        assert_uuid_shape(&id_b);
        assert_ne!(id_a, id_b, "two inits must mint two different identities");

        // Stable across reopen: the getter re-opens per call.
        assert_eq!(crate::api::db_identity(&a).expect("re-read a"), id_a);

        // The embedded-sources path (the deployed CLI `init`) mints too — it
        // applies the same 900_bootstrap.sql, just from memory.
        let files = numbered_sql_files(&schema_dir()).expect("list schema files");
        let named: Vec<(String, String)> = files
            .iter()
            .map(|p| {
                (
                    p.file_name().unwrap().to_str().unwrap().to_string(),
                    fs::read_to_string(p).expect("read schema file"),
                )
            })
            .collect();
        let sources: Vec<(&str, &str)> = named
            .iter()
            .map(|(n, s)| (n.as_str(), s.as_str()))
            .collect();
        let c = dir.path().join("c.duckdb");
        init_schema_from_sources(&c, &sources).expect("init from sources");
        let id_c = crate::api::db_identity(&c).expect("identity c");
        assert_uuid_shape(&id_c);
        assert_ne!(id_c, id_a, "sources-path init mints its own identity");
    }
}
