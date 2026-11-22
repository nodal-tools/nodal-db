//! The CLI on a real file of an older schema (`0.6-metadata`, built from the
//! frozen DDL in `nodal-db-core/tests/fixtures/schema_0_6/`): every verb that
//! reads first (erase-alternative reads the `run` count before erasing)
//! fails with the schema-mismatch message naming the file's version, never a
//! binder error from this build's views or advice to refresh them.

use std::path::{Path, PathBuf};
use std::process::Command;

use nodal_db_core::duckdb::Connection;

fn real_0_6_file(dir: &Path) -> PathBuf {
    let db = dir.join("old.duckdb");
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../nodal-db-core/tests/fixtures/schema_0_6");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&fixture)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    let conn = Connection::open(&db).unwrap();
    for f in files {
        conn.execute_batch(&std::fs::read_to_string(f).unwrap())
            .unwrap();
    }
    conn.execute_batch(
        "INSERT INTO alternative (id, name, valid_from_commit)
           VALUES (nextval('seq_alternative'), 'Base', 1);",
    )
    .unwrap();
    conn.close().unwrap();
    db
}

#[test]
fn erase_alternative_on_an_older_schema_file_names_its_version() {
    let dir = tempfile::tempdir().unwrap();
    let db = real_0_6_file(dir.path());
    let out = Command::new(env!("CARGO_BIN_EXE_nodal-db"))
        .arg("erase-alternative")
        .arg(&db)
        .arg("Base")
        .args(["--user", "t"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{stderr}");
    assert!(stderr.contains("\"0.6-metadata\""), "{stderr}");
    assert!(stderr.contains("re-create"), "{stderr}");
    assert!(!stderr.contains("Binder"), "{stderr}");
    assert!(!stderr.contains("refresh-views"), "{stderr}");
}
