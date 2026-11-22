//! The `created_by` core property (pre-release review 2, OD-4): every init
//! records the build that created the file; compaction keeps it (it
//! rebuilds, it does not create); a history-free extract is a new file and
//! records its own at its commit E.

use std::path::Path;

use duckdb::Connection;

use nodal_db_core::compaction::compact;
use nodal_db_core::extract::{extract, ExtractOpts};
use nodal_db_core::schema_init::{
    created_by, init_schema, init_schema_from_sources, EMBEDDED_SCHEMA,
};

/// `(value, valid_from_commit)` of the current `created_by` row, and the
/// number of such rows.
fn row(db: &Path) -> (String, i64, i64) {
    let conn = Connection::open(db).unwrap();
    let r = conn
        .query_row(
            "SELECT any_value(value), any_value(valid_from_commit), count(*) \
             FROM database_property WHERE key = 'created_by' AND valid_to_commit IS NULL",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    conn.close().unwrap();
    r
}

#[test]
fn init_records_the_creating_build() {
    let want = created_by();
    assert!(
        want.starts_with(concat!("nodal-db-core ", env!("CARGO_PKG_VERSION"))),
        "{want}"
    );
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.duckdb");
    init_schema(&a).unwrap();
    assert_eq!(row(&a), (want.clone(), 1, 1), "at the bootstrap commit");
    let b = dir.path().join("b.duckdb");
    init_schema_from_sources(&b, EMBEDDED_SCHEMA).unwrap();
    assert_eq!(row(&b).0, want);
}

#[test]
fn compaction_keeps_it_and_extract_writes_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("src.duckdb");
    init_schema(&db).unwrap();
    // Stand in for a file created by another build.
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        "UPDATE database_property SET value = 'nodal-db-core 0.0.1 (git feedface)' \
         WHERE key = 'created_by'",
    )
    .unwrap();
    conn.close().unwrap();
    compact(&db).unwrap();
    assert_eq!(row(&db).0, "nodal-db-core 0.0.1 (git feedface)");

    let dest = dir.path().join("x.duckdb");
    let r = extract(
        &db,
        &dest,
        &ExtractOpts {
            as_of: None,
            user: "tester",
            note: None,
        },
    )
    .unwrap();
    let (value, from, n) = row(&dest);
    assert_eq!((value, n), (created_by(), 1));
    let conn = Connection::open(&dest).unwrap();
    let e: i64 = conn
        .query_row("SELECT max(id) FROM commit_log", [], |r| r.get(0))
        .unwrap();
    conn.close().unwrap();
    assert_eq!(from, e, "written at the extract's commit E ({r:?})");
}
