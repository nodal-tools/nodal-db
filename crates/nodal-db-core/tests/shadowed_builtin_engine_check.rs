//! The engine fold check (`types::check_engine_fold`, engine rule 8) cannot
//! be fooled or poisoned by a file that shadows `lower()`: it runs once per
//! process on the first write connection and caches its answer, so a
//! shadowed file reaching it first would either pass a hijacked `lower()`
//! or cache a refusal of every later write, on every file. The shadowing
//! guard (`conn::guard_builtins`) refuses the file on open, before the
//! check runs. Its own binary with one test: nothing else in this process
//! may write before it.

use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::write_parameter;
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::WriteOptions;

fn write(db: &std::path::Path) -> Result<(), DbError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
        ],
    )
    .unwrap();
    write_parameter(
        db,
        "unit",
        "capacity",
        "Base",
        batch,
        &[],
        WriteOptions::default(),
        "t",
        "m",
    )
    .map(|_| ())
}

#[test]
fn a_shadowed_file_never_reaches_the_engine_check() {
    let dir = tempfile::tempdir().unwrap();
    let (bad, good) = (
        dir.path().join("bad.duckdb"),
        dir.path().join("good.duckdb"),
    );
    for db in [&bad, &good] {
        init_schema_with_default(db, Some("Base")).expect("schema");
    }
    // An identity macro: the engine check would see every cased code point
    // differ and cache that refusal for the whole process, so a clean file
    // could not be written afterwards. The check must never see it.
    let conn = Connection::open(&bad).unwrap();
    conn.execute_batch("CREATE MACRO lower(x) AS x;").unwrap();
    conn.close().unwrap();

    // The first write of this process: refused by the guard, not by the
    // engine check (which would say `Internal`).
    match write(&bad) {
        Err(DbError::ShadowedBuiltin { name, .. }) => assert_eq!(name, "lower"),
        other => panic!("expected ShadowedBuiltin, got {other:?}"),
    }
    // The engine check then runs, on a clean file, and passes: nothing was
    // cached from the shadowed one.
    write(&good).expect("a clean file still writes");
}
