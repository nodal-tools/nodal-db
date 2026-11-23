//! A released CLI binary or Python wheel has no source tree: every run-time
//! path must apply the schema embedded at compile time
//! (`schema_init::EMBEDDED_SCHEMA`), never `schema/*.sql` from the
//! `CARGO_MANIFEST_DIR` the build saw. This binary runs init, a write,
//! compaction (also through `compact_if_due`), extract and the read-surface
//! refresh, and checks that none of them resolved or read the schema
//! directory (`schema_init::disk_schema_reads`).
//!
//! One test in its own binary on purpose: the counter is process-wide, and
//! another test's directory helper running in parallel would race it.

use std::path::Path;
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;

use nodal_db_core::api::write_parameter;
use nodal_db_core::compaction::{compact, compact_if_due, CompactionPolicy};
use nodal_db_core::extract::{extract, ExtractOpts};
use nodal_db_core::read_surface::refresh_read_surface;
use nodal_db_core::schema_init::{disk_schema_reads, init_schema, init_schema_with_default};
use nodal_db_core::types::WriteOptions;

fn write_scalar(db: &Path, entity: &str, v: f64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity])) as ArrayRef,
            Arc::new(Float64Array::from(vec![v])) as ArrayRef,
        ],
    )
    .unwrap();
    write_parameter(
        db,
        "node",
        "demand",
        "Base",
        batch,
        &[],
        WriteOptions::default(),
        "tester",
        "embedded-schema write",
    )
    .expect("write_parameter");
}

#[test]
fn run_time_paths_never_read_the_schema_directory() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.duckdb");
    let before = disk_schema_reads();

    init_schema(&dir.path().join("plain.duckdb")).expect("init");
    init_schema_with_default(&db, Some("Base")).expect("init with default");
    write_scalar(&db, "n1", 1.0);
    write_scalar(&db, "n1", 2.0);
    compact(&db).expect("compact");
    compact_if_due(&db, CompactionPolicy::default(), None).expect("compact_if_due");
    write_scalar(&db, "n2", 3.0);
    let dest = dir.path().join("extract.duckdb");
    extract(
        &db,
        &dest,
        &ExtractOpts {
            as_of: None,
            user: "tester",
            note: None,
        },
    )
    .expect("extract");
    refresh_read_surface(&dest, true).expect("refresh");

    assert_eq!(
        disk_schema_reads(),
        before,
        "a run-time path read schema/*.sql from the source tree; a released \
         binary or wheel has none (use schema_init::EMBEDDED_SCHEMA)"
    );
}
