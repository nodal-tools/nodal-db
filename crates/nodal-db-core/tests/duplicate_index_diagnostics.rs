//! Duplicate index tuples and repeated domain labels: both are refused, and
//! the errors say where the problem is in the caller's terms.
//!
//! - A duplicate tuple names the entity, class, parameter and alternative,
//!   and shows the tuple as written (quoted labels, integers, ISO
//!   timestamps), never the internal axis positions.
//! - A label domain that repeats a label is refused before any value is
//!   canonicalized, whichever channel carried it (the in-process arg or the
//!   `spinal:label_domain` field metadata).

use std::collections::HashMap;
use std::sync::Arc;

use duckdb::arrow::array::TimestampMicrosecondArray;
use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{ingest_segments, Segment};
use nodal_db_core::canon::{CanonError, LabelDomain};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::WriteOptions;

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("dup.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("schema");
    (dir, db)
}

fn commits(db: &std::path::Path) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM commit", [], |r| r.get(0))
        .unwrap();
    conn.close().unwrap();
    n
}

/// Rank-1 label batch `(entity, mode, value)`; `domain` (if any) rides in
/// the `spinal:label_domain` field metadata.
fn label_batch(rows: &[(&str, &str, f64)], domain: Option<&[&str]>) -> RecordBatch {
    let mut mode = Field::new("mode", DataType::Utf8, false);
    if let Some(d) = domain {
        mode = mode.with_metadata(HashMap::from([(
            "spinal:label_domain".to_string(),
            serde_json::to_string(d).unwrap(),
        )]));
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        mode,
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

fn seg(parameter: &str, batch: RecordBatch, domains: Vec<LabelDomain>) -> Segment {
    Segment {
        class: "node".into(),
        parameter: parameter.into(),
        alternative: "Base".into(),
        batches: vec![batch],
        domains,
    }
}

fn ingest(db: &std::path::Path, segments: Vec<Segment>) -> Result<(), DbError> {
    ingest_segments(db, segments, "tester", "", WriteOptions::default()).map(|_| ())
}

#[test]
fn duplicate_label_tuple_names_the_value_and_the_labels() {
    let (_dir, db) = fresh_db();
    let batch = label_batch(
        &[
            ("n1", "a", 1.0),
            ("n2", "a", 1.0),
            ("n2", "b", 2.0),
            ("n2", "a", 3.0),
        ],
        Some(&["a", "b"]),
    );
    let err = ingest(&db, vec![seg("mode_cost", batch, vec![])]).expect_err("duplicate tuple");
    assert!(
        matches!(&err, DbError::Canon(CanonError::DuplicateTuple { .. })),
        "{err:?}"
    );
    let msg = err.to_string();
    assert_eq!(
        msg,
        "entity \"n2\" (class \"node\", parameter \"mode_cost\", alternative \"Base\"): \
         duplicate index tuple (\"a\"); a value maps one measure per index tuple"
    );
    assert_eq!(commits(&db), 1, "nothing committed");
}

#[test]
fn duplicate_tuple_in_a_multi_segment_call_names_segment_and_entity() {
    let (_dir, db) = fresh_db();
    let ok = label_batch(&[("n1", "a", 1.0)], Some(&["a"]));
    let bad = label_batch(&[("n3", "b", 1.0), ("n3", "b", 2.0)], Some(&["a", "b"]));
    let err = ingest(
        &db,
        vec![seg("first", ok, vec![]), seg("second", bad, vec![])],
    )
    .expect_err("duplicate tuple");
    let msg = err.to_string();
    assert!(
        msg.starts_with("segment 2 (node:second): entity \"n3\" (class \"node\", "),
        "{msg}"
    );
    assert!(msg.contains("duplicate index tuple (\"b\")"), "{msg}");
    assert_eq!(commits(&db), 1, "the good segment rolled back too");
}

#[test]
fn duplicate_datetime_integer_tuple_renders_iso_and_integer() {
    let (_dir, db) = fresh_db();
    let hour = 3_600_000_000_i64;
    let t0 = 1_893_456_000_000_000_i64; // 2030-01-01T00:00:00
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("step", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["n1"; 3])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![
                t0,
                t0 + hour,
                t0 + hour,
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 7, 7])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
        ],
    )
    .unwrap();
    let err = ingest(&db, vec![seg("profile", batch, vec![])]).expect_err("duplicate");
    let msg = err.to_string();
    assert!(
        msg.contains("duplicate index tuple (2030-01-01T01:00:00, 7)"),
        "{msg}"
    );
    assert!(msg.contains("entity \"n1\""), "{msg}");
}

/// A NULL value is an absent cell: it is dropped before the duplicate check,
/// so a tuple repeated only by a NULL row is not a duplicate.
#[test]
fn a_null_row_does_not_make_a_duplicate() {
    let (_dir, db) = fresh_db();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("mode", DataType::Utf8, false).with_metadata(HashMap::from([(
            "spinal:label_domain".to_string(),
            "[\"a\"]".to_string(),
        )])),
        Field::new("value", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["n1", "n1"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "a"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.0), None])) as ArrayRef,
        ],
    )
    .unwrap();
    ingest(&db, vec![seg("mode_cost", batch, vec![])]).expect("NULL row is absent");
}

#[test]
fn repeated_label_in_metadata_domain_is_refused() {
    let (_dir, db) = fresh_db();
    let batch = label_batch(&[("n1", "x", 1.0)], Some(&["y", "x", "y"]));
    let err = ingest(&db, vec![seg("mode_cost", batch, vec![])]).expect_err("repeated label");
    assert!(
        matches!(
            &err,
            DbError::Canon(CanonError::RepeatedDomainLabel { label, position: 1, .. })
                if label == "y"
        ),
        "{err:?}"
    );
    assert_eq!(
        err.to_string(),
        "the label domain of dimension 1 (\"mode\") repeats the label \"y\"; each label \
         may appear only once in a domain"
    );
    assert_eq!(commits(&db), 1, "nothing committed");
}

#[test]
fn repeated_label_in_argument_domain_is_refused() {
    let (_dir, db) = fresh_db();
    let batch = label_batch(&[("n1", "x", 1.0)], None);
    let err = ingest(
        &db,
        vec![seg(
            "mode_cost",
            batch,
            vec![LabelDomain {
                position: 1,
                labels: vec!["x".into(), "z".into(), "x".into()],
            }],
        )],
    )
    .expect_err("repeated label");
    assert!(err.to_string().contains("repeats the label \"x\""), "{err}");
    assert_eq!(commits(&db), 1, "nothing committed");
}

/// Labels are compared byte-exact: labels that differ only in case are two
/// labels, not a repeat.
#[test]
fn case_variants_in_a_domain_are_distinct_labels() {
    let (_dir, db) = fresh_db();
    let batch = label_batch(&[("n1", "x", 1.0), ("n1", "X", 2.0)], Some(&["x", "X"]));
    ingest(&db, vec![seg("mode_cost", batch, vec![])]).expect("distinct labels");
}
