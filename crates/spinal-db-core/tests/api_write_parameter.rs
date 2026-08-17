//! End-to-end tests for the public write path (`api::write_parameter` +
//! `api::read_parameter`, Phase 3b). Each test builds a fresh schema DB via
//! `schema_init`, writes a logical-long Arrow batch, and reads it back —
//! exercising name-resolution auto-create (decision B), the DB declared-type
//! lookup, tiering, and the history protocol as one integrated path.

use std::sync::Arc;

use duckdb::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringArray,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use spinal_db_core::api::{read_parameter, write_parameter};
use spinal_db_core::schema_init::init_schema;
use spinal_db_core::types::{Tier, WarningKind, WriteOptions};

// --- batch builders --------------------------------------------------------

fn scalar_batch(entity: &str, v: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let ent = Arc::new(StringArray::from(vec![entity])) as ArrayRef;
    let val = Arc::new(Float64Array::from(vec![v])) as ArrayRef;
    RecordBatch::try_new(schema, vec![ent, val]).unwrap()
}

/// Rank-1 integer-indexed float batch (drives the array tier when the coords
/// form a full Cartesian grid).
fn indexed_float_batch(entity: &str, dim: &str, coords: &[i64], vals: &[f64]) -> RecordBatch {
    let n = coords.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new(dim, DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let ent = Arc::new(StringArray::from(vec![entity; n])) as ArrayRef;
    let d = Arc::new(Int64Array::from(coords.to_vec())) as ArrayRef;
    let val = Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef;
    RecordBatch::try_new(schema, vec![ent, d, val]).unwrap()
}

/// Rank-1 integer-indexed integer batch (int measure ⇒ always the long tier).
fn indexed_int_batch(entity: &str, dim: &str, coords: &[i64], vals: &[i64]) -> RecordBatch {
    let n = coords.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new(dim, DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let ent = Arc::new(StringArray::from(vec![entity; n])) as ArrayRef;
    let d = Arc::new(Int64Array::from(coords.to_vec())) as ArrayRef;
    let val = Arc::new(Int64Array::from(vals.to_vec())) as ArrayRef;
    RecordBatch::try_new(schema, vec![ent, d, val]).unwrap()
}

// --- RecordBatch column helpers --------------------------------------------

fn col_str(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let i = batch.schema().index_of(name).unwrap();
    let a = batch.column(i).as_any().downcast_ref::<StringArray>().unwrap();
    (0..a.len())
        .map(|r| if a.is_null(r) { None } else { Some(a.value(r).to_string()) })
        .collect()
}

fn col_f64(batch: &RecordBatch, name: &str) -> Vec<Option<f64>> {
    let i = batch.schema().index_of(name).unwrap();
    let a = batch.column(i).as_any().downcast_ref::<Float64Array>().unwrap();
    (0..a.len())
        .map(|r| if a.is_null(r) { None } else { Some(a.value(r)) })
        .collect()
}

fn col_i64(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let i = batch.schema().index_of(name).unwrap();
    let a = batch.column(i).as_any().downcast_ref::<Int64Array>().unwrap();
    (0..a.len())
        .map(|r| if a.is_null(r) { None } else { Some(a.value(r)) })
        .collect()
}

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("api.duckdb");
    init_schema(&db).expect("schema");
    (dir, db)
}

// --- tests -----------------------------------------------------------------

#[test]
fn scalar_new_class_auto_creates_and_reads_back() {
    let (_dir, db) = fresh_db();

    let report = write_parameter(
        &db,
        "node",
        "demand",
        "scen_a",
        scalar_batch("node_1", 42.5),
        &[],
        WriteOptions::default(),
        "tester",
        "scalar write",
    )
    .expect("write");

    // Every missing referent auto-created, each a machine-readable AutoCreated
    // warning carrying the object_type + name (decision B).
    let created: std::collections::HashSet<(String, String)> = report
        .warnings
        .iter()
        .inspect(|w| assert_eq!(w.kind, WarningKind::AutoCreated))
        .map(|w| (w.object_type.clone(), w.name.clone()))
        .collect();
    assert!(created.contains(&("entity_class".into(), "node".into())));
    assert!(created.contains(&("parameter_definition".into(), "demand".into())));
    assert!(created.contains(&("alternative".into(), "scen_a".into())));
    assert!(created.contains(&("entity".into(), "node_1".into())));
    assert_eq!(created.len(), 4, "exactly the four referents auto-created");

    assert_eq!(report.values_written, 1);
    assert_eq!(report.tiers, vec![Tier::Scalar]);
    assert!(report.commit_id > 0);
    for w in &report.warnings {
        assert!(w.id > 0, "warning carries the created id");
    }

    // read_parameter returns the value.
    let got = read_parameter(&db, "node", "demand", "scen_a").expect("read");
    assert_eq!(got.num_rows(), 1);
    assert_eq!(col_str(&got, "tier"), vec![Some("scalar".into())]);
    assert_eq!(col_f64(&got, "v_float"), vec![Some(42.5)]);
    assert_eq!(col_str(&got, "entity"), vec![Some("node_1".into())]);
}

#[test]
fn dense_float_writes_array_tier_and_reads_back() {
    let (_dir, db) = fresh_db();

    let report = write_parameter(
        &db,
        "node",
        "profile",
        "Base",
        indexed_float_batch("node_1", "step", &[0, 1, 2], &[1.5, 2.5, 3.5]),
        &[],
        WriteOptions::default(),
        "tester",
        "array write",
    )
    .expect("write");
    assert_eq!(report.tiers, vec![Tier::Array], "full Cartesian float ⇒ array tier");
    assert_eq!(report.values_written, 1);

    let got = read_parameter(&db, "node", "profile", "Base").expect("read");
    assert_eq!(got.num_rows(), 3);
    for t in col_str(&got, "tier") {
        assert_eq!(t, Some("array".into()));
    }
    let mut vals: Vec<f64> = col_f64(&got, "v_float").into_iter().flatten().collect();
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(vals, vec![1.5, 2.5, 3.5]);
}

#[test]
fn sparse_int_writes_long_tier_and_reads_back() {
    let (_dir, db) = fresh_db();

    let report = write_parameter(
        &db,
        "node",
        "counts",
        "Base",
        indexed_int_batch("node_1", "step", &[10, 20, 30], &[100, 200, 300]),
        &[],
        WriteOptions::default(),
        "tester",
        "long write",
    )
    .expect("write");
    assert_eq!(report.tiers, vec![Tier::Long], "int measure ⇒ long tier");
    assert_eq!(report.rows_written, 3);

    let got = read_parameter(&db, "node", "counts", "Base").expect("read");
    assert_eq!(got.num_rows(), 3);
    for t in col_str(&got, "tier") {
        assert_eq!(t, Some("long".into()));
    }
    let mut vals: Vec<i64> = col_i64(&got, "v_int").into_iter().flatten().collect();
    vals.sort_unstable();
    assert_eq!(vals, vec![100, 200, 300]);
    // The integer axis coords render into idx1_num.
    let mut coords: Vec<i64> = col_i64(&got, "idx1_num").into_iter().flatten().collect();
    coords.sort_unstable();
    assert_eq!(coords, vec![10, 20, 30]);
}

#[test]
fn empty_user_is_rejected_and_empty_message_is_fine() {
    let (_dir, db) = fresh_db();
    let opts = WriteOptions::default();

    let err = write_parameter(&db, "node", "demand", "Base", scalar_batch("n", 1.0), &[], opts, "  ", "m")
        .expect_err("blank user must be rejected");
    assert!(err.to_string().contains("user"), "unexpected error: {err}");

    write_parameter(&db, "node", "demand", "Base", scalar_batch("n", 1.0), &[], opts, "tool", "")
        .expect("empty message is allowed");
}

#[test]
fn updating_a_scalar_bumps_the_version() {
    let (_dir, db) = fresh_db();
    let opts = WriteOptions::default();

    write_parameter(&db, "node", "demand", "Base", scalar_batch("node_1", 1.0), &[], opts, "t", "v1")
        .expect("write v1");
    let r2 = write_parameter(&db, "node", "demand", "Base", scalar_batch("node_1", 2.0), &[], opts, "t", "v2")
        .expect("write v2");
    // Second call finds every referent existing ⇒ no auto-create warnings.
    assert!(r2.warnings.is_empty(), "no re-creation on the update: {:?}", r2.warnings);

    let conn = Connection::open(&db).unwrap();
    let rows: Vec<(i16, Option<i64>, f64)> = conn
        .prepare(
            "SELECT version, valid_to_commit, v_float FROM value_scalar \
             WHERE v_float IS NOT NULL OR valid_to_commit IS NOT NULL ORDER BY version",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, Option<f64>>(2)?.unwrap_or(f64::NAN))))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    conn.close().unwrap();

    assert_eq!(rows.len(), 2, "one superseded + one current");
    assert_eq!(rows[0].0, 1, "version 1 first");
    assert!(rows[0].1.is_some(), "version 1 closed out");
    assert_eq!(rows[1].0, 2, "version 2 current");
    assert!(rows[1].1.is_none(), "version 2 is current");
    assert_eq!(rows[1].2, 2.0);
}

#[test]
fn retain_content_false_stubs_the_superseded_row() {
    let (_dir, db) = fresh_db();

    // Pre-create a class + a retain_content=FALSE definition (a result class),
    // against the bootstrap commit (id 1).
    {
        let conn = Connection::open(&db).unwrap();
        let cls: i64 = conn
            .query_row(
                "INSERT INTO entity_class (name, valid_from_commit) VALUES ('unit', 1) RETURNING id",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute(
            "INSERT INTO parameter_definition (entity_class_id, name, retain_content, valid_from_commit) \
             VALUES (?, 'result', FALSE, 1)",
            duckdb::params![cls],
        )
        .unwrap();
        conn.close().unwrap();
    }

    let opts = WriteOptions::default();
    write_parameter(&db, "unit", "result", "Base", scalar_batch("u1", 1.0), &[], opts, "t", "v1")
        .expect("write v1");
    write_parameter(&db, "unit", "result", "Base", scalar_batch("u1", 2.0), &[], opts, "t", "v2")
        .expect("write v2");

    let conn = Connection::open(&db).unwrap();
    let rows: Vec<(i16, bool, Option<f64>, Option<i64>)> = conn
        .prepare(
            "SELECT version, content_retained, v_float, valid_to_commit FROM value_scalar \
             ORDER BY version",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    conn.close().unwrap();

    assert_eq!(rows.len(), 2);
    // Superseded row: stubbed — content_retained FALSE, payload nulled.
    assert_eq!(rows[0].0, 1);
    assert!(!rows[0].1, "superseded row must NOT retain content (retain_content=FALSE wired through)");
    assert_eq!(rows[0].2, None, "stubbed payload is NULL");
    assert!(rows[0].3.is_some(), "superseded row is closed");
    // Current row keeps its content.
    assert_eq!(rows[1].0, 2);
    assert!(rows[1].1, "current row retains content");
    assert_eq!(rows[1].2, Some(2.0));
}
