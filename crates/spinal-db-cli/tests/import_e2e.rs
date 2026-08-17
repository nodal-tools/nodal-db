//! End-to-end tests for `spinal-db init` + `spinal-db import`
//! (spec/arrow_ipc_bulk_format.md): each test BUILDS a real IPC container with
//! the pinned arrow-ipc StreamWriter, drives the actual binary, and verifies
//! against the database through the core read path. One behavior per test.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Array, Int64Array, ListBuilder, Int64Builder, Float64Builder, StringArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use spinal_db_core::api::read_parameter;
use spinal_db_core::duckdb::Connection;

// --- harness ---------------------------------------------------------------

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_spinal-db")
}

/// `spinal-db init` into a fresh dir (embedded schema — the deployment path).
fn init_db(dir: &Path) -> PathBuf {
    let db = dir.join("model.sdb");
    let out = Command::new(bin()).arg("init").arg(&db).output().expect("run init");
    assert!(out.status.success(), "init failed: {}", String::from_utf8_lossy(&out.stderr));
    db
}

fn import(db: &Path, file: &Path, extra: &[&str]) -> Output {
    Command::new(bin())
        .arg("import")
        .arg(file)
        .arg("--db")
        .arg(db)
        .args(extra)
        .output()
        .expect("run import")
}

fn commits(db: &Path) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row("SELECT count(*) FROM commit", [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

fn segment_metadata(class: &str, parameter: &str, form: &str, count: usize) -> HashMap<String, String> {
    HashMap::from([
        ("spinal:format".into(), "spinal-db-bulk".into()),
        ("spinal:format_version".into(), "1.0".into()),
        ("spinal:segment_count".into(), count.to_string()),
        ("spinal:class".into(), class.into()),
        ("spinal:parameter".into(), parameter.into()),
        ("spinal:form".into(), form.into()),
    ])
}

/// Serialize one complete IPC stream (schema + batches + EOS) — one segment.
fn stream(schema: Arc<Schema>, batches: &[RecordBatch]) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).expect("stream writer");
        for b in batches {
            w.write(b).expect("write batch");
        }
        w.finish().expect("finish stream");
    }
    buf
}

/// A rank-1 long segment: (entity Utf8, step Int64, value Float64).
fn long_segment(
    class: &str,
    parameter: &str,
    count: usize,
    rows: &[(&str, i64, f64)],
) -> Vec<u8> {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("step", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ],
        segment_metadata(class, parameter, "long", count),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(Int64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(Float64Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())) as ArrayRef,
        ],
    )
    .unwrap();
    stream(schema, &[batch])
}

fn write_file(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, bytes).unwrap();
    p
}

fn col_f64(batch: &RecordBatch, name: &str) -> Vec<f64> {
    let i = batch.schema().index_of(name).unwrap();
    let a = batch.column(i).as_any().downcast_ref::<Float64Array>().unwrap();
    (0..a.len()).filter(|&r| !arrow::array::Array::is_null(a, r)).map(|r| a.value(r)).collect()
}

// --- success paths ---------------------------------------------------------

/// `init` then `import` from a fresh dir: single long segment lands, the
/// summary names the commit, and the values read back through the core.
#[test]
fn init_then_import_single_long_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "in.arrows",
        &long_segment("node", "profile", 1, &[("n1", 0, 1.5), ("n1", 1, 2.5), ("n1", 2, 3.5)]),
    );

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(stdout.contains("commit"), "summary must name the commit: {stdout}");
    assert!(stdout.contains("warning: auto-created"), "auto-create warnings must print: {stdout}");

    let got = read_parameter(&db, "node", "profile", "Base").expect("read back");
    assert_eq!(got.num_rows(), 3);
    let mut vals = col_f64(&got, "v_float");
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(vals, vec![1.5, 2.5, 3.5]);
}

/// Two segments (two parameters) in one container: both land under ONE commit
/// row (whole-invocation atomicity, spec §9.2).
#[test]
fn multi_segment_two_parameters_one_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let mut container = long_segment("node", "demand", 2, &[("n1", 0, 1.0), ("n1", 1, 2.0)]);
    container.extend(long_segment("unit", "capacity", 2, &[("u1", 0, 9.0)]));
    let file = write_file(dir.path(), "two.arrows", &container);

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("2 segment(s)"), "unexpected summary: {stdout}");

    assert_eq!(commits(&db), 2, "bootstrap + exactly ONE import commit");
    assert_eq!(read_parameter(&db, "node", "demand", "Base").unwrap().num_rows(), 2);
    assert_eq!(read_parameter(&db, "unit", "capacity", "Base").unwrap().num_rows(), 1);
}

/// Dense form: the importer expands (core row-major law), the CORE picks the
/// tier (array for a full float grid), and the values round-trip.
#[test]
fn dense_form_expands_and_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());

    let mut steps = ListBuilder::new(Int64Builder::new());
    steps.values().append_slice(&[0, 1, 2]);
    steps.append(true);
    let steps = Arc::new(steps.finish()) as ArrayRef;
    let mut vals = ListBuilder::new(Float64Builder::new());
    vals.values().append_slice(&[1.5, 2.5, 3.5]);
    vals.append(true);
    let vals = Arc::new(vals.finish()) as ArrayRef;

    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("step", steps.data_type().clone(), false),
            Field::new("value", vals.data_type().clone(), false),
        ],
        segment_metadata("node", "price", "dense", 1),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["n1"])) as ArrayRef, steps, vals],
    )
    .unwrap();
    let file = write_file(dir.path(), "dense.arrows", &stream(schema, &[batch]));

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("array"), "core must tier the full float grid as array: {stdout}");

    let got = read_parameter(&db, "node", "price", "Base").expect("read back");
    assert_eq!(got.num_rows(), 3);
    let mut v = col_f64(&got, "v_float");
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(v, vec![1.5, 2.5, 3.5]);
}

/// Millisecond timestamps (Arrow.jl's natural DateTime output) are upcast
/// losslessly to µs and accepted (spec §5.2.1).
#[test]
fn ms_timestamp_dimension_is_upcast_and_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("t", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("value", DataType::Float64, true),
        ],
        segment_metadata("node", "flow", "long", 1),
    ));
    // Two hourly stamps, in ms since epoch.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["n1", "n1"])) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(vec![3_600_000i64, 7_200_000])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
        ],
    )
    .unwrap();
    let file = write_file(dir.path(), "ms.arrows", &stream(schema, &[batch]));

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));

    // The stored instants are the same instants in µs: verify via the axis
    // parametric start (µs precision), not just row count.
    let conn = Connection::open(&db).unwrap();
    let (start, step): (i64, i64) = conn
        .query_row(
            "SELECT epoch_us(p_start_ts), p_step_micros FROM axis WHERE kind = 'datetime'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    conn.close().unwrap();
    assert_eq!(start, 3_600_000_000, "1h in µs — ms upcast ×1000");
    assert_eq!(step, 3_600_000_000, "hourly step in µs");
}

// --- error paths (spec §9.1 classes) ---------------------------------------

fn assert_error_leaves_db_untouched(db: &Path, out: &Output, class: &str) {
    assert!(!out.status.success(), "import must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(class), "expected {class} in stderr, got: {stderr}");
    assert_eq!(commits(db), 1, "nothing imported (bootstrap commit only)");
}

/// Stripping the final end-of-stream marker (a producer crash before close)
/// must be E10, not a silent partial import (spec §2.2.1).
#[test]
fn truncated_final_eos_is_e10_and_imports_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let mut bytes = long_segment("node", "profile", 1, &[("n1", 0, 1.0)]);
    bytes.truncate(bytes.len() - 8); // strip the EOS marker
    let file = write_file(dir.path(), "trunc.arrows", &bytes);

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E10");
}

/// A container that lost a whole trailing segment (count says 2, file holds 1)
/// is E10 (spec §2.2.1 completeness declaration).
#[test]
fn missing_declared_segment_is_e10() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let bytes = long_segment("node", "profile", 2, &[("n1", 0, 1.0)]); // declares 2, holds 1
    let file = write_file(dir.path(), "short.arrows", &bytes);

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E10");
}

/// A format major this reader does not support is rejected before any write
/// (spec §3.2).
#[test]
fn version_major_mismatch_is_e3() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let mut md = segment_metadata("node", "profile", "long", 1);
    md.insert("spinal:format_version".into(), "2.0".into());
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ],
        md,
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["n1"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
        ],
    )
    .unwrap();
    let file = write_file(dir.path(), "v2.arrows", &stream(schema, &[batch]));

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E3");
}

/// Two segments whose targets differ only by case resolve to one definition —
/// duplicate target, E7 (spec §4.2, case-insensitive comparison).
#[test]
fn duplicate_case_aliased_target_is_e7() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let mut container = long_segment("node", "Flow", 2, &[("n1", 0, 1.0)]);
    container.extend(long_segment("Node", "flow", 2, &[("n2", 0, 2.0)]));
    let file = write_file(dir.path(), "dup.arrows", &container);

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E7");
}

/// A zero-row segment must not import and must not auto-create (spec §2.3).
#[test]
fn zero_row_segment_is_e6() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ],
        segment_metadata("node", "profile", "long", 1),
    ));
    let file = write_file(dir.path(), "empty.arrows", &stream(schema, &[]));

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
    let conn = Connection::open(&db).unwrap();
    let classes: i64 = conn.query_row("SELECT count(*) FROM entity_class", [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    assert_eq!(classes, 0, "no auto-create from a zero-row segment");
}

/// NaN can never be stored: rejected before the core call, naming the row
/// (spec §5.3.1).
#[test]
fn nan_float_value_is_e6() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "nan.arrows",
        &long_segment("node", "profile", 1, &[("n1", 0, 1.0), ("n1", 1, f64::NAN)]),
    );

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("NaN"),
        "error must name the NaN"
    );
}
