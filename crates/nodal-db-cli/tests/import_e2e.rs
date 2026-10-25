//! End-to-end tests for `nodal-db init` + `nodal-db import`
//! (specs/arrow_ipc_bulk_format.md): each test BUILDS a real IPC container with
//! the pinned arrow-ipc StreamWriter, drives the actual binary, and verifies
//! against the database through the core read path. One behavior per test.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Float64Array, Float64Builder, Int64Array, Int64Builder,
    ListBuilder, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, TimestampSecondBuilder,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::{CompressionType, MetadataVersion};
use arrow::record_batch::RecordBatch;

use nodal_db_core::api::read_parameter;
use nodal_db_core::duckdb::Connection;

// --- harness ---------------------------------------------------------------

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_nodal-db")
}

/// `nodal-db init` into a fresh dir (embedded schema — the deployment path).
fn init_db(dir: &Path) -> PathBuf {
    let db = dir.join("model.sdb");
    let out = Command::new(bin())
        .arg("init")
        .arg(&db)
        .output()
        .expect("run init");
    assert!(
        out.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // The deployment create path must produce DuckDB's native 2.0 storage
    // format (1.5-format files log the whole list column to the WAL on every
    // array write; see nodal-db-core/tests/storage_format.rs).
    let conn = Connection::open(&db).expect("open init'd db");
    let tag: String = conn
        .query_row(
            "SELECT tags['storage_version'] FROM duckdb_databases() \
             WHERE database_name = current_database()",
            [],
            |r| r.get(0),
        )
        .expect("storage_version tag");
    conn.close().expect("close");
    assert_eq!(
        tag, "v2.0.0+",
        "nodal-db init did not create a native-format file"
    );
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

/// `import` with explicit environment control (`Some` sets, `None` removes) —
/// for the `--user` env-fallback tests.
fn import_env(db: &Path, file: &Path, extra: &[&str], env: &[(&str, Option<&str>)]) -> Output {
    let mut cmd = Command::new(bin());
    cmd.arg("import").arg(file).arg("--db").arg(db).args(extra);
    for (k, v) in env {
        match v {
            Some(v) => cmd.env(k, v),
            None => cmd.env_remove(k),
        };
    }
    cmd.output().expect("run import")
}

fn commits(db: &Path) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM commit", [], |r| r.get(0))
        .unwrap();
    conn.close().unwrap();
    n
}

fn segment_metadata(
    class: &str,
    parameter: &str,
    form: &str,
    count: usize,
) -> HashMap<String, String> {
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

/// The fixed (entity Utf8, step Int64, value Float64) long schema with
/// caller-supplied segment metadata.
fn long_schema(md: HashMap<String, String>) -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("step", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ],
        md,
    ))
}

/// One batch of (entity, step, value) rows under `schema`.
fn long_batch(schema: &Arc<Schema>, rows: &[(&str, i64, f64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

/// A rank-1 long segment with custom metadata: (entity, step, value).
fn long_segment_md(md: HashMap<String, String>, rows: &[(&str, i64, f64)]) -> Vec<u8> {
    let schema = long_schema(md);
    let batch = long_batch(&schema, rows);
    stream(schema, &[batch])
}

/// A rank-1 long segment: (entity Utf8, step Int64, value Float64).
fn long_segment(class: &str, parameter: &str, count: usize, rows: &[(&str, i64, f64)]) -> Vec<u8> {
    long_segment_md(segment_metadata(class, parameter, "long", count), rows)
}

/// A rank-1 long segment with a label dimension: (entity, scenario Utf8,
/// value Float64), with `spinal:label_domain` field metadata when given.
fn label_segment(domain: Option<&str>, rows: &[(&str, &str, f64)]) -> Vec<u8> {
    let mut scenario = Field::new("scenario", DataType::Utf8, false);
    if let Some(d) = domain {
        scenario = scenario.with_metadata(HashMap::from([(
            "spinal:label_domain".to_string(),
            d.to_string(),
        )]));
    }
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", DataType::Utf8, false),
            scenario,
            Field::new("value", DataType::Float64, true),
        ],
        segment_metadata("node", "profile", "long", 1),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
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
    let a = batch
        .column(i)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..a.len())
        .filter(|&r| !arrow::array::Array::is_null(a, r))
        .map(|r| a.value(r))
        .collect()
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
        &long_segment(
            "node",
            "profile",
            1,
            &[("n1", 0, 1.5), ("n1", 1, 2.5), ("n1", 2, 3.5)],
        ),
    );

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("commit"),
        "summary must name the commit: {stdout}"
    );
    assert!(
        stdout.contains("warning: auto-created"),
        "auto-create warnings must print: {stdout}"
    );

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
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("2 segment(s)"),
        "unexpected summary: {stdout}"
    );

    assert_eq!(commits(&db), 2, "bootstrap + exactly ONE import commit");
    assert_eq!(
        read_parameter(&db, "node", "demand", "Base")
            .unwrap()
            .num_rows(),
        2
    );
    assert_eq!(
        read_parameter(&db, "unit", "capacity", "Base")
            .unwrap()
            .num_rows(),
        1
    );
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
        vec![
            Arc::new(StringArray::from(vec!["n1"])) as ArrayRef,
            steps,
            vals,
        ],
    )
    .unwrap();
    let file = write_file(dir.path(), "dense.arrows", &stream(schema, &[batch]));

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("array"),
        "core must tier the full float grid as array: {stdout}"
    );

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
            Arc::new(TimestampMillisecondArray::from(vec![
                3_600_000i64,
                7_200_000,
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
        ],
    )
    .unwrap();
    let file = write_file(dir.path(), "ms.arrows", &stream(schema, &[batch]));

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

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
    assert!(
        stderr.contains(class),
        "expected {class} in stderr, got: {stderr}"
    );
    // Messages are self-explanatory: no references into private design docs.
    assert!(
        !stderr.contains('§') && !stderr.contains("specs/"),
        "private spec ref leaked: {stderr}"
    );
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
    let classes: i64 = conn
        .query_row("SELECT count(*) FROM entity_class", [], |r| r.get(0))
        .unwrap();
    conn.close().unwrap();
    assert_eq!(classes, 0, "no auto-create from a zero-row segment");
}

/// A NaN VALUE is a real stored value (spec §5.3.1): it is accepted,
/// canonicalized by the core, and — being a present cell, not absence — keeps a
/// dense float grid on the Array tier, round-tripping as NaN at its position.
#[test]
fn nan_float_value_is_stored_on_array_tier() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    // coords {0, 1} form a full rank-1 Cartesian float grid; one cell is NaN.
    let file = write_file(
        dir.path(),
        "nan.arrows",
        &long_segment("node", "profile", 1, &[("n1", 0, 1.0), ("n1", 1, f64::NAN)]),
    );

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let conn = Connection::open(&db).unwrap();
    // A NaN cell is PRESENT, so the grid stays Cartesian → Array tier, NOT Long.
    let arrays: i64 = conn
        .query_row("SELECT count(*) FROM value_array", [], |r| r.get(0))
        .unwrap();
    let longs: i64 = conn
        .query_row("SELECT count(*) FROM value_long", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        (arrays, longs),
        (1, 0),
        "NaN-bearing dense grid stays on the array tier"
    );
    // Round-trip: payload is [1.0, NaN] in flat (position) order.
    let payload: Vec<f64> = conn
        .prepare("SELECT unnest(values) AS v FROM value_array")
        .unwrap()
        .query_map([], |r| r.get::<_, f64>(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    conn.close().unwrap();
    assert_eq!(payload.len(), 2);
    assert_eq!(payload[0], 1.0, "position 0 preserved exactly");
    assert!(
        payload[1].is_nan(),
        "the NaN survives the round-trip at position 1"
    );
}

/// Critique F1 regression: a batch body whose LAST 8 BYTES coincidentally
/// equal the EOS sequence (final Float64 = f64::from_bits(0xFFFF_FFFF) — as
/// Int64 the bytes of 4294967295 — flush at the aligned body end), truncated
/// at that batch boundary, must still be E10. A raw-byte tail comparison would
/// pass it and silently drop the second batch's entity; only a message-frame
/// walk sees there is no EOS *frame* there.
#[test]
fn truncation_at_boundary_with_eos_lookalike_body_is_e10() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let schema = long_schema(segment_metadata("node", "profile", "long", 1));
    // 8 rows so the 64-byte Float64 value buffer ends flush at the aligned
    // body end; the last value's LE bytes are ff ff ff ff 00 00 00 00.
    let rows1: Vec<(&str, i64, f64)> = (0..8)
        .map(|i| {
            (
                "n1",
                i,
                if i == 7 {
                    f64::from_bits(0x0000_0000_FFFF_FFFF)
                } else {
                    i as f64
                },
            )
        })
        .collect();
    let b1 = long_batch(&schema, &rows1);
    let b2 = long_batch(&schema, &[("n2", 0, 9.0)]);
    let full = stream(schema.clone(), &[b1.clone(), b2]);
    let one = stream(schema, &[b1]);
    let cut = one.len() - 8; // end of batch 1 = where a producer crash truncates
    let truncated = &full[..cut];
    // Repro precondition: the raw tail IS the EOS byte sequence.
    assert_eq!(
        &truncated[cut - 8..],
        &[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0],
        "fixture must end with the EOS lookalike bytes"
    );
    let file = write_file(dir.path(), "lookalike.arrows", truncated);

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E10");
}

/// Critique F2 regression: a dense row with empty dim lists must be E8, not a
/// silent drop of that entity (spec §9.3 — no partial batch silently dropped).
#[test]
fn dense_row_with_empty_dim_list_is_e8_not_silent_drop() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());

    let mut steps = ListBuilder::new(Int64Builder::new());
    steps.values().append_slice(&[0, 1]);
    steps.append(true); // n1: [0, 1]
    steps.append(true); // ghost: []
    let steps = Arc::new(steps.finish()) as ArrayRef;
    let mut vals = ListBuilder::new(Float64Builder::new());
    vals.values().append_slice(&[1.5, 2.5]);
    vals.append(true); // n1: [1.5, 2.5]
    vals.append(true); // ghost: []
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
        vec![
            Arc::new(StringArray::from(vec!["n1", "ghost"])) as ArrayRef,
            steps,
            vals,
        ],
    )
    .unwrap();
    let file = write_file(dir.path(), "ghost.arrows", &stream(schema, &[batch]));

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E8");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("ghost"),
        "error must name the dropped-entity row"
    );
}

/// Critique F3 regression: alternatives aliased only under Unicode lowercasing
/// (`Ä`/`ä` — no ASCII charset CHECK on alternative names) are ONE target
/// under DuckDB `lower()` and must be a loud, located E7 — never a raw CHECK
/// constraint error from inside the transaction.
#[test]
fn unicode_case_aliased_alternative_target_is_e7() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let mut md1 = segment_metadata("node", "profile", "long", 2);
    md1.insert("spinal:alternative".into(), "Ä".into());
    let mut md2 = segment_metadata("node", "profile", "long", 2);
    md2.insert("spinal:alternative".into(), "ä".into());
    let mut container = long_segment_md(md1, &[("n1", 0, 1.0)]);
    container.extend(long_segment_md(md2, &[("n1", 0, 2.0)]));
    let file = write_file(dir.path(), "umlaut.arrows", &container);

    let out = import(&db, &file, &["--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E7");
    assert!(
        !String::from_utf8_lossy(&out.stderr).contains("CHECK constraint"),
        "must be the located E7 pre-check, not a raw constraint error"
    );
}

/// Critique F5: a set-but-blank env var must fall through the
/// USER/USERNAME/LOGNAME chain to the next non-blank source.
#[test]
fn blank_user_env_var_falls_through_the_chain() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "envuser.arrows",
        &long_segment("node", "profile", 1, &[("n1", 0, 1.0)]),
    );

    let out = import_env(
        &db,
        &file,
        &["--alternative", "Base"],
        &[
            ("USER", Some("  ")),
            ("USERNAME", None),
            ("LOGNAME", Some("envuser")),
        ],
    );
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let conn = Connection::open(&db).unwrap();
    let user: String = conn
        .query_row(
            "SELECT \"user\" FROM commit ORDER BY id DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    conn.close().unwrap();
    assert_eq!(user, "envuser", "blank USER must fall through to LOGNAME");
}

/// Critique F5: no author anywhere is the CLI's own usage error — before the
/// container is parsed and never dressed up as an E6 payload error.
#[test]
fn missing_user_everywhere_is_a_usage_error_not_e6() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "nouser.arrows",
        &long_segment("node", "profile", 1, &[("n1", 0, 1.0)]),
    );

    let out = import_env(
        &db,
        &file,
        &["--alternative", "Base"],
        &[("USER", Some("")), ("USERNAME", None), ("LOGNAME", None)],
    );
    assert!(!out.status.success(), "import must fail without any author");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("--user"),
        "must point at the flag: {stderr}"
    );
    assert!(
        !stderr.contains("E6"),
        "not a payload-class error: {stderr}"
    );
    assert_eq!(commits(&db), 1, "DB untouched");
}

/// Critique F8(a): a well-formed legacy-format stream (pre-0.15 framing,
/// 4-byte EOS) is still E10 — the 8-byte marker is normative — but the error
/// must name the legacy-format possibility instead of misdiagnosing truncation.
#[test]
fn legacy_format_stream_is_e10_naming_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let schema = long_schema(segment_metadata("node", "profile", "long", 1));
    let batch = long_batch(&schema, &[("n1", 0, 1.0)]);
    let opts = IpcWriteOptions::try_new(8, true, MetadataVersion::V4).expect("legacy options");
    let mut buf = Vec::new();
    {
        let mut w =
            StreamWriter::try_new_with_options(&mut buf, &schema, opts).expect("legacy writer");
        w.write(&batch).expect("write batch");
        w.finish().expect("finish stream");
    }
    let file = write_file(dir.path(), "legacy.arrows", &buf);

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E10");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("legacy"),
        "must name the legacy-format possibility: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Critique F8(b): truncation MID-message (not at a batch boundary) surfaces
/// as E6 — the reader's decode error — per the §9.1 table.
#[test]
fn mid_message_truncation_is_e6() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let mut bytes = long_segment("node", "profile", 1, &[("n1", 0, 1.0)]);
    bytes.truncate(bytes.len() - 20); // into the final batch's body
    let file = write_file(dir.path(), "midcut.arrows", &bytes);

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
}

/// Critique F10: an s→µs upcast that overflows i64 must be a loud E6 naming
/// the value, never a release-mode wrap into a garbage instant.
#[test]
fn dense_timestamp_upcast_overflow_is_e6() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());

    let mut ts = ListBuilder::new(TimestampSecondBuilder::new());
    ts.values().append_value(i64::MAX);
    ts.append(true);
    let ts = Arc::new(ts.finish()) as ArrayRef;
    let mut vals = ListBuilder::new(Float64Builder::new());
    vals.values().append_value(1.0);
    vals.append(true);
    let vals = Arc::new(vals.finish()) as ArrayRef;
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("t", ts.data_type().clone(), false),
            Field::new("value", vals.data_type().clone(), false),
        ],
        segment_metadata("node", "price", "dense", 1),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["n1"])) as ArrayRef,
            ts,
            vals,
        ],
    )
    .unwrap();
    let file = write_file(dir.path(), "overflow.arrows", &stream(schema, &[batch]));

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("overflow"), "must say overflow: {stderr}");
    assert!(
        stderr.contains(&i64::MAX.to_string()),
        "must name the value: {stderr}"
    );
}

// --- F9: the remaining E-table / normative-surface pins ---------------------

/// E1: a zero-byte file is not an IPC stream (spec §9.1, critique F16).
#[test]
fn zero_byte_file_is_e1() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(dir.path(), "empty.arrows", b"");
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E1");
}

/// E1: arbitrary non-Arrow bytes are rejected before any DB touch.
#[test]
fn garbage_file_is_e1() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "garbage.arrows",
        b"definitely not an arrow stream",
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E1");
}

/// E2: a segment without the `spinal:format` key is rejected per segment.
#[test]
fn missing_format_key_is_e2() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let mut md = segment_metadata("node", "profile", "long", 1);
    md.remove("spinal:format");
    let file = write_file(
        dir.path(),
        "noformat.arrows",
        &long_segment_md(md, &[("n1", 0, 1.0)]),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E2");
}

/// E4: an unknown `spinal:form` value is rejected (the §3.2 minor-version door).
#[test]
fn unknown_form_value_is_e4() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "wide.arrows",
        &long_segment_md(
            segment_metadata("node", "profile", "wide", 1),
            &[("n1", 0, 1.0)],
        ),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E4");
}

/// E5 is a WARNING, not an error (spec §3.2): an unknown `spinal:` key is
/// named on stdout and the import proceeds.
#[test]
fn unknown_spinal_key_is_e5_warning_not_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let mut md = segment_metadata("node", "profile", "long", 1);
    md.insert("spinal:future_hint".into(), "x".into());
    let file = write_file(
        dir.path(),
        "e5.arrows",
        &long_segment_md(md, &[("n1", 0, 1.0)]),
    );

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("spinal:future_hint") && stdout.contains("E5"),
        "warning must name the key: {stdout}"
    );
    assert_eq!(
        read_parameter(&db, "node", "profile", "Base")
            .unwrap()
            .num_rows(),
        1
    );
}

/// Nanosecond timestamps are rejected with the producer-side fix named
/// (spec §5.2.1) — lossy truncation is never silent.
#[test]
fn ns_timestamp_is_e6_naming_the_fix() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("value", DataType::Float64, true),
        ],
        segment_metadata("node", "flow", "long", 1),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["n1"])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000i64])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
        ],
    )
    .unwrap();
    let file = write_file(dir.path(), "ns.arrows", &stream(schema, &[batch]));

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("datetime64[us]"),
        "must name the producer-side fix: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A tz-annotated timestamp column is accepted (well-defined instants) and the
/// core's TimezoneNormalized warning is printed (spec §5.2.2).
#[test]
fn tz_annotated_timestamp_imports_with_normalized_warning() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let tz: Arc<str> = Arc::from("Europe/Helsinki");
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Microsecond, Some(tz.clone())),
                false,
            ),
            Field::new("value", DataType::Float64, true),
        ],
        segment_metadata("node", "flow", "long", 1),
    ));
    let stamps =
        TimestampMicrosecondArray::from(vec![3_600_000_000i64, 7_200_000_000]).with_timezone(tz);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["n1", "n1"])) as ArrayRef,
            Arc::new(stamps) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
        ],
    )
    .unwrap();
    let file = write_file(dir.path(), "tz.arrows", &stream(schema, &[batch]));

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("timezone-normalized") && stdout.contains("Europe/Helsinki"),
        "TimezoneNormalized warning must print: {stdout}"
    );
}

/// A dictionary-encoded entity column is decoded to plain Utf8 (spec §2.4).
#[test]
fn dictionary_encoded_entity_column_is_decoded() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let entities: DictionaryArray<Int32Type> = vec!["n1", "n2", "n1"].into_iter().collect();
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", entities.data_type().clone(), false),
            Field::new("step", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ],
        segment_metadata("node", "profile", "long", 1),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(entities) as ArrayRef,
            Arc::new(Int64Array::from(vec![0i64, 0, 1])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
        ],
    )
    .unwrap();
    let file = write_file(dir.path(), "dict.arrows", &stream(schema, &[batch]));

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let conn = Connection::open(&db).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM entity", [], |r| r.get(0))
        .unwrap();
    conn.close().unwrap();
    assert_eq!(n, 2, "two distinct entities decoded from the dictionary");
    assert_eq!(
        read_parameter(&db, "node", "profile", "Base")
            .unwrap()
            .num_rows(),
        3
    );
}

/// A zstd-compressed stream imports end-to-end (spec §2.4, OPEN-Q3-9).
#[test]
fn zstd_compressed_stream_imports() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let schema = long_schema(segment_metadata("node", "profile", "long", 1));
    let batch = long_batch(&schema, &[("n1", 0, 1.5), ("n1", 1, 2.5)]);
    let opts = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .expect("zstd options");
    let mut buf = Vec::new();
    {
        let mut w =
            StreamWriter::try_new_with_options(&mut buf, &schema, opts).expect("zstd writer");
        w.write(&batch).expect("write batch");
        w.finish().expect("finish stream");
    }
    let file = write_file(dir.path(), "zstd.arrows", &buf);

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let mut vals = col_f64(
        &read_parameter(&db, "node", "profile", "Base").unwrap(),
        "v_float",
    );
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(vals, vec![1.5, 2.5]);
}

/// §7.2: a label dimension without `spinal:label_domain` is E6 naming the field.
#[test]
fn label_dimension_without_domain_is_e6() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "nodomain.arrows",
        &label_segment(None, &[("n1", "high", 1.0)]),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("spinal:label_domain"),
        "must name the missing key: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// §7.1: a present label absent from the declared domain is E6 (non-superset).
#[test]
fn present_label_missing_from_domain_is_e6() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "notindomain.arrows",
        &label_segment(Some(r#"["low"]"#), &[("n1", "high", 1.0)]),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
}

/// A label domain that repeats a label is E6, naming the label and the
/// dimension (the order of a repeated label is ambiguous).
#[test]
fn repeated_label_in_domain_is_e6() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "repeated.arrows",
        &label_segment(Some(r#"["low","high","low"]"#), &[("n1", "high", 1.0)]),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains(r#"label domain of dimension 1 ("scenario") repeats the label "low""#),
        "{stderr}"
    );
}

/// Two rows with the same entity and index tuple are E6, and the message
/// says which value and which tuple, in the file's own terms.
#[test]
fn duplicate_tuple_is_e6_naming_entity_and_labels() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "duptuple.arrows",
        &label_segment(
            Some(r#"["low","high"]"#),
            &[("n1", "high", 1.0), ("n1", "low", 2.0), ("n1", "high", 3.0)],
        ),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains(
            r#"entity "n1" (class "node", parameter "profile", alternative "Base"): duplicate index tuple ("high")"#
        ),
        "{stderr}"
    );
}

/// §7.1 happy path: a superset domain is accepted, and ordinals come from the
/// DOMAIN order, not wire order (wire emits high before low; the stored axis
/// orders low before high).
#[test]
fn superset_domain_accepted_with_domain_ordinals() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "domain.arrows",
        &label_segment(
            Some(r#"["low","mid","high"]"#),
            &[("n1", "high", 2.0), ("n1", "low", 1.0)],
        ),
    );

    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        read_parameter(&db, "node", "profile", "Base")
            .unwrap()
            .num_rows(),
        2
    );
    let conn = Connection::open(&db).unwrap();
    let mut stmt = conn
        .prepare("SELECT label FROM axis_label ORDER BY ordinal")
        .unwrap();
    let labels: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    drop(stmt);
    conn.close().unwrap();
    assert_eq!(
        labels,
        vec!["low", "high"],
        "ordinals from the domain, not wire order"
    );
}

/// OPEN-Q3-5: `spinal:alternative` metadata conflicting with `--alternative`
/// is E4 — no precedence, a conflict is always a mistake (spec §4.3).
#[test]
fn alternative_flag_conflicting_with_metadata_is_e4() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let mut md = segment_metadata("node", "profile", "long", 1);
    md.insert("spinal:alternative".into(), "Base".into());
    let file = write_file(
        dir.path(),
        "conflict.arrows",
        &long_segment_md(md, &[("n1", 0, 1.0)]),
    );
    let out = import(&db, &file, &["--alternative", "Other", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E4");
}

/// OPEN-Q3-6b: `--class` is an assertion, not a selector — a non-matching
/// segment errors (E4), never skips.
#[test]
fn class_assertion_mismatch_is_e4() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "assert.arrows",
        &long_segment("node", "profile", 1, &[("n1", 0, 1.0)]),
    );
    let out = import(
        &db,
        &file,
        &[
            "--alternative",
            "Base",
            "--user",
            "tester",
            "--class",
            "unit",
        ],
    );
    assert_error_leaves_db_untouched(&db, &out, "E4");
}

/// OPEN-Q3-6b: matching `--class`/`--parameter` assertions pass through.
#[test]
fn matching_class_and_parameter_assertions_pass() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "assert_ok.arrows",
        &long_segment("node", "profile", 1, &[("n1", 0, 1.0)]),
    );
    let out = import(
        &db,
        &file,
        &[
            "--alternative",
            "Base",
            "--user",
            "tester",
            "--class",
            "node",
            "--parameter",
            "profile",
        ],
    );
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// §3.2: a same-major NEWER minor version is attempted, not rejected.
#[test]
fn newer_minor_version_is_attempted() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let mut md = segment_metadata("node", "profile", "long", 1);
    md.insert("spinal:format_version".into(), "1.7".into());
    let file = write_file(
        dir.path(),
        "minor.arrows",
        &long_segment_md(md, &[("n1", 0, 1.0)]),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `--force-replace` reaches the core: the superseded pre-image is stubbed
/// (`content_retained = FALSE`) instead of retained.
#[test]
fn force_replace_passes_through_to_core() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "force.arrows",
        &long_segment(
            "node",
            "profile",
            1,
            &[("n1", 0, 1.0), ("n1", 1, 2.0), ("n1", 2, 3.0)],
        ),
    );

    let first = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        first.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    // The second import carries a modified payload: an identical one is an
    // unchanged write, which supersedes nothing and so stubs nothing
    // (skip_unchanged_writes_PLAN D3(i): --force-replace never versions an
    // unchanged value).
    let changed = write_file(
        dir.path(),
        "force2.arrows",
        &long_segment(
            "node",
            "profile",
            1,
            &[("n1", 0, 1.0), ("n1", 1, 2.5), ("n1", 2, 3.0)],
        ),
    );
    let second = import(
        &db,
        &changed,
        &[
            "--alternative",
            "Base",
            "--user",
            "tester",
            "--force-replace",
        ],
    );
    assert!(
        second.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&second.stderr)
    );

    assert_eq!(commits(&db), 3, "bootstrap + two import commits");
    let conn = Connection::open(&db).unwrap();
    let stubs: i64 = conn
        .query_row(
            "SELECT count(*) FROM value_array_hist WHERE NOT content_retained",
            [],
            |r| r.get(0),
        )
        .unwrap();
    conn.close().unwrap();
    assert_eq!(
        stubs, 1,
        "force-replace must stub the pre-image, proving pass-through"
    );
}

/// Re-importing the same file changes nothing (skip_unchanged_writes_PLAN
/// §6.2): the second import prints "no changes", makes no commit and adds
/// no history row, with and without `--force-replace` (D3(i)).
#[test]
fn reimport_of_the_same_file_commits_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let file = write_file(
        dir.path(),
        "same.arrows",
        &long_segment(
            "node",
            "profile",
            1,
            &[("n1", 0, 1.0), ("n1", 1, 2.0), ("n1", 2, 3.0)],
        ),
    );
    let args = ["--alternative", "Base", "--user", "tester"];
    let first = import(&db, &file, &args);
    assert!(first.status.success());
    let hist = |db: &Path| -> i64 {
        let conn = Connection::open(db).unwrap();
        let n = conn
            .query_row("SELECT count(*) FROM value_array_hist", [], |r| r.get(0))
            .unwrap();
        conn.close().unwrap();
        n
    };
    for extra in [&[][..], &["--force-replace"][..]] {
        let mut a = args.to_vec();
        a.extend_from_slice(extra);
        let again = import(&db, &file, &a);
        assert!(
            again.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&again.stderr)
        );
        let stdout = String::from_utf8_lossy(&again.stdout);
        assert!(
            stdout.contains("no changes (nothing committed)")
                && stdout.contains("1 value(s) unchanged"),
            "{stdout}"
        );
        assert_eq!(commits(&db), 2, "bootstrap + the first import only");
        assert_eq!(hist(&db), 0, "no history row");
    }
}

/// Critique F6: `init` onto an existing database refuses cleanly instead of
/// dying on (or worse, partially applying past) the first DDL collision.
#[test]
fn init_onto_existing_db_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let out = Command::new(bin())
        .arg("init")
        .arg(&db)
        .output()
        .expect("run init");
    assert!(!out.status.success(), "re-init must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("already exists; refusing to re-apply schema"),
        "clean refusal expected: {stderr}"
    );
}

// --- timestamp upcast and range (never a silent NULL) ----------------------

/// One long-form segment of `node:when` from explicit columns: entity, then
/// the given (field, array) columns (dims..., value last).
fn ts_segment(entities: &[&str], cols: Vec<(Field, ArrayRef)>) -> Vec<u8> {
    let mut fields = vec![Field::new("entity", DataType::Utf8, false)];
    let mut arrays: Vec<ArrayRef> =
        vec![Arc::new(StringArray::from(entities.to_vec())) as ArrayRef];
    for (f, a) in cols {
        fields.push(f);
        arrays.push(a);
    }
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        segment_metadata("node", "when", "long", 1),
    ));
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    stream(schema, &[batch])
}

fn int_dim(v: Vec<i64>) -> (Field, ArrayRef) {
    (
        Field::new("step", DataType::Int64, false),
        Arc::new(Int64Array::from(v)) as ArrayRef,
    )
}

fn ts_value_s(v: Vec<i64>) -> (Field, ArrayRef) {
    (
        Field::new("value", DataType::Timestamp(TimeUnit::Second, None), true),
        Arc::new(TimestampSecondArray::from(v)) as ArrayRef,
    )
}

/// The exact silent-drop repro: a timestamp[s] value whose ×10⁶ upcast
/// overflows i64 used to become NULL (= absence) and vanish with exit 0.
#[test]
fn long_value_timestamp_upcast_overflow_is_e6_not_silent_drop() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let big = 1i64 << 62;
    let file = write_file(
        dir.path(),
        "ovf.arrows",
        &ts_segment(
            &["n1", "n1"],
            vec![int_dim(vec![1, 2]), ts_value_s(vec![0, big])],
        ),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
    let stderr = String::from_utf8_lossy(&out.stderr);
    for needle in [
        "overflow",
        "4611686018427387904",
        "\"value\"",
        "row 1",
        "\"n1\"",
    ] {
        assert!(stderr.contains(needle), "missing {needle:?}: {stderr}");
    }
}

/// Rank 0: the overflow is named, not the misleading "exactly one non-null
/// value, got 0" the vanished cell used to produce.
#[test]
fn long_scalar_timestamp_upcast_overflow_names_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let value = (
        Field::new(
            "value",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Arc::new(TimestampMillisecondArray::from(vec![i64::MAX])) as ArrayRef,
    );
    let file = write_file(dir.path(), "ovf0.arrows", &ts_segment(&["n1"], vec![value]));
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("overflow"), "{stderr}");
    assert!(
        !stderr.contains("rank-0 scalar must have exactly one non-null value"),
        "{stderr}"
    );
}

/// A dimension column: the overflow is named, not "null index coordinate".
#[test]
fn long_dimension_timestamp_upcast_overflow_is_e6_naming_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let dim = (
        Field::new("t", DataType::Timestamp(TimeUnit::Second, None), false),
        Arc::new(TimestampSecondArray::from(vec![1i64 << 62])) as ArrayRef,
    );
    let value = (
        Field::new("value", DataType::Float64, true),
        Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
    );
    let file = write_file(
        dir.path(),
        "ovfd.arrows",
        &ts_segment(&["n1"], vec![dim, value]),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("overflow") && stderr.contains("dimension column"),
        "{stderr}"
    );
    assert!(!stderr.contains("null index coordinate"), "{stderr}");
}

/// The reported row is relative to the segment, not to the record batch.
#[test]
fn long_overflow_row_is_segment_relative_across_batches() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("step", DataType::Int64, false),
            Field::new("value", DataType::Timestamp(TimeUnit::Second, None), true),
        ],
        segment_metadata("node", "when", "long", 1),
    ));
    let mk = |steps: Vec<i64>, vals: Vec<i64>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["n1", "n1"])) as ArrayRef,
                Arc::new(Int64Array::from(steps)) as ArrayRef,
                Arc::new(TimestampSecondArray::from(vals)) as ArrayRef,
            ],
        )
        .unwrap()
    };
    let b0 = mk(vec![0, 1], vec![0, 1]);
    let b1 = mk(vec![2, 3], vec![1i64 << 62, 2]);
    let file = write_file(
        dir.path(),
        "rows.arrows",
        &stream(schema.clone(), &[b0, b1]),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("row 2"),
        "segment-relative row expected: {stderr}"
    );
}

/// A NULL timestamp value cell is still absence, whatever bits lie under the
/// null slot: the checked upcast must not multiply (and reject) them.
#[test]
fn long_null_timestamp_value_cell_is_still_absence() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let vals = TimestampSecondArray::new(
        vec![0, i64::MAX].into(),
        Some(NullBuffer::from(vec![true, false])),
    );
    let value = (
        Field::new("value", DataType::Timestamp(TimeUnit::Second, None), true),
        Arc::new(vals) as ArrayRef,
    );
    let file = write_file(
        dir.path(),
        "null.arrows",
        &ts_segment(&["n1", "n1"], vec![int_dim(vec![1, 2]), value]),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let got = read_parameter(&db, "node", "when", "Base").expect("read back");
    assert_eq!(
        got.num_rows(),
        1,
        "the null cell is absent, the other is stored"
    );
}

/// An upcast s column keeps its timezone, so the core still reports the
/// normalization (the existing tz test uses µs, which is never upcast).
#[test]
fn second_unit_tz_timestamp_keeps_timezone_warning() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let tz: Arc<str> = Arc::from("Europe/Helsinki");
    let dim = (
        Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Second, Some(tz.clone())),
            false,
        ),
        Arc::new(TimestampSecondArray::from(vec![3_600i64, 7_200]).with_timezone(tz)) as ArrayRef,
    );
    let value = (
        Field::new("value", DataType::Float64, true),
        Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
    );
    let file = write_file(
        dir.path(),
        "tz_s.arrows",
        &ts_segment(&["n1", "n1"], vec![dim, value]),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("timezone-normalized") && stdout.contains("Europe/Helsinki"),
        "TimezoneNormalized warning must print: {stdout}"
    );
}

/// Below DuckDB's minimum without any i64 overflow: an i64-only check misses
/// this (the value used to be written and then poison every read).
#[test]
fn long_negative_second_timestamp_below_duckdb_min_is_e6() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let raw = -9_223_372_036_854i64;
    let file = write_file(
        dir.path(),
        "neg.arrows",
        &ts_segment(&["n1"], vec![int_dim(vec![1]), ts_value_s(vec![raw])]),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("overflow") && stderr.contains(&raw.to_string()),
        "{stderr}"
    );
}

/// The same below-minimum value as a dense timestamp[s] coordinate.
#[test]
fn dense_negative_second_timestamp_below_duckdb_min_is_e6() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let raw = -9_223_372_036_854i64;
    let mut ts = ListBuilder::new(TimestampSecondBuilder::new());
    ts.values().append_value(raw);
    ts.append(true);
    let ts = Arc::new(ts.finish()) as ArrayRef;
    let mut vals = ListBuilder::new(Float64Builder::new());
    vals.values().append_value(1.0);
    vals.append(true);
    let vals = Arc::new(vals.finish()) as ArrayRef;
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("t", ts.data_type().clone(), false),
            Field::new("value", vals.data_type().clone(), false),
        ],
        segment_metadata("node", "price", "dense", 1),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["n1"])) as ArrayRef,
            ts,
            vals,
        ],
    )
    .unwrap();
    let file = write_file(dir.path(), "dneg.arrows", &stream(schema, &[batch]));
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert_error_leaves_db_untouched(&db, &out, "E6");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(&raw.to_string()), "{stderr}");
}

/// The bounds are exactly DuckDB's: both edges import AND read back cleanly.
#[test]
fn timestamp_range_edges_round_trip() {
    const MAX: i64 = 9_223_372_036_854_775_806;
    const MIN: i64 = -9_223_372_022_400_000_000;
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let dim = (
        Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Arc::new(TimestampMicrosecondArray::from(vec![MAX, MIN])) as ArrayRef,
    );
    let value = (
        Field::new(
            "value",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Arc::new(TimestampMicrosecondArray::from(vec![MIN, MAX])) as ArrayRef,
    );
    let file = write_file(
        dir.path(),
        "edges.arrows",
        &ts_segment(&["e1", "e2"], vec![dim, value]),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let conn = Connection::open_with_flags(
        &db,
        nodal_db_core::duckdb::Config::default()
            .access_mode(nodal_db_core::duckdb::AccessMode::ReadOnly)
            .unwrap(),
    )
    .unwrap();
    let rows: Vec<(String, i64, i64)> = conn
        .prepare(
            "SELECT entity, epoch_us(idx1_ts), epoch_us(v_datetime) \
             FROM read_parameter('node', 'when') ORDER BY entity",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    conn.close().unwrap();
    assert_eq!(
        rows,
        vec![("e1".to_string(), MAX, MIN), ("e2".to_string(), MIN, MAX)]
    );
}

/// Native µs input is not upcast, so the core's range check is what rejects
/// it: i64::MIN (used to be accepted and poison reads) and i64::MAX (DuckDB's
/// `infinity`, used to surface a raw conversion error).
#[test]
fn native_micros_timestamp_out_of_range_is_e6() {
    for bad in [i64::MIN, i64::MAX] {
        let dir = tempfile::tempdir().unwrap();
        let db = init_db(dir.path());
        let value = (
            Field::new(
                "value",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Arc::new(TimestampMicrosecondArray::from(vec![bad])) as ArrayRef,
        );
        let file = write_file(
            dir.path(),
            "us.arrows",
            &ts_segment(&["n1"], vec![int_dim(vec![1]), value]),
        );
        let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
        assert_error_leaves_db_untouched(&db, &out, "E6");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("outside the supported range"), "{stderr}");
        assert!(!stderr.contains("Conversion Error"), "{stderr}");
    }
}

// --- --db must name an existing, initialized database ----------------------

/// A missing --db is a usage error (no E-class), raised before the container
/// is read, and nothing is created at the path.
#[test]
fn import_into_missing_db_is_usage_error_and_creates_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("nope.sdb");
    let file = write_file(
        dir.path(),
        "in.arrows",
        &long_segment("node", "profile", 1, &[("n1", 0, 1.0)]),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("no such database file") && stderr.contains("nodal-db init"),
        "{stderr}"
    );
    assert!(
        !stderr.contains("E6"),
        "a usage error carries no E-class: {stderr}"
    );
    assert!(!db.exists(), "no database may be created");
    assert!(!dir.path().join("nope.sdb.wal").exists());
}

/// An existing DuckDB file that nodal-db never initialized is E6 naming the
/// cause, not a raw Catalog Error; the foreign file is left as it was.
#[test]
fn import_into_uninitialized_duckdb_is_e6_naming_init() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("foreign.duckdb");
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch("CREATE TABLE x(i INT); INSERT INTO x VALUES (7);")
            .unwrap();
        conn.close().unwrap();
    }
    let file = write_file(
        dir.path(),
        "in.arrows",
        &long_segment("node", "profile", 1, &[("n1", 0, 1.0)]),
    );
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("E6") && stderr.contains("not an initialized nodal-db database"),
        "{stderr}"
    );
    assert!(!stderr.contains("Catalog Error"), "{stderr}");
    let conn = Connection::open(&db).unwrap();
    let x: i64 = conn
        .query_row("SELECT count(*) FROM x", [], |r| r.get(0))
        .unwrap();
    let commit_tables: i64 = conn
        .query_row(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'commit'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    conn.close().unwrap();
    assert_eq!((x, commit_tables), (1, 0), "foreign file untouched");
}

/// erase-alternative (with and without --preview) on a missing path: a clean
/// message, no raw DuckDB IO error, nothing created.
#[test]
fn erase_alternative_missing_db_is_clean_and_creates_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("nope.sdb");
    for preview in [false, true] {
        let mut cmd = Command::new(bin());
        cmd.arg("erase-alternative")
            .arg(&db)
            .arg("Base")
            .args(["--user", "t"]);
        if preview {
            cmd.arg("--preview");
        }
        let out = cmd.output().expect("run erase-alternative");
        assert!(!out.status.success());
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("no such database file"), "{stderr}");
        assert!(!stderr.contains("IO Error"), "{stderr}");
        assert!(!db.exists());
    }
}

// --- value floats are stored canonical ---------------------------------------

/// (input bits, expected stored bits): -0.0 -> +0.0, every NaN -> 0x7ff8….
const FLOAT_PATTERNS: [(u64, u64); 9] = [
    (0x8000_0000_0000_0000, 0x0000_0000_0000_0000),
    (0x7ff8_0000_0000_0000, 0x7ff8_0000_0000_0000),
    (0xfff8_0000_0000_0000, 0x7ff8_0000_0000_0000),
    (0x7ff0_0000_0000_0001, 0x7ff8_0000_0000_0000),
    (0x7fff_ffff_ffff_ffff, 0x7ff8_0000_0000_0000),
    (0xfff0_0000_0000_0001, 0x7ff8_0000_0000_0000),
    (0xfff0_0000_0000_0000, 0xfff0_0000_0000_0000),
    (0x8000_0000_0000_0001, 0x8000_0000_0000_0001),
    (0x3ff8_0000_0000_0000, 0x3ff8_0000_0000_0000),
];

fn pattern_inputs() -> Vec<f64> {
    FLOAT_PATTERNS
        .iter()
        .map(|(i, _)| f64::from_bits(*i))
        .collect()
}

fn pattern_expected() -> Vec<u64> {
    FLOAT_PATTERNS.iter().map(|(_, e)| *e).collect()
}

fn stored_bits(db: &Path, sql: &str) -> Vec<u64> {
    let conn = Connection::open(db).unwrap();
    let v: Vec<u64> = conn
        .prepare(sql)
        .unwrap()
        .query_map([], |r| r.get::<_, f64>(0))
        .unwrap()
        .map(|x| x.unwrap().to_bits())
        .collect();
    conn.close().unwrap();
    v
}

/// Long form: a full-grid entity (array tier) and a sparse entity (long tier),
/// both carrying -0.0 and NaN bit patterns in the Float64 value column.
#[test]
fn long_form_float_values_are_stored_canonical() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let n = FLOAT_PATTERNS.len() as i64;
    let mut ent = Vec::new();
    let mut a = Vec::new();
    let mut b = Vec::new();
    let mut v = Vec::new();
    for (h, x) in pattern_inputs().into_iter().enumerate() {
        // "grid": (h, 0) for every h -> full grid -> array tier.
        ent.push("grid");
        a.push(h as i64);
        b.push(0i64);
        v.push(x);
        // "sparse": (h, h % 3) -> not a full grid -> long tier.
        ent.push("sparse");
        a.push(h as i64);
        b.push(h as i64 % 3);
        v.push(x);
    }
    assert_eq!(a.len() as i64, 2 * n);
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ],
        segment_metadata("node", "profile", "long", 1),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ent)) as ArrayRef,
            Arc::new(Int64Array::from(a)) as ArrayRef,
            Arc::new(Int64Array::from(b)) as ArrayRef,
            Arc::new(Float64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap();
    let file = write_file(dir.path(), "floats.arrows", &stream(schema, &[batch]));
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        stored_bits(&db, "SELECT unnest(values) FROM value_array"),
        pattern_expected(),
        "array tier"
    );
    assert_eq!(
        stored_bits(&db, "SELECT v_float FROM value_long ORDER BY i1_pos"),
        pattern_expected(),
        "long tier"
    );
}

/// Dense form (List<Float64>): the same patterns, array tier.
#[test]
fn dense_form_float_values_are_stored_canonical() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db(dir.path());
    let n = FLOAT_PATTERNS.len() as i64;
    let mut steps = ListBuilder::new(Int64Builder::new());
    steps.values().append_slice(&(0..n).collect::<Vec<_>>());
    steps.append(true);
    let steps = Arc::new(steps.finish()) as ArrayRef;
    let mut vals = ListBuilder::new(Float64Builder::new());
    vals.values().append_slice(&pattern_inputs());
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
        vec![
            Arc::new(StringArray::from(vec!["n1"])) as ArrayRef,
            steps,
            vals,
        ],
    )
    .unwrap();
    let file = write_file(dir.path(), "dfloats.arrows", &stream(schema, &[batch]));
    let out = import(&db, &file, &["--alternative", "Base", "--user", "tester"]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        stored_bits(&db, "SELECT unnest(values) FROM value_array"),
        pattern_expected()
    );
}
