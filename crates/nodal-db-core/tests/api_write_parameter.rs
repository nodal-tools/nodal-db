//! End-to-end tests for the public write path (`api::write_parameter` +
//! `api::read_parameter`, Phase 3b). Each test builds a fresh schema DB via
//! `schema_init`, writes a logical-long Arrow batch, and reads it back —
//! exercising name-resolution auto-create (decision B), the DB declared-type
//! lookup, tiering, and the history protocol as one integrated path.

use std::sync::Arc;

use duckdb::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{read_parameter, write_parameter};
use nodal_db_core::canon::LabelDomain;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{Tier, WarningKind, WriteOptions};

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

/// Two-entity rank-0 float batch (arbitrary entity spellings).
fn scalar_batch2(entities: &[&str], vals: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let ent = Arc::new(StringArray::from(entities.to_vec())) as ArrayRef;
    let val = Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef;
    RecordBatch::try_new(schema, vec![ent, val]).unwrap()
}

/// Rank-1 label-indexed float batch, optionally carrying a
/// `spinal:label_domain` JSON metadata domain on the dim field (decision D).
fn label_batch(
    entity: &str,
    dim: &str,
    metadata_json: Option<&str>,
    coords: &[&str],
    vals: &[f64],
) -> RecordBatch {
    let n = coords.len();
    let mut field = Field::new(dim, DataType::Utf8, false);
    if let Some(js) = metadata_json {
        field = field.with_metadata(std::collections::HashMap::from([(
            "spinal:label_domain".to_string(),
            js.to_string(),
        )]));
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        field,
        Field::new("value", DataType::Float64, false),
    ]));
    let ent = Arc::new(StringArray::from(vec![entity; n])) as ArrayRef;
    let d = Arc::new(StringArray::from(coords.to_vec())) as ArrayRef;
    let val = Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef;
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
    // Seed `Base` so the first write to it does not emit an AutoCreated warning
    // (decision D3: a fresh DB no longer bootstraps `Base`; tests that assert on
    // a clean first Base write seed it explicitly).
    init_schema_with_default(&db, Some("Base")).expect("schema");
    (dir, db)
}

fn count(db: &std::path::Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
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

/// NaN-as-VALUE (spec §3.3.1): a dense rank-1 float grid where ONE cell is NaN
/// stays on the **Array tier** (a NaN is a present value, not absence, so the
/// grid is still full Cartesian — it is NOT forced into the long tier), and the
/// NaN round-trips at its position with every other value exact. This is the
/// whole point of the value-half NaN reopening.
#[test]
fn dense_float_with_nan_stays_array_tier_and_round_trips() {
    let (_dir, db) = fresh_db();

    // Six coords → a full rank-1 Cartesian grid; the cell at coord 2 is NaN.
    let coords = [0i64, 1, 2, 3, 4, 5];
    let vals = [10.0, 20.0, f64::NAN, 40.0, 50.0, 60.0];
    let report = write_parameter(
        &db,
        "node",
        "profile",
        "Base",
        indexed_float_batch("node_1", "step", &coords, &vals),
        &[],
        WriteOptions::default(),
        "tester",
        "nan array write",
    )
    .expect("write");

    // (a) tier == Array, NOT Long: the NaN cell is present, so the grid is
    // still a complete Cartesian float grid.
    assert_eq!(
        report.tiers,
        vec![Tier::Array],
        "a NaN-bearing dense float grid must stay on the array tier"
    );
    assert_eq!(report.values_written, 1);

    // (b) read-back preserves the NaN at coord 2 and every other value exactly.
    let got = read_parameter(&db, "node", "profile", "Base").expect("read");
    assert_eq!(got.num_rows(), 6);
    for t in col_str(&got, "tier") {
        assert_eq!(t, Some("array".into()));
    }
    let coords_back = col_i64(&got, "idx1_num");
    let vals_back = col_f64(&got, "v_float");
    let by_coord: std::collections::HashMap<i64, f64> = coords_back
        .into_iter()
        .zip(vals_back)
        .map(|(c, v)| (c.unwrap(), v.unwrap()))
        .collect();
    assert_eq!(by_coord.len(), 6);
    assert_eq!(by_coord[&0], 10.0);
    assert_eq!(by_coord[&1], 20.0);
    assert!(by_coord[&2].is_nan(), "the NaN survives the round-trip at coord 2");
    assert_eq!(by_coord[&3], 40.0);
    assert_eq!(by_coord[&4], 50.0);
    assert_eq!(by_coord[&5], 60.0);
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

// --- api critique fixes (F1-F11) -------------------------------------------

/// F1: two same-process writers on one DB file must serialize, not silently
/// diverge — two distinct commits, both rows present, correct pairing.
#[test]
fn concurrent_writers_serialize_in_process() {
    let (_dir, db) = fresh_db();
    let spawn = |db: std::path::PathBuf, ent: &'static str, v: f64, msg: &'static str| {
        std::thread::spawn(move || {
            write_parameter(
                &db, "racecls", "p", "Base", scalar_batch(ent, v), &[],
                WriteOptions::default(), "t", msg,
            )
        })
    };
    let t1 = spawn(db.clone(), "e1", 1.0, "w1");
    let t2 = spawn(db.clone(), "e2", 2.0, "w2");
    let r1 = t1.join().unwrap().expect("writer 1");
    let r2 = t2.join().unwrap().expect("writer 2");
    assert_ne!(r1.commit_id, r2.commit_id, "each writer gets its own commit");

    let got = read_parameter(&db, "racecls", "p", "Base").expect("read");
    let mut pairs: Vec<(String, f64)> = col_str(&got, "entity")
        .into_iter()
        .zip(col_f64(&got, "v_float"))
        .map(|(e, v)| (e.unwrap(), v.unwrap()))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(pairs, vec![("e1".to_string(), 1.0), ("e2".to_string(), 2.0)]);
}

/// F2: a batch whose measure kind contradicts the stored declaration is loud.
#[test]
fn declared_value_kind_mismatch_is_rejected() {
    let (_dir, db) = fresh_db();
    let o = WriteOptions::default();
    write_parameter(&db, "node", "p", "Base", scalar_batch("n1", 7.0), &[], o, "t", "float")
        .expect("float declare");
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let strs = RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec!["n2"])) as ArrayRef,
        Arc::new(StringArray::from(vec!["oops"])) as ArrayRef,
    ])
    .unwrap();
    let err = write_parameter(&db, "node", "p", "Base", strs, &[], o, "t", "drift")
        .expect_err("kind drift must be loud");
    let msg = err.to_string();
    assert!(
        msg.contains("\"p\"") && msg.contains("float") && msg.contains("string"),
        "unexpected: {msg}"
    );
}

/// F3: case-aliased entity names in one batch are ambiguous (DB resolution is
/// case-insensitive) — an Api error, before any DB work.
#[test]
fn case_aliased_entity_names_in_one_batch_are_rejected() {
    let (_dir, db) = fresh_db();
    let err = write_parameter(
        &db, "node", "p", "Base", scalar_batch2(&["N1", "n1"], &[1.0, 2.0]), &[],
        WriteOptions::default(), "t", "m",
    )
    .expect_err("case alias must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("N1") && msg.contains("n1"), "unexpected: {msg}");
    assert_eq!(count(&db, "SELECT count(*) FROM entity_class"), 0, "no DB work happened");
}

/// F4: dim-column names must match the declared dimension names in declared
/// order — a rename/transposition is loud, naming both sides.
#[test]
fn dimension_name_mismatch_is_rejected() {
    let (_dir, db) = fresh_db();
    let o = WriteOptions::default();
    write_parameter(
        &db, "node", "flow", "Base",
        indexed_float_batch("e1", "region", &[0, 1], &[1.0, 2.0]), &[], o, "t", "declare",
    )
    .expect("declare with dim 'region'");
    let err = write_parameter(
        &db, "node", "flow", "Base",
        indexed_float_batch("e2", "season", &[0, 1], &[1.0, 2.0]), &[], o, "t", "rename",
    )
    .expect_err("dim name mismatch must be loud");
    let msg = err.to_string();
    assert!(msg.contains("region") && msg.contains("season"), "unexpected: {msg}");
}

/// F5: blank referent names (class/parameter/alternative/entity) fail as Api
/// errors before any DB work — never as raw CHECK-constraint errors.
#[test]
fn blank_referent_names_are_rejected_before_db_work() {
    let (_dir, db) = fresh_db();
    let o = WriteOptions::default();
    for (c, p, a, what) in [
        ("  ", "p", "Base", "class"),
        ("node", "", "Base", "parameter"),
        ("node", "p", " ", "alternative"),
    ] {
        let err = write_parameter(&db, c, p, a, scalar_batch("n", 1.0), &[], o, "t", "m")
            .expect_err("blank name must be rejected");
        assert!(err.to_string().contains(what), "missing {what:?} in: {err}");
    }
    let err = write_parameter(&db, "node", "p", "Base", scalar_batch(" ", 1.0), &[], o, "t", "m")
        .expect_err("blank entity name must be rejected");
    assert!(err.to_string().contains("entity"), "unexpected: {err}");
    assert_eq!(count(&db, "SELECT count(*) FROM entity_class"), 0, "no DB work happened");
    assert_eq!(count(&db, "SELECT count(*) FROM commit"), 1, "bootstrap commit only");
}

/// F6: a tz-bearing Timestamp column is accepted (it is a well-defined instant)
/// but surfaces a machine-readable TimezoneNormalized warning.
#[test]
fn tz_bearing_timestamp_value_warns_timezone_normalized() {
    let (_dir, db) = fresh_db();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Timestamp(TimeUnit::Microsecond, Some("+05:00".into())), false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec!["n1"])) as ArrayRef,
        Arc::new(
            TimestampMicrosecondArray::from(vec![1_700_000_000_000_000i64])
                .with_timezone("+05:00"),
        ) as ArrayRef,
    ])
    .unwrap();
    let report = write_parameter(
        &db, "node", "ts", "Base", batch, &[], WriteOptions::default(), "t", "m",
    )
    .expect("tz-aware instants are accepted");
    assert_eq!(report.values_written, 1);
    let expected = WarningKind::TimezoneNormalized {
        column: "value".into(),
        tz: "+05:00".into(),
    };
    assert!(
        report.warnings.iter().any(|w| w.kind == expected),
        "expected a TimezoneNormalized warning, got: {:?}",
        report.warnings
    );
}

/// F7: materializing a missing parameter_type for an EXISTING definition is an
/// auto-create and must warn (decision B: never silent).
#[test]
fn materializing_parameter_type_for_existing_definition_warns() {
    let (_dir, db) = fresh_db();
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
            "INSERT INTO parameter_definition (entity_class_id, name, valid_from_commit) VALUES (?, 'p', 1)",
            duckdb::params![cls],
        )
        .unwrap();
        conn.close().unwrap();
    }
    let report = write_parameter(
        &db, "unit", "p", "Base", scalar_batch("u1", 1.0), &[], WriteOptions::default(), "t", "m",
    )
    .expect("write");
    let w = report
        .warnings
        .iter()
        .find(|w| w.object_type == "parameter_type")
        .expect("materialized parameter_type must warn");
    assert_eq!(w.kind, WarningKind::AutoCreated);
    assert_eq!(w.name, "p");
    assert!(w.id > 0);
    assert_eq!(count(&db, "SELECT count(*) FROM parameter_type"), 1);
    // D11: the auto-materialize path binds the freshly created member row.
    assert_eq!(
        count(&db, "SELECT parameter_type_id FROM value_scalar WHERE valid_to_commit IS NULL"),
        w.id,
        "the written value binds the materialized parameter_type"
    );
}

/// F8: a zero-row batch is refused with no side effects — no auto-creates, no
/// commit row.
#[test]
fn empty_batch_is_rejected_without_side_effects() {
    let (_dir, db) = fresh_db();
    let err = write_parameter(
        &db, "node", "p", "Base", scalar_batch2(&[], &[]), &[],
        WriteOptions::default(), "t", "m",
    )
    .expect_err("empty batch must be rejected");
    assert!(err.to_string().contains("empty batch"), "unexpected: {err}");
    assert_eq!(count(&db, "SELECT count(*) FROM entity_class"), 0, "no auto-creates");
    assert_eq!(count(&db, "SELECT count(*) FROM commit"), 1, "bootstrap commit only");
}

/// F9 (updated for D2(a) set semantics): schema-permitted declaration damage is
/// refused, never read nondeterministically. Multiple current parameter_type
/// rows are now the LEGAL declared-set state — what is refused is a set whose
/// members are indistinguishable (here: two identical `(float, 0)` rows, so
/// exactly-one-member matching is impossible) and non-contiguous dimension
/// positions.
#[test]
fn indistinguishable_or_noncontiguous_declarations_are_refused() {
    let (_dir, db) = fresh_db();
    {
        let conn = Connection::open(&db).unwrap();
        let cls: i64 = conn
            .query_row(
                "INSERT INTO entity_class (name, valid_from_commit) VALUES ('node', 1) RETURNING id",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let pd: i64 = conn
            .query_row(
                "INSERT INTO parameter_definition (entity_class_id, name, valid_from_commit) \
                 VALUES (?, 'dup', 1) RETURNING id",
                duckdb::params![cls],
                |r| r.get(0),
            )
            .unwrap();
        for _ in 0..2 {
            conn.execute(
                "INSERT INTO parameter_type (parameter_definition_id, value_kind, rank, valid_from_commit) \
                 VALUES (?, 'float', 0, 1)",
                duckdb::params![pd],
            )
            .unwrap();
        }
        let pd2: i64 = conn
            .query_row(
                "INSERT INTO parameter_definition (entity_class_id, name, valid_from_commit) \
                 VALUES (?, 'gap', 1) RETURNING id",
                duckdb::params![cls],
                |r| r.get(0),
            )
            .unwrap();
        let pt2: i64 = conn
            .query_row(
                "INSERT INTO parameter_type (parameter_definition_id, value_kind, rank, valid_from_commit) \
                 VALUES (?, 'indexed', 1, 1) RETURNING id",
                duckdb::params![pd2],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute(
            "INSERT INTO parameter_type_dimension \
               (parameter_type_id, position, dimension_name, kind, valid_from_commit) \
             VALUES (?, 2, 'step', 'integer', 1)",
            duckdb::params![pt2],
        )
        .unwrap();
        conn.close().unwrap();
    }
    let o = WriteOptions::default();
    let err = write_parameter(&db, "node", "dup", "Base", scalar_batch("n", 1.0), &[], o, "t", "m")
        .expect_err("indistinguishable declaration must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("indistinguishable") && msg.contains("parameter_type rows"),
        "unexpected: {msg}"
    );
    let err = write_parameter(
        &db, "node", "gap", "Base",
        indexed_float_batch("n", "step", &[0, 1], &[1.0, 2.0]), &[], o, "t", "m",
    )
    .expect_err("non-contiguous positions must be refused");
    assert!(err.to_string().contains("contiguous"), "unexpected: {err}");
}

/// F10: auto-creating an entity in an N-D class is refused — its identity is a
/// member tuple the batch does not carry.
#[test]
fn auto_create_entity_in_nd_class_is_refused() {
    let (_dir, db) = fresh_db();
    {
        let conn = Connection::open(&db).unwrap();
        let node: i64 = conn
            .query_row(
                "INSERT INTO entity_class (name, valid_from_commit) VALUES ('node', 1) RETURNING id",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let flow: i64 = conn
            .query_row(
                "INSERT INTO entity_class (name, valid_from_commit) VALUES ('flow', 1) RETURNING id",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute(
            "INSERT INTO entity_class_dimension \
               (entity_class_id, position, dimension_class_id, valid_from_commit) \
             VALUES (?, 1, ?, 1)",
            duckdb::params![flow, node],
        )
        .unwrap();
        conn.close().unwrap();
    }
    let err = write_parameter(
        &db, "flow", "p", "Base", scalar_batch("f1", 1.0), &[], WriteOptions::default(), "t", "m",
    )
    .expect_err("N-D auto-create must be refused");
    let msg = err.to_string();
    assert!(msg.contains("auto-create") && msg.contains("f1"), "unexpected: {msg}");
}

/// F11: a rank mismatch against the stored declaration names the parameter and
/// counts ranks correctly (no canon internals, no off-by-one).
#[test]
fn rank_mismatch_names_the_parameter_and_counts_ranks() {
    let (_dir, db) = fresh_db();
    let o = WriteOptions::default();
    write_parameter(
        &db, "node", "profile", "Base",
        indexed_float_batch("e", "step", &[0, 1], &[1.0, 2.0]), &[], o, "t", "declare",
    )
    .expect("rank-1 declare");
    let err = write_parameter(&db, "node", "profile", "Base", scalar_batch("e", 1.0), &[], o, "t", "m")
        .expect_err("rank mismatch must be loud");
    let msg = err.to_string();
    assert!(
        msg.contains("\"profile\"") && msg.contains("rank 0") && msg.contains("rank 1"),
        "unexpected: {msg}"
    );
}

// --- declared allowed-type sets (D2(a), template_ir_plan §3.3) -------------

/// Rank-1 datetime-indexed float batch (naive µs timestamps).
fn datetime_indexed_batch(entity: &str, dim: &str, coords: &[i64], vals: &[f64]) -> RecordBatch {
    let n = coords.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new(dim, DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("value", DataType::Float64, false),
    ]));
    let ent = Arc::new(StringArray::from(vec![entity; n])) as ArrayRef;
    let d = Arc::new(TimestampMicrosecondArray::from(coords.to_vec())) as ArrayRef;
    let val = Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef;
    RecordBatch::try_new(schema, vec![ent, d, val]).unwrap()
}

/// Declare `class.param` with an explicit allowed-type SET via SQL against the
/// bootstrap commit — the template loader's eventual job. Each member is
/// `(value_kind, dims)`; rank derives as `dims.len()` (rank = the type's own
/// dimensionality, duplicates across members legal). Returns the created
/// `parameter_type` ids in declaration order (the D11 bindings values persist).
fn declare_definition(
    db: &std::path::Path,
    class: &str,
    param: &str,
    members: &[(&str, &[(&str, &str)])],
) -> Vec<i64> {
    let conn = Connection::open(db).unwrap();
    let cls: i64 = conn
        .query_row(
            "INSERT INTO entity_class (name, valid_from_commit) VALUES (?, 1) RETURNING id",
            duckdb::params![class],
            |r| r.get(0),
        )
        .unwrap();
    let pd: i64 = conn
        .query_row(
            "INSERT INTO parameter_definition (entity_class_id, name, valid_from_commit) \
             VALUES (?, ?, 1) RETURNING id",
            duckdb::params![cls, param],
            |r| r.get(0),
        )
        .unwrap();
    let mut pt_ids = Vec::with_capacity(members.len());
    for (kind, dims) in members {
        let pt: i64 = conn
            .query_row(
                "INSERT INTO parameter_type (parameter_definition_id, value_kind, rank, valid_from_commit) \
                 VALUES (?, ?, ?, 1) RETURNING id",
                duckdb::params![pd, kind, dims.len() as i8],
                |r| r.get(0),
            )
            .unwrap();
        for (pos, (dname, dkind)) in dims.iter().enumerate() {
            conn.execute(
                "INSERT INTO parameter_type_dimension \
                   (parameter_type_id, position, dimension_name, kind, valid_from_commit) \
                 VALUES (?, ?, ?, ?, 1)",
                duckdb::params![pt, (pos + 1) as i8, dname, dkind],
            )
            .unwrap();
        }
        pt_ids.push(pt);
    }
    conn.close().unwrap();
    pt_ids
}

/// D2(a) acceptance: a definition declaring the set {scalar float, 1-D
/// integer-indexed map} accepts a value of EACH member type — the batch binds
/// to the member it instantiates, the tier rule applies per value unchanged —
/// and the set is never auto-materialized over (count stays 2).
#[test]
fn multi_type_set_accepts_each_declared_member() {
    let (_dir, db) = fresh_db();
    let pts = declare_definition(&db, "unit", "availability", &[
        ("float", &[]),
        ("indexed", &[("step", "integer")]),
    ]);
    let o = WriteOptions::default();

    // Member 1: scalar float.
    let r1 = write_parameter(
        &db, "unit", "availability", "Base", scalar_batch("u1", 0.95), &[], o, "t", "scalar member",
    )
    .expect("scalar member write");
    assert_eq!(r1.tiers, vec![Tier::Scalar]);
    assert!(
        !r1.warnings.iter().any(|w| w.object_type == "parameter_type"),
        "no parameter_type materialization against a declared set: {:?}",
        r1.warnings
    );

    // Member 2: 1-D integer-indexed map (another alternative — the union is
    // per value; a full grid of floats lands on the array tier as ever).
    let r2 = write_parameter(
        &db, "unit", "availability", "hourly",
        indexed_float_batch("u1", "step", &[0, 1, 2], &[0.9, 0.8, 0.7]), &[], o, "t",
        "indexed member",
    )
    .expect("indexed member write");
    assert_eq!(r2.tiers, vec![Tier::Array], "tier rule unchanged per value");

    // Both read back at their member shapes.
    let got = read_parameter(&db, "unit", "availability", "Base").expect("read scalar");
    assert_eq!(col_f64(&got, "v_float"), vec![Some(0.95)]);
    let got = read_parameter(&db, "unit", "availability", "hourly").expect("read indexed");
    assert_eq!(got.num_rows(), 3);

    // The declared set is untouched: still exactly the two declared rows.
    assert_eq!(
        count(&db, "SELECT count(*) FROM parameter_type WHERE valid_to_commit IS NULL"),
        2
    );

    // D11: each value row persists the id of the member it instantiated.
    assert_eq!(
        count(&db, "SELECT parameter_type_id FROM value_scalar WHERE valid_to_commit IS NULL"),
        pts[0],
        "the scalar value binds the scalar-float member"
    );
    assert_eq!(
        count(&db, "SELECT parameter_type_id FROM value_array"),
        pts[1],
        "the indexed value binds the 1-D map member"
    );
}

/// D2(a) rejection: a batch instantiating NO declared member is refused with an
/// error naming the allowed set, and the writer never widens the set (no new
/// parameter_type rows appear).
#[test]
fn multi_type_set_rejects_nonmember_and_never_widens() {
    let (_dir, db) = fresh_db();
    declare_definition(&db, "unit", "availability", &[
        ("float", &[]),
        ("indexed", &[("step", "integer")]),
    ]);
    let o = WriteOptions::default();

    // Non-member measure kind (string scalar).
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let strs = RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec!["u1"])) as ArrayRef,
        Arc::new(StringArray::from(vec!["high"])) as ArrayRef,
    ])
    .unwrap();
    let err = write_parameter(&db, "unit", "availability", "Base", strs, &[], o, "t", "m")
        .expect_err("non-member kind must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("string") && msg.contains("float rank 0") && msg.contains("step"),
        "the error must name the batch kind and the full allowed set: {msg}"
    );

    // Non-member dimension name at a declared rank.
    let err = write_parameter(
        &db, "unit", "availability", "Base",
        indexed_float_batch("u1", "season", &[0, 1], &[1.0, 2.0]), &[], o, "t", "m",
    )
    .expect_err("non-member dim name must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("season") && msg.contains("step"), "unexpected: {msg}");

    // Non-member rank (rank 2 vs declared {0, 1}).
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("step", DataType::Int64, false),
        Field::new("layer", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let rank2 = RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec!["u1"])) as ArrayRef,
        Arc::new(Int64Array::from(vec![0])) as ArrayRef,
        Arc::new(Int64Array::from(vec![0])) as ArrayRef,
        Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
    ])
    .unwrap();
    let err = write_parameter(&db, "unit", "availability", "Base", rank2, &[], o, "t", "m")
        .expect_err("non-member rank must be rejected");
    assert!(err.to_string().contains("rank 2"), "unexpected: {err}");

    // No widening happened on any of the rejections.
    assert_eq!(
        count(&db, "SELECT count(*) FROM parameter_type"),
        2,
        "rejected batches must never widen the declared set"
    );
}

/// D2(a) member matching by index KIND at equal rank (duplicate ranks are
/// legal): {1-D datetime map, 1-D integer map} — an Int64 column binds the
/// integer member, a Timestamp column the datetime member, deterministically.
#[test]
fn multi_type_same_rank_members_distinguished_by_index_kind() {
    let (_dir, db) = fresh_db();
    let pts = declare_definition(&db, "node", "profile", &[
        ("indexed", &[("t", "datetime")]),
        ("indexed", &[("t", "integer")]),
    ]);
    let o = WriteOptions::default();

    let r1 = write_parameter(
        &db, "node", "profile", "Base",
        indexed_float_batch("n1", "t", &[0, 1], &[1.0, 2.0]), &[], o, "t", "integer member",
    )
    .expect("integer-indexed write");
    assert_eq!(r1.tiers, vec![Tier::Array]);

    let hour = 3_600_000_000i64;
    let r2 = write_parameter(
        &db, "node", "profile", "ts",
        datetime_indexed_batch("n1", "t", &[0, hour], &[3.0, 4.0]), &[], o, "t", "datetime member",
    )
    .expect("datetime-indexed write");
    assert_eq!(r2.tiers, vec![Tier::Array]);

    // Both current, one per alternative; the declared set is untouched.
    assert_eq!(count(&db, "SELECT count(*) FROM value_array"), 2);
    assert_eq!(count(&db, "SELECT count(*) FROM parameter_type"), 2);

    // D11: each row binds the member its index KIND matched.
    assert_eq!(
        count(
            &db,
            "SELECT va.parameter_type_id FROM value_array va \
             JOIN alternative a ON a.id = va.alternative_id WHERE a.cur_name = 'base'",
        ),
        pts[1],
        "the Int64-indexed row binds the integer member"
    );
    assert_eq!(
        count(
            &db,
            "SELECT va.parameter_type_id FROM value_array va \
             JOIN alternative a ON a.id = va.alternative_id WHERE a.cur_name = 'ts'",
        ),
        pts[0],
        "the Timestamp-indexed row binds the datetime member"
    );
}

/// D2(a)/D11 read contract, pinned (080_views_read.sql): two same-rank
/// same-index-kind members differing only in dimension NAME ([t: integer] vs
/// [step: integer]) are legal (distinguishable at entry by name) and both
/// writable. Every writer-produced row persists the member it bound to
/// (D11 `parameter_type_id`), so member attribution IS exact on the tables —
/// but the read macro's index columns remain positional (idxN_*), never
/// dimension-named, and the macro does not (yet) expose the binding: a client
/// wanting per-row dimension names resolves them via the persisted binding,
/// not from the macro output. The blanket rename by definition alone stays
/// well-defined only when all current members agree on the name vector per
/// (rank, kind) signature; NULL-binding rows (pre-D11 / out-of-contract) are
/// unattributable. This test documents that contract.
#[test]
fn same_kind_members_differing_only_in_dim_name_write_but_blur_on_read() {
    let (_dir, db) = fresh_db();
    let pts = declare_definition(&db, "node", "usage", &[
        ("indexed", &[("t", "integer")]),
        ("indexed", &[("step", "integer")]),
    ]);
    let o = WriteOptions::default();

    // Both members writable, each bound by its dimension name at entry.
    write_parameter(
        &db, "node", "usage", "Base",
        indexed_float_batch("n1", "t", &[0, 1], &[1.0, 2.0]), &[], o, "t", "t member",
    )
    .expect("t-member write");
    write_parameter(
        &db, "node", "usage", "alt2",
        indexed_float_batch("n1", "step", &[10, 20], &[3.0, 4.0]), &[], o, "t", "step member",
    )
    .expect("step-member write");
    assert_eq!(count(&db, "SELECT count(*) FROM value_array"), 2, "both rows land");

    // CAN distinguish: by alternative and axis content, on the positional
    // integer index column.
    let base = read_parameter(&db, "node", "usage", "Base").expect("read Base");
    let mut c: Vec<i64> = col_i64(&base, "idx1_num").into_iter().flatten().collect();
    c.sort_unstable();
    assert_eq!(c, vec![0, 1]);
    let alt2 = read_parameter(&db, "node", "usage", "alt2").expect("read alt2");
    let mut c: Vec<i64> = col_i64(&alt2, "idx1_num").into_iter().flatten().collect();
    c.sort_unstable();
    assert_eq!(c, vec![10, 20]);

    // The read macro exposes no dimension-named column and no binding column —
    // per-row member attribution goes through the persisted binding instead.
    assert!(base.schema().index_of("t").is_err(), "index columns are positional, not named");
    assert!(alt2.schema().index_of("step").is_err(), "index columns are positional, not named");

    // D11: the tables carry the exact member binding for each row.
    assert_eq!(
        count(
            &db,
            "SELECT va.parameter_type_id FROM value_array va \
             JOIN alternative a ON a.id = va.alternative_id WHERE a.cur_name = 'base'",
        ),
        pts[0],
        "the row written at dims [t] binds the t member"
    );
    assert_eq!(
        count(
            &db,
            "SELECT va.parameter_type_id FROM value_array va \
             JOIN alternative a ON a.id = va.alternative_id WHERE a.cur_name = 'alt2'",
        ),
        pts[1],
        "the row written at dims [step] binds the step member"
    );
}

// --- decision-D metadata channel (promoted critique probes) ----------------

/// Metadata channel happy path: the `spinal:label_domain` field metadata alone
/// supplies the ordered domain; present labels take their domain ordinals.
#[test]
fn metadata_label_domain_sets_ordinals() {
    let (_dir, db) = fresh_db();
    let batch = label_batch(
        "e1", "level", Some(r#"["low","mid","high"]"#), &["low", "high"], &[1.0, 2.0],
    );
    write_parameter(&db, "node", "lvl", "Base", batch, &[], WriteOptions::default(), "t", "m")
        .expect("metadata-channel write");
    let conn = Connection::open(&db).unwrap();
    let rows: Vec<(i32, String, i32)> = conn
        .prepare("SELECT pos, label, ordinal FROM axis_label ORDER BY pos")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    conn.close().unwrap();
    assert_eq!(
        rows,
        vec![(0, "low".to_string(), 0), (1, "high".to_string(), 2)],
        "ordinals follow the metadata domain order, positions dense"
    );
}

/// Malformed `spinal:label_domain` JSON fails loudly BEFORE any DB work.
#[test]
fn malformed_label_domain_metadata_fails_before_db_work() {
    let (_dir, db) = fresh_db();
    let batch = label_batch("e1", "level", Some(r#"["low", oops"#), &["low"], &[1.0]);
    let err = write_parameter(&db, "node", "lvl", "Base", batch, &[], WriteOptions::default(), "t", "m")
        .expect_err("malformed metadata must be loud");
    assert!(err.to_string().contains("spinal:label_domain"), "unexpected: {err}");
    assert_eq!(count(&db, "SELECT count(*) FROM entity_class"), 0, "DB untouched");
    assert_eq!(count(&db, "SELECT count(*) FROM commit"), 1, "bootstrap commit only");
}

/// Precedence: the in-process `domains` arg wins over conflicting metadata
/// (decision D — arg primary).
#[test]
fn domains_arg_wins_over_conflicting_metadata() {
    let (_dir, db) = fresh_db();
    let batch = label_batch(
        "e1", "level", Some(r#"["low","mid","high"]"#), &["low", "high"], &[1.0, 2.0],
    );
    let arg = [LabelDomain {
        position: 1,
        labels: vec!["high".into(), "low".into()],
    }];
    write_parameter(&db, "node", "lvl", "Base", batch, &arg, WriteOptions::default(), "t", "m")
        .expect("arg-channel write");
    let conn = Connection::open(&db).unwrap();
    let rows: Vec<(i32, String, i32)> = conn
        .prepare("SELECT pos, label, ordinal FROM axis_label ORDER BY pos")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    conn.close().unwrap();
    assert_eq!(
        rows,
        vec![(0, "high".to_string(), 0), (1, "low".to_string(), 1)],
        "ordinals follow the arg's order, not the metadata's"
    );
}
