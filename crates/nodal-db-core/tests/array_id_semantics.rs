//! Functional tests for the `array_id` surrogate key on `value_array`:
//! uniqueness of the key triple, id semantics across tier transitions, the
//! as-of macro's columns, the read surface's unchanged column sets, and row
//! parity of the single-branch `value_array_cells` against the old two-branch
//! form. (The plan-shape guard lives in `array_id_index_scan.rs`; replace /
//! delete id stability in `api_write_parameter.rs` / `api_delete_values.rs`.)

use std::path::Path;
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::write_parameter;
use nodal_db_core::axis::AxisCache;
use nodal_db_core::canon::CanonicalValue;
use nodal_db_core::conn::with_write_txn;
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis};
use nodal_db_core::history::write_value;
use nodal_db_core::schema_init::{init_schema, init_schema_with_default};
use nodal_db_core::types::{ScalarValue, ShapedData, Tier, ValueKey, ValueKind, WriteOptions};

fn indexed_float_batch(entity: &str, coords: &[i64], vals: &[f64]) -> RecordBatch {
    let n = coords.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("step", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; n])) as ArrayRef,
            Arc::new(Int64Array::from(coords.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn write_array(db: &Path, entity: &str, vals: &[f64], msg: &str) -> i64 {
    let coords: Vec<i64> = (0..vals.len() as i64).collect();
    write_parameter(
        db,
        "node",
        "profile",
        "Base",
        indexed_float_batch(entity, &coords, vals),
        &[],
        WriteOptions::default(),
        "tester",
        msg,
    )
    .expect("write")
    .commit_id
}

fn api_db(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("sem.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    db
}

fn q_i64(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn column_names(conn: &Connection, sql: &str) -> Vec<String> {
    conn.prepare(&format!("DESCRIBE {sql}"))
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn duplicate_live_triple_is_rejected_by_unique() {
    let dir = tempfile::tempdir().unwrap();
    let db = api_db(dir.path());
    write_array(&db, "n1", &[1.0, 2.0], "v1");
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .execute(
            "INSERT INTO value_array (entity_id, parameter_definition_id, alternative_id, \
               valid_from_commit, version, axis1_id, values) \
             SELECT entity_id, parameter_definition_id, alternative_id, valid_from_commit, \
               version, axis1_id, values FROM value_array",
            [],
        )
        .expect_err("a second live row for one triple must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("constraint"),
        "unexpected error: {err}"
    );
    conn.close().unwrap();
}

// --- tier transitions through history::write_value --------------------------

fn int_axis(length: i32) -> CanonicalAxis {
    CanonicalAxis {
        length,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerParametric { start: 0, step: 1 },
    }
}

fn array_value(vals: &[f64]) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Array,
        value_kind: ValueKind::Float,
        axes: vec![int_axis(vals.len() as i32)],
        shape: ShapedData::Array(vals.to_vec()),
    }
}

fn scalar_value(v: f64) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Scalar,
        value_kind: ValueKind::Float,
        axes: vec![],
        shape: ShapedData::Scalar(ScalarValue::Float(v)),
    }
}

const KEY: ValueKey = ValueKey {
    entity_id: 7,
    parameter_definition_id: 11,
    alternative_id: 13,
};

fn write_steps(db: &Path, steps: &[CanonicalValue]) {
    with_write_txn(db, "tester", "steps", |conn, c| {
        let mut cache = AxisCache::new();
        for v in steps {
            write_value(conn, c, KEY, v, &mut cache, &WriteOptions::default(), None)?;
        }
        Ok(())
    })
    .expect("write steps");
}

/// array → scalar → array: the re-inserted array row gets a NEW id, the first
/// array row's pre-image keeps the old one, array versions continue per triple
/// across both ids, and every audit stays clean.
fn assert_transition_state(db: &Path, ctx: &str) {
    let conn = Connection::open(db).unwrap();
    let live: (i64, i16) = conn
        .query_row("SELECT array_id, version FROM value_array", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    let hist: (i64, i16) = conn
        .query_row("SELECT array_id, version FROM value_array_hist", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_ne!(
        live.0, hist.0,
        "{ctx}: re-insert after a transition mints a new id"
    );
    assert_eq!(live.0 % 2, 1, "{ctx}: odd id");
    assert_eq!(hist.0 % 2, 1, "{ctx}: odd id");
    assert_eq!(
        (hist.1, live.1),
        (1, 2),
        "{ctx}: versions continue per triple"
    );
    for audit in [
        "audit_array_id_coherence",
        "audit_history_tiling",
        "audit_cross_tier_duplicate",
    ] {
        assert_eq!(
            q_i64(&conn, &format!("SELECT count(*) FROM {audit}")),
            0,
            "{ctx}: {audit}"
        );
    }
    conn.close().unwrap();
}

/// array → scalar → array, one commit per step. (Writing one key twice in a
/// single commit is refused by the span-coherence CHECK — a zero-length
/// history span — for every tier, so the transitions need separate commits.)
#[test]
fn tier_transition_array_scalar_array_mints_a_new_id() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    write_steps(&db, &[array_value(&[1.0, 2.0, 3.0])]);
    write_steps(&db, &[scalar_value(5.0)]);
    write_steps(&db, &[array_value(&[4.0, 5.0, 6.0])]);
    assert_transition_state(&db, "array -> scalar -> array");
}

/// After a tier change re-mints the id, a later in-place replace keeps the NEW
/// id (the UPDATE never touches array_id) and copies it into `_hist`.
#[test]
fn replace_after_reinsert_keeps_the_new_id() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("t2.duckdb");
    init_schema(&db).unwrap();
    write_steps(&db, &[array_value(&[1.0, 2.0, 3.0])]);
    write_steps(&db, &[scalar_value(5.0)]);
    write_steps(&db, &[array_value(&[4.0, 5.0, 6.0])]);
    let conn = Connection::open(&db).unwrap();
    let before = q_i64(&conn, "SELECT array_id FROM value_array");
    conn.close().unwrap();
    write_steps(&db, &[array_value(&[7.0, 8.0, 9.0])]);
    let conn = Connection::open(&db).unwrap();
    assert_eq!(q_i64(&conn, "SELECT array_id FROM value_array"), before);
    assert_eq!(q_i64(&conn, "SELECT version FROM value_array"), 3);
    assert_eq!(
        q_i64(
            &conn,
            &format!("SELECT count(*) FROM value_array_hist WHERE array_id = {before}")
        ),
        1,
        "the replaced version's pre-image carries the re-minted id"
    );
    conn.close().unwrap();
}

// --- as-of and the read surface ----------------------------------------------

/// `value_array_asof` projects `array_id` and `parameter_type_id` from BOTH
/// legs (the live leg and the `_hist` leg).
#[test]
fn value_array_asof_carries_array_id_and_parameter_type_from_both_legs() {
    let dir = tempfile::tempdir().unwrap();
    let db = api_db(dir.path());
    let c1 = write_array(&db, "n1", &[1.0, 2.0], "v1");
    let c2 = write_array(&db, "n1", &[3.0, 4.0], "v2");
    let conn = Connection::open(&db).unwrap();
    let (live_id, pt): (i64, Option<i64>) = conn
        .query_row(
            "SELECT array_id, parameter_type_id FROM value_array",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert!(pt.is_some(), "the API binds a parameter_type member");
    for (c, leg) in [(c1, "hist"), (c2, "live")] {
        let got: (i64, Option<i64>, i16) = conn
            .query_row(
                &format!("SELECT array_id, parameter_type_id, version FROM value_array_asof({c})"),
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        let want_version = if leg == "hist" { 1 } else { 2 };
        assert_eq!(got, (live_id, pt, want_version), "{leg} leg");
    }
    conn.close().unwrap();
}

/// The logical read surface does not expose `array_id`: its column sets are
/// exactly what they were before the surrogate key (snapshot).
#[test]
fn read_surface_column_sets_are_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let db = api_db(dir.path());
    write_array(&db, "n1", &[1.0, 2.0], "v1");
    let conn = Connection::open(&db).unwrap();
    let cases: [(&str, &[&str]); 4] = [
        (
            "SELECT * FROM read_parameter_dense('node', 'profile')",
            &[
                "entity_id",
                "alternative_id",
                "axis1_id",
                "axis2_id",
                "axis3_id",
                "axis4_id",
                "values",
                "values_int",
                "v_min",
                "v_max",
            ],
        ),
        (
            "value_array_cells",
            &[
                "entity_id",
                "parameter_definition_id",
                "alternative_id",
                "axis1_id",
                "axis2_id",
                "axis3_id",
                "axis4_id",
                "i1_pos",
                "i2_pos",
                "i3_pos",
                "i4_pos",
                "value_kind",
                "v_float",
                "v_int",
            ],
        ),
        (
            "value_cells",
            &[
                "entity_id",
                "parameter_definition_id",
                "alternative_id",
                "tier",
                "value_kind",
                "axis1_id",
                "axis2_id",
                "axis3_id",
                "axis4_id",
                "i1_pos",
                "i2_pos",
                "i3_pos",
                "i4_pos",
                "v_float",
                "v_int",
                "v_bool",
                "v_str",
                "v_datetime",
                "v_dur_months",
                "v_dur_micros",
            ],
        ),
        (
            "SELECT * FROM read_parameter('node', 'profile')",
            &[
                "entity",
                "alternative",
                "tier",
                "value_kind",
                "idx1_text",
                "idx2_text",
                "idx3_text",
                "idx4_text",
                "idx1_ts",
                "idx2_ts",
                "idx3_ts",
                "idx4_ts",
                "idx1_num",
                "idx2_num",
                "idx3_num",
                "idx4_num",
                "idx1_ordinal",
                "idx2_ordinal",
                "idx3_ordinal",
                "idx4_ordinal",
                "idx1_coord",
                "idx2_coord",
                "idx3_coord",
                "idx4_coord",
                "v_float",
                "v_int",
                "v_bool",
                "v_str",
                "v_datetime",
                "v_dur_months",
                "v_dur_micros",
            ],
        ),
    ];
    for (sql, want) in cases {
        assert_eq!(column_names(&conn, sql), want, "{sql}");
    }
    conn.close().unwrap();
}

/// The single-branch self-join `value_array_cells` returns exactly the rows of
/// the old two-branch form (kept as an oracle fixture) on float, int (incl.
/// 2^53 + 1, not representable as a double), rank-2/3/4 and empty-list rows.
#[test]
fn value_array_cells_single_branch_matches_the_two_branch_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let db = api_db(dir.path());
    write_array(&db, "n1", &[1.5, -2.5, 3.25], "float rank 1");
    let conn = Connection::open(&db).unwrap();
    let (e, pd, alt, ax, c): (i64, i64, i64, i64, i64) = conn
        .query_row(
            "SELECT entity_id, parameter_definition_id, alternative_id, axis1_id, \
               valid_from_commit FROM value_array",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();
    // Hand-planted payload shapes (inserted through the array_id DEFAULT; the
    // decode view does not validate lengths, so the axis (length 3) is reused).
    let big = (1i64 << 53) + 1;
    let ins = |alt_off: i64, axes: &str, payload: &str| {
        conn.execute(
            &format!(
                "INSERT INTO value_array (entity_id, parameter_definition_id, alternative_id, \
                   valid_from_commit, version, axis1_id, axis2_id, axis3_id, axis4_id, \
                   values, values_int) VALUES ({e}, {pd}, {}, {c}, 1, {axes}, {payload})",
                alt + alt_off
            ),
            [],
        )
        .unwrap();
    };
    ins(
        1000,
        &format!("{ax}, NULL, NULL, NULL"),
        &format!("NULL, [{big}, -1, 0]"),
    );
    ins(
        2000,
        &format!("{ax}, {ax}, NULL, NULL"),
        "range(9)::DOUBLE[], NULL",
    );
    ins(3000, &format!("{ax}, {ax}, {ax}, NULL"), "NULL, range(27)");
    ins(
        4000,
        &format!("{ax}, {ax}, {ax}, {ax}"),
        "range(81)::DOUBLE[], NULL",
    );
    ins(
        5000,
        &format!("{ax}, NULL, NULL, NULL"),
        "[]::DOUBLE[], NULL",
    );
    ins(
        6000,
        &format!("{ax}, NULL, NULL, NULL"),
        "NULL, []::BIGINT[]",
    );

    conn.execute_batch(include_str!("fixtures/value_array_cells_two_branch.sql"))
        .unwrap();
    let total = q_i64(&conn, "SELECT count(*) FROM value_array_cells");
    assert_eq!(total, 3 + 3 + 9 + 27 + 81, "expected cell count");
    let diff = |a: &str, b: &str| {
        q_i64(
            &conn,
            &format!("SELECT count(*) FROM (SELECT * FROM {a} EXCEPT ALL SELECT * FROM {b})"),
        )
    };
    assert_eq!(
        diff("value_array_cells", "value_array_cells_two_branch"),
        0,
        "rows only in the new view"
    );
    assert_eq!(
        diff("value_array_cells_two_branch", "value_array_cells"),
        0,
        "rows only in the old view"
    );
    let v: i64 = q_i64(
        &conn,
        &format!(
            "SELECT v_int FROM value_array_cells WHERE alternative_id = {} AND i1_pos = 0",
            alt + 1000
        ),
    );
    assert_eq!(v, big, "int payload stays exact");
    conn.close().unwrap();
}
