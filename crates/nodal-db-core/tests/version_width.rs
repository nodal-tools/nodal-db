//! The `version` column is INTEGER and `WriteOutcome.version` is `u32`
//! (pre-release review 2, M1). Schema 0.6 had `SMALLINT` / `u16`, and the
//! array path wrapped silently at 32 768 (`as u16` then `as i16`). These
//! tests start keys just below the old limit (by setting the stored version
//! with plain SQL) and write through every version path: the batch writers
//! of `write_parameter` (scalar, array) and the single-value `write_value`
//! (array, scalar). Near `i32::MAX` a write fails loudly, never wraps.

use std::path::Path;
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{head_commit, write_parameter};
use nodal_db_core::axis::AxisCache;
use nodal_db_core::canon::CanonicalValue;
use nodal_db_core::conn::with_write_txn;
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis};
use nodal_db_core::history::write_value;
use nodal_db_core::schema_init::{init_schema, init_schema_with_default};
use nodal_db_core::types::{ScalarValue, ShapedData, Tier, ValueKey, ValueKind, WriteOptions};

const OLD_LIMIT: i64 = i16::MAX as i64; // 32 767, the last SMALLINT version

fn scalar_batch(entity: &str, v: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity])) as ArrayRef,
            Arc::new(Float64Array::from(vec![v])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn array_batch(entity: &str, vals: &[f64]) -> RecordBatch {
    let n = vals.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("step", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; n])) as ArrayRef,
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn write(db: &Path, param: &str, batch: RecordBatch) -> Result<(), nodal_db_core::error::DbError> {
    write_parameter(
        db,
        "node",
        param,
        "Base",
        batch,
        &[],
        "tester",
        "version width",
        WriteOptions::default(),
    )
    .map(|_| ())
}

fn exec(db: &Path, sql: &str) {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch(sql).unwrap();
    conn.close().unwrap();
}

fn current_version(db: &Path, table: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    // value_array holds current rows only (pre-images go to its _hist twin).
    let sql = if table == "value_array" {
        "SELECT version FROM value_array".to_string()
    } else {
        format!("SELECT version FROM {table} WHERE valid_to_commit IS NULL")
    };
    let v = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    v
}

#[test]
fn version_column_is_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    let conn = Connection::open(&db).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT table_name, data_type FROM duckdb_columns() \
             WHERE column_name = 'version' AND NOT internal \
               AND table_name IN (SELECT table_name FROM duckdb_tables()) \
             ORDER BY table_name",
        )
        .unwrap();
    let rows: Vec<(String, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(rows.len() >= 30, "every versioned table: {rows:?}");
    for (table, ty) in rows {
        assert_eq!(ty, "INTEGER", "{table}.version");
    }
}

/// The batch writers past the old SMALLINT limit: scalar and array keys at
/// 32 767 get 32 768, then 32 769.
#[test]
fn batch_writes_pass_the_old_smallint_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("b.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();
    write(&db, "demand", scalar_batch("n1", 1.0)).unwrap();
    write(&db, "profile", array_batch("n2", &[1.0, 2.0])).unwrap();
    exec(
        &db,
        &format!(
            "UPDATE value_scalar SET version = {OLD_LIMIT}; \
             UPDATE value_array SET version = {OLD_LIMIT};"
        ),
    );
    for (i, want) in [(2.0, OLD_LIMIT + 1), (3.0, OLD_LIMIT + 2)] {
        write(&db, "demand", scalar_batch("n1", i)).unwrap();
        write(&db, "profile", array_batch("n2", &[i, i + 1.0])).unwrap();
        assert_eq!(current_version(&db, "value_scalar"), want, "scalar");
        assert_eq!(current_version(&db, "value_array"), want, "array");
    }
    let conn = Connection::open(&db).unwrap();
    let hist: i64 = conn
        .query_row("SELECT max(version) FROM value_array_hist", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(hist, OLD_LIMIT + 1, "the pre-image keeps its version");
}

/// At `i32::MAX` the next version does not fit: the write fails, nothing is
/// committed, and the stored version is untouched (no wrap).
#[test]
fn batch_write_at_integer_max_fails_loudly() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("m.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();
    write(&db, "demand", scalar_batch("n1", 1.0)).unwrap();
    write(&db, "profile", array_batch("n2", &[1.0, 2.0])).unwrap();
    exec(
        &db,
        "UPDATE value_scalar SET version = 2147483647; \
         UPDATE value_array SET version = 2147483647;",
    );
    let head = head_commit(&db).unwrap();
    assert!(write(&db, "demand", scalar_batch("n1", 2.0)).is_err());
    assert!(write(&db, "profile", array_batch("n2", &[5.0, 6.0])).is_err());
    assert_eq!(head_commit(&db).unwrap(), head, "nothing committed");
    assert_eq!(current_version(&db, "value_scalar"), 2_147_483_647);
    assert_eq!(current_version(&db, "value_array"), 2_147_483_647);
}

fn int_axis(length: i32) -> CanonicalAxis {
    CanonicalAxis {
        length,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerParametric { start: 0, step: 1 },
    }
}

const KEY: ValueKey = ValueKey {
    entity_id: 7,
    parameter_definition_id: 11,
    alternative_id: 13,
};

/// The single-value path reports the version as `u32`, past the old limit.
#[test]
fn write_value_reports_u32_versions() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("w.duckdb");
    init_schema(&db).unwrap();
    let array = |vals: &[f64]| CanonicalValue {
        tier: Tier::Array,
        value_kind: ValueKind::Float,
        axes: vec![int_axis(vals.len() as i32)],
        shape: ShapedData::Array(vals.to_vec()),
    };
    let scalar = |v: f64| CanonicalValue {
        tier: Tier::Scalar,
        value_kind: ValueKind::Float,
        axes: vec![],
        shape: ShapedData::Scalar(ScalarValue::Float(v)),
    };
    let step = |v: &CanonicalValue| -> u32 {
        let mut out = 0;
        with_write_txn(&db, "tester", "step", |conn, c| {
            let mut cache = AxisCache::new();
            out = write_value(conn, c, KEY, v, &mut cache, &WriteOptions::default(), None)?.version;
            Ok(())
        })
        .unwrap();
        out
    };
    assert_eq!(step(&array(&[1.0, 2.0])), 1);
    exec(
        &db,
        &format!("UPDATE value_array SET version = {OLD_LIMIT}"),
    );
    let v: u32 = step(&array(&[2.0, 3.0]));
    assert_eq!(v, 32_768);
    assert_eq!(step(&array(&[3.0, 4.0])), 32_769);
    // A tier change starts the new tier's own counter (module doc).
    assert_eq!(step(&scalar(1.0)), 1);
    exec(
        &db,
        &format!("UPDATE value_scalar SET version = {OLD_LIMIT}"),
    );
    assert_eq!(step(&scalar(2.0)), 32_768);
}
