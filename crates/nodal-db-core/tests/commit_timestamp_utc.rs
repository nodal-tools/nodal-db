//! `commit.timestamp` (read through `commit_log`) is naive UTC: the column
//! default is `now()::TIMESTAMP`, which is UTC only because the bundled
//! DuckDB has no ICU extension (with ICU loaded, `now()` is a TIMESTAMPTZ
//! and the cast yields local time). Enabling ICU for any reason would
//! silently switch new commits to local time, so this guards both the
//! cause (ICU absent on the core's connections) and the effect (a commit
//! written through the core carries the current UTC time).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;

use nodal_db_core::api::write_parameter;
use nodal_db_core::conn::with_read;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::WriteOptions;

fn utc_now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

#[test]
fn commit_timestamps_are_naive_utc() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("t.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["n1"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
        ],
    )
    .unwrap();
    let before = utc_now_micros();
    let c = write_parameter(
        &db,
        "node",
        "demand",
        "Base",
        batch,
        &[],
        "tester",
        "utc",
        WriteOptions::default(),
    )
    .unwrap()
    .commit_id;
    let after = utc_now_micros();

    let (ty, ts, icu_loaded): (String, i64, i64) = with_read(&db, |conn| {
        Ok(conn.query_row(
            "SELECT typeof(\"timestamp\"), epoch_us(\"timestamp\"), \
                    (SELECT count(*) FROM duckdb_functions() \
                     WHERE function_name = 'icu_sort_key') \
             FROM commit_log WHERE id = ?",
            [c],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?)
    })
    .unwrap();
    assert_eq!(ty, "TIMESTAMP", "naive, never TIMESTAMPTZ");
    assert_eq!(
        icu_loaded, 0,
        "ICU is loaded on the core's connection: now()::TIMESTAMP would be local time; \
         commit.timestamp must stay UTC (convert explicitly if ICU is ever enabled)"
    );
    // Seconds of slack for the commit cycle; a timezone offset is >= 15 min.
    let slack = 60_000_000;
    assert!(
        ts >= before - slack && ts <= after + slack,
        "commit timestamp {ts} µs is not the current UTC time ({before}..{after} µs)"
    );
}
