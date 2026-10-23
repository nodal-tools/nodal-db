//! The pipeline rejects timestamps outside the store's range
//! (`canon::TS_MIN_US..=TS_MAX_US`, DuckDB's TIMESTAMP span) in both a
//! datetime dimension and a datetime value column; the bounds themselves pass.
//! Below the minimum DuckDB would write the value and then fail every read of
//! the column; `i64::MAX` is DuckDB's `infinity` sentinel.

use std::sync::Arc;

use nodal_db_core::canon::{canonicalize, CanonError, TS_MAX_US, TS_MIN_US};
use nodal_db_core::duckdb::arrow::array::{
    ArrayRef, Float64Array, Int64Array, TimestampMicrosecondArray,
};
use nodal_db_core::duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use nodal_db_core::duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::types::{DeclaredType, IndexKind, ValueKind};

fn ts_field(name: &str) -> Field {
    Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true)
}

/// Rank 1: a µs datetime dimension with a float value.
fn dim_case(t: i64) -> Result<(), CanonError> {
    let schema = Arc::new(Schema::new(vec![
        ts_field("t"),
        Field::new("v", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![t])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
        ],
    )
    .unwrap();
    let declared = DeclaredType {
        positions: vec![(1, IndexKind::Datetime)],
    };
    canonicalize(&batch, &declared, ValueKind::Float, &[]).map(|_| ())
}

/// Rank 1: an integer dimension with a µs datetime value.
fn value_case(v: i64) -> Result<(), CanonError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, false),
        ts_field("v"),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![v])) as ArrayRef,
        ],
    )
    .unwrap();
    let declared = DeclaredType {
        positions: vec![(1, IndexKind::Integer)],
    };
    canonicalize(&batch, &declared, ValueKind::DateTime, &[]).map(|_| ())
}

#[test]
fn bounds_are_accepted() {
    for t in [TS_MIN_US, TS_MAX_US, 0] {
        dim_case(t).expect("in-range dimension coordinate");
        value_case(t).expect("in-range value");
    }
}

#[test]
fn dimension_below_minimum_is_rejected() {
    let err = dim_case(TS_MIN_US - 1).unwrap_err();
    assert!(
        matches!(err, CanonError::TimestampOutOfRange { value, column: 0 } if value == TS_MIN_US - 1),
        "{err:?}"
    );
    assert!(
        err.to_string().contains("outside the supported range"),
        "{err}"
    );
}

#[test]
fn value_above_maximum_is_rejected() {
    let err = value_case(TS_MAX_US + 1).unwrap_err();
    assert!(
        matches!(
            err,
            CanonError::TimestampOutOfRange {
                value: i64::MAX,
                column: 1
            }
        ),
        "{err:?}"
    );
}

#[test]
fn i64_min_value_is_rejected() {
    assert!(matches!(
        value_case(i64::MIN),
        Err(CanonError::TimestampOutOfRange { .. })
    ));
}
