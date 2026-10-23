//! Paired A/B write-path sample (appender_write_path_PLAN §8): one invocation
//! = one measurement of one mode, printed as milliseconds per unit (per
//! value, or per key for `read_long`). Built twice (old core / new core) and
//! driven interleaved by `ab.py`. DB init and seeding are never timed; every
//! write goes through `history::write_value` or `api::write_parameter`.
//!
//! Modes: long_insert, long_replace (8 760-cell long values), array_insert,
//! array_replace (8 760-element arrays, 20 per transaction), axis_insert (a
//! long value on a new irregular 8 760-point datetime axis, whole commit),
//! scalar_insert, scalar_replace (400 per transaction), api_100_scalars (the
//! gate's write_100_new_scalar_entities shape), read_long (current-cell scan
//! of 100 x 876-cell long values written by the same build).

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::api::write_parameter;
use nodal_db_core::axis::AxisCache;
use nodal_db_core::canon::CanonicalValue;
use nodal_db_core::conn::{with_read, with_write_txn};
use nodal_db_core::duckdb;
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis, Duration};
use nodal_db_core::history::write_value;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    LongRow, ScalarValue, ShapedData, Tier, ValueKey, ValueKind, WriteOptions,
};

const T0: i64 = 1_735_689_600 * 1_000_000;
const H: i64 = 3_600_000_000;

fn key(e: i64) -> ValueKey {
    ValueKey {
        entity_id: e,
        parameter_definition_id: 1,
        alternative_id: 1,
    }
}

fn hourly(n: i32) -> CanonicalAxis {
    CanonicalAxis {
        length: n,
        properties: AxisProperties::default(),
        body: AxisBody::DatetimeParametric {
            start_ts: T0,
            step: Duration {
                months: 0,
                micros: H,
            },
        },
    }
}

fn long_v(seed: i64, n: i32, axis: CanonicalAxis) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Long,
        value_kind: ValueKind::Float,
        axes: vec![axis],
        shape: ShapedData::Long(
            (0..n)
                .map(|h| LongRow {
                    positions: vec![h],
                    value: ScalarValue::Float((seed + h as i64) as f64 * 1.5),
                })
                .collect(),
        ),
    }
}

fn array_v(seed: i64) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Array,
        value_kind: ValueKind::Float,
        axes: vec![hourly(8760)],
        shape: ShapedData::Array((0..8760).map(|h| (seed + h) as f64 * 0.25).collect()),
    }
}

fn scalar_v(x: f64) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Scalar,
        value_kind: ValueKind::Float,
        axes: vec![],
        shape: ShapedData::Scalar(ScalarValue::Float(x)),
    }
}

/// Write `vals` (key, value) in ONE transaction; returns elapsed ms.
fn txn(db: &Path, vals: &[(ValueKey, CanonicalValue)]) -> f64 {
    let t = Instant::now();
    with_write_txn(db, "ab", "ab", |conn, c| {
        let mut cache = AxisCache::new();
        for (k, v) in vals {
            write_value(conn, c, *k, v, &mut cache, &WriteOptions::default(), None)?;
        }
        Ok(())
    })
    .expect("write");
    t.elapsed().as_secs_f64() * 1e3
}

fn scalar_batch(names: &[String], vals: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args[1].as_str();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("ab.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();

    let ms_per_unit = match mode {
        // 3 fresh 8 760-cell long values, one txn each.
        "long_insert" => {
            let mut tot = 0.0;
            for e in 1..=3 {
                tot += txn(&db, &[(key(e), long_v(e, 8760, hourly(8760)))]);
            }
            tot / 3.0
        }
        // seed one, then 3 full replaces (close-out 8 760 + reinsert).
        "long_replace" => {
            txn(&db, &[(key(1), long_v(0, 8760, hourly(8760)))]);
            let mut tot = 0.0;
            for s in 1..=3 {
                tot += txn(&db, &[(key(1), long_v(s, 8760, hourly(8760)))]);
            }
            tot / 3.0
        }
        // 20 fresh 8 760-element arrays in one txn.
        "array_insert" => {
            let v: Vec<_> = (1..=20).map(|e| (key(e), array_v(e))).collect();
            txn(&db, &v) / 20.0
        }
        // seed 20, then replace all 20 in one txn.
        "array_replace" => {
            let v: Vec<_> = (1..=20).map(|e| (key(e), array_v(e))).collect();
            txn(&db, &v);
            let v: Vec<_> = (1..=20).map(|e| (key(e), array_v(e + 7))).collect();
            txn(&db, &v) / 20.0
        }
        // one long value on a NEW irregular 8 760-coordinate datetime axis
        // (8 760 axis_datetime child rows), minus nothing: whole txn.
        "axis_insert" => {
            let coords: Vec<i64> = (0..8760).map(|h| T0 + h * H + (h % 7)).collect();
            let ax = CanonicalAxis {
                length: 8760,
                properties: AxisProperties::default(),
                body: AxisBody::DatetimeMaterialized { coords },
            };
            txn(&db, &[(key(1), long_v(1, 8760, ax))])
        }
        // 400 fresh scalars in one txn (µs/value reported as ms).
        "scalar_insert" => {
            let v: Vec<_> = (1..=400).map(|e| (key(e), scalar_v(e as f64))).collect();
            txn(&db, &v) / 400.0
        }
        "scalar_replace" => {
            let v: Vec<_> = (1..=400).map(|e| (key(e), scalar_v(e as f64))).collect();
            txn(&db, &v);
            let v: Vec<_> = (1..=400).map(|e| (key(e), scalar_v(e as f64 + 0.5))).collect();
            txn(&db, &v) / 400.0
        }
        // The gate's write_100_new_scalar_entities, through the API.
        "api_100_scalars" => {
            let mut tot = 0.0;
            for run in 0..3 {
                let names: Vec<String> = (0..100).map(|i| format!("w{run}_{i:03}")).collect();
                let vals: Vec<f64> = (0..100).map(|i| i as f64).collect();
                let t = Instant::now();
                write_parameter(
                    &db,
                    "node",
                    "p",
                    "Base",
                    scalar_batch(&names, &vals),
                    &[],
                    WriteOptions::default(),
                    "ab",
                    "ab",
                )
                .unwrap();
                tot += t.elapsed().as_secs_f64() * 1e3;
            }
            tot / 300.0
        }
        // Read row: 100 keys x 876-cell long values written by THIS build,
        // then the current-cell scan of each key by triple (x5), per key.
        "read_long" => {
            let v: Vec<_> = (1..=100)
                .map(|e| (key(e), long_v(e, 876, hourly(876))))
                .collect();
            txn(&db, &v);
            let t = Instant::now();
            with_read(&db, |conn| {
                let mut stmt = conn.prepare(
                    "SELECT sum(v_float), count(*) FROM value_long \
                     WHERE entity_id = ? AND parameter_definition_id = 1 \
                       AND alternative_id = 1 AND valid_to_commit IS NULL",
                )?;
                for _ in 0..5 {
                    for e in 1..=100i64 {
                        let n: i64 = stmt.query_row([e], |r| r.get(1))?;
                        assert_eq!(n, 876);
                    }
                }
                Ok(())
            })
            .unwrap();
            t.elapsed().as_secs_f64() * 1e3 / 500.0
        }
        other => panic!("unknown mode {other}"),
    };
    println!("{ms_per_unit:.6}");
}
