//! DB-backed axis dedup conformance (rust_implementation_plan.md Part 3 `axis`;
//! VMP Part 4). Each test builds a fresh DuckDB from `schema/*.sql` via
//! `schema_init`, then drives `axis::dedup_or_insert` and asserts the resulting
//! `axis`-row population.
//!
//! Scenarios (task scope 4):
//!   - same canonical axis twice → ONE row, same id (reuse via DB byte-compare
//!     with a fresh cache, and via the client cache);
//!   - two genuinely different axes → two rows;
//!   - a parametric axis vs its materialized-equivalent → distinct rows (F6:
//!     they are NOT byte-identical; uniqueness rests on parametric-first);
//!   - two axes identical except calendar/timezone/crs → distinct rows;
//!   - a canon-derived axis and a hand-built identical one dedup together.
//! Kinds covered: datetime-parametric, integer-materialized, float-parametric,
//! label (+ integer-parametric for the F6 pair).

use std::path::PathBuf;
use std::sync::Arc;

use duckdb::arrow::array::{Float64Array, Int64Array};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use spinal_db_core::axis::{dedup_or_insert, AxisCache};
use spinal_db_core::canon::canonicalize;
use spinal_db_core::conn::with_write_txn;
use spinal_db_core::encoding::{
    AxisBody, AxisProperties, CanonicalAxis, Duration, LabelPos,
};
use spinal_db_core::schema_init::init_schema;
use spinal_db_core::types::{DeclaredType, IndexKind, ValueKind};

/// Fresh schema-built DuckDB file; the temp path is returned so the caller can
/// open write/read connections against it (open-close discipline).
fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test.duckdb");
    init_schema(&path).expect("init schema");
    (dir, path)
}

fn axis_count(path: &PathBuf) -> i64 {
    let conn = Connection::open(path).expect("open");
    let n = conn
        .query_row("SELECT count(*) FROM axis", [], |r| r.get::<_, i64>(0))
        .expect("count");
    n
}

fn dt_parametric() -> CanonicalAxis {
    // hourly, 24 steps from 2020-01-01 (µs), pure-micros step.
    CanonicalAxis {
        length: 24,
        properties: AxisProperties::default(),
        body: AxisBody::DatetimeParametric {
            start_ts: 1_577_836_800_000_000,
            step: Duration {
                months: 0,
                micros: 3_600_000_000,
            },
        },
    }
}

#[test]
fn same_axis_dedups_to_one_row() {
    let (_dir, path) = fresh_db();
    let axis = dt_parametric();

    // Two inserts inside one write txn, one cache: first inserts, second must
    // reuse (via the client cache).
    let (id1, id2) = with_write_txn(&path, "tester", "dedup same", |conn, _c| {
        let mut cache = AxisCache::new();
        let a = dedup_or_insert(conn, &mut cache, &axis)?;
        let b = dedup_or_insert(conn, &mut cache, &axis)?;
        Ok((a, b))
    })
    .expect("write txn");
    assert_eq!(id1, id2, "cache reuse must return the same id");

    // A *fresh* cache in a new txn forces the DB byte-compare path.
    let id3 = with_write_txn(&path, "tester", "dedup via db", |conn, _c| {
        let mut cache = AxisCache::new();
        dedup_or_insert(conn, &mut cache, &axis)
    })
    .expect("write txn 2");
    assert_eq!(id1, id3, "DB byte-compare must reuse the same id");

    assert_eq!(axis_count(&path), 1, "one axis row after three dedups");
}

#[test]
fn distinct_axes_get_distinct_rows() {
    let (_dir, path) = fresh_db();
    // Two genuinely different integer-materialized axes (irregular sets).
    let a = CanonicalAxis {
        length: 3,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerMaterialized {
            coords: vec![1, 4, 9],
        },
    };
    let b = CanonicalAxis {
        length: 3,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerMaterialized {
            coords: vec![1, 4, 16],
        },
    };

    let (ia, ib) = with_write_txn(&path, "tester", "distinct", |conn, _c| {
        let mut cache = AxisCache::new();
        let ia = dedup_or_insert(conn, &mut cache, &a)?;
        let ib = dedup_or_insert(conn, &mut cache, &b)?;
        Ok((ia, ib))
    })
    .expect("write txn");
    assert_ne!(ia, ib, "different content → different ids");
    assert_eq!(axis_count(&path), 2);

    // And the materialized child rows round-trip so a re-dedup reuses.
    let ia2 = with_write_txn(&path, "tester", "reuse mat", |conn, _c| {
        let mut cache = AxisCache::new();
        dedup_or_insert(conn, &mut cache, &a)
    })
    .expect("txn");
    assert_eq!(ia, ia2, "materialized axis reconstructs + byte-matches");
    assert_eq!(axis_count(&path), 2, "no new row on reuse");
}

#[test]
fn parametric_vs_materialized_are_distinct() {
    // F6: an integer-parametric [0,1,2] and a materialized [0,1,2] describe the
    // same coordinates but are NOT byte-identical (form byte + body differ), so
    // they must occupy distinct rows — uniqueness rests on parametric-first.
    let (_dir, path) = fresh_db();
    let parametric = CanonicalAxis {
        length: 3,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerParametric { start: 0, step: 1 },
    };
    let materialized = CanonicalAxis {
        length: 3,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerMaterialized {
            coords: vec![0, 1, 2],
        },
    };

    let (ip, im) = with_write_txn(&path, "tester", "f6", |conn, _c| {
        let mut cache = AxisCache::new();
        let ip = dedup_or_insert(conn, &mut cache, &parametric)?;
        let im = dedup_or_insert(conn, &mut cache, &materialized)?;
        Ok((ip, im))
    })
    .expect("write txn");
    assert_ne!(ip, im, "parametric and materialized are distinct axes (F6)");
    assert_eq!(axis_count(&path), 2);
}

#[test]
fn properties_break_dedup() {
    // Two datetime-parametric axes identical except calendar/timezone/crs must
    // NOT dedup — properties are hash + byte input (VMP Part 4, hash caveat 2).
    let (_dir, path) = fresh_db();
    let plain = dt_parametric();
    let mut noleap = dt_parametric();
    noleap.properties = AxisProperties {
        calendar: Some("noleap".to_string()),
        timezone: Some("UTC".to_string()),
        crs: None,
    };

    let (a, b) = with_write_txn(&path, "tester", "props", |conn, _c| {
        let mut cache = AxisCache::new();
        let a = dedup_or_insert(conn, &mut cache, &plain)?;
        let b = dedup_or_insert(conn, &mut cache, &noleap)?;
        Ok((a, b))
    })
    .expect("write txn");
    assert_ne!(a, b, "differing calendar/timezone must not dedup");
    assert_eq!(axis_count(&path), 2);

    // Re-dedup each with a fresh cache: the DB byte-compare must reconstruct the
    // properties and match the right row.
    let (a2, b2) = with_write_txn(&path, "tester", "props reuse", |conn, _c| {
        let mut cache = AxisCache::new();
        let a2 = dedup_or_insert(conn, &mut cache, &plain)?;
        let b2 = dedup_or_insert(conn, &mut cache, &noleap)?;
        Ok((a2, b2))
    })
    .expect("write txn");
    assert_eq!(a, a2);
    assert_eq!(b, b2);
    assert_eq!(axis_count(&path), 2, "no new rows on reuse");
}

#[test]
fn float_parametric_and_label_dedup() {
    let (_dir, path) = fresh_db();
    let fpar = CanonicalAxis {
        length: 4,
        properties: AxisProperties::default(),
        body: AxisBody::FloatParametric {
            start: 0.0,
            step: 0.25,
        },
    };
    let label = CanonicalAxis {
        length: 3,
        properties: AxisProperties::default(),
        body: AxisBody::Label {
            positions: vec![
                LabelPos {
                    ordinal: 0,
                    label: "coal".to_string(),
                },
                LabelPos {
                    ordinal: 1,
                    label: "gas".to_string(),
                },
                LabelPos {
                    ordinal: 2,
                    label: "wind".to_string(),
                },
            ],
        },
    };

    let (fid, lid) = with_write_txn(&path, "tester", "float+label", |conn, _c| {
        let mut cache = AxisCache::new();
        let fid = dedup_or_insert(conn, &mut cache, &fpar)?;
        let lid = dedup_or_insert(conn, &mut cache, &label)?;
        Ok((fid, lid))
    })
    .expect("write txn");
    assert_ne!(fid, lid);
    assert_eq!(axis_count(&path), 2);

    // Fresh cache → DB reconstruction path for both a parametric-float and a
    // materialized-child (label) axis.
    let (fid2, lid2) = with_write_txn(&path, "tester", "reuse", |conn, _c| {
        let mut cache = AxisCache::new();
        let fid2 = dedup_or_insert(conn, &mut cache, &fpar)?;
        let lid2 = dedup_or_insert(conn, &mut cache, &label)?;
        Ok((fid2, lid2))
    })
    .expect("write txn");
    assert_eq!(fid, fid2, "float-parametric reconstructs + reuses");
    assert_eq!(lid, lid2, "label axis reconstructs + reuses");
    assert_eq!(axis_count(&path), 2);
}

#[test]
fn canon_derived_axis_matches_hand_built() {
    // The axis dedup consumes exactly what `canon` produced (invariant 2). Feed
    // canon a rank-1 integer-dimension batch with the pinned concrete Arrow
    // types (Int64 dim, Float64 value — the F3 boundary), extract its derived
    // axis, and confirm it dedups against a hand-built identical axis.
    let (_dir, path) = fresh_db();
    let dim = Int64Array::from(vec![10_i64, 20, 30]);
    let val = Float64Array::from(vec![1.0_f64, 2.0, 3.0]);
    let schema = Schema::new(vec![
        Field::new("d", DataType::Int64, false),
        Field::new("v", DataType::Float64, true),
    ]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dim), Arc::new(val)])
        .expect("record batch");
    let declared = DeclaredType {
        positions: vec![(0, IndexKind::Integer)],
    };
    let cv = canonicalize(&batch, &declared, ValueKind::Float, &[]).expect("canon");
    let derived = cv.axes[0].clone();
    // Regular [10,20,30] must have canonicalized to parametric (start 10, step 10).
    assert!(matches!(
        derived.body,
        AxisBody::IntegerParametric { start: 10, step: 10 }
    ));

    let hand = CanonicalAxis {
        length: 3,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerParametric { start: 10, step: 10 },
    };

    let (id_derived, id_hand) = with_write_txn(&path, "tester", "canon", |conn, _c| {
        let mut cache = AxisCache::new();
        let a = dedup_or_insert(conn, &mut cache, &derived)?;
        // fresh cache → forces DB byte-compare between the two encodings.
        let mut cache2 = AxisCache::new();
        let b = dedup_or_insert(conn, &mut cache2, &hand)?;
        Ok((a, b))
    })
    .expect("write txn");
    assert_eq!(id_derived, id_hand, "canon output and hand-built dedup together");
    assert_eq!(axis_count(&path), 1);
}
