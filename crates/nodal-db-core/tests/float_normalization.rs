//! Value floats are stored canonical: `-0.0` as `+0.0`, every NaN as the one
//! quiet NaN `0x7ff8000000000000` — the same rule the canonical encoding
//! applies, so what is stored is what is hashed. Axis floats store `-0.0` as
//! `+0.0` too (NaN is rejected there). Every check compares raw bits: `-0.0 ==
//! 0.0` and NaN never compares equal, so value equality cannot see this.
//!
//! Inputs are built with `f64::from_bits` and moved through `Vec<f64>` / Arrow
//! buffers only (no arithmetic), so signaling-NaN payloads survive to the
//! writer on x86-64 and aarch64.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{ingest_segments, read_parameter, write_parameter, Segment};
use nodal_db_core::axis::{dedup_or_insert, AxisCache};
use nodal_db_core::canon::{canonicalize, CanonicalValue};
use nodal_db_core::conn::with_write_txn;
use nodal_db_core::encoding::{AxisBody, CanonicalAxis, PeriodPos, CANONICAL_NAN_BITS};
use nodal_db_core::history::write_value;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    DeclaredType, IndexKind, ScalarValue, ShapedData, Tier, ValueKey, ValueKind, WriteOptions,
};

const QNAN: u64 = CANONICAL_NAN_BITS;

/// (input bits, expected stored bits).
const PATTERNS: [(u64, u64); 14] = [
    (0x8000_0000_0000_0000, 0x0000_0000_0000_0000), // -0.0
    (0x0000_0000_0000_0000, 0x0000_0000_0000_0000), // +0.0
    (0x7ff8_0000_0000_0000, QNAN),                  // canonical NaN
    (0xfff8_0000_0000_0000, QNAN),                  // negative quiet NaN
    (0x7ff0_0000_0000_0001, QNAN),                  // signaling NaN
    (0x7ff4_0000_0000_0000, QNAN),                  // signaling, high payload
    (0x7fff_ffff_ffff_ffff, QNAN),                  // quiet, max payload
    (0xfff0_0000_0000_0001, QNAN),                  // negative signaling
    (0x7ff0_0000_0000_0000, 0x7ff0_0000_0000_0000), // +inf
    (0xfff0_0000_0000_0000, 0xfff0_0000_0000_0000), // -inf
    (0x0000_0000_0000_0001, 0x0000_0000_0000_0001), // +min subnormal
    (0x8000_0000_0000_0001, 0x8000_0000_0000_0001), // -min subnormal
    (0x3ff8_0000_0000_0000, 0x3ff8_0000_0000_0000), // 1.5
    (0xbff8_0000_0000_0000, 0xbff8_0000_0000_0000), // -1.5
];

fn inputs() -> Vec<f64> {
    PATTERNS.iter().map(|(i, _)| f64::from_bits(*i)).collect()
}

fn expected() -> Vec<u64> {
    PATTERNS.iter().map(|(_, e)| *e).collect()
}

fn bits(v: &[f64]) -> Vec<u64> {
    v.iter().map(|x| x.to_bits()).collect()
}

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("f.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();
    (dir, db)
}

fn query_f64(db: &Path, sql: &str) -> Vec<f64> {
    let conn = Connection::open(db).unwrap();
    let v = conn
        .prepare(sql)
        .unwrap()
        .query_map([], |r| r.get::<_, f64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    conn.close().unwrap();
    v
}

/// (entity, value) rank-0 batch, one entity per value.
fn scalar_batch(entities: &[String], vals: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(entities.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

/// (entity, d0, [d1,] value) integer-indexed batch.
fn indexed_batch(entity: &str, dims: Vec<Vec<i64>>, vals: &[f64]) -> RecordBatch {
    let n = vals.len();
    let mut fields = vec![Field::new("entity", DataType::Utf8, false)];
    let mut cols: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![entity; n]))];
    for (i, d) in dims.into_iter().enumerate() {
        fields.push(Field::new(format!("d{i}"), DataType::Int64, false));
        cols.push(Arc::new(Int64Array::from(d)));
    }
    fields.push(Field::new("value", DataType::Float64, false));
    cols.push(Arc::new(Float64Array::from(vals.to_vec())));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

fn float_batch(dims: Vec<Vec<i64>>, vals: &[f64]) -> RecordBatch {
    let mut fields = Vec::new();
    let mut cols: Vec<ArrayRef> = Vec::new();
    for (i, d) in dims.into_iter().enumerate() {
        fields.push(Field::new(format!("d{i}"), DataType::Int64, false));
        cols.push(Arc::new(Int64Array::from(d)));
    }
    fields.push(Field::new("value", DataType::Float64, true));
    cols.push(Arc::new(Float64Array::from(vals.to_vec())));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

fn int_dims(rank: usize) -> DeclaredType {
    DeclaredType {
        positions: (1..=rank as u8).map(|p| (p, IndexKind::Integer)).collect(),
    }
}

fn write(db: &Path, class: &str, param: &str, batch: RecordBatch) -> Vec<Tier> {
    write_parameter(
        db,
        class,
        param,
        "Base",
        batch,
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .expect("write")
    .tiers
}

// --- the pipeline (no DB) ---------------------------------------------------

#[test]
fn canon_scalar_floats_are_canonical() {
    for (input, want) in PATTERNS {
        let b = float_batch(vec![], &[f64::from_bits(input)]);
        let cv = canonicalize(&b, &int_dims(0), ValueKind::Float, &[]).unwrap();
        match cv.shape {
            ShapedData::Scalar(ScalarValue::Float(x)) => {
                assert_eq!(x.to_bits(), want, "input {input:#018x}")
            }
            other => panic!("unexpected shape {other:?}"),
        }
    }
}

#[test]
fn canon_long_rows_are_canonical() {
    // Sparse rank-2 (h, h % 3): never a full grid -> long tier.
    let n = PATTERNS.len() as i64;
    let h: Vec<i64> = (0..n).collect();
    let k: Vec<i64> = h.iter().map(|x| x % 3).collect();
    let cv = canonicalize(
        &float_batch(vec![h, k], &inputs()),
        &int_dims(2),
        ValueKind::Float,
        &[],
    )
    .unwrap();
    assert_eq!(cv.tier, Tier::Long);
    let ShapedData::Long(rows) = cv.shape else {
        panic!("long shape expected")
    };
    let got: Vec<u64> = rows
        .iter()
        .map(|r| match r.value {
            ScalarValue::Float(x) => x.to_bits(),
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(got, expected());
}

#[test]
fn canon_array_payload_is_canonical_and_nan_keeps_the_array_tier() {
    let n = PATTERNS.len() as i64;
    let cv = canonicalize(
        &float_batch(vec![(0..n).collect()], &inputs()),
        &int_dims(1),
        ValueKind::Float,
        &[],
    )
    .unwrap();
    assert_eq!(cv.tier, Tier::Array, "NaN cells are present cells");
    let ShapedData::Array(vals) = cv.shape else {
        panic!("array shape expected")
    };
    assert_eq!(bits(&vals), expected());
}

// --- storage, per write path ------------------------------------------------

#[test]
fn scalar_storage_is_canonical() {
    let (_d, db) = fresh_db();
    let ents: Vec<String> = (0..PATTERNS.len()).map(|i| format!("e{i:02}")).collect();
    let tiers = write(&db, "node", "x", scalar_batch(&ents, &inputs()));
    assert_eq!(tiers, vec![Tier::Scalar]);
    let stored = query_f64(
        &db,
        "SELECT vs.v_float FROM value_scalar vs JOIN entity e ON e.id = vs.entity_id \
         ORDER BY e.name",
    );
    assert_eq!(bits(&stored), expected());
    // And through the read path (the read_parameter view).
    let rb = read_parameter(&db, "node", "x", "Base").unwrap();
    let ent = rb
        .column(rb.schema().index_of("entity").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    let v = rb
        .column(rb.schema().index_of("v_float").unwrap())
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .clone();
    let mut pairs: Vec<(String, u64)> = (0..rb.num_rows())
        .map(|i| (ent.value(i).to_string(), v.value(i).to_bits()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs.into_iter().map(|p| p.1).collect::<Vec<_>>(),
        expected()
    );
}

#[test]
fn long_storage_is_canonical() {
    let (_d, db) = fresh_db();
    let n = PATTERNS.len() as i64;
    let h: Vec<i64> = (0..n).collect();
    let k: Vec<i64> = h.iter().map(|x| x % 3).collect();
    let tiers = write(&db, "node", "x", indexed_batch("n1", vec![h, k], &inputs()));
    assert_eq!(tiers, vec![Tier::Long]);
    // value_long rows sorted by position tuple == input order here (h ascending).
    let stored = query_f64(
        &db,
        "SELECT v_float FROM value_long ORDER BY i1_pos, i2_pos",
    );
    assert_eq!(bits(&stored), expected());
}

#[test]
fn array_storage_and_min_max_are_canonical() {
    let (_d, db) = fresh_db();
    let n = PATTERNS.len() as i64;
    let tiers = write(
        &db,
        "node",
        "x",
        indexed_batch("n1", vec![(0..n).collect()], &inputs()),
    );
    assert_eq!(tiers, vec![Tier::Array]);
    let stored = query_f64(&db, "SELECT unnest(values) FROM value_array");
    assert_eq!(bits(&stored), expected());
    // NaN present: v_max is the canonical NaN (DuckDB total order), v_min the
    // smallest number.
    let vmin = query_f64(&db, "SELECT v_min FROM value_array");
    let vmax = query_f64(&db, "SELECT v_max FROM value_array");
    assert_eq!(vmin[0], f64::NEG_INFINITY);
    assert_eq!(vmax[0].to_bits(), QNAN);
}

/// The five arrays of the v_min/v_max finding: none is flagged by
/// audit_array_integrity, all-NaN gets NaN bounds (not inverted ±inf), and no
/// bound is -0.0.
#[test]
fn array_min_max_follow_duckdb_order_and_pass_the_audit() {
    let (_d, db) = fresh_db();
    let neg_nan = f64::from_bits(0xfff8_0000_0000_0000);
    let cases: [(&str, [f64; 3]); 5] = [
        ("finite", [1.0, 1.0, 3.0]),
        ("one_nan", [1.0, f64::NAN, 3.0]),
        ("neg_nan", [1.0, neg_nan, 3.0]),
        ("all_nan", [f64::NAN, f64::NAN, f64::NAN]),
        ("negzero", [-0.0, 1.0, 3.0]),
    ];
    for (ent, vals) in cases {
        let tiers = write(
            &db,
            "node",
            "x",
            indexed_batch(ent, vec![vec![0, 1, 2]], &vals),
        );
        assert_eq!(tiers, vec![Tier::Array], "{ent}");
    }
    let conn = Connection::open(&db).unwrap();
    let flagged: i64 = conn
        .query_row("SELECT count(*) FROM audit_array_integrity", [], |r| {
            r.get(0)
        })
        .unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT e.name, va.v_min, va.v_max FROM value_array va \
             JOIN entity e ON e.id = va.entity_id ORDER BY e.name",
        )
        .unwrap();
    let rows: Vec<(String, f64, f64)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    drop(stmt);
    conn.close().unwrap();
    assert_eq!(flagged, 0, "no array is an audit false positive");
    let get = |name: &str| rows.iter().find(|r| r.0 == name).unwrap();
    assert_eq!((get("finite").1, get("finite").2), (1.0, 3.0));
    assert_eq!(get("one_nan").1, 1.0);
    assert_eq!(get("one_nan").2.to_bits(), QNAN);
    assert_eq!(get("neg_nan").2.to_bits(), QNAN);
    assert_eq!(get("all_nan").1.to_bits(), QNAN);
    assert_eq!(get("all_nan").2.to_bits(), QNAN);
    assert_eq!(get("negzero").1.to_bits(), 0, "v_min is +0.0, never -0.0");
}

/// One ingest_segments call with a scalar and an array segment: both go
/// through the shared per-segment body.
#[test]
fn batched_multi_segment_storage_is_canonical() {
    let (_d, db) = fresh_db();
    let neg_nan = f64::from_bits(0xfff8_0000_0000_0000);
    let seg = |param: &str, batch: RecordBatch| Segment {
        class: "node".into(),
        parameter: param.into(),
        alternative: "Base".into(),
        batches: vec![batch],
        domains: vec![],
    };
    ingest_segments(
        &db,
        vec![
            seg(
                "s",
                scalar_batch(&["a".to_string(), "b".to_string()], &[-0.0, neg_nan]),
            ),
            seg(
                "arr",
                indexed_batch("n1", vec![vec![0, 1]], &[-0.0, neg_nan]),
            ),
        ],
        "tester",
        "",
        WriteOptions::default(),
    )
    .unwrap();
    let s = query_f64(
        &db,
        "SELECT vs.v_float FROM value_scalar vs JOIN entity e ON e.id = vs.entity_id \
         ORDER BY e.name",
    );
    assert_eq!(bits(&s), vec![0, QNAN]);
    let a = query_f64(&db, "SELECT unnest(values) FROM value_array");
    assert_eq!(bits(&a), vec![0, QNAN]);
}

/// The superseded version in history holds the canonical bits (the pre-image
/// copy is verbatim of a canonical row).
#[test]
fn history_pre_image_is_canonical() {
    let (_d, db) = fresh_db();
    write(&db, "node", "x", scalar_batch(&["a".to_string()], &[-0.0]));
    write(&db, "node", "x", scalar_batch(&["a".to_string()], &[1.0]));
    let closed = query_f64(
        &db,
        "SELECT v_float FROM value_scalar WHERE valid_to_commit IS NOT NULL",
    );
    assert_eq!(bits(&closed), vec![0]);

    let n = 2;
    write(
        &db,
        "node",
        "arr",
        indexed_batch("n1", vec![(0..n).collect()], &[-0.0, 2.0]),
    );
    write(
        &db,
        "node",
        "arr",
        indexed_batch("n1", vec![(0..n).collect()], &[5.0, 6.0]),
    );
    let hist = query_f64(&db, "SELECT unnest(values) FROM value_array_hist");
    assert_eq!(bits(&hist), vec![0, 2.0f64.to_bits()]);
}

// --- axis floats ----------------------------------------------------------------

fn axis_floats(db: &Path, sql: &str) -> Vec<u64> {
    bits(&query_f64(db, sql))
}

#[test]
fn axis_float_storage_normalizes_negative_zero() {
    let (_d, db) = fresh_db();
    let mat = CanonicalAxis {
        length: 2,
        properties: Default::default(),
        body: AxisBody::FloatMaterialized {
            coords: vec![-0.0, 1.0],
        },
    };
    let par = CanonicalAxis {
        length: 3,
        properties: Default::default(),
        body: AxisBody::FloatParametric {
            start: -0.0,
            step: 1.0,
        },
    };
    let period = CanonicalAxis {
        length: 1,
        properties: Default::default(),
        body: AxisBody::Period {
            positions: vec![PeriodPos {
                start_ts: 0,
                end_ts: 3_600_000_000,
                years_presented: Some(-0.0),
            }],
        },
    };
    let ids = with_write_txn(&db, "t", "", |conn, _c| {
        let mut cache = AxisCache::new();
        Ok((
            dedup_or_insert(conn, &mut cache, &mat)?,
            dedup_or_insert(conn, &mut cache, &par)?,
            dedup_or_insert(conn, &mut cache, &period)?,
        ))
    })
    .unwrap();
    assert_eq!(
        axis_floats(&db, "SELECT coord FROM axis_float ORDER BY pos"),
        vec![0, 1.0f64.to_bits()]
    );
    assert_eq!(
        axis_floats(
            &db,
            "SELECT p_start_dbl FROM axis WHERE p_start_dbl IS NOT NULL"
        ),
        vec![0]
    );
    assert_eq!(
        axis_floats(&db, "SELECT years_presented FROM axis_period"),
        vec![0]
    );

    // The +0.0 twins dedup onto the same rows (fresh cache: DB byte-compare).
    let twin = |a: &CanonicalAxis| {
        let mut t = a.clone();
        match &mut t.body {
            AxisBody::FloatMaterialized { coords } => coords[0] = 0.0,
            AxisBody::FloatParametric { start, .. } => *start = 0.0,
            AxisBody::Period { positions } => positions[0].years_presented = Some(0.0),
            _ => unreachable!(),
        }
        t
    };
    let again = with_write_txn(&db, "t", "", |conn, _c| {
        let mut cache = AxisCache::new();
        Ok((
            dedup_or_insert(conn, &mut cache, &twin(&mat))?,
            dedup_or_insert(conn, &mut cache, &twin(&par))?,
            dedup_or_insert(conn, &mut cache, &twin(&period))?,
        ))
    })
    .unwrap();
    assert_eq!(ids, again);
}

// --- out-of-contract callers ------------------------------------------------

/// `history::write_value` never re-canonicalizes; a hand-built value holding
/// `-0.0` is caught by its debug assertion.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "non-canonical value float")]
fn write_value_debug_asserts_canonical_floats() {
    let (_d, db) = fresh_db();
    let value = CanonicalValue {
        tier: Tier::Scalar,
        value_kind: ValueKind::Float,
        axes: vec![],
        shape: ShapedData::Scalar(ScalarValue::Float(-0.0)),
    };
    let key = ValueKey {
        entity_id: 1,
        parameter_definition_id: 1,
        alternative_id: 1,
    };
    let _ = with_write_txn(&db, "t", "", |conn, c| {
        let mut cache = AxisCache::new();
        write_value(
            conn,
            c,
            key,
            &value,
            &mut cache,
            &WriteOptions::default(),
            None,
        )
    });
}
