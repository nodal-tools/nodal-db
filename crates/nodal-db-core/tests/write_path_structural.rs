//! Structural tests of the columnar-bind write path
//! (appender_write_path_PLAN §7.1 large cases, §7.3 rollback and
//! read-after-write). The physical-write golden (`write_path_golden.rs`)
//! pins small values byte for byte; these cover the shapes too large for a
//! golden: values crossing the 16 384-cell insert chunk several times and a
//! 525 600-element array, each read back and compared with the input, with
//! the physical row order (`rowid`) checked against canonical cell order
//! (critique C6: the long tier's series-ordered zone maps rely on it).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::types::Value;
use duckdb::Connection;

use nodal_db_core::api::{commit_changes, Segment};
use nodal_db_core::axis::AxisCache;
use nodal_db_core::canon::CanonicalValue;
use nodal_db_core::conn::with_write_txn;
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis, CANONICAL_NAN_BITS};
use nodal_db_core::error::DbError;
use nodal_db_core::history::write_value;
use nodal_db_core::schema_init::{init_schema, init_schema_with_default};
use nodal_db_core::types::{
    LongRow, ObjectDeleteKey, ObjectDeleteOpts, ObjectDeleteResult, ObjectKind, ObjectRef,
    ScalarValue, ShapedData, Tier, ValueKey, ValueKind, WriteOptions,
};

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.duckdb");
    init_schema(&path).unwrap();
    (dir, path)
}

fn key(e: i64) -> ValueKey {
    ValueKey {
        entity_id: e,
        parameter_definition_id: 1,
        alternative_id: 1,
    }
}

fn int_axis(len: i32) -> CanonicalAxis {
    CanonicalAxis {
        length: len,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerParametric { start: 0, step: 1 },
    }
}

/// Deterministic, canonical cell values with a sprinkling of NaN.
fn cell(i: usize) -> f64 {
    if i % 4099 == 7 {
        f64::from_bits(CANONICAL_NAN_BITS)
    } else {
        (i as f64) * 0.25 - 1000.0
    }
}

/// A rank-1 long value over every `stride`-th position of an `n`-long axis.
fn long_value(n: i32, stride: i32) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Long,
        value_kind: ValueKind::Float,
        axes: vec![int_axis(n)],
        shape: ShapedData::Long(
            (0..n)
                .step_by(stride as usize)
                .map(|p| LongRow {
                    positions: vec![p],
                    value: ScalarValue::Float(cell(p as usize)),
                })
                .collect(),
        ),
    }
}

fn write(db: &Path, k: ValueKey, v: &CanonicalValue) {
    with_write_txn(db, "t", "w", |conn, c| {
        write_value(
            conn,
            c,
            k,
            v,
            &mut AxisCache::new(),
            &WriteOptions::default(),
            None,
        )
    })
    .expect("write");
}

/// The live long rows of `k` in PHYSICAL order: (pos, i1_num, v_float bits).
fn live_long_by_rowid(db: &Path, k: ValueKey) -> Vec<(i32, i64, u64)> {
    let conn = Connection::open(db).unwrap();
    let rows = conn
        .prepare(
            "SELECT i1_pos, i1_num, v_float FROM value_long \
             WHERE entity_id = ? AND valid_to_commit IS NULL ORDER BY rowid",
        )
        .unwrap()
        .query_map([k.entity_id], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get::<_, f64>(2)?.to_bits()))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    conn.close().unwrap();
    rows
}

fn expected_long(v: &CanonicalValue) -> Vec<(i32, i64, u64)> {
    match &v.shape {
        ShapedData::Long(rows) => rows
            .iter()
            .map(|r| match r.value {
                ScalarValue::Float(x) => (r.positions[0], r.positions[0] as i64, x.to_bits()),
                _ => unreachable!(),
            })
            .collect(),
        _ => unreachable!(),
    }
}

fn assert_audits_clean(db: &Path) {
    let conn = Connection::open(db).unwrap();
    for a in [
        "audit_history_tiling",
        "audit_cross_tier_duplicate",
        "audit_array_integrity",
        "audit_index_accelerator",
        "audit_array_id_coherence",
    ] {
        let n: i64 = conn
            .query_row(&format!("SELECT count(*) FROM {a}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "{a} must be empty");
    }
    conn.close().unwrap();
}

/// The `version` of every live long row of `k`, in physical order.
fn live_long_versions(db: &Path, k: ValueKey) -> Vec<i16> {
    let conn = Connection::open(db).unwrap();
    let v = conn
        .prepare(
            "SELECT version FROM value_long \
             WHERE entity_id = ? AND valid_to_commit IS NULL ORDER BY rowid",
        )
        .unwrap()
        .query_map([k.entity_id], |r| r.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    conn.close().unwrap();
    v
}

/// Write an `n`-cell long value, replace it by a sparser one (close-out +
/// reinsert), and check both generations against the input in physical order,
/// including the per-cell versions on both sides of every chunk boundary.
fn long_round_trip(n: i32) {
    let (_d, db) = fresh_db();
    let k = key(1);
    let v = long_value(n, 1);
    write(&db, k, &v);
    assert_eq!(live_long_by_rowid(&db, k), expected_long(&v), "n={n}");
    let vers = live_long_versions(&db, k);
    assert_eq!(vers.len(), n as usize, "n={n}");
    assert!(
        vers.iter().all(|&x| x == 1),
        "n={n}: fresh cells are version 1"
    );
    let v2 = long_value(n, 3);
    write(&db, k, &v2);
    assert_eq!(
        live_long_by_rowid(&db, k),
        expected_long(&v2),
        "n={n} replaced"
    );
    assert!(
        live_long_versions(&db, k).iter().all(|&x| x == 2),
        "n={n}: every replaced cell is version 2"
    );
    // Back to dense: versions now differ cell by cell (3 where the sparse
    // generation had a cell, else 2), so a chunk-misaligned version column
    // would show.
    write(&db, k, &v);
    assert_eq!(
        live_long_by_rowid(&db, k),
        expected_long(&v),
        "n={n} dense again"
    );
    let want: Vec<i16> = (0..n).map(|p| if p % 3 == 0 { 3 } else { 2 }).collect();
    assert_eq!(live_long_versions(&db, k), want, "n={n}: per-cell versions");
    assert_audits_clean(&db);
}

/// 16 384 cells = exactly one full insert chunk, no tail.
#[test]
fn long_value_of_exactly_one_insert_chunk_round_trips_in_cell_order() {
    long_round_trip(16_384);
}

/// 16 385 cells = one full insert chunk + 1.
#[test]
fn long_value_one_cell_past_the_insert_chunk_round_trips_in_cell_order() {
    long_round_trip(16_385);
}

/// 70 000 cells = four full chunks + a tail.
#[test]
fn long_value_across_several_insert_chunks_round_trips_in_cell_order() {
    long_round_trip(70_000);
}

#[test]
fn array_of_525600_elements_round_trips_bit_exactly() {
    // One row, one bound DOUBLE[] parameter (arrays cannot be chunked).
    let (_d, db) = fresh_db();
    let vals: Vec<f64> = (0..525_600).map(cell).collect();
    let v = CanonicalValue {
        tier: Tier::Array,
        value_kind: ValueKind::Float,
        axes: vec![int_axis(vals.len() as i32)],
        shape: ShapedData::Array(vals.clone()),
    };
    write(&db, key(1), &v);
    // Replace it in place (the bound-list UPDATE), then check the live row.
    let vals2: Vec<f64> = vals.iter().map(|x| x * 2.0).collect();
    let v2 = CanonicalValue {
        shape: ShapedData::Array(vals2.clone()),
        ..v
    };
    write(&db, key(1), &v2);
    let conn = Connection::open(&db).unwrap();
    for (sql, want) in [
        ("SELECT values FROM value_array", &vals2),
        ("SELECT values FROM value_array_hist", &vals),
    ] {
        let got: Value = conn.query_row(sql, [], |r| r.get(0)).unwrap();
        let Value::List(items) = got else {
            panic!("not a list: {sql}")
        };
        assert_eq!(items.len(), want.len(), "{sql}");
        for (i, (g, w)) in items.iter().zip(want.iter()).enumerate() {
            match g {
                Value::Double(x) => assert_eq!(x.to_bits(), w.to_bits(), "{sql} elem {i}"),
                other => panic!("{sql} elem {i}: {other:?}"),
            }
        }
    }
    conn.close().unwrap();
    assert_audits_clean(&db);
}

// --- §7.3 rollback and read-after-write ------------------------------------

fn table_counts(db: &Path) -> Vec<(String, i64)> {
    let conn = Connection::open(db).unwrap();
    let v = [
        "commit",
        "axis",
        "axis_integer",
        "value_scalar",
        "value_long",
        "value_array",
        "value_array_hist",
    ]
    .iter()
    .map(|t| {
        let n: i64 = conn
            .query_row(&format!("SELECT count(*) FROM {t}"), [], |r| r.get(0))
            .unwrap();
        (t.to_string(), n)
    })
    .collect();
    conn.close().unwrap();
    v
}

#[test]
fn a_failing_last_key_rolls_back_the_whole_commit() {
    let (_d, db) = fresh_db();
    write(&db, key(1), &long_value(10, 1));
    let before = table_counts(&db);

    // Several good keys of every tier (one on a new materialized axis), then
    // a long value whose two cells share a position: the columnar insert
    // itself raises the unique violation (an intra-statement duplicate).
    let dup = CanonicalValue {
        tier: Tier::Long,
        value_kind: ValueKind::Float,
        axes: vec![int_axis(4)],
        shape: ShapedData::Long(vec![
            LongRow {
                positions: vec![2],
                value: ScalarValue::Float(1.0),
            },
            LongRow {
                positions: vec![2],
                value: ScalarValue::Float(2.0),
            },
        ]),
    };
    let err = with_write_txn(&db, "t", "doomed", |conn, c| {
        let mut cache = AxisCache::new();
        let o = WriteOptions::default();
        let mat = CanonicalValue {
            tier: Tier::Long,
            value_kind: ValueKind::Float,
            axes: vec![CanonicalAxis {
                length: 3,
                properties: AxisProperties::default(),
                body: AxisBody::IntegerMaterialized {
                    coords: vec![1, 5, 6],
                },
            }],
            shape: ShapedData::Long(vec![LongRow {
                positions: vec![1],
                value: ScalarValue::Float(3.0),
            }]),
        };
        write_value(conn, c, key(1), &long_value(10, 2), &mut cache, &o, None)?;
        write_value(conn, c, key(2), &mat, &mut cache, &o, None)?;
        let arr = CanonicalValue {
            tier: Tier::Array,
            value_kind: ValueKind::Float,
            axes: vec![int_axis(2)],
            shape: ShapedData::Array(vec![1.0, 2.0]),
        };
        write_value(conn, c, key(3), &arr, &mut cache, &o, None)?;
        let sc = CanonicalValue {
            tier: Tier::Scalar,
            value_kind: ValueKind::Float,
            axes: vec![],
            shape: ShapedData::Scalar(ScalarValue::Float(4.0)),
        };
        write_value(conn, c, key(4), &sc, &mut cache, &o, None)?;
        write_value(conn, c, key(5), &dup, &mut cache, &o, None)
    })
    .expect_err("the duplicate cell must fail the commit");
    assert!(matches!(err, DbError::Duckdb(_)), "got {err:?}");
    assert!(
        err.to_string().contains("UNIQUE constraint violation"),
        "got {err}"
    );
    assert_eq!(table_counts(&db), before, "nothing of the failed commit");

    // The array insert consumed a sequence value; the stride-2 parity holds.
    let conn = Connection::open(&db).unwrap();
    let last: Option<i64> = conn
        .query_row(
            "SELECT last_value FROM duckdb_sequences() WHERE sequence_name = 'seq_value_array'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    if let Some(v) = last {
        assert_eq!(v % 2, 1, "seq_value_array parity");
    }
    conn.close().unwrap();
    // And the database still takes the same writes afterwards.
    write(&db, key(5), &long_value(4, 1));
    assert_audits_clean(&db);
}

fn long_batch(entity: &str, coords: &[i64], vals: &[i64]) -> RecordBatch {
    let n = coords.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; n])) as ArrayRef,
            Arc::new(Int64Array::from(coords.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

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

fn seg(parameter: &str, alternative: &str, b: RecordBatch) -> Segment {
    Segment {
        class: "node".into(),
        parameter: parameter.into(),
        alternative: alternative.into(),
        batches: vec![b],
        domains: vec![],
    }
}

#[test]
fn rows_written_earlier_in_a_commit_are_visible_to_its_object_delete() {
    // The columnar insert is one ordinary SQL statement: its rows are
    // visible at once to every later statement of the same transaction. A
    // RESTRICT object delete later in the same commit counts them.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("api.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();
    commit_changes(
        &db,
        vec![seg("demand", "altV", scalar_batch("n1", 1.0))],
        vec![],
        vec![],
        vec![],
        "t",
        "seed",
        WriteOptions::default(),
    )
    .unwrap();
    let r = commit_changes(
        &db,
        vec![seg(
            "flow",
            "altV",
            long_batch("n1", &[0, 3, 7], &[1, 2, 3]),
        )],
        vec![],
        vec![],
        vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Alternative,
                class: None,
                target: ObjectRef::Name("altV".into()),
            },
            ObjectDeleteOpts { cascade: false },
        )],
        "t",
        "write then restrict",
        WriteOptions::default(),
    )
    .unwrap();
    let ObjectDeleteResult::Restricted { blockers } = &r.object_deletes[0].result else {
        panic!("expected Restricted, got {:?}", r.object_deletes[0].result)
    };
    let long_rows = blockers
        .iter()
        .find(|b| b.table == "value_long")
        .map(|b| b.count);
    assert_eq!(
        long_rows,
        Some(3),
        "the 3 long cells written earlier in this commit are visible"
    );
}

// --- axis child rows (plan D8) -----------------------------------------------

/// Materialized axes larger than one columnar chunk: their child rows must
/// round-trip (a fresh dedup reconstructs each from the DB and byte-compares
/// it, so it returns the same id), and the label ids minted by the column
/// DEFAULT must follow position order.
#[test]
fn materialized_axes_across_a_chunk_round_trip() {
    use nodal_db_core::axis::dedup_or_insert;
    use nodal_db_core::encoding::{LabelPos, TimePatternPos, TimePatternTerm};

    let (_d, db) = fresh_db();
    let n = 16_390usize;
    let axes = [
        CanonicalAxis {
            length: n as i32,
            properties: AxisProperties::default(),
            body: AxisBody::DatetimeMaterialized {
                coords: (0..n as i64).map(|i| i * 3_600_000_000 + i % 5).collect(),
            },
        },
        CanonicalAxis {
            length: n as i32,
            properties: AxisProperties::default(),
            body: AxisBody::Label {
                positions: (0..n)
                    .map(|i| LabelPos {
                        ordinal: i as i32,
                        label: format!("l{i:05}"),
                    })
                    .collect(),
            },
        },
        CanonicalAxis {
            length: 3,
            properties: AxisProperties::default(),
            body: AxisBody::TimePattern {
                positions: (0..3)
                    .map(|i| TimePatternPos {
                        ordinal: i,
                        terms: (1..=(i + 1) as i16)
                            .map(|m| TimePatternTerm {
                                unit: "M".into(),
                                range_start: m,
                                range_end: m,
                            })
                            .collect(),
                    })
                    .collect(),
            },
        },
    ];
    let dedup_all = |msg: &str| -> Vec<i64> {
        with_write_txn(&db, "t", msg, |conn, _c| {
            let mut cache = AxisCache::new();
            axes.iter()
                .map(|a| dedup_or_insert(conn, &mut cache, a))
                .collect()
        })
        .unwrap()
    };
    let ids = dedup_all("axes");
    assert_eq!(
        dedup_all("axes again"),
        ids,
        "every axis reconstructs byte-identically"
    );

    let conn = Connection::open(&db).unwrap();
    let out_of_order: i64 = conn
        .query_row(
            "SELECT count(*) FROM (SELECT id, lag(id) OVER (ORDER BY pos) AS prev \
             FROM axis_label WHERE axis_id = ?) WHERE id <= prev",
            [ids[1]],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(out_of_order, 0, "label ids follow position order");
    let terms: i64 = conn
        .query_row("SELECT count(*) FROM axis_time_pattern", [], |r| r.get(0))
        .unwrap();
    assert_eq!(terms, 6);
    conn.close().unwrap();
}

// ---------------------------------------------------------------------------
// Set-based scalar write (set_based_scalar_write_PLAN §5.2, §5.3)
// ---------------------------------------------------------------------------

fn scalar_names_batch(names: &[String], vals: &[f64]) -> RecordBatch {
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

fn q_i64(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

/// One commit of 16 385 new scalar entities crosses the 16 384-key round
/// (and the entity insert chunk) by one: ids are contiguous in batch order,
/// every value is version 1, and a replace of all of them closes 16 385
/// rows at version 2.
#[test]
fn scalar_batch_one_key_past_the_round_writes_and_replaces() {
    const N: usize = 16_385;
    let (_d, db) = fresh_db();
    let names: Vec<String> = (0..N).map(|i| format!("e{i:05}")).collect();
    let vals: Vec<f64> = (0..N).map(|i| i as f64 * 0.5).collect();
    let r = commit_changes(
        &db,
        vec![seg("p", "Base", scalar_names_batch(&names, &vals))],
        vec![],
        vec![],
        vec![],
        "t",
        "fresh",
        WriteOptions::default(),
    )
    .expect("fresh");
    assert_eq!((r.values_written, r.rows_written, r.rows_closed), (N, N, 0));
    let ids: Vec<i64> = r
        .warnings
        .iter()
        .filter(|w| w.object_type == "entity")
        .map(|w| w.id)
        .collect();
    assert_eq!(ids.len(), N);
    assert!(ids.windows(2).all(|w| w[1] == w[0] + 1), "contiguous ids");
    // The id order is the batch order, and each value sits on its entity.
    assert_eq!(
        q_i64(
            &db,
            "SELECT count(*) FROM entity e JOIN value_scalar v ON v.entity_id = e.id \
             WHERE v.version = 1 AND v.v_float = (e.id - (SELECT min(id) FROM entity)) * 0.5 \
               AND e.name = printf('e%05d', e.id - (SELECT min(id) FROM entity))"
        ),
        N as i64
    );

    let vals2: Vec<f64> = (0..N).map(|i| i as f64 + 0.25).collect();
    let r = commit_changes(
        &db,
        vec![seg("p", "Base", scalar_names_batch(&names, &vals2))],
        vec![],
        vec![],
        vec![],
        "t",
        "replace",
        WriteOptions::default(),
    )
    .expect("replace");
    assert_eq!((r.values_written, r.rows_written, r.rows_closed), (N, N, N));
    assert!(r.warnings.is_empty());
    assert_eq!(
        q_i64(
            &db,
            "SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NULL AND version = 2"
        ),
        N as i64
    );
    assert_eq!(
        q_i64(
            &db,
            "SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NOT NULL \
               AND version = 1 AND content_retained"
        ),
        N as i64
    );
    assert_audits_clean(&db);
}

/// 30 segments × 1 entity in one commit: one set-based scalar write across
/// segments; the report counts every value once and the audits stay empty.
#[test]
fn scalar_batch_across_thirty_segments() {
    let (_d, db) = fresh_db();
    let segs = |v: f64| -> Vec<Segment> {
        (0..30)
            .map(|i| {
                seg(
                    &format!("p{i:02}"),
                    "Base",
                    scalar_batch("n1", v + i as f64),
                )
            })
            .collect()
    };
    let r = commit_changes(
        &db,
        segs(0.0),
        vec![],
        vec![],
        vec![],
        "t",
        "a",
        WriteOptions::default(),
    )
    .expect("first");
    assert_eq!(
        (r.values_written, r.rows_written, r.rows_closed),
        (30, 30, 0)
    );
    assert_eq!(r.tiers, vec![Tier::Scalar]);
    let r = commit_changes(
        &db,
        segs(100.0),
        vec![],
        vec![],
        vec![],
        "t",
        "b",
        WriteOptions::default(),
    )
    .expect("second");
    assert_eq!(
        (r.values_written, r.rows_written, r.rows_closed),
        (30, 30, 30)
    );
    assert_eq!(
        q_i64(
            &db,
            "SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NULL \
               AND version = 2 AND v_float >= 100"
        ),
        30
    );
    assert_audits_clean(&db);
}

/// A damaged key live in BOTH scalar and long (planted by direct SQL): a
/// scalar write follows the per-key precedence (scalar first), so it closes
/// the scalar row and leaves the long row alone, as `live_tier` did.
#[test]
fn scalar_write_on_a_doubly_live_key_closes_the_scalar_row() {
    let (_d, db) = fresh_db();
    let k = key(1);
    write(&db, k, &long_value(3, 1));
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id, \
               valid_from_commit, version, value_kind, v_float) \
             VALUES (1, 1, 1, (SELECT max(id) FROM \"commit\"), 1, 'float', 7.0)",
            [],
        )
        .unwrap();
        conn.close().unwrap();
    }
    let s = CanonicalValue {
        tier: Tier::Scalar,
        value_kind: ValueKind::Float,
        axes: vec![],
        shape: ShapedData::Scalar(ScalarValue::Float(8.0)),
    };
    let o = with_write_txn(&db, "t", "w", |conn, c| {
        write_value(
            conn,
            c,
            k,
            &s,
            &mut AxisCache::new(),
            &WriteOptions::default(),
            None,
        )
    })
    .expect("write");
    assert_eq!((o.version, o.rows_closed), (2, 1));
    assert_eq!(
        q_i64(
            &db,
            "SELECT count(*) FROM value_long WHERE valid_to_commit IS NULL"
        ),
        3,
        "the long row stays live"
    );
    assert_eq!(
        q_i64(
            &db,
            "SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NULL AND v_float = 8.0"
        ),
        1
    );
    assert_eq!(
        q_i64(
            &db,
            "SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NOT NULL AND v_float = 7.0"
        ),
        1
    );
}

/// Erasure removes rows with no close-out edge; a rewrite of the erased
/// alternative's scalars starts from what remains (nothing): version 1,
/// audits empty.
#[test]
fn scalars_rewritten_after_erasure_restart_at_version_one() {
    use nodal_db_core::erasure::{erase_alternative, EraseOpts};
    let (_d, db) = fresh_db();
    let names: Vec<String> = (0..5).map(|i| format!("r{i}")).collect();
    for (msg, v) in [("v1", 1.0), ("v2", 2.0)] {
        commit_changes(
            &db,
            vec![seg("p", "run1", scalar_names_batch(&names, &[v; 5]))],
            vec![],
            vec![],
            vec![],
            "t",
            msg,
            WriteOptions::default(),
        )
        .expect("write");
    }
    let rep = erase_alternative(
        &db,
        &ObjectDeleteKey {
            kind: ObjectKind::Alternative,
            class: None,
            target: ObjectRef::Name("run1".into()),
        },
        &EraseOpts::default(),
        "t",
    )
    .expect("erase");
    assert!(rep.erased);
    assert_eq!(q_i64(&db, "SELECT count(*) FROM value_scalar"), 0);
    commit_changes(
        &db,
        vec![seg("p", "run1", scalar_names_batch(&names, &[3.0; 5]))],
        vec![],
        vec![],
        vec![],
        "t",
        "rewrite",
        WriteOptions::default(),
    )
    .expect("rewrite");
    assert_eq!(
        q_i64(
            &db,
            "SELECT count(*) FROM value_scalar WHERE version = 1 AND valid_to_commit IS NULL"
        ),
        5
    );
    assert_audits_clean(&db);
}

/// Entities resolved by an earlier segment of the same commit (the
/// per-commit entity cache): a name auto-created by segment 1 is reused by
/// later segments, under any spelling DuckDB folds to it, and two such
/// spellings inside one later segment are still rejected as the Api error.
#[test]
fn entities_resolved_by_earlier_segments_of_a_commit_are_reused() {
    let (_d, db) = fresh_db();
    let r = commit_changes(
        &db,
        vec![
            seg("p1", "Base", scalar_batch("Äx", 1.0)),
            seg("p2", "Base", scalar_batch("Äx", 2.0)),
            seg("p3", "Base", scalar_batch("äx", 3.0)),
        ],
        vec![],
        vec![],
        vec![],
        "t",
        "m",
        WriteOptions::default(),
    )
    .expect("write");
    let minted = r
        .warnings
        .iter()
        .filter(|w| w.object_type == "entity")
        .count();
    assert_eq!(minted, 1, "one entity, reused by segments 2 and 3");
    assert_eq!(
        q_i64(&db, "SELECT count(DISTINCT entity_id) FROM value_scalar"),
        1
    );
    assert_eq!(
        q_i64(&db, "SELECT count(*) FROM value_scalar WHERE version = 1"),
        3
    );
    let err = commit_changes(
        &db,
        vec![
            seg("p1", "Base", scalar_batch("Äx", 4.0)),
            seg(
                "p2",
                "Base",
                scalar_names_batch(&["Äx".to_string(), "äx".to_string()], &[5.0, 6.0]),
            ),
        ],
        vec![],
        vec![],
        vec![],
        "t",
        "m",
        WriteOptions::default(),
    )
    .expect_err("an alias pair in one segment is ambiguous");
    assert_eq!(
        err.to_string(),
        "segment 2 (node:p2): entity names \"Äx\" and \"äx\" resolve to the same entity \
         (name resolution is case-insensitive); use one spelling per batch"
    );
    assert_audits_clean(&db);
}
