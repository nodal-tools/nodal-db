//! Unchanged writes (skip_unchanged_writes_PLAN P1, §6.2; critique C1-C3).
//!
//! A write whose stored form equals the current stored value writes
//! nothing in any tier: no close-out, no insert, no `_hist` row. A
//! `commit_changes` call (and its wrappers) whose every item is a known
//! no-op makes no commit (D1): it reports `commit_id == 0` and the head does
//! not move. The census oracle (critique C2) checks every no-op call shape:
//! the row count and an order-independent content hash of every base table
//! are equal before and after.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{
    commit_changes, delete_objects, delete_values, head_commit, test_hooks, write_parameter,
    DeleteKey, Segment,
};
use nodal_db_core::axis::AxisCache;
use nodal_db_core::canon::CanonicalValue;
use nodal_db_core::conn::with_write_txn;
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis, CANONICAL_NAN_BITS};
use nodal_db_core::error::DbError;
use nodal_db_core::history::write_value;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    LongRow, ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, ScalarValue, ShapedData,
    StructuralChange, Tier, ValueKey, ValueKind, WarningKind, WriteOptions, WriteOutcome,
    WriteReport,
};

// --- fixtures ----------------------------------------------------------------

fn fresh(dir: &Path) -> PathBuf {
    let db = dir.join("skip.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    db
}

fn scalar_batch(entities: &[&str], vals: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(StringArray::from(entities.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn string_batch(entities: &[&str], vals: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(StringArray::from(entities.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

/// A rank-1 series per entity over `t` (Int64): an `Int64` measure lands in
/// the long tier, a dense `Float64` one in the array tier.
fn series(entities: &[&str], t: &[i64], vals: ArrayRef) -> RecordBatch {
    let mut ent = Vec::new();
    let mut tt = Vec::new();
    for e in entities {
        for &x in t {
            ent.push(*e);
            tt.push(x);
        }
    }
    let vtype = vals.data_type().clone();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("t", DataType::Int64, false),
            Field::new("value", vtype, false),
        ])),
        vec![
            Arc::new(StringArray::from(ent)) as ArrayRef,
            Arc::new(Int64Array::from(tt)) as ArrayRef,
            vals,
        ],
    )
    .unwrap()
}

fn long_batch(entities: &[&str], t: &[i64], vals: &[i64]) -> RecordBatch {
    series(entities, t, Arc::new(Int64Array::from(vals.to_vec())))
}

fn array_batch(entities: &[&str], t: &[i64], vals: &[f64]) -> RecordBatch {
    series(entities, t, Arc::new(Float64Array::from(vals.to_vec())))
}

fn seg(parameter: &str, batch: RecordBatch) -> Segment {
    Segment {
        class: "node".into(),
        parameter: parameter.into(),
        alternative: "Base".into(),
        batches: vec![batch],
        domains: vec![],
    }
}

fn commit(db: &Path, segments: Vec<Segment>) -> WriteReport {
    commit_with(db, segments, vec![], vec![], WriteOptions::default())
}

fn commit_with(
    db: &Path,
    segments: Vec<Segment>,
    structural: Vec<StructuralChange>,
    deletes: Vec<DeleteKey>,
    opts: WriteOptions,
) -> WriteReport {
    commit_changes(db, segments, structural, deletes, vec![], "t", "m", opts).expect("commit")
}

fn dk(parameter: &str, entity: &str) -> DeleteKey {
    DeleteKey {
        class: "node".into(),
        parameter: parameter.into(),
        alternative: "Base".into(),
        entity: entity.into(),
    }
}

/// The census oracle (critique C2): per base table, its row count and an
/// order-independent hash of its rows. Equal censuses before and after a
/// call mean the call changed no stored row anywhere.
fn census(db: &Path) -> Vec<(String, i64, i128)> {
    let conn = Connection::open(db).unwrap();
    let tables: Vec<String> = conn
        .prepare(
            "SELECT table_name FROM duckdb_tables() \
             WHERE database_name = current_database() AND schema_name = 'main' \
             ORDER BY table_name",
        )
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let out = tables
        .into_iter()
        .map(|t| {
            let (n, h): (i64, i128) = conn
                .query_row(
                    &format!("SELECT count(*), coalesce(sum(hash(x)::HUGEINT), 0) FROM \"{t}\" x"),
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            (t, n, h)
        })
        .collect();
    conn.close().unwrap();
    out
}

fn q(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

/// The audits a write may never leave dirty.
fn assert_audits_clean(db: &Path) {
    for a in [
        "audit_history_tiling",
        "audit_cross_tier_duplicate",
        "audit_array_integrity",
        "audit_index_accelerator",
        "audit_array_id_coherence",
        "audit_array_payload_xor",
        "audit_position_range",
    ] {
        assert_eq!(q(db, &format!("SELECT count(*) FROM {a}")), 0, "{a}");
    }
}

/// Run `call` and assert it was a full no-op: `commit_id == 0`, the head
/// did not move and the census is unchanged.
fn assert_noop(db: &Path, what: &str, call: impl FnOnce() -> WriteReport) -> WriteReport {
    let before = census(db);
    let head = head_commit(db).unwrap();
    let r = call();
    assert_eq!(r.commit_id, 0, "{what}: commit id of a no-op call");
    assert_eq!(head_commit(db).unwrap(), head, "{what}: head moved");
    assert_eq!(census(db), before, "{what}: a table changed");
    r
}

// --- one identical re-write per tier ------------------------------------------

/// Through `commit_changes`, for one entity (the point statements) and for
/// three (the set-based round), in every tier: the second write is
/// unchanged, writes no row anywhere and makes no commit.
#[test]
fn identical_rewrite_is_unchanged_in_every_tier_and_commits_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    for ents in [&["a"][..], &["b", "c", "d"][..]] {
        let n = ents.len();
        let vals: Vec<f64> = (0..n).map(|i| 1.5 + i as f64).collect();
        let segs = || {
            vec![
                seg("s", scalar_batch(ents, &vals)),
                seg(
                    "l",
                    long_batch(ents, &[0, 2, 5], &(0..3 * n as i64).collect::<Vec<_>>()),
                ),
                seg(
                    "a",
                    array_batch(
                        ents,
                        &[0, 1, 2, 3],
                        &(0..4 * n).map(|i| i as f64 * 0.25).collect::<Vec<_>>(),
                    ),
                ),
            ]
        };
        let first = commit(&db, segs());
        assert!(first.commit_id > 0);
        assert_eq!(first.values_written, 3 * n);
        let r = assert_noop(&db, "identical rewrite", || commit(&db, segs()));
        assert_eq!((r.values_written, r.values_unchanged), (0, 3 * n), "{r:?}");
        assert_eq!((r.rows_written, r.rows_closed), (0, 0));
        assert!(r.tiers.is_empty(), "{:?}", r.tiers);
        assert_eq!(r.axes_created, 0);
        assert_audits_clean(&db);
    }
}

/// Through `history::write_value` inside a caller's own transaction: the
/// outcome says unchanged and no value row is written. D1 does not apply
/// to direct writers: the caller's commit row stays (critique C2 scope).
#[test]
fn direct_identical_rewrite_reports_unchanged_per_tier() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    let ax = |len: i32| CanonicalAxis {
        length: len,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerParametric { start: 0, step: 1 },
    };
    let scalar = CanonicalValue {
        tier: Tier::Scalar,
        value_kind: ValueKind::Duration,
        axes: vec![],
        shape: ShapedData::Scalar(ScalarValue::Duration {
            months: 2,
            micros: -5,
        }),
    };
    let long = CanonicalValue {
        tier: Tier::Long,
        value_kind: ValueKind::String,
        axes: vec![ax(4)],
        shape: ShapedData::Long(
            [0, 3]
                .iter()
                .map(|&p| LongRow {
                    positions: vec![p],
                    value: ScalarValue::Str(format!("s{p}")),
                })
                .collect(),
        ),
    };
    let array = CanonicalValue {
        tier: Tier::Array,
        value_kind: ValueKind::Float,
        axes: vec![ax(3)],
        shape: ShapedData::Array(vec![f64::from_bits(CANONICAL_NAN_BITS), 0.0, 2.0]),
    };
    let write_all = || -> Vec<WriteOutcome> {
        with_write_txn(&db, "t", "direct", |conn, c| {
            let mut cache = AxisCache::new();
            let o = WriteOptions::default();
            let mut out = Vec::new();
            for (e, v) in [(1, &scalar), (2, &long), (3, &array)] {
                let key = ValueKey {
                    entity_id: e,
                    parameter_definition_id: 900,
                    alternative_id: 900,
                };
                out.push(write_value(conn, c, key, v, &mut cache, &o, Some(7))?);
            }
            Ok(out)
        })
        .unwrap()
    };
    let first = write_all();
    assert!(first.iter().all(|o| !o.unchanged));
    let rows = |db: &Path| {
        q(
            db,
            "SELECT (SELECT count(*) FROM value_scalar) + (SELECT count(*) FROM value_long) \
                  + (SELECT count(*) FROM value_array) + (SELECT count(*) FROM value_array_hist)",
        )
    };
    let before = rows(&db);
    let second = write_all();
    assert_eq!(rows(&db), before, "an unchanged write wrote rows");
    for (o, (tier, version, cells)) in second.iter().zip([
        (Tier::Scalar, 1, 0),
        (Tier::Long, 1, 2),
        (Tier::Array, 1, 0),
    ]) {
        assert!(o.unchanged, "{o:?}");
        assert_eq!(
            (
                o.tier,
                o.version,
                o.cells_unchanged,
                o.rows_written,
                o.rows_closed
            ),
            (Some(tier), version, cells, 0, 0),
            "{o:?}"
        );
    }
    assert_audits_clean(&db);
}

// --- canonical-equality edges (critique C3) ------------------------------------

/// Write `first`, then `second` to one direct key (binding `pt1` / `pt2`);
/// returns whether the second write was unchanged.
fn direct_pair(
    db: &Path,
    e: i64,
    first: &CanonicalValue,
    pt1: Option<i64>,
    second: &CanonicalValue,
    pt2: Option<i64>,
) -> bool {
    let key = ValueKey {
        entity_id: e,
        parameter_definition_id: 901,
        alternative_id: 901,
    };
    let w = |v: &CanonicalValue, pt: Option<i64>| {
        with_write_txn(db, "t", "edge", |conn, c| {
            write_value(
                conn,
                c,
                key,
                v,
                &mut AxisCache::new(),
                &WriteOptions::default(),
                pt,
            )
        })
        .unwrap()
    };
    w(first, pt1);
    w(second, pt2).unchanged
}

fn sv(v: ScalarValue) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Scalar,
        value_kind: v.kind(),
        axes: vec![],
        shape: ShapedData::Scalar(v),
    }
}

#[test]
fn equality_edges_err_toward_changed() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    let nan = f64::from_bits(CANONICAL_NAN_BITS);
    let f = |x: f64| sv(ScalarValue::Float(x));
    // NaN (canonical) round-trips bit-exact: unchanged.
    assert!(direct_pair(&db, 1, &f(nan), None, &f(nan), None), "NaN");
    // The next float up: changed.
    assert!(
        !direct_pair(
            &db,
            2,
            &f(1.0),
            None,
            &f(f64::from_bits(1.0f64.to_bits() + 1)),
            None
        ),
        "1.0 vs next_up"
    );
    // Int 1 vs float 1.0: another kind, changed.
    assert!(
        !direct_pair(&db, 3, &f(1.0), None, &sv(ScalarValue::Int(1)), None),
        "int vs float"
    );
    // The binding is part of equality (D8): another member, or a stored
    // NULL against a member, is a change (which rebinds).
    assert!(
        !direct_pair(&db, 4, &f(2.0), Some(1), &f(2.0), Some(2)),
        "binding"
    );
    assert!(
        !direct_pair(&db, 5, &f(2.0), None, &f(2.0), Some(1)),
        "NULL binding"
    );
    assert!(
        direct_pair(&db, 6, &f(2.0), Some(3), &f(2.0), Some(3)),
        "same binding"
    );
    // Strings compare by their stored bytes: NFC-equal but byte-different
    // spellings are a change (critique C3.2: never content_hash).
    let s = |x: &str| sv(ScalarValue::Str(x.into()));
    assert!(
        !direct_pair(&db, 7, &s("\u{e9}"), None, &s("e\u{301}"), None),
        "NFC"
    );
    assert!(
        direct_pair(&db, 8, &s("\u{e9}"), None, &s("\u{e9}"), None),
        "same bytes"
    );
    // Datetime and duration.
    let dt = |x: i64| sv(ScalarValue::DateTime(x));
    assert!(direct_pair(
        &db,
        9,
        &dt(1_600_000_000_000_001),
        None,
        &dt(1_600_000_000_000_001),
        None
    ));
    assert!(!direct_pair(
        &db,
        10,
        &dt(1_600_000_000_000_001),
        None,
        &dt(1_600_000_000_000_002),
        None
    ));
    // The same array payload on a shifted axis: changed.
    let arr = |start: i64| CanonicalValue {
        tier: Tier::Array,
        value_kind: ValueKind::Float,
        axes: vec![CanonicalAxis {
            length: 2,
            properties: AxisProperties::default(),
            body: AxisBody::IntegerParametric { start, step: 1 },
        }],
        shape: ShapedData::Array(vec![1.0, 2.0]),
    };
    assert!(
        !direct_pair(&db, 11, &arr(0), None, &arr(1), None),
        "array axis"
    );
    assert!(
        direct_pair(&db, 12, &arr(0), None, &arr(0), None),
        "array same"
    );
    // Same bounds, different interior: the v_min/v_max prefilter passes
    // and the payload bytes decide (changed).
    let arr3 = |mid: f64| CanonicalValue {
        tier: Tier::Array,
        value_kind: ValueKind::Float,
        axes: vec![CanonicalAxis {
            length: 3,
            properties: AxisProperties::default(),
            body: AxisBody::IntegerParametric { start: 0, step: 1 },
        }],
        shape: ShapedData::Array(vec![0.0, mid, 9.0]),
    };
    assert!(
        !direct_pair(&db, 13, &arr3(4.0), None, &arr3(5.0), None),
        "array interior"
    );
    assert_audits_clean(&db);
}

/// `-0.0` and `0.0` are one canonical value (canon normalizes), so a
/// re-import of `-0.0` over `0.0` is unchanged; the same for NaN payloads.
#[test]
fn canonical_float_spellings_are_unchanged_through_the_api() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    commit(
        &db,
        vec![seg("s", scalar_batch(&["a", "b"], &[0.0, f64::NAN]))],
    );
    let other_nan = f64::from_bits(0x7ff8_0000_0000_0abc);
    let r = assert_noop(&db, "-0.0 / NaN", || {
        commit(
            &db,
            vec![seg("s", scalar_batch(&["a", "b"], &[-0.0, other_nan]))],
        )
    });
    assert_eq!(r.values_unchanged, 2);
}

/// A tz-aware timestamp equal to the stored UTC instant is unchanged; the
/// TimezoneNormalized warning is still reported, and alone it does not
/// make the call commit.
#[test]
fn tz_aware_timestamp_equal_to_the_stored_instant_is_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    let batch = |tz: Option<&str>| {
        let vals = TimestampMicrosecondArray::from(vec![1_700_000_000_000_000i64]);
        let vals = match tz {
            Some(tz) => vals.with_timezone(tz),
            None => vals,
        };
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("entity", DataType::Utf8, false),
                Field::new(
                    "value",
                    DataType::Timestamp(TimeUnit::Microsecond, tz.map(Into::into)),
                    false,
                ),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                Arc::new(vals) as ArrayRef,
            ],
        )
        .unwrap()
    };
    commit(&db, vec![seg("when", batch(None))]);
    let r = assert_noop(&db, "tz", || {
        commit(&db, vec![seg("when", batch(Some("+02:00")))])
    });
    assert_eq!(r.values_unchanged, 1);
    assert!(r
        .warnings
        .iter()
        .any(|w| matches!(w.kind, WarningKind::TimezoneNormalized { .. })));
}

/// A stored `values_int` payload (or a current stub) never equals a new
/// write: the write goes ahead.
#[test]
fn stored_values_int_and_current_stub_are_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    commit(
        &db,
        vec![seg("a", array_batch(&["a"], &[0, 1], &[1.0, 2.0]))],
    );
    commit(&db, vec![seg("s", scalar_batch(&["a"], &[3.0]))]);
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "UPDATE value_array SET values = NULL, values_int = [1, 2]; \
             UPDATE value_scalar SET content_retained = FALSE \
               WHERE valid_to_commit IS NULL;",
        )
        .unwrap();
        conn.close().unwrap();
    }
    let r = commit(
        &db,
        vec![
            seg("a", array_batch(&["a"], &[0, 1], &[1.0, 2.0])),
            seg("s", scalar_batch(&["a"], &[3.0])),
        ],
    );
    assert!(r.commit_id > 0);
    assert_eq!((r.values_written, r.values_unchanged), (2, 0), "{r:?}");
}

// --- mixed and no-op calls (D1, critique C2) -----------------------------------

#[test]
fn mixed_call_commits_only_the_changed_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    commit(&db, vec![seg("s", scalar_batch(&["a", "b"], &[1.0, 2.0]))]);
    let rows = q(&db, "SELECT count(*) FROM value_scalar");
    let r = commit(&db, vec![seg("s", scalar_batch(&["a", "b"], &[1.0, 2.5]))]);
    assert!(r.commit_id > 0);
    assert_eq!(head_commit(&db).unwrap(), r.commit_id);
    assert_eq!(
        (
            r.values_written,
            r.values_unchanged,
            r.rows_written,
            r.rows_closed
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(r.tiers, vec![Tier::Scalar]);
    assert_eq!(q(&db, "SELECT count(*) FROM value_scalar"), rows + 1);
    assert_eq!(
        q(
            &db,
            &format!(
                "SELECT count(*) FROM value_scalar WHERE valid_from_commit = {}",
                r.commit_id
            )
        ),
        1,
        "only the changed value carries the new commit"
    );
}

/// Every no-op call shape: unchanged values, structural NoOps, deletes that
/// miss (absent value, unknown referent), object deletes that find
/// nothing, and all of them together. None commits; the census is equal.
#[test]
fn every_no_op_call_shape_commits_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    commit_with(
        &db,
        vec![seg("s", scalar_batch(&["a"], &[1.0]))],
        vec![StructuralChange::CreateAlternative {
            name: "alt1".into(),
            description: None,
        }],
        vec![],
        WriteOptions::default(),
    );
    let existing_alt = || StructuralChange::CreateAlternative {
        name: "alt1".into(),
        description: None,
    };
    assert_noop(&db, "structural NoOp", || {
        commit_with(
            &db,
            vec![],
            vec![existing_alt()],
            vec![],
            WriteOptions::default(),
        )
    });
    let r = assert_noop(&db, "delete of an absent value", || {
        delete_values(&db, vec![dk("s", "b")], "t", "m", WriteOptions::default()).unwrap()
    });
    assert!(matches!(
        r.warnings[0].kind,
        WarningKind::UnknownReferent { .. }
    ));
    commit(&db, vec![seg("s", scalar_batch(&["b"], &[1.0]))]);
    delete_values(&db, vec![dk("s", "b")], "t", "m", WriteOptions::default()).unwrap();
    let r = assert_noop(&db, "delete of a deleted value", || {
        delete_values(&db, vec![dk("s", "b")], "t", "m", WriteOptions::default()).unwrap()
    });
    assert!(matches!(
        r.warnings[0].kind,
        WarningKind::NothingToDelete { .. }
    ));
    assert_noop(&db, "delete with an unknown parameter", || {
        delete_values(
            &db,
            vec![dk("nope", "a")],
            "t",
            "m",
            WriteOptions::default(),
        )
        .unwrap()
    });
    assert_noop(&db, "object delete that finds nothing", || {
        delete_objects(
            &db,
            vec![(
                ObjectDeleteKey {
                    kind: ObjectKind::Alternative,
                    class: None,
                    target: ObjectRef::Name("nope".into()),
                },
                ObjectDeleteOpts { cascade: true },
            )],
            "t",
            "m",
            WriteOptions::default(),
        )
        .unwrap()
    });
    let r = assert_noop(&db, "everything at once", || {
        commit_with(
            &db,
            vec![seg("s", scalar_batch(&["a"], &[1.0]))],
            vec![existing_alt()],
            vec![dk("s", "b"), dk("s", "zz")],
            WriteOptions::default(),
        )
    });
    assert_eq!(r.values_unchanged, 1);
    // force_replace stubs superseded rows only: an unchanged write
    // supersedes nothing, so it is still a no-op (D3(i)).
    assert_noop(&db, "force_replace", || {
        commit_with(
            &db,
            vec![seg("s", scalar_batch(&["a"], &[1.0]))],
            vec![],
            vec![],
            WriteOptions {
                force_replace: true,
                ..WriteOptions::default()
            },
        )
    });
    assert_audits_clean(&db);
}

/// An unchanged value next to an auto-created referent (here a new
/// parameter definition, whose value is new) commits. (Through the API an
/// auto-create always comes with a new value; the predicate's own refusal
/// of an `AutoCreated` warning is unit-tested in `api`.)
#[test]
fn an_auto_created_referent_commits() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    commit(&db, vec![seg("s", scalar_batch(&["a"], &[1.0]))]);
    let before = head_commit(&db).unwrap();
    let r = commit(
        &db,
        vec![
            seg("s", scalar_batch(&["a"], &[1.0])),
            seg("s2", scalar_batch(&["a"], &[1.0])),
        ],
    );
    assert!(r.commit_id > before, "{r:?}");
    assert_eq!((r.values_written, r.values_unchanged), (1, 1));
    assert!(r
        .warnings
        .iter()
        .any(|w| matches!(w.kind, WarningKind::AutoCreated)));
}

/// A stale no-op is still Stale (the precondition runs first); a fresh one
/// commits nothing.
#[test]
fn expect_commit_on_a_no_op_call() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    let first = commit(&db, vec![seg("s", scalar_batch(&["a"], &[1.0]))]);
    let opts = |c: i64| WriteOptions {
        expect_commit: Some(c),
        ..WriteOptions::default()
    };
    let err = commit_changes(
        &db,
        vec![seg("s", scalar_batch(&["a"], &[1.0]))],
        vec![],
        vec![],
        vec![],
        "t",
        "m",
        opts(first.commit_id - 1),
    )
    .expect_err("stale");
    assert!(matches!(err, DbError::Stale { .. }), "{err:?}");
    let r = assert_noop(&db, "fresh no-op", || {
        commit_with(
            &db,
            vec![seg("s", scalar_batch(&["a"], &[1.0]))],
            vec![],
            vec![],
            opts(first.commit_id),
        )
    });
    assert_eq!(r.values_unchanged, 1);
    // The head did not move, so the same expectation still holds.
    let r = commit_with(
        &db,
        vec![seg("s", scalar_batch(&["a"], &[2.0]))],
        vec![],
        vec![],
        opts(first.commit_id),
    );
    assert!(r.commit_id > first.commit_id);
}

/// A conflict retry re-runs the probes: the outcome of a no-op call and of
/// a mixed one is the same as without the conflict.
#[test]
fn a_conflict_retry_re_probes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    commit(
        &db,
        vec![
            seg("s", scalar_batch(&["a", "b"], &[1.0, 2.0])),
            seg("l", long_batch(&["a"], &[0, 1], &[1, 2])),
        ],
    );
    test_hooks::inject_conflicts(1);
    let r = assert_noop(&db, "retried no-op", || {
        commit(
            &db,
            vec![
                seg("s", scalar_batch(&["a", "b"], &[1.0, 2.0])),
                seg("l", long_batch(&["a"], &[0, 1], &[1, 2])),
            ],
        )
    });
    assert_eq!(r.values_unchanged, 3);
    test_hooks::inject_conflicts(1);
    let r = commit(
        &db,
        vec![
            seg("s", scalar_batch(&["a", "b"], &[1.0, 2.5])),
            seg("l", long_batch(&["a"], &[0, 1], &[1, 2])),
        ],
    );
    assert!(r.commit_id > 0);
    assert_eq!((r.values_written, r.values_unchanged), (1, 2), "{r:?}");
    assert_audits_clean(&db);
}

/// `write_parameter` (the Python skin's `write_parameter`) of an identical
/// value: commit 0, one value unchanged.
#[test]
fn write_parameter_identical_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    let w = |s: &str| {
        write_parameter(
            &db,
            "node",
            "label",
            "Base",
            string_batch(&["a"], &[s]),
            &[],
            WriteOptions::default(),
            "t",
            "m",
        )
        .unwrap()
    };
    assert!(w("x").commit_id > 0);
    let r = assert_noop(&db, "write_parameter", || w("x"));
    assert_eq!((r.values_written, r.values_unchanged), (0, 1));
    assert!(w("y").commit_id > 0);
}
