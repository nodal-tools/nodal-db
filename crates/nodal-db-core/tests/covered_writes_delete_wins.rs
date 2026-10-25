//! `WriteOptions::covered_writes`: a value write that a delete in the same
//! `commit_changes` call covers (skip_unchanged_writes_PLAN D9: a value
//! delete of the same key; D10: an object delete whose cascade reaches the
//! value) is refused by default (`CoveredWrites::Refuse`, tested at length
//! in `object_delete_covers_write.rs` and `write_path_structural.rs`). Under
//! `CoveredWrites::DeleteWins` the delete wins: each covered write is
//! dropped before anything runs and reported as a
//! `WarningKind::DroppedByDelete` warning, the deletes run as usual and the
//! uncovered writes proceed. A dropped write is not a write (the no-op
//! commit rule), and it auto-creates nothing. The delete wins only when it
//! takes effect: a write whose every covering delete fails to (a RESTRICT
//! delete that ends `Restricted`) makes the whole call refused, with
//! nothing committed. An object delete that reaches an object a structural
//! change of the same call creates is refused under both policies.
//!
//! The randomized test checks the defining property: a `DeleteWins` call
//! that commits leaves every table exactly as the same call without its
//! covered writes does under the default policy (which must then accept
//! it), and drops exactly the writes a model of the coverage predicts; a
//! call with a covered write whose covering deletes all fail to take effect
//! is refused and changes nothing.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{commit_changes, head_commit, DeleteKey, Segment};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    CoveredWrites, CoveringDelete, ObjectDeleteKey, ObjectDeleteOpts, ObjectDeleteResult,
    ObjectKind, ObjectRef, StructuralChange, WarningKind, WriteOptions, WriteReport,
};

const DELETE_WINS: WriteOptions = WriteOptions {
    force_replace: false,
    retain_override: None,
    expect_commit: None,
    covered_writes: CoveredWrites::DeleteWins,
};

// ---------------------------------------------------------------------------
// Batches and keys
// ---------------------------------------------------------------------------

fn scalar_batch(entities: &[&str], v: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(entities.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vec![v; entities.len()])) as ArrayRef,
        ],
    )
    .unwrap()
}

/// A rank-1 integer series per entity, `[v, v + 1, v + 2]` at t = 0..3:
/// integer values are the long tier, float values the array tier.
fn series_batch(entities: &[&str], v: i64, long: bool) -> RecordBatch {
    let mut names = Vec::new();
    let mut t = Vec::new();
    let mut ints = Vec::new();
    for e in entities {
        for i in 0..3 {
            names.push(*e);
            t.push(i);
            ints.push(v + i);
        }
    }
    let value: ArrayRef = if long {
        Arc::new(Int64Array::from(ints))
    } else {
        Arc::new(Float64Array::from(
            ints.iter().map(|&x| x as f64).collect::<Vec<_>>(),
        ))
    };
    let vtype = if long {
        DataType::Int64
    } else {
        DataType::Float64
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", vtype, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Int64Array::from(t)) as ArrayRef,
            value,
        ],
    )
    .unwrap()
}

fn seg(class: &str, parameter: &str, alternative: &str, b: RecordBatch) -> Segment {
    Segment {
        class: class.into(),
        parameter: parameter.into(),
        alternative: alternative.into(),
        batches: vec![b],
        domains: vec![],
    }
}

fn dkey(class: &str, parameter: &str, alternative: &str, entity: &str) -> DeleteKey {
    DeleteKey {
        class: class.into(),
        parameter: parameter.into(),
        alternative: alternative.into(),
        entity: entity.into(),
    }
}

fn okey(kind: ObjectKind, class: Option<&str>, name: &str) -> ObjectDeleteKey {
    ObjectDeleteKey {
        kind,
        class: class.map(str::to_string),
        target: ObjectRef::Name(name.into()),
    }
}

fn cascade(c: bool) -> ObjectDeleteOpts {
    ObjectDeleteOpts { cascade: c }
}

fn exec(db: &Path, sql: &str) {
    Connection::open(db).unwrap().execute_batch(sql).unwrap();
}

/// The parameters of each class: (name, tier) where tier is "s", "l", "a".
const PARAMS: [(&str, &[(&str, &str)]); 3] = [
    ("node", &[("ps", "s"), ("pl", "l"), ("pa", "a")]),
    ("link", &[("cap", "s"), ("lf", "l")]),
    ("unit", &[("ps", "s")]),
];
const ALTS: [&str; 3] = ["Base", "A", "B"];

fn entities_of(class: &str) -> &'static [&'static str] {
    match class {
        "node" => &["n1", "n2", "n3"],
        "link" => &["l12", "l23"],
        _ => &["u1", "u2"],
    }
}

fn batch_for(tier: &str, entities: &[&str], v: i64) -> RecordBatch {
    match tier {
        "s" => scalar_batch(entities, v as f64),
        "l" => series_batch(entities, v, true),
        _ => series_batch(entities, v, false),
    }
}

/// Seed: classes `node` (n1..n3), `unit` (u1, u2) and `link` typed (node,
/// node) with l12 = (n1, n2) and l23 = (n2, n3); every parameter of every
/// class holds value 1 for every entity in every alternative (Base, A, B).
fn seeded() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("seed.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();
    let mut segs = Vec::new();
    for (class, params) in PARAMS {
        if class == "link" {
            continue;
        }
        for (p, tier) in params {
            for a in ALTS {
                segs.push(seg(class, p, a, batch_for(tier, entities_of(class), 1)));
            }
        }
    }
    commit_changes(
        &db,
        segs,
        vec![],
        vec![],
        vec![],
        "t",
        "seed",
        WriteOptions::default(),
    )
    .unwrap();
    exec(
        &db,
        "INSERT INTO entity_class (name, valid_from_commit)
           VALUES ('link', (SELECT max(id) FROM commit));
         INSERT INTO entity_class_dimension (entity_class_id, position, dimension_class_id, valid_from_commit)
           SELECT (SELECT id FROM entity_class WHERE cur_name='link'), p,
                  (SELECT id FROM entity_class WHERE cur_name='node'), (SELECT max(id) FROM commit)
           FROM (VALUES (1), (2)) v(p);
         INSERT INTO entity (entity_class_id, name, valid_from_commit)
           SELECT (SELECT id FROM entity_class WHERE cur_name='link'), n, (SELECT max(id) FROM commit)
           FROM (VALUES ('l12'), ('l23')) v(n);
         INSERT INTO entity_element (entity_id, position, element_entity_id, valid_from_commit)
           SELECT (SELECT id FROM entity WHERE cur_name=l), p,
                  (SELECT id FROM entity WHERE cur_name=e), (SELECT max(id) FROM commit)
           FROM (VALUES ('l12', 1, 'n1'), ('l12', 2, 'n2'), ('l23', 1, 'n2'), ('l23', 2, 'n3')) v(l, p, e);",
    );
    let mut segs = Vec::new();
    for (p, tier) in PARAMS[1].1 {
        for a in ALTS {
            segs.push(seg("link", p, a, batch_for(tier, entities_of("link"), 1)));
        }
    }
    commit_changes(
        &db,
        segs,
        vec![],
        vec![],
        vec![],
        "t",
        "seed links",
        WriteOptions::default(),
    )
    .unwrap();
    (dir, db)
}

/// A copy of the seed file (closed, so the file holds everything).
fn copy_of(seed: &Path, dir: &Path, name: &str) -> PathBuf {
    let to = dir.join(name);
    let _ = std::fs::remove_file(&to);
    let _ = std::fs::remove_file(to.with_extension("duckdb.wal"));
    std::fs::copy(seed, &to).unwrap();
    to
}

/// Every row of every table (the commit timestamps aside), one digest per
/// table: two files with equal dumps hold the same data, ids included.
fn dump(db: &Path) -> Vec<(String, i64, String)> {
    let conn = Connection::open(db).unwrap();
    let tables: Vec<String> = conn
        .prepare(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'main' AND table_type = 'BASE TABLE' ORDER BY 1",
        )
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    tables
        .into_iter()
        .map(|t| {
            let cols = if t == "commit" {
                "* EXCLUDE (timestamp)"
            } else {
                "*"
            };
            let (n, h): (i64, Option<String>) = conn
                .query_row(
                    &format!(
                        "SELECT count(*), md5(string_agg(CAST(x AS VARCHAR), '|' \
                         ORDER BY CAST(x AS VARCHAR))) FROM (SELECT {cols} FROM \"{t}\") x"
                    ),
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            (t, n, h.unwrap_or_default())
        })
        .collect()
}

/// Live value rows (any tier) of `class:parameter/alternative/entity`.
fn live(db: &Path, class: &str, parameter: &str, alternative: &str, entity: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let mut n = 0;
    // `value_array` holds current rows only (history in `value_array_hist`).
    for (t, cur) in [
        ("value_scalar", "v.valid_to_commit IS NULL"),
        ("value_long", "v.valid_to_commit IS NULL"),
        ("value_array", "TRUE"),
    ] {
        n += conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM {t} v
                       JOIN entity e ON e.id = v.entity_id
                       JOIN entity_class c ON c.id = e.entity_class_id
                       JOIN parameter_definition p ON p.id = v.parameter_definition_id
                       JOIN alternative a ON a.id = v.alternative_id
                     WHERE {cur} AND c.cur_name = lower(?)
                       AND p.cur_name = lower(?) AND a.cur_name = lower(?)
                       AND lower(e.name) = lower(?)"
                ),
                duckdb::params![class, parameter, alternative, entity],
                |r| r.get::<_, i64>(0),
            )
            .unwrap();
    }
    n
}

fn count(db: &Path, sql: &str) -> i64 {
    Connection::open(db)
        .unwrap()
        .query_row(sql, [], |r| r.get(0))
        .unwrap()
}

/// The dropped writes a report lists: (segment index, entity name as given
/// in `class:parameter/alternative/entity`, covering delete).
fn dropped(r: &WriteReport) -> Vec<(usize, String, CoveringDelete)> {
    r.warnings
        .iter()
        .filter_map(|w| match w.kind {
            WarningKind::DroppedByDelete { segment_index, by } => {
                assert_eq!(w.object_type, "value");
                assert_eq!(w.id, 0);
                Some((segment_index, w.name.clone(), by))
            }
            _ => None,
        })
        .collect()
}

fn value(i: usize) -> CoveringDelete {
    CoveringDelete::Value { key_index: i }
}

fn object(i: usize) -> CoveringDelete {
    CoveringDelete::Object { key_index: i }
}

/// Run a call under `DeleteWins` and expect the "did not take effect"
/// refusal with nothing written.
fn assert_not_effective(
    db: &Path,
    segments: Vec<Segment>,
    deletes: Vec<DeleteKey>,
    object_deletes: Vec<(ObjectDeleteKey, ObjectDeleteOpts)>,
) -> String {
    let before = dump(db);
    let head = head_commit(db).unwrap();
    let err = commit_changes(
        db,
        segments,
        vec![],
        deletes,
        object_deletes,
        "t",
        "not effective",
        DELETE_WINS,
    )
    .expect_err("a covered write whose deletes do not take effect is refused");
    let DbError::Api(m) = &err else {
        panic!("expected DbError::Api, got {err:?}")
    };
    assert!(m.contains("did not take effect"), "{m}");
    assert_eq!(dump(db), before, "a refused call writes nothing");
    assert_eq!(head_commit(db).unwrap(), head, "no commit");
    m.clone()
}

/// Run `call` under the default policy and expect the D9/D10 refusal with
/// nothing written.
fn assert_refused_by_default(
    db: &Path,
    segments: Vec<Segment>,
    deletes: Vec<DeleteKey>,
    object_deletes: Vec<(ObjectDeleteKey, ObjectDeleteOpts)>,
) {
    let before = dump(db);
    let err = commit_changes(
        db,
        segments,
        vec![],
        deletes,
        object_deletes,
        "t",
        "refused",
        WriteOptions::default(),
    )
    .expect_err("the default policy refuses a covered write");
    assert!(
        matches!(&err, DbError::Api(m) if m.contains("in one call")),
        "{err:?}"
    );
    assert_eq!(dump(db), before, "a refused call writes nothing");
}

// ---------------------------------------------------------------------------
// D9: a value write and a value delete of the same key
// ---------------------------------------------------------------------------

#[test]
fn d9_default_refuses_and_delete_wins_drops_the_write_in_every_tier() {
    let (_d, seed) = seeded();
    for (p, tier) in [("ps", "s"), ("pl", "l"), ("pa", "a")] {
        // Changed (7) and unchanged (1) content alike.
        for v in [7, 1] {
            let dir = tempfile::tempdir().unwrap();
            let db = copy_of(&seed, dir.path(), "t.duckdb");
            let call = || {
                (
                    vec![
                        seg("node", p, "A", batch_for(tier, &["n1", "n2"], v)),
                        seg("unit", "ps", "A", batch_for("s", &["u1"], 5)),
                    ],
                    vec![dkey("NODE", p, "a", "N1")],
                )
            };
            let (s, d) = call();
            assert_refused_by_default(&db, s, d, vec![]);
            let head = head_commit(&db).unwrap();
            let (s, d) = call();
            let r = commit_changes(&db, s, vec![], d, vec![], "t", "dw", DELETE_WINS)
                .unwrap_or_else(|e| panic!("{p} {v}: {e:?}"));
            assert!(r.commit_id > head, "{p} {v}: committed");
            assert_eq!(
                dropped(&r),
                vec![(0, format!("node:{p}/A/n1"), value(0))],
                "{p} {v}"
            );
            assert_eq!(live(&db, "node", p, "A", "n1"), 0, "{p} {v}: n1 deleted");
            assert!(live(&db, "node", p, "A", "n2") > 0, "{p} {v}");
            // The n1 delete (a delete counts as a value written), u1, and
            // n2 when changed (unchanged at v = 1).
            let expect_written = if v == 1 { 2 } else { 3 };
            assert_eq!(r.values_written, expect_written, "{p} {v}");
        }
    }
}

/// Spellings DuckDB's `lower()` folds together but Rust's does not (final
/// sigma): the default refuses on the resolved ids, `DeleteWins` drops the
/// write because coverage is decided by the database's fold.
#[test]
fn d9_coverage_follows_the_database_fold() {
    let (_d, db) = seeded();
    commit_changes(
        &db,
        vec![seg("node", "ps", "Base", scalar_batch(&["ΑΣ"], 1.0))],
        vec![],
        vec![],
        vec![],
        "t",
        "sigma",
        WriteOptions::default(),
    )
    .unwrap();
    let s = || vec![seg("node", "ps", "Base", scalar_batch(&["ΑΣ"], 2.0))];
    let d = || vec![dkey("node", "ps", "Base", "ασ")];
    assert_refused_by_default(&db, s(), d(), vec![]);
    let r = commit_changes(&db, s(), vec![], d(), vec![], "t", "dw", DELETE_WINS).unwrap();
    assert_eq!(dropped(&r), vec![(0, "node:ps/Base/ΑΣ".into(), value(0))]);
    assert!(r.commit_id > 0);
    assert_eq!(live(&db, "node", "ps", "Base", "ασ"), 0);
}

// ---------------------------------------------------------------------------
// D10: a value write and an object delete covering it
// ---------------------------------------------------------------------------

/// Every object kind, by name, element tuple and through the cascade
/// closure (a container entity; a class over a class whose dimension it
/// is), with `cascade` true and false: the default refuses; `DeleteWins`
/// drops exactly the covered writes, runs the delete as the deletes-only
/// call would, and writes the rest.
#[test]
fn d10_default_refuses_and_delete_wins_drops_the_covered_writes() {
    let (_d, seed) = seeded();
    // (what, object delete, the writes it covers among the call's writes)
    let elements = ObjectDeleteKey {
        kind: ObjectKind::Entity,
        class: Some("link".into()),
        target: ObjectRef::Elements(vec!["n1".into(), "n2".into()]),
    };
    let cases: Vec<(&str, ObjectDeleteKey, Vec<&str>)> = vec![
        (
            "alternative",
            okey(ObjectKind::Alternative, None, "a"),
            vec!["node:pl/A/n1", "node:pl/A/n3", "link:cap/A/l12"],
        ),
        (
            "entity by name",
            okey(ObjectKind::Entity, Some("Node"), "N1"),
            vec!["node:pl/A/n1", "node:ps/B/n1", "link:cap/A/l12"],
        ),
        ("entity by elements", elements, vec!["link:cap/A/l12"]),
        (
            "parameter",
            okey(ObjectKind::Parameter, Some("node"), "PL"),
            vec!["node:pl/A/n1", "node:pl/A/n3"],
        ),
        (
            "class over its dependent class",
            okey(ObjectKind::Class, None, "node"),
            vec![
                "node:pl/A/n1",
                "node:pl/A/n3",
                "node:ps/B/n1",
                "link:cap/A/l12",
            ],
        ),
    ];
    // The call's writes: long n1/n3 in A (changed), scalar n1 in B
    // (unchanged), link cap l12 in A (changed), unit u1 in Base (changed).
    let writes = || {
        vec![
            seg("node", "pl", "A", batch_for("l", &["n1", "n3"], 9)),
            seg("node", "ps", "B", batch_for("s", &["n1"], 1)),
            seg("link", "cap", "A", batch_for("s", &["l12"], 9)),
            seg("unit", "ps", "Base", batch_for("s", &["u1"], 9)),
        ]
    };
    for (what, od, covered) in cases {
        for c in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let db = copy_of(&seed, dir.path(), "dw.duckdb");
            assert_refused_by_default(&db, writes(), vec![], vec![(od.clone(), cascade(c))]);
            if !c {
                // Every target has dependents, so RESTRICT ends `Restricted`:
                // the delete does not take effect and the call is refused.
                assert_not_effective(&db, writes(), vec![], vec![(od.clone(), cascade(c))]);
                continue;
            }
            let r = commit_changes(
                &db,
                writes(),
                vec![],
                vec![],
                vec![(od.clone(), cascade(c))],
                "t",
                "dw",
                DELETE_WINS,
            )
            .unwrap_or_else(|e| panic!("{what} cascade {c}: {e:?}"));
            let got: BTreeSet<String> = dropped(&r).into_iter().map(|(_, n, _)| n).collect();
            let want: BTreeSet<String> = covered.iter().map(|s| s.to_string()).collect();
            assert_eq!(got, want, "{what} cascade {c}");
            assert!(
                dropped(&r).iter().all(|(_, _, by)| *by == object(0)),
                "{what}"
            );
            // The same call without the covered writes, default policy.
            let ref_db = copy_of(&seed, dir.path(), "ref.duckdb");
            let kept: Vec<Segment> = writes()
                .into_iter()
                .filter_map(|s| without(s, &want))
                .collect();
            let rr = commit_changes(
                &ref_db,
                kept,
                vec![],
                vec![],
                vec![(od.clone(), cascade(c))],
                "t",
                "dw",
                WriteOptions::default(),
            )
            .unwrap_or_else(|e| panic!("{what} cascade {c} reference: {e:?}"));
            assert_eq!(without_drops(&r), rr, "{what} cascade {c}: report");
            assert_eq!(dump(&db), dump(&ref_db), "{what} cascade {c}: state");
            // u1 is never covered.
            assert_eq!(r.values_written, rr.values_written);
            assert!(live(&db, "unit", "ps", "Base", "u1") > 0);
        }
    }
}

/// The review's repro (the partial-value problem): alternative A's long
/// value [1, 2, 3] is rewritten [1, 9, 3] in a call that cascade-deletes A.
/// Under `DeleteWins` no live cell of A survives.
#[test]
fn d10_no_partial_value_survives_the_cascade() {
    let (_d, db) = seeded();
    let mut b = series_batch(&["n1"], 1, true);
    let v = Int64Array::from(vec![1, 9, 3]);
    b = RecordBatch::try_new(
        b.schema(),
        vec![b.column(0).clone(), b.column(1).clone(), Arc::new(v)],
    )
    .unwrap();
    let r = commit_changes(
        &db,
        vec![seg("node", "pl", "A", b)],
        vec![],
        vec![],
        vec![(okey(ObjectKind::Alternative, None, "A"), cascade(true))],
        "t",
        "dw",
        DELETE_WINS,
    )
    .unwrap();
    assert!(matches!(
        r.object_deletes[0].result,
        ObjectDeleteResult::Deleted { .. }
    ));
    assert_eq!(dropped(&r).len(), 1);
    assert_eq!(r.values_written, 0);
    assert_eq!(
        count(
            &db,
            "SELECT count(*) FROM value_long v JOIN alternative a ON a.id = v.alternative_id \
             WHERE a.cur_name = 'a' AND v.valid_to_commit IS NULL"
        ),
        0
    );
}

// ---------------------------------------------------------------------------
// The no-op commit rule and auto-create
// ---------------------------------------------------------------------------

/// A call whose only effect would be dropped writes plus deletes that
/// delete nothing creates no commit, and the dropped writes auto-create
/// nothing (no entity, class, parameter or alternative row).
#[test]
fn dropped_writes_plus_deletes_that_find_nothing_make_no_commit() {
    let (_d, db) = seeded();
    let referents = |db: &Path| {
        count(
            db,
            "SELECT (SELECT count(*) FROM entity) + (SELECT count(*) FROM entity_class) \
             + (SELECT count(*) FROM parameter_definition) + (SELECT count(*) FROM alternative)",
        )
    };
    type Call = (
        &'static str,
        Vec<Segment>,
        Vec<DeleteKey>,
        Vec<(ObjectDeleteKey, ObjectDeleteOpts)>,
    );
    let calls: Vec<Call> = vec![
        (
            "value delete of a new entity",
            vec![seg("node", "ps", "Base", scalar_batch(&["n9"], 3.0))],
            vec![dkey("node", "ps", "Base", "n9")],
            vec![],
        ),
        (
            "value delete of a new parameter in a new alternative",
            vec![seg("node", "pnew", "Cnew", scalar_batch(&["n1"], 3.0))],
            vec![dkey("node", "pnew", "Cnew", "n1")],
            vec![],
        ),
        (
            "entity delete of a new entity",
            vec![seg("unit", "ps", "A", scalar_batch(&["u9"], 3.0))],
            vec![],
            vec![(okey(ObjectKind::Entity, Some("unit"), "u9"), cascade(true))],
        ),
        (
            "class delete of a new class",
            vec![seg("newc", "p", "Base", scalar_batch(&["x"], 3.0))],
            vec![],
            vec![(okey(ObjectKind::Class, None, "NewC"), cascade(false))],
        ),
        (
            "alternative delete of a new alternative",
            vec![seg("node", "ps", "Cnew", scalar_batch(&["n1", "n9"], 3.0))],
            vec![],
            vec![(okey(ObjectKind::Alternative, None, "cnew"), cascade(true))],
        ),
    ];
    for (what, s, d, o) in calls {
        let before = dump(&db);
        let head = head_commit(&db).unwrap();
        let n_refs = referents(&db);
        let n_writes: usize = s.iter().map(|s| s.batches[0].num_rows()).sum();
        let r = commit_changes(&db, s, vec![], d, o, "t", "no-op", DELETE_WINS)
            .unwrap_or_else(|e| panic!("{what}: {e:?}"));
        assert_eq!(r.commit_id, 0, "{what}: no commit");
        assert_eq!(dropped(&r).len(), n_writes, "{what}: {r:?}");
        assert_eq!(head_commit(&db).unwrap(), head, "{what}");
        assert_eq!(referents(&db), n_refs, "{what}: nothing auto-created");
        assert_eq!(dump(&db), before, "{what}: nothing written");
    }
}

/// The same dropped write beside a delete that does delete something
/// commits (the delete).
#[test]
fn dropped_writes_beside_an_effective_delete_commit_the_delete() {
    let (_d, db) = seeded();
    let r = commit_changes(
        &db,
        vec![seg("node", "ps", "Base", scalar_batch(&["n1"], 1.0))],
        vec![],
        vec![dkey("node", "ps", "Base", "n1")],
        vec![],
        "t",
        "dw",
        DELETE_WINS,
    )
    .unwrap();
    assert!(r.commit_id > 0);
    assert_eq!(r.rows_closed, 1);
    assert_eq!(live(&db, "node", "ps", "Base", "n1"), 0);
}

/// A RESTRICT object delete that ends `Restricted` keeps its object: the
/// writes it covers must not be dropped, so the call is refused and rolled
/// back (nothing committed, the uncovered write included). A covering value
/// delete beside it takes effect, so a write it also covers is dropped.
#[test]
fn a_restricted_covering_delete_refuses_the_call() {
    let (_d, db) = seeded();
    // n2 has values and containers, so RESTRICT ends `Restricted`.
    let od = || vec![(okey(ObjectKind::Entity, Some("node"), "n2"), cascade(false))];
    let m = assert_not_effective(
        &db,
        vec![
            seg("node", "ps", "A", scalar_batch(&["n2"], 8.0)),
            seg("unit", "ps", "A", scalar_batch(&["u1"], 8.0)),
        ],
        vec![],
        od(),
    );
    assert!(m.contains("node:ps/A/n2"), "{m}");
    assert!(m.contains("object-delete key 0 (restricted)"), "{m}");
    assert_eq!(live(&db, "node", "ps", "A", "n2"), 1);

    // A value delete covers the same write too: it takes effect, so the
    // write is dropped and the call commits (the object delete still ends
    // `Restricted`, as alone).
    let r = commit_changes(
        &db,
        vec![seg("node", "ps", "A", scalar_batch(&["n2"], 8.0))],
        vec![],
        vec![dkey("node", "ps", "A", "n2")],
        od(),
        "t",
        "value delete covers too",
        DELETE_WINS,
    )
    .unwrap();
    assert!(r.commit_id > 0);
    assert_eq!(
        dropped(&r),
        vec![(0, "node:ps/A/n2".into(), value(0))],
        "the warning names the value delete first"
    );
    assert!(matches!(
        r.object_deletes[0].result,
        ObjectDeleteResult::Restricted { .. }
    ));
    assert_eq!(live(&db, "node", "ps", "A", "n2"), 0);
}

/// A RESTRICT delete that does take effect (its object has no other
/// dependents) still drops the writes it covers, and the call commits.
#[test]
fn an_effective_restrict_delete_drops_its_writes() {
    let (_d, db) = seeded();
    // Alternative `C` exists but holds nothing.
    commit_changes(
        &db,
        vec![],
        vec![StructuralChange::CreateAlternative {
            name: "C".into(),
            description: None,
        }],
        vec![],
        vec![],
        "t",
        "create C",
        WriteOptions::default(),
    )
    .unwrap();
    let r = commit_changes(
        &db,
        vec![
            seg("node", "ps", "C", scalar_batch(&["n1", "n2"], 8.0)),
            seg("node", "ps", "Base", scalar_batch(&["n3"], 8.0)),
        ],
        vec![],
        vec![],
        vec![(okey(ObjectKind::Alternative, None, "c"), cascade(false))],
        "t",
        "restrict C",
        DELETE_WINS,
    )
    .unwrap();
    assert!(r.commit_id > 0);
    assert!(matches!(
        r.object_deletes[0].result,
        ObjectDeleteResult::Deleted { .. }
    ));
    assert_eq!(
        dropped(&r),
        vec![
            (0, "node:ps/C/n1".into(), object(0)),
            (0, "node:ps/C/n2".into(), object(0)),
        ]
    );
    assert_eq!(
        count(
            &db,
            "SELECT count(*) FROM alternative WHERE cur_name = 'c' AND valid_to_commit IS NULL"
        ),
        0
    );
    assert_eq!(live(&db, "node", "ps", "Base", "n3"), 1);
    assert_eq!(r.values_written, 1);
}

/// A structural create plus an object delete of the same object (the
/// review's C2 repro): the same-commit guard would leave the created object
/// live while the covered write is dropped. Refused under both policies,
/// by name before the transaction and on the resolved ids after the
/// structural changes (a created container in a deleted entity's cascade).
#[test]
fn a_structural_create_and_an_object_delete_of_it_are_refused() {
    let (_d, db) = seeded();
    let before = dump(&db);
    for opts in [WriteOptions::default(), DELETE_WINS] {
        for (what, segments, change, od) in [
            (
                "create alternative C, delete C, write to C",
                vec![seg("node", "ps", "C", scalar_batch(&["n1"], 5.0))],
                StructuralChange::CreateAlternative {
                    name: "C".into(),
                    description: None,
                },
                okey(ObjectKind::Alternative, None, "C"),
            ),
            (
                "create alternative C, delete c, no write",
                vec![],
                StructuralChange::CreateAlternative {
                    name: "C".into(),
                    description: None,
                },
                okey(ObjectKind::Alternative, None, "c"),
            ),
            (
                "create entity u5, delete class unit",
                vec![],
                StructuralChange::CreateEntity {
                    class: "unit".into(),
                    name: Some("u5".into()),
                    elements: vec![],
                },
                okey(ObjectKind::Class, None, "Unit"),
            ),
            (
                "create entity u5, delete u5",
                vec![],
                StructuralChange::CreateEntity {
                    class: "unit".into(),
                    name: Some("u5".into()),
                    elements: vec![],
                },
                okey(ObjectKind::Entity, Some("unit"), "U5"),
            ),
            (
                "create class k, delete k",
                vec![],
                StructuralChange::CreateEntityClass {
                    name: "k".into(),
                    dims: vec![],
                },
                okey(ObjectKind::Class, None, "K"),
            ),
        ] {
            let err = commit_changes(
                &db,
                segments,
                vec![change],
                vec![],
                vec![(od, cascade(true))],
                "t",
                "create + delete",
                opts,
            )
            .expect_err(what);
            assert!(
                matches!(&err, DbError::Api(m) if m.contains("object-deleted in one commit")),
                "{what}: {err:?}"
            );
        }
        // No name in the call connects them: the new link l13 = (n1, n3)
        // is a container of n1, which the entity delete cascades over.
        let err = commit_changes(
            &db,
            vec![seg("link", "cap", "B", scalar_batch(&["n1__n3"], 5.0))],
            vec![StructuralChange::CreateEntity {
                class: "link".into(),
                name: None,
                elements: vec!["n1".into(), "n3".into()],
            }],
            vec![],
            vec![(okey(ObjectKind::Entity, Some("node"), "n1"), cascade(true))],
            "t",
            "create container + delete element",
            opts,
        )
        .expect_err("resolved create-and-delete");
        assert!(
            matches!(&err, DbError::Api(m) if m.contains("structural change of this call creates")),
            "{err:?}"
        );
    }
    assert_eq!(dump(&db), before, "nothing written");
}

// ---------------------------------------------------------------------------
// The defining property, randomized
// ---------------------------------------------------------------------------

/// A small deterministic PRNG (xorshift64*).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
    /// A random-case spelling of an ASCII name.
    fn spell(&mut self, s: &str) -> String {
        s.chars()
            .map(|c| {
                if self.chance(30) {
                    c.to_ascii_uppercase()
                } else {
                    c
                }
            })
            .collect()
    }
}

/// The class of a link entity's elements.
fn elements_of(link: &str) -> [&'static str; 2] {
    match link {
        "l12" => ["n1", "n2"],
        _ => ["n2", "n3"],
    }
}

/// One object delete of the random call, as the model sees it.
#[derive(Clone)]
enum Od {
    Alt(&'static str),
    Class(&'static str),
    Param(&'static str, &'static str),
    /// By name (class, entity); the entity may not exist.
    Entity(&'static str, &'static str),
    /// A link entity by its element tuple.
    Elements(&'static str),
}

impl Od {
    /// The target does not exist in the seed (the delete is `NotFound`).
    fn absent(&self) -> bool {
        matches!(self, Od::Entity(_, "n9"))
    }

    fn covers(&self, c: &str, p: &str, a: &str, e: &str) -> bool {
        match self {
            Od::Alt(x) => x.eq_ignore_ascii_case(a),
            // `link` declares `node` as a dimension: a node class delete
            // reaches link and its values.
            Od::Class(x) => *x == c || (*x == "node" && c == "link"),
            Od::Param(xc, xp) => *xc == c && *xp == p,
            Od::Entity(xc, xe) => {
                (*xc == c && *xe == e)
                    || (*xc == "node" && c == "link" && elements_of(e).contains(xe))
            }
            Od::Elements(l) => c == "link" && *l == e,
        }
    }

    fn key(&self, rng: &mut Rng) -> ObjectDeleteKey {
        match self {
            Od::Alt(x) => okey(ObjectKind::Alternative, None, &rng.spell(x)),
            Od::Class(x) => okey(ObjectKind::Class, None, &rng.spell(x)),
            Od::Param(c, p) => okey(ObjectKind::Parameter, Some(&rng.spell(c)), &rng.spell(p)),
            Od::Entity(c, e) => okey(ObjectKind::Entity, Some(&rng.spell(c)), &rng.spell(e)),
            Od::Elements(l) => ObjectDeleteKey {
                kind: ObjectKind::Entity,
                class: Some("link".into()),
                target: ObjectRef::Elements(elements_of(l).iter().map(|e| rng.spell(e)).collect()),
            },
        }
    }
}

/// A write of the random call: (class, parameter, tier, alternative,
/// entities, value).
type W = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    Vec<&'static str>,
    i64,
);

fn segment_of(w: &W) -> Segment {
    seg(w.0, w.1, w.3, batch_for(w.2, &w.4, w.5))
}

/// `s` without the entities whose `class:parameter/alternative/entity`
/// name is in `drop`; `None` when nothing is left.
fn without(s: Segment, drop: &BTreeSet<String>) -> Option<Segment> {
    let b = &s.batches[0];
    let names = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let keep: Vec<u32> = (0..b.num_rows())
        .filter(|&i| {
            let n = format!(
                "{}:{}/{}/{}",
                s.class,
                s.parameter,
                s.alternative,
                names.value(i)
            );
            !drop.contains(&n)
        })
        .map(|i| i as u32)
        .collect();
    if keep.is_empty() {
        return None;
    }
    let b = duckdb::arrow::compute::take_record_batch(
        b,
        &duckdb::arrow::array::UInt32Array::from(keep),
    )
    .unwrap();
    Some(Segment {
        batches: vec![b],
        ..s
    })
}

/// A report without its `DroppedByDelete` warnings.
fn without_drops(r: &WriteReport) -> WriteReport {
    let mut r = r.clone();
    r.warnings
        .retain(|w| !matches!(w.kind, WarningKind::DroppedByDelete { .. }));
    r
}

/// Random calls of writes (changed, unchanged, new entities; every tier),
/// value deletes and object deletes of every kind, over random-case
/// spellings: the `DeleteWins` call leaves every table exactly as the
/// default-policy call without the covered writes does (same report apart
/// from the drop warnings, same rows, ids included), and the dropped writes
/// are exactly the ones the model predicts. For the covered keys that is the
/// end state of the deletes alone, since nothing else in either call touches
/// them.
#[test]
fn delete_wins_equals_the_call_without_its_covered_writes() {
    let (_d, seed) = seeded();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let ods = [
        Od::Alt("A"),
        Od::Alt("B"),
        Od::Class("node"),
        Od::Class("link"),
        Od::Class("unit"),
        Od::Param("node", "pl"),
        Od::Param("node", "pa"),
        Od::Param("link", "cap"),
        Od::Param("unit", "ps"),
        Od::Entity("node", "n1"),
        Od::Entity("node", "n3"),
        Od::Entity("node", "n9"),
        Od::Entity("link", "l23"),
        Od::Entity("unit", "u2"),
        Od::Elements("l12"),
    ];
    let mut covered_total = 0;
    let mut refused = 0;
    let seed_dump = dump(&seed);
    for round in 0..30 {
        // Writes: some (class, parameter, alternative) targets, each with
        // some of its entities and maybe a new one.
        let mut writes: Vec<W> = Vec::new();
        for (class, params) in PARAMS {
            for (p, tier) in params.iter() {
                for a in ALTS {
                    if !rng.chance(25) {
                        continue;
                    }
                    let mut es: Vec<&'static str> = entities_of(class)
                        .iter()
                        .copied()
                        .filter(|_| rng.chance(60))
                        .collect();
                    if class != "link" && rng.chance(30) {
                        es.push(if class == "node" { "n9" } else { "u9" });
                    }
                    if es.is_empty() {
                        continue;
                    }
                    let v = if rng.chance(40) {
                        1
                    } else {
                        2 + rng.below(5) as i64
                    };
                    writes.push((class, p, tier, a, es, v));
                }
            }
        }
        if writes.is_empty() {
            continue;
        }
        // Value deletes: distinct keys, some of them written.
        let mut vdels: Vec<(&str, &str, &str, &str)> = Vec::new();
        for _ in 0..rng.below(5) {
            let k = if rng.chance(60) {
                let w = &writes[rng.below(writes.len())];
                (w.0, w.1, w.3, w.4[rng.below(w.4.len())])
            } else {
                let (class, params) = PARAMS[rng.below(3)];
                let (p, _) = params[rng.below(params.len())];
                let es = entities_of(class);
                (class, p, ALTS[rng.below(3)], es[rng.below(es.len())])
            };
            if !vdels.contains(&k) {
                vdels.push(k);
            }
        }
        // Object deletes: distinct targets.
        let mut picked: Vec<usize> = Vec::new();
        for _ in 0..rng.below(3) {
            let i = rng.below(ods.len());
            if !picked.contains(&i) {
                picked.push(i);
            }
        }
        // The model's covered writes.
        let mut covered: BTreeSet<String> = BTreeSet::new();
        for w in &writes {
            for e in &w.4 {
                let by_value = vdels.contains(&(w.0, w.1, w.3, *e));
                let by_object = picked.iter().any(|&i| ods[i].covers(w.0, w.1, w.3, e));
                if by_value || by_object {
                    covered.insert(format!("{}:{}/{}/{}", w.0, w.1, w.3, e));
                }
            }
        }
        covered_total += covered.len();

        let spellings: Vec<DeleteKey> = vdels
            .iter()
            .map(|(c, p, a, e)| dkey(&rng.spell(c), &rng.spell(p), &rng.spell(a), &rng.spell(e)))
            .collect();
        let okeys: Vec<(ObjectDeleteKey, ObjectDeleteOpts)> = picked
            .iter()
            .map(|&i| (ods[i].key(&mut rng), cascade(rng.chance(60))))
            .collect();

        let dir = tempfile::tempdir().unwrap();
        let db = copy_of(&seed, dir.path(), "dw.duckdb");
        let ref_db = copy_of(&seed, dir.path(), "ref.duckdb");
        let kept: Vec<Segment> = writes
            .iter()
            .map(segment_of)
            .filter_map(|s| without(s, &covered))
            .collect();
        if kept.is_empty() && spellings.is_empty() && okeys.is_empty() {
            continue;
        }
        // The reference: the call without its covered writes, default
        // policy. Its object-delete outcomes are those of the deletes in
        // the full call (every dependent an uncovered write could add is
        // itself covered), so they predict which deletes take effect.
        let rr = commit_changes(
            &ref_db,
            kept,
            vec![],
            spellings.clone(),
            okeys.clone(),
            "t",
            "random",
            WriteOptions::default(),
        )
        .unwrap_or_else(|e| panic!("round {round}: the reference call: {e:?}"));
        let took_effect = |j: usize| {
            ods[picked[j]].absent()
                || rr.object_deletes.iter().any(|o| {
                    o.key_index == j && matches!(o.result, ObjectDeleteResult::Deleted { .. })
                })
        };
        let lost = writes.iter().any(|w| {
            w.4.iter().any(|e| {
                let by_value = vdels.contains(&(w.0, w.1, w.3, *e));
                let by_object: Vec<usize> = (0..picked.len())
                    .filter(|&j| ods[picked[j]].covers(w.0, w.1, w.3, e))
                    .collect();
                !by_value && !by_object.is_empty() && !by_object.iter().any(|&j| took_effect(j))
            })
        });

        let call = commit_changes(
            &db,
            writes.iter().map(segment_of).collect(),
            vec![],
            spellings,
            okeys,
            "t",
            "random",
            DELETE_WINS,
        );
        if lost {
            refused += 1;
            let err = call.expect_err(&format!("round {round}: a lost write is refused"));
            assert!(
                matches!(&err, DbError::Api(m) if m.contains("did not take effect")),
                "round {round}: {err:?}"
            );
            assert_eq!(dump(&db), seed_dump, "round {round}: nothing written");
            continue;
        }
        let r = call.unwrap_or_else(|e| panic!("round {round}: {e:?}"));
        let got: BTreeSet<String> = dropped(&r).into_iter().map(|(_, n, _)| n).collect();
        assert_eq!(got, covered, "round {round}: dropped writes");
        // Each warning's segment and `by` name a write of that segment and
        // a delete that covers it.
        for (si, name, by) in dropped(&r) {
            let w = &writes[si];
            let e = name.rsplit('/').next().unwrap().to_string();
            assert_eq!(
                name,
                format!("{}:{}/{}/{e}", w.0, w.1, w.3),
                "round {round}"
            );
            let ok = match by {
                CoveringDelete::Value { key_index } => {
                    vdels[key_index] == (w.0, w.1, w.3, e.as_str())
                }
                CoveringDelete::Object { key_index } => {
                    ods[picked[key_index]].covers(w.0, w.1, w.3, &e)
                }
            };
            assert!(ok, "round {round}: {name} by {by:?}");
        }
        assert_eq!(without_drops(&r), rr, "round {round}: report");
        assert_eq!(dump(&db), dump(&ref_db), "round {round}: state");
    }
    assert!(
        covered_total > 30,
        "the rounds exercise coverage: {covered_total}"
    );
    assert!(
        refused > 0,
        "some rounds hit a delete that does not take effect"
    );
}
