//! Values of several declared types for one (class, parameter,
//! alternative) in ONE call: a segment's batches may differ in column
//! types. The core groups them by column types; each group binds to the
//! declared member it instantiates (the entry check runs per group), while
//! the entity dedup runs across the whole segment. A group no member
//! declares is refused as `DbError::UndeclaredValueType`, with nothing
//! written.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{commit_changes, commit_structural, head_commit, DeleteKey, Segment};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::template::ir::{DimSpec, TypeDecl};
use nodal_db_core::types::{CoveredWrites, StructuralChange, WarningKind, WriteOptions};

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.duckdb");
    init_schema_with_default(&path, Some("Base")).unwrap();
    (dir, path)
}

fn scalar(kind: &str) -> TypeDecl {
    TypeDecl {
        kind: kind.into(),
        dims: None,
    }
}

fn indexed_t() -> TypeDecl {
    TypeDecl {
        kind: "indexed".into(),
        dims: Some(vec![DimSpec {
            name: "t".into(),
            kind: "integer".into(),
        }]),
    }
}

/// Class `node` with parameter `level` declaring `types`.
fn declare(db: &Path, types: Vec<TypeDecl>) {
    commit_structural(
        db,
        vec![
            StructuralChange::CreateEntityClass {
                name: "node".into(),
                dims: vec![],
                description: None,
            },
            StructuralChange::CreateParameter {
                class: "node".into(),
                name: "level".into(),
                types,
                value_list: None,
                description: None,
            },
        ],
        "t",
        "declare",
        WriteOptions::default(),
    )
    .expect("declare");
}

fn batch(entities: &[&str], value: ArrayRef) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", value.data_type().clone(), false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(entities.to_vec())) as ArrayRef,
            value,
        ],
    )
    .unwrap()
}

fn floats(entities: &[&str], v: &[f64]) -> RecordBatch {
    batch(entities, Arc::new(Float64Array::from(v.to_vec())))
}

fn strings(entities: &[&str], v: &[&str]) -> RecordBatch {
    batch(entities, Arc::new(StringArray::from(v.to_vec())))
}

fn bools(entities: &[&str], v: &[bool]) -> RecordBatch {
    batch(entities, Arc::new(BooleanArray::from(v.to_vec())))
}

/// A rank-1 series over integer dimension `t` (t = 0, 1, ...).
fn series(entity: &str, value: ArrayRef) -> RecordBatch {
    let n = value.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", value.data_type().clone(), false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; n])) as ArrayRef,
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())) as ArrayRef,
            value,
        ],
    )
    .unwrap()
}

fn segment(batches: Vec<RecordBatch>) -> Segment {
    Segment {
        class: "node".into(),
        parameter: "level".into(),
        alternative: "Base".into(),
        batches,
        domains: vec![],
    }
}

fn write(
    db: &Path,
    segments: Vec<Segment>,
    deletes: Vec<DeleteKey>,
    opts: WriteOptions,
) -> Result<nodal_db_core::types::WriteReport, DbError> {
    commit_changes(db, segments, vec![], deletes, vec![], "t", "m", opts)
}

fn query<T: duckdb::types::FromSql>(db: &Path, sql: &str) -> Vec<Vec<T>> {
    let conn = Connection::open(db).unwrap();
    let out = {
        let mut stmt = conn.prepare(sql).unwrap();
        // Every column, read as `T` (the queries cast to one type).
        let rows = stmt
            .query_map([], |r| {
                let mut v = Vec::new();
                let mut i = 0;
                while let Ok(x) = r.get::<_, T>(i) {
                    v.push(x);
                    i += 1;
                }
                Ok(v)
            })
            .unwrap();
        rows.collect::<Result<Vec<_>, _>>().unwrap()
    };
    conn.close().unwrap();
    out
}

/// The current values of `node.level` through the `read_parameter` read
/// macro, as text: (entity, tier, value_kind, t, value).
fn read_back(db: &Path) -> Vec<Vec<String>> {
    query::<String>(
        db,
        "SELECT entity, tier, value_kind, coalesce(idx1_num::VARCHAR, '-'), \
                coalesce(v_float::VARCHAR, v_str, v_bool::VARCHAR, v_int::VARCHAR) \
         FROM read_parameter('node', 'level') ORDER BY entity, idx1_num",
    )
}

/// Each current value's D11 binding: (entity, member value_kind, rank).
fn bindings(db: &Path) -> Vec<Vec<String>> {
    query::<String>(
        db,
        "SELECT DISTINCT e.name, pt.value_kind, pt.rank::VARCHAR FROM ( \
           SELECT entity_id, parameter_type_id FROM value_scalar WHERE valid_to_commit IS NULL \
           UNION ALL SELECT entity_id, parameter_type_id FROM value_long WHERE valid_to_commit IS NULL \
           UNION ALL SELECT entity_id, parameter_type_id FROM value_array) v \
         JOIN entity e ON e.id = v.entity_id \
         JOIN parameter_type pt ON pt.id = v.parameter_type_id ORDER BY 1",
    )
}

fn count(db: &Path, sql: &str) -> i64 {
    query::<i64>(db, sql)[0][0]
}

fn row(xs: &[&str]) -> Vec<String> {
    xs.iter().map(|s| s.to_string()).collect()
}

fn assert_audits_clean(db: &Path) {
    for a in [
        "audit_history_tiling",
        "audit_cross_tier_duplicate",
        "audit_array_integrity",
        "audit_type_conformance",
        "audit_dangling_ref",
    ] {
        assert_eq!(
            count(db, &format!("SELECT count(*) FROM {a}")),
            0,
            "{a} must be empty"
        );
    }
}

/// The editor request's conformance case: north = 12.0 and south = "high"
/// for one parameter and alternative, in one call, one commit; both read
/// back, each bound to its own declared member.
#[test]
fn float_and_string_in_one_call_one_commit() {
    let (_d, db) = fresh_db();
    declare(&db, vec![scalar("float"), scalar("string")]);
    let head = head_commit(&db).unwrap();
    let commits = count(&db, "SELECT count(*) FROM commit");

    let report = write(
        &db,
        vec![segment(vec![
            floats(&["north"], &[12.0]),
            strings(&["south"], &["high"]),
        ])],
        vec![],
        WriteOptions::default(),
    )
    .expect("mixed write");

    assert_eq!(report.commit_id, head + 1);
    assert_eq!(head_commit(&db).unwrap(), head + 1);
    assert_eq!(count(&db, "SELECT count(*) FROM commit"), commits + 1);
    assert_eq!(report.values_written, 2);
    assert_eq!(
        read_back(&db),
        vec![
            row(&["north", "scalar", "float", "-", "12.0"]),
            row(&["south", "scalar", "string", "-", "high"]),
        ]
    );
    assert_eq!(
        bindings(&db),
        vec![
            row(&["north", "float", "0"]),
            row(&["south", "string", "0"])
        ]
    );
    // One commit for both values.
    assert_eq!(
        count(
            &db,
            "SELECT count(DISTINCT valid_from_commit) FROM value_scalar WHERE valid_to_commit IS NULL"
        ),
        1
    );
    // A write never widens: still the two declared members.
    assert_eq!(count(&db, "SELECT count(*) FROM parameter_type"), 2);
    assert_audits_clean(&db);
}

/// Re-sending the same mixed segment changes nothing: no commit.
#[test]
fn resending_a_mixed_segment_is_a_noop() {
    let (_d, db) = fresh_db();
    declare(&db, vec![scalar("float"), scalar("string")]);
    let seg = || {
        segment(vec![
            floats(&["north", "east"], &[12.0, 1.5]),
            strings(&["south"], &["high"]),
        ])
    };
    write(&db, vec![seg()], vec![], WriteOptions::default()).expect("first");
    let head = head_commit(&db).unwrap();
    let again = write(&db, vec![seg()], vec![], WriteOptions::default()).expect("again");
    assert_eq!(again.commit_id, 0);
    assert_eq!(again.values_unchanged, 3);
    assert_eq!(again.values_written, 0);
    assert_eq!(head_commit(&db).unwrap(), head);
}

/// One entity in batches of two types is two values for one key: refused
/// before any DB work, whatever the values.
#[test]
fn one_entity_in_two_type_groups_is_refused() {
    let (_d, db) = fresh_db();
    declare(&db, vec![scalar("float"), scalar("string")]);
    let head = head_commit(&db).unwrap();
    let err = write(
        &db,
        vec![segment(vec![
            floats(&["north", "east"], &[12.0, 1.0]),
            strings(&["south", "north"], &["high", "low"]),
        ])],
        vec![],
        WriteOptions::default(),
    )
    .expect_err("one entity, two types");
    assert!(matches!(err, DbError::Api(_)), "{err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains("\"north\"") && msg.contains("two value types"),
        "{msg}"
    );
    assert_eq!(head_commit(&db).unwrap(), head);
    assert_eq!(count(&db, "SELECT count(*) FROM entity"), 0);
}

/// A group whose type the parameter does not declare is refused as the
/// typed error, naming the group's first entity, its entity count and its
/// type; the other group's values are not written either.
#[test]
fn undeclared_type_group_is_refused_typed() {
    let (_d, db) = fresh_db();
    declare(&db, vec![scalar("float"), scalar("string")]);
    let head = head_commit(&db).unwrap();
    let err = write(
        &db,
        vec![segment(vec![
            floats(&["north"], &[12.0]),
            bools(&["south", "west"], &[true, false]),
        ])],
        vec![],
        WriteOptions::default(),
    )
    .expect_err("bool is not declared");
    let DbError::UndeclaredValueType(d) = &err else {
        panic!("expected UndeclaredValueType, got {err:?}");
    };
    assert_eq!(d.segment_index, 0);
    assert_eq!(
        (
            d.class.as_str(),
            d.parameter.as_str(),
            d.alternative.as_str()
        ),
        ("node", "level", "Base")
    );
    assert_eq!((d.entity.as_str(), d.entity_count), ("south", 2));
    assert_eq!(d.value_kind, "bool");
    assert!(d.dimensions.is_empty());
    assert_eq!(d.declared, vec!["float rank 0", "string rank 0"]);
    let msg = err.to_string();
    assert!(
        msg.contains("entity \"south\" and 1 more")
            && msg.contains("instantiates none of the 2 declared member")
            && msg.contains("bool"),
        "{msg}"
    );
    // Nothing written: no commit, no entity, no value.
    assert_eq!(head_commit(&db).unwrap(), head);
    assert_eq!(count(&db, "SELECT count(*) FROM entity"), 0);
    assert_eq!(count(&db, "SELECT count(*) FROM value_scalar"), 0);

    // In a call of several segments the variant is kept, naming the
    // segment by index and in the message.
    let other = Segment {
        class: "node".into(),
        parameter: "other".into(),
        alternative: "Base".into(),
        batches: vec![floats(&["north"], &[1.0])],
        domains: vec![],
    };
    let err = write(
        &db,
        vec![other, segment(vec![bools(&["south"], &[true])])],
        vec![],
        WriteOptions::default(),
    )
    .expect_err("bool is not declared");
    let DbError::UndeclaredValueType(d) = &err else {
        panic!("expected UndeclaredValueType, got {err:?}");
    };
    assert_eq!((d.segment_index, d.entity_count), (1, 1));
    assert!(
        d.message.starts_with("segment 2 (node:level): "),
        "{}",
        d.message
    );
    assert!(!d.message.contains("more of this type"), "{}", d.message);
    assert_eq!(head_commit(&db).unwrap(), head);
}

/// A single-type batch of an undeclared type is the same typed refusal.
#[test]
fn single_type_undeclared_batch_is_the_same_typed_error() {
    let (_d, db) = fresh_db();
    declare(&db, vec![scalar("float")]);
    let err = write(
        &db,
        vec![segment(vec![series(
            "north",
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
        )])],
        vec![],
        WriteOptions::default(),
    )
    .expect_err("indexed is not declared");
    let DbError::UndeclaredValueType(d) = &err else {
        panic!("expected UndeclaredValueType, got {err:?}");
    };
    assert_eq!((d.entity.as_str(), d.entity_count), ("north", 1));
    assert_eq!(d.value_kind, "float");
    assert_eq!(d.dimensions, vec![("t".to_string(), "integer".to_string())]);
}

/// A type change of an existing value inside a mixed call is what it is
/// across commits: the float row of north is closed out and the string
/// row becomes current, bound to the string member.
#[test]
fn type_change_in_one_call_matches_separate_commits() {
    // Reference: the change in its own commit.
    let (_d1, sep) = fresh_db();
    declare(&sep, vec![scalar("float"), scalar("string")]);
    write(
        &sep,
        vec![segment(vec![floats(&["north", "south"], &[12.0, 2.0])])],
        vec![],
        WriteOptions::default(),
    )
    .unwrap();
    let r_sep = write(
        &sep,
        vec![segment(vec![strings(&["north"], &["high"])])],
        vec![],
        WriteOptions::default(),
    )
    .unwrap();

    // The same change as part of one mixed call (south rewritten as a float).
    let (_d2, one) = fresh_db();
    declare(&one, vec![scalar("float"), scalar("string")]);
    write(
        &one,
        vec![segment(vec![floats(&["north", "south"], &[12.0, 2.0])])],
        vec![],
        WriteOptions::default(),
    )
    .unwrap();
    let r_one = write(
        &one,
        vec![segment(vec![
            floats(&["south"], &[3.0]),
            strings(&["north"], &["high"]),
        ])],
        vec![],
        WriteOptions::default(),
    )
    .unwrap();

    assert_eq!(r_sep.rows_closed, 1);
    assert_eq!(r_one.rows_closed, 2); // north's float and south's old float
    let north_history = |db: &Path| {
        query::<String>(
            db,
            "SELECT vs.value_kind, coalesce(vs.v_float::VARCHAR, vs.v_str), \
                    (vs.valid_to_commit IS NULL)::VARCHAR, pt.value_kind \
             FROM value_scalar vs JOIN entity e ON e.id = vs.entity_id \
             JOIN parameter_type pt ON pt.id = vs.parameter_type_id \
             WHERE e.name = 'north' ORDER BY vs.valid_from_commit",
        )
    };
    let expected = vec![
        row(&["float", "12.0", "false", "float"]),
        row(&["string", "high", "true", "string"]),
    ];
    assert_eq!(north_history(&sep), expected);
    assert_eq!(north_history(&one), expected);
    assert_audits_clean(&sep);
    assert_audits_clean(&one);
}

/// Groups of different ranks and tiers in one segment: a float constant,
/// a float series and a string series. Both series bind to the one indexed
/// member (an indexed member does not fix the measure kind).
#[test]
fn scalar_and_indexed_groups_in_one_segment() {
    let (_d, db) = fresh_db();
    declare(&db, vec![scalar("float"), indexed_t()]);
    let report = write(
        &db,
        vec![segment(vec![
            floats(&["north"], &[5.0]),
            series("south", Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]))),
            series("east", Arc::new(StringArray::from(vec!["lo", "hi"]))),
        ])],
        vec![],
        WriteOptions::default(),
    )
    .expect("mixed ranks");
    assert_eq!(report.values_written, 3);
    let rows = read_back(&db);
    let values: Vec<(String, String, String)> = rows
        .iter()
        .map(|r| (r[0].clone(), r[3].clone(), r[4].clone()))
        .collect();
    let want = |e: &str, t: &str, v: &str| (e.to_string(), t.to_string(), v.to_string());
    assert_eq!(
        values,
        vec![
            want("east", "0", "lo"),
            want("east", "1", "hi"),
            want("north", "-", "5.0"),
            want("south", "0", "1.0"),
            want("south", "1", "2.0"),
            want("south", "2", "3.0"),
        ]
    );
    assert_eq!(
        bindings(&db),
        vec![
            row(&["east", "indexed", "1"]),
            row(&["north", "float", "0"]),
            row(&["south", "indexed", "1"]),
        ]
    );
    // Re-sent: a no-op.
    let again = write(
        &db,
        vec![segment(vec![
            floats(&["north"], &[5.0]),
            series("south", Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]))),
            series("east", Arc::new(StringArray::from(vec!["lo", "hi"]))),
        ])],
        vec![],
        WriteOptions::default(),
    )
    .unwrap();
    assert_eq!(again.commit_id, 0);
    assert_audits_clean(&db);
}

/// Batches of one type still concatenate into one value per entity (rows
/// of one series split across batches), next to a group of another type.
#[test]
fn same_type_batches_still_concatenate_beside_another_type() {
    let (_d, db) = fresh_db();
    declare(&db, vec![scalar("string"), indexed_t()]);
    let part = |t: &[i64], v: &[f64]| {
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("t", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["south"; t.len()])) as ArrayRef,
                Arc::new(Int64Array::from(t.to_vec())) as ArrayRef,
                Arc::new(Float64Array::from(v.to_vec())) as ArrayRef,
            ],
        )
        .unwrap()
    };
    write(
        &db,
        vec![segment(vec![
            part(&[0, 1], &[1.0, 2.0]),
            strings(&["north"], &["high"]),
            part(&[2], &[3.0]),
        ])],
        vec![],
        WriteOptions::default(),
    )
    .expect("split series");
    assert_eq!(
        count(
            &db,
            "SELECT count(*) FROM read_parameter('node', 'level') WHERE entity = 'south'"
        ),
        3
    );
}

/// On a parameter with no declared type, the first group materializes its
/// type; a later group of another type is held to it and refused, as a
/// second commit would be (a write never widens). Nothing is written.
#[test]
fn auto_created_parameter_takes_the_first_groups_type_only() {
    let (_d, db) = fresh_db();
    let head = head_commit(&db).unwrap();
    let err = write(
        &db,
        vec![segment(vec![
            floats(&["north"], &[12.0]),
            strings(&["south"], &["high"]),
        ])],
        vec![],
        WriteOptions::default(),
    )
    .expect_err("the string group is not the auto-created type");
    let DbError::UndeclaredValueType(d) = &err else {
        panic!("expected UndeclaredValueType, got {err:?}");
    };
    assert_eq!(
        (d.entity.as_str(), d.value_kind.as_str()),
        ("south", "string")
    );
    assert_eq!(d.declared, vec!["float rank 0"]);
    assert_eq!(head_commit(&db).unwrap(), head);
    assert_eq!(count(&db, "SELECT count(*) FROM parameter_definition"), 0);
}

/// Under `DeleteWins`, a group whose every write a delete drops resolves
/// nothing, so its type is never checked; the other group is written.
#[test]
fn delete_wins_dropping_a_whole_group_skips_its_type_check() {
    let (_d, db) = fresh_db();
    declare(&db, vec![scalar("float"), scalar("string")]);
    write(
        &db,
        vec![segment(vec![floats(&["south"], &[1.0])])],
        vec![],
        WriteOptions::default(),
    )
    .unwrap();
    let report = write(
        &db,
        vec![segment(vec![
            floats(&["north"], &[12.0]),
            bools(&["south"], &[true]),
        ])],
        vec![DeleteKey {
            class: "node".into(),
            parameter: "level".into(),
            alternative: "Base".into(),
            entity: "south".into(),
        }],
        WriteOptions {
            covered_writes: CoveredWrites::DeleteWins,
            ..Default::default()
        },
    )
    .expect("the bool group is dropped, not checked");
    assert!(report
        .warnings
        .iter()
        .any(|w| matches!(w.kind, WarningKind::DroppedByDelete { .. })));
    assert_eq!(
        read_back(&db),
        vec![row(&["north", "scalar", "float", "-", "12.0"])]
    );
}
