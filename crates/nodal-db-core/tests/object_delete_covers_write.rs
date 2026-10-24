//! A value write and an object delete covering that value in ONE
//! `commit_changes` call are refused up front (skip_unchanged_writes_PLAN
//! D10, from the P2 review). The cascade closes only rows older than the
//! commit, so under the long cell diff it would close the unchanged old
//! cells and keep the changed cells the write just inserted: a partial
//! value under a closed object. The refusal is a `DbError::Api` whatever
//! the content, the tier or the `cascade` option, for every object kind
//! whose delete reaches values (entity, by name or element tuple and as a
//! container; alternative; parameter definition; class, including a class
//! whose dimension is the deleted class). The name-level check runs before
//! the transaction; the commit repeats it on the resolved ids over the full
//! cascade closure (the `entity id` in the message marks that path).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{commit_changes, head_commit, Segment};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    ObjectDeleteKey, ObjectDeleteOpts, ObjectDeleteResult, ObjectKind, ObjectRef, WriteOptions,
};

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

/// A rank-1 integer-indexed integer series: the long tier.
fn long_batch(entity: &str, t: &[i64], v: &[i64]) -> RecordBatch {
    let n = t.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; n])) as ArrayRef,
            Arc::new(Int64Array::from(t.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(v.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

/// A rank-1 integer-indexed float series: the array tier.
fn array_batch(entity: &str, t: &[i64], v: &[f64]) -> RecordBatch {
    let n = t.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; n])) as ArrayRef,
            Arc::new(Int64Array::from(t.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(v.to_vec())) as ArrayRef,
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

/// Every row count and every live-row count of the tables a write or an
/// object delete touches: a refused call must leave all of them as they were.
fn census(db: &Path) -> Vec<(String, i64)> {
    let conn = Connection::open(db).unwrap();
    let mut out = Vec::new();
    for t in ["commit", "value_array", "value_array_hist"] {
        let n: i64 = conn
            .query_row(&format!("SELECT count(*) FROM {t}"), [], |r| r.get(0))
            .unwrap();
        out.push((t.to_string(), n));
    }
    for t in [
        "value_scalar",
        "value_long",
        "entity",
        "entity_element",
        "alternative",
        "parameter_definition",
        "entity_class",
    ] {
        let (n, live): (i64, i64) = conn
            .query_row(
                &format!("SELECT count(*), count(*) FILTER (valid_to_commit IS NULL) FROM {t}"),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        out.push((t.to_string(), n));
        out.push((format!("{t} live"), live));
    }
    out
}

/// Seed: class `node` with node_1 (long `flow` = [1, 2, 3] in alternative
/// `A`, scalar `demand` and array `profile` in Base) and node_2 (scalar
/// `demand` in Base); class `link` typed (node, node) with the N-D entity
/// link_ab = (node_1, node_2) carrying scalar `cap` in Base.
fn seeded() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("t.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();
    commit_changes(
        &db,
        vec![
            seg(
                "node",
                "flow",
                "A",
                long_batch("node_1", &[0, 1, 2], &[1, 2, 3]),
            ),
            seg("node", "demand", "Base", scalar_batch("node_1", 1.0)),
            seg(
                "node",
                "profile",
                "Base",
                array_batch("node_1", &[0, 1, 2], &[1.0, 2.0, 3.0]),
            ),
        ],
        vec![],
        vec![],
        vec![],
        "t",
        "seed",
        WriteOptions::default(),
    )
    .unwrap();
    commit_changes(
        &db,
        vec![seg("node", "demand", "Base", scalar_batch("node_2", 2.0))],
        vec![],
        vec![],
        vec![],
        "t",
        "seed node_2",
        WriteOptions::default(),
    )
    .unwrap();
    exec(
        &db,
        "INSERT INTO entity_class (name, valid_from_commit)
           VALUES ('link', (SELECT max(id) FROM commit));
         INSERT INTO entity_class_dimension (entity_class_id, position, dimension_class_id, valid_from_commit)
           VALUES ((SELECT id FROM entity_class WHERE cur_name=lower('link')), 1,
                   (SELECT id FROM entity_class WHERE cur_name=lower('node')), (SELECT max(id) FROM commit)),
                  ((SELECT id FROM entity_class WHERE cur_name=lower('link')), 2,
                   (SELECT id FROM entity_class WHERE cur_name=lower('node')), (SELECT max(id) FROM commit));
         INSERT INTO entity (entity_class_id, name, valid_from_commit)
           VALUES ((SELECT id FROM entity_class WHERE cur_name=lower('link')), 'link_ab', (SELECT max(id) FROM commit));
         INSERT INTO entity_element (entity_id, position, element_entity_id, valid_from_commit)
           VALUES ((SELECT id FROM entity WHERE cur_name=lower('link_ab')), 1,
                   (SELECT id FROM entity WHERE cur_name=lower('node_1')), (SELECT max(id) FROM commit)),
                  ((SELECT id FROM entity WHERE cur_name=lower('link_ab')), 2,
                   (SELECT id FROM entity WHERE cur_name=lower('node_2')), (SELECT max(id) FROM commit));",
    );
    commit_changes(
        &db,
        vec![seg("link", "cap", "Base", scalar_batch("link_ab", 5.0))],
        vec![],
        vec![],
        vec![],
        "t",
        "seed link_ab",
        WriteOptions::default(),
    )
    .unwrap();
    (dir, db)
}

/// How the refusal must have been found.
#[derive(Clone, Copy, Debug)]
enum Path_ {
    /// The up-front name check (no ids in the message).
    Names,
    /// The commit-time check on the resolved ids.
    Ids,
}

/// Run one write + object delete call and assert the D10 refusal: an
/// `Api` error from the expected check, nothing written, no commit.
fn assert_refused(
    db: &Path,
    what: &str,
    segments: Vec<Segment>,
    od: ObjectDeleteKey,
    opts: ObjectDeleteOpts,
    path: Path_,
) {
    let before = census(db);
    let head = head_commit(db).unwrap();
    let err = commit_changes(
        db,
        segments,
        vec![],
        vec![],
        vec![(od, opts)],
        "t",
        "write + object delete",
        WriteOptions::default(),
    )
    .expect_err(what);
    let DbError::Api(m) = &err else {
        panic!("{what}: expected DbError::Api, got {err:?}")
    };
    assert!(m.contains("its object deleted in one call"), "{what}: {m}");
    match path {
        Path_::Names => assert!(!m.contains("entity id"), "{what}: name check expected: {m}"),
        Path_::Ids => assert!(m.contains("entity id"), "{what}: id check expected: {m}"),
    }
    assert_eq!(census(db), before, "{what}: nothing written");
    assert_eq!(head_commit(db).unwrap(), head, "{what}: no commit");
}

/// The review's repro: alternative A has node_1 flow = [1, 2, 3]; one call
/// writes [1, 9, 3] to A and cascade-deletes A. Before D10 this committed
/// one live cell (pos 1 = 9) under the closed alternative.
#[test]
fn alternative_delete_with_a_changed_long_write_is_refused() {
    let (_d, db) = seeded();
    for (spelling, c) in [("A", true), ("a", true), ("A", false)] {
        assert_refused(
            &db,
            &format!("alternative {spelling} cascade {c}"),
            vec![seg(
                "node",
                "flow",
                "A",
                long_batch("node_1", &[0, 1, 2], &[1, 9, 3]),
            )],
            okey(ObjectKind::Alternative, None, spelling),
            cascade(c),
            Path_::Names,
        );
    }
}

/// Whatever the content: an identical (unchanged) write is refused too, in
/// every tier.
#[test]
fn unchanged_writes_are_refused_in_every_tier() {
    let (_d, db) = seeded();
    let cases = [
        (
            "long",
            seg(
                "node",
                "flow",
                "A",
                long_batch("node_1", &[0, 1, 2], &[1, 2, 3]),
            ),
            "A",
        ),
        (
            "scalar",
            seg("node", "demand", "Base", scalar_batch("node_1", 1.0)),
            "Base",
        ),
        (
            "array",
            seg(
                "node",
                "profile",
                "Base",
                array_batch("node_1", &[0, 1, 2], &[1.0, 2.0, 3.0]),
            ),
            "Base",
        ),
    ];
    for (what, s, alt) in cases {
        assert_refused(
            &db,
            what,
            vec![s],
            okey(ObjectKind::Alternative, None, alt),
            cascade(true),
            Path_::Names,
        );
    }
}

#[test]
fn entity_delete_by_name_is_refused() {
    let (_d, db) = seeded();
    for (class, name, c) in [
        ("node", "node_1", true),
        ("NODE", "Node_1", true),
        ("node", "node_1", false),
    ] {
        assert_refused(
            &db,
            &format!("entity {class}/{name} cascade {c}"),
            vec![seg(
                "node",
                "flow",
                "A",
                long_batch("node_1", &[0, 1, 2], &[1, 9, 3]),
            )],
            okey(ObjectKind::Entity, Some(class), name),
            cascade(c),
            Path_::Names,
        );
    }
}

/// An N-D entity addressed by its element tuple resolves only in the DB:
/// the commit-time check refuses it.
#[test]
fn entity_delete_by_element_tuple_is_refused_on_the_ids() {
    let (_d, db) = seeded();
    assert_refused(
        &db,
        "link (node_1, node_2)",
        vec![seg("link", "cap", "Base", scalar_batch("link_ab", 6.0))],
        ObjectDeleteKey {
            kind: ObjectKind::Entity,
            class: Some("link".into()),
            target: ObjectRef::Elements(vec!["node_1".into(), "node_2".into()]),
        },
        cascade(true),
        Path_::Ids,
    );
}

/// Deleting node_1 cascades into its container link_ab: a write to
/// link_ab's value is covered, though no name in the call says so.
#[test]
fn entity_delete_covering_a_container_value_is_refused_on_the_ids() {
    let (_d, db) = seeded();
    assert_refused(
        &db,
        "node_1 container link_ab",
        vec![seg("link", "cap", "Base", scalar_batch("link_ab", 6.0))],
        okey(ObjectKind::Entity, Some("node"), "node_1"),
        cascade(true),
        Path_::Ids,
    );
}

#[test]
fn parameter_delete_is_refused() {
    let (_d, db) = seeded();
    for (class, name) in [("node", "flow"), ("Node", "FLOW")] {
        assert_refused(
            &db,
            &format!("parameter {class}/{name}"),
            vec![seg(
                "node",
                "flow",
                "A",
                long_batch("node_1", &[0, 1, 2], &[1, 9, 3]),
            )],
            okey(ObjectKind::Parameter, Some(class), name),
            cascade(true),
            Path_::Names,
        );
    }
}

#[test]
fn class_delete_is_refused() {
    let (_d, db) = seeded();
    assert_refused(
        &db,
        "class node",
        vec![seg(
            "node",
            "flow",
            "A",
            long_batch("node_1", &[0, 1, 2], &[1, 9, 3]),
        )],
        okey(ObjectKind::Class, None, "NODE"),
        cascade(true),
        Path_::Names,
    );
    // `link` declares `node` as a dimension: deleting `node` cascades into
    // `link` and its values (B2), found on the ids.
    assert_refused(
        &db,
        "class node over link",
        vec![seg("link", "cap", "Base", scalar_batch("link_ab", 6.0))],
        okey(ObjectKind::Class, None, "node"),
        cascade(true),
        Path_::Ids,
    );
}

/// The resolved-id check sees what the Rust fold misses: Rust folds `ΑΣ` to
/// `ας` (final sigma) and keeps it apart from `ασ`, but DuckDB's `lower()`
/// resolves both to one entity.
#[test]
fn a_spelling_only_duckdb_folds_together_is_refused_on_the_ids() {
    let (_d, db) = seeded();
    commit_changes(
        &db,
        vec![seg("node", "demand", "Base", scalar_batch("ΑΣ", 1.0))],
        vec![],
        vec![],
        vec![],
        "t",
        "sigma",
        WriteOptions::default(),
    )
    .unwrap();
    assert_refused(
        &db,
        "final sigma",
        vec![seg("node", "demand", "Base", scalar_batch("ΑΣ", 2.0))],
        okey(ObjectKind::Entity, Some("node"), "ασ"),
        cascade(true),
        Path_::Ids,
    );
}

/// A write to a key no object delete of the call covers still commits
/// with it, in one commit.
#[test]
fn a_write_to_an_unrelated_key_alongside_an_object_delete_commits() {
    let (_d, db) = seeded();
    let r = commit_changes(
        &db,
        vec![seg("node", "demand", "Base", scalar_batch("node_2", 7.0))],
        vec![],
        vec![],
        vec![
            (okey(ObjectKind::Alternative, None, "A"), cascade(true)),
            (
                okey(ObjectKind::Parameter, Some("node"), "profile"),
                cascade(true),
            ),
        ],
        "t",
        "unrelated",
        WriteOptions::default(),
    )
    .expect("an unrelated write and object deletes commit together");
    assert!(r.commit_id > 0);
    assert_eq!(r.values_written, 1);
    for o in &r.object_deletes {
        assert!(
            matches!(o.result, ObjectDeleteResult::Deleted { .. }),
            "{o:?}"
        );
    }
    let conn = Connection::open(&db).unwrap();
    let v: f64 = conn
        .query_row(
            "SELECT vs.v_float FROM value_scalar vs JOIN entity e ON e.id = vs.entity_id \
             WHERE e.cur_name = 'node_2' AND vs.valid_to_commit IS NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(v, 7.0);
    let live_flow: i64 = conn
        .query_row(
            "SELECT count(*) FROM value_long WHERE valid_to_commit IS NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(live_flow, 0, "alternative A's flow cells are closed");
}

/// No false positives across classes: a parameter delete of `cap` in
/// `link` beside a write of `cap` in `node`, and an entity delete of
/// `node_2` (cascading into link_ab) beside a write to `node_1`, commit.
#[test]
fn same_names_in_another_class_or_a_sibling_entity_are_not_covered() {
    let (_d, db) = seeded();
    let r = commit_changes(
        &db,
        vec![
            seg("node", "cap", "Base", scalar_batch("node_1", 3.0)),
            seg("node", "demand", "Base", scalar_batch("node_1", 4.0)),
        ],
        vec![],
        vec![],
        vec![
            (
                okey(ObjectKind::Parameter, Some("link"), "cap"),
                cascade(true),
            ),
            (
                okey(ObjectKind::Entity, Some("node"), "node_2"),
                cascade(true),
            ),
            (
                okey(ObjectKind::Entity, Some("link"), "node_1"),
                cascade(true),
            ),
        ],
        "t",
        "same names elsewhere",
        WriteOptions::default(),
    )
    .expect("writes outside every object delete's closure commit");
    assert_eq!(r.values_written, 2);
    let results: Vec<bool> = r
        .object_deletes
        .iter()
        .map(|o| matches!(o.result, ObjectDeleteResult::Deleted { .. }))
        .collect();
    assert_eq!(results, vec![true, true, false], "{:?}", r.object_deletes);
}
