//! Every write cycle refuses a file of another schema version before it
//! writes anything (`conn::run_txn_once`): `DbError::SchemaMismatch`,
//! whatever the write. Before the gate, the outcome on an older file
//! depended on the statements a write happened to issue (on a
//! `0.4-array-id` file a 0.5 core's scalar write over an array succeeded
//! through the retained `SELECT *` pre-image copy, while the stub form,
//! which names `content_hash`, failed).
//!
//! The fixture is a current file relabelled `0.4-array-id`: the gate reads
//! only the version, so the label is what it decides on.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{commit_changes, delete_values, DeleteKey, Segment};
use nodal_db_core::compaction::SCHEMA_VERSION;
use nodal_db_core::conn::with_write_txn;
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{WriteOptions, WriteReport};

const OLD: &str = "0.4-array-id";

fn batch(entities: &[&str], t: Option<&[i64]>, vals: ArrayRef) -> RecordBatch {
    let mut fields = vec![Field::new("entity", DataType::Utf8, false)];
    let mut cols: Vec<ArrayRef> = vec![];
    let ents: Vec<&str> = match t {
        None => entities.to_vec(),
        Some(t) => entities
            .iter()
            .flat_map(|e| std::iter::repeat_n(*e, t.len()))
            .collect(),
    };
    cols.push(Arc::new(StringArray::from(ents)));
    if let Some(t) = t {
        fields.push(Field::new("t", DataType::Int64, false));
        let tt: Vec<i64> = entities.iter().flat_map(|_| t.iter().copied()).collect();
        cols.push(Arc::new(Int64Array::from(tt)));
    }
    fields.push(Field::new("value", vals.data_type().clone(), false));
    cols.push(vals);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

fn seg(parameter: &str, b: RecordBatch) -> Segment {
    Segment {
        class: "node".into(),
        parameter: parameter.into(),
        alternative: "Base".into(),
        batches: vec![b],
        domains: vec![],
    }
}

fn scalar(e: &str, v: f64) -> Segment {
    seg(
        "s",
        batch(&[e], None, Arc::new(Float64Array::from(vec![v]))),
    )
}
fn long(e: &str, k: i64) -> Segment {
    let t = [0, 2, 5];
    seg(
        "l",
        batch(
            &[e],
            Some(&t),
            Arc::new(Int64Array::from(vec![k, k + 1, k + 2])),
        ),
    )
}
fn array(param: &str, e: &str, k: f64) -> Segment {
    let t = [0, 1, 2];
    seg(
        param,
        batch(
            &[e],
            Some(&t),
            Arc::new(Float64Array::from(vec![k, k + 1.0, k + 2.0])),
        ),
    )
}

fn commit(db: &Path, segs: Vec<Segment>, opts: WriteOptions) -> Result<WriteReport, DbError> {
    commit_changes(db, segs, vec![], vec![], vec![], "t", "m", opts)
}

/// Per base table: row count and an order-independent hash of its rows.
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

/// A file with a scalar, a long and two arrays, relabelled `OLD`.
fn old_file(dir: &Path) -> PathBuf {
    let db = dir.join("old.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    commit(
        &db,
        vec![
            scalar("a", 1.0),
            long("a", 10),
            array("p", "a", 1.0),
            array("q", "a", 1.0),
        ],
        WriteOptions::default(),
    )
    .expect("seed");
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "UPDATE database_property SET value = ? \
         WHERE lower(key) = 'schema_version' AND valid_to_commit IS NULL",
        [OLD],
    )
    .unwrap();
    conn.close().unwrap();
    db
}

fn assert_refused(what: &str, r: Result<impl std::fmt::Debug, DbError>) {
    match r {
        Err(DbError::SchemaMismatch {
            found,
            expected,
            message,
        }) => {
            assert_eq!(found, OLD, "{what}");
            assert_eq!(expected, SCHEMA_VERSION, "{what}");
            assert!(message.contains("write refused"), "{what}: {message}");
        }
        other => panic!("{what}: expected SchemaMismatch, got {other:?}"),
    }
}

#[test]
fn every_write_to_an_older_file_is_refused_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = old_file(dir.path());
    let before = census(&db);
    let retained = WriteOptions {
        retain_override: Some(true),
        ..Default::default()
    };
    let stub = WriteOptions {
        retain_override: Some(false),
        ..Default::default()
    };
    let force = WriteOptions {
        force_replace: true,
        ..Default::default()
    };
    for (name, opts) in [
        ("default", WriteOptions::default()),
        ("retained", retained),
        ("stub", stub),
        ("force_replace", force),
    ] {
        // New keys in every tier, replaces in every tier, and the tier
        // transitions: a scalar over the array p, an array over the long.
        let cases: Vec<(&str, Vec<Segment>)> = vec![
            ("new scalar", vec![scalar("b", 2.0)]),
            ("new long", vec![long("b", 20)]),
            ("new array", vec![array("p", "b", 2.0)]),
            ("scalar replace", vec![scalar("a", 3.0)]),
            ("long replace", vec![long("a", 30)]),
            ("array replace", vec![array("q", "a", 3.0)]),
            (
                "scalar over an array",
                vec![seg(
                    "p",
                    batch(&["a"], None, Arc::new(Float64Array::from(vec![5.0]))),
                )],
            ),
            ("array over a long", vec![array("l", "a", 7.0)]),
        ];
        for (what, segs) in cases {
            assert_refused(&format!("{what} ({name})"), commit(&db, segs, opts));
        }
        assert_refused(
            &format!("delete ({name})"),
            delete_values(
                &db,
                vec![DeleteKey {
                    class: "node".into(),
                    parameter: "q".into(),
                    alternative: "Base".into(),
                    entity: "a".into(),
                }],
                "t",
                "del",
                opts,
            ),
        );
    }
    assert_refused(
        "a new entity (verb)",
        nodal_db_core::verbs::ensure_entity(&db, "node", Some("zz"), &[], "t"),
    );
    assert_refused(
        "a caller's own write transaction",
        with_write_txn(&db, "t", "m", |_c, _id| Ok(())),
    );
    assert_eq!(census(&db), before, "nothing written");
}

/// A file in the pre-0.6 layout (its schema version in the `metadata` table,
/// no `database_property`) is named by its own version on every path that
/// gates on it — a write, compaction, extract and erase — never refused with
/// a raw "table does not exist" catalog error.
#[test]
fn a_pre_0_6_layout_file_is_named_by_its_own_version() {
    const PRE: &str = "0.5-array-hash";
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("pre.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    commit(&db, vec![scalar("a", 1.0)], WriteOptions::default()).expect("seed");
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(&format!(
        "CREATE TABLE metadata (id BIGINT, name VARCHAR, value VARCHAR, \
           valid_from_commit BIGINT, valid_to_commit BIGINT, version SMALLINT); \
         INSERT INTO metadata SELECT 1, key, value, valid_from_commit, NULL, 1 \
           FROM database_property WHERE valid_to_commit IS NULL; \
         UPDATE metadata SET value = '{PRE}' WHERE name = 'schema_version'; \
         DROP TABLE database_property; \
         DROP TABLE value_metadata_link; DROP TABLE object_metadata_link; \
         DROP TABLE source_license;"
    ))
    .unwrap();
    conn.close().unwrap();
    let named = |what: &str, r: Result<(), DbError>| match r {
        Err(DbError::SchemaMismatch { found, .. }) => assert_eq!(found, PRE, "{what}"),
        other => panic!("{what}: expected SchemaMismatch, got {other:?}"),
    };
    named(
        "write",
        commit(&db, vec![scalar("b", 2.0)], WriteOptions::default()).map(|_| ()),
    );
    // Read paths that touch the new tables are named too.
    named(
        "database_property",
        nodal_db_core::api::database_property(&db, "title").map(|_| ()),
    );
    named(
        "database_properties",
        nodal_db_core::api::database_properties(&db).map(|_| ()),
    );
    named(
        "preview_object_deletes",
        nodal_db_core::api::preview_object_deletes(
            &db,
            vec![nodal_db_core::types::ObjectDeleteKey {
                kind: nodal_db_core::types::ObjectKind::Entity,
                class: Some("node".into()),
                target: nodal_db_core::types::ObjectRef::Name("a".into()),
            }],
        )
        .map(|_| ()),
    );
    named(
        "preview_erase_alternative",
        nodal_db_core::erasure::preview_erase_alternative(
            &db,
            &nodal_db_core::types::ObjectDeleteKey {
                kind: nodal_db_core::types::ObjectKind::Alternative,
                class: None,
                target: nodal_db_core::types::ObjectRef::Name("Base".into()),
            },
            &nodal_db_core::erasure::EraseOpts::default(),
        )
        .map(|_| ()),
    );
    named(
        "preview_purge",
        nodal_db_core::api::preview_purge(
            &db,
            vec![nodal_db_core::types::PurgeRequest {
                scope: nodal_db_core::types::PurgeScope::Values {
                    class: None,
                    parameter: None,
                    alternative: None,
                },
                allow_outside_scope: false,
                allow_on_template: false,
            }],
        )
        .map(|_| ()),
    );
    named("compact", nodal_db_core::compaction::compact(&db).map(|_| ()));
    named(
        "extract",
        nodal_db_core::extract::extract(
            &db,
            &dir.path().join("x.duckdb"),
            &nodal_db_core::extract::ExtractOpts {
                as_of: None,
                user: "t",
                note: None,
            },
        )
        .map(|_| ()),
    );
    named(
        "erase",
        nodal_db_core::erasure::erase_alternative(
            &db,
            &nodal_db_core::types::ObjectDeleteKey {
                kind: nodal_db_core::types::ObjectKind::Alternative,
                class: None,
                target: nodal_db_core::types::ObjectRef::Name("Base".into()),
            },
            &nodal_db_core::erasure::EraseOpts::default(),
            "t",
        )
        .map(|_| ()),
    );
}
