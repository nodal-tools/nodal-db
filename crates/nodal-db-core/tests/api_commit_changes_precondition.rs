//! The base-version precondition of the write path (`WriteOptions::
//! expect_commit` / `conn::TxnOptions`): checked after `BEGIN` and before the
//! commit row, `!=` against the head, surfaced as `DbError::Stale`, never
//! retried, nothing written on a refusal.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{
    commit_changes, commit_structural, delete_objects, delete_values, head_commit, ingest_segments,
    write_parameter, DeleteKey, Segment,
};
use nodal_db_core::compaction::compact;
use nodal_db_core::conn::{with_write_txn_retrying_opts, TxnOptions};
use nodal_db_core::erasure::{erase_alternative, EraseOpts};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, StructuralChange, WriteOptions,
    WriteReport,
};

// --- helpers ---------------------------------------------------------------

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

fn fresh(dir: &Path) -> PathBuf {
    let db = dir.join("p.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    db
}

fn expect(c: i64) -> WriteOptions {
    WriteOptions {
        expect_commit: Some(c),
        ..Default::default()
    }
}

fn write(db: &Path, entity: &str, v: f64, opts: WriteOptions) -> Result<WriteReport, DbError> {
    write_parameter(
        db,
        "node",
        "demand",
        "Base",
        scalar_batch(entity, v),
        &[],
        opts,
        "tester",
        "precondition",
    )
}

fn q(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

fn commits(db: &Path) -> i64 {
    q(db, "SELECT count(*) FROM \"commit\"")
}

fn value_rows(db: &Path) -> i64 {
    q(db, "SELECT count(*) FROM value_scalar")
}

#[track_caller]
fn assert_stale(r: Result<WriteReport, DbError>, expected: i64, found: i64) {
    match r {
        Err(DbError::Stale {
            expected: e,
            found: f,
        }) => {
            assert_eq!((e, f), (expected, found));
        }
        other => panic!("expected Stale {{ {expected}, {found} }}, got {other:?}"),
    }
}

// --- tests -----------------------------------------------------------------

/// Plan test 1: `expect_commit` = head commits, and the new commit is above it.
#[test]
fn expect_head_commits() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    write(&db, "n1", 1.0, WriteOptions::default()).unwrap();
    let head = head_commit(&db).unwrap();
    let r = write(&db, "n1", 2.0, expect(head)).expect("expect_commit = head commits");
    assert!(r.commit_id > head);
    assert_eq!(head_commit(&db).unwrap(), r.commit_id);
}

/// Plan test 2: A commit in between: `Stale { expected, found }`, nothing written,
/// no orphan commit row.
#[test]
fn intervening_commit_is_stale_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    write(&db, "n1", 1.0, WriteOptions::default()).unwrap();
    let loaded = head_commit(&db).unwrap();
    // Someone else commits after the "load".
    let other = write(&db, "n2", 5.0, WriteOptions::default()).unwrap();
    let (c0, v0) = (commits(&db), value_rows(&db));
    assert_stale(
        write(&db, "n1", 2.0, expect(loaded)),
        loaded,
        other.commit_id,
    );
    assert_eq!(commits(&db), c0, "no commit row");
    assert_eq!(value_rows(&db), v0, "no value rows");
    assert_eq!(
        q(&db, "SELECT count(*) FROM entity WHERE cur_name = 'n1'"),
        1,
        "nothing else either"
    );
}

/// Plan test 3: The rule is `!=`: an expected id above or below the head is stale.
#[test]
fn expect_above_or_below_head_is_stale() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    write(&db, "n1", 1.0, WriteOptions::default()).unwrap();
    write(&db, "n1", 2.0, WriteOptions::default()).unwrap();
    let head = head_commit(&db).unwrap();
    assert_stale(write(&db, "n1", 3.0, expect(head + 1)), head + 1, head);
    assert_stale(write(&db, "n1", 3.0, expect(head - 1)), head - 1, head);
    assert_eq!(head_commit(&db).unwrap(), head);
}

/// Plan test 4: `None` keeps today's behavior: an intervening commit does not matter.
#[test]
fn none_is_unchecked() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    write(&db, "n1", 1.0, WriteOptions::default()).unwrap();
    write(&db, "n2", 1.0, WriteOptions::default()).unwrap();
    write(&db, "n1", 2.0, WriteOptions::default()).expect("no precondition");
}

/// Plan test 5: Every `commit_changes` wrapper honors the field.
#[test]
fn every_wrapper_honors_expect_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    write(&db, "n1", 1.0, WriteOptions::default()).unwrap();
    write(&db, "n2", 1.0, WriteOptions::default()).unwrap();

    type Call = Box<dyn Fn(&Path, WriteOptions) -> Result<WriteReport, DbError>>;
    let calls: Vec<(&str, Call)> = vec![
        (
            "write_parameter",
            Box::new(|db: &Path, o| write(db, "n1", 7.0, o)),
        ),
        (
            "ingest_segments",
            Box::new(|db: &Path, o| {
                ingest_segments(
                    db,
                    vec![Segment {
                        class: "node".into(),
                        parameter: "demand".into(),
                        alternative: "Base".into(),
                        batches: vec![scalar_batch("n2", 8.0)],
                        domains: vec![],
                    }],
                    "tester",
                    "",
                    o,
                )
            }),
        ),
        (
            "delete_values",
            Box::new(|db: &Path, o| {
                delete_values(
                    db,
                    vec![DeleteKey {
                        class: "node".into(),
                        parameter: "demand".into(),
                        alternative: "Base".into(),
                        entity: "n2".into(),
                    }],
                    "tester",
                    "",
                    o,
                )
            }),
        ),
        (
            "commit_structural",
            Box::new(|db: &Path, o| {
                commit_structural(
                    db,
                    vec![StructuralChange::CreateAlternative {
                        name: "Alt2".into(),
                        description: None,
                    }],
                    "tester",
                    "",
                    o,
                )
            }),
        ),
        (
            "delete_objects",
            Box::new(|db: &Path, o| {
                delete_objects(
                    db,
                    vec![(
                        ObjectDeleteKey {
                            kind: ObjectKind::Alternative,
                            class: None,
                            target: ObjectRef::Name("Alt2".into()),
                        },
                        ObjectDeleteOpts { cascade: true },
                    )],
                    "tester",
                    "",
                    o,
                )
            }),
        ),
        (
            "commit_changes",
            Box::new(|db: &Path, o| {
                commit_changes(
                    db,
                    vec![Segment {
                        class: "node".into(),
                        parameter: "demand".into(),
                        alternative: "Base".into(),
                        batches: vec![scalar_batch("n1", 9.0)],
                        domains: vec![],
                    }],
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    "tester",
                    "",
                    o,
                )
            }),
        ),
    ];
    for (name, call) in &calls {
        let head = head_commit(&db).unwrap();
        let c0 = commits(&db);
        match call(&db, expect(head - 1)) {
            Err(DbError::Stale { expected, found }) => {
                assert_eq!((expected, found), (head - 1, head), "{name}")
            }
            other => panic!("{name}: expected Stale, got {other:?}"),
        }
        assert_eq!(commits(&db), c0, "{name}: nothing written");
        let r = call(&db, expect(head)).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(r.commit_id, head + 1, "{name}");
    }
}

/// Plan test 6: A compaction between load and save never makes the save stale: the
/// repack preserves the commit log and the sequence state.
#[test]
fn compaction_between_load_and_save_is_not_stale() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    write(&db, "n1", 1.0, WriteOptions::default()).unwrap();
    let loaded = head_commit(&db).unwrap();
    let r = compact(&db).expect("compact");
    assert!(r.swapped);
    assert_eq!(head_commit(&db).unwrap(), loaded);
    let w = write(&db, "n1", 2.0, expect(loaded)).expect("not stale after compaction");
    assert_eq!(w.commit_id, loaded + 1);
}

/// Plan test 7: A non-nodal-db file with `expect_commit` set gets the "not an
/// initialized nodal-db database" message, not a raw binder error.
#[test]
fn non_nodal_db_file_gets_the_init_message() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("plain.duckdb");
    {
        let c = Connection::open(&db).unwrap();
        c.execute_batch("CREATE TABLE t (x INT)").unwrap();
        c.close().unwrap();
    }
    let err = write(&db, "n1", 1.0, expect(1)).unwrap_err();
    assert!(
        matches!(err, DbError::Api(_)) && err.to_string().contains("not an initialized nodal-db"),
        "{err:?}"
    );
}

/// Plan test 7a: Through the transaction layer directly: `Stale` comes back as
/// `Stale` (not `Conflict`, not `Api`), on the first attempt (the closure
/// never runs), with no commit row and no sequence value used.
#[test]
fn stale_is_not_retried_and_uses_no_sequence_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    let head = head_commit(&db).unwrap();
    let runs = std::sync::atomic::AtomicUsize::new(0);
    let err = with_write_txn_retrying_opts(
        &db,
        "tester",
        "never",
        TxnOptions {
            expect_commit: Some(head + 5),
        },
        |_conn, _c| {
            runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, DbError::Stale { expected, found } if expected == head + 5 && found == head),
        "{err:?}"
    );
    assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(head_commit(&db).unwrap(), head, "no commit row");
    // No sequence value used: the next commit is exactly head + 1.
    let r = write(&db, "n1", 1.0, expect(head)).unwrap();
    assert_eq!(r.commit_id, head + 1);
}

/// Plan test 7b: An erasure between load and save moves the head (its marker commit),
/// so a cached view that may still show the removed rows is `Stale`.
#[test]
fn erasure_between_load_and_save_is_stale() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    write(&db, "n1", 1.0, WriteOptions::default()).unwrap();
    write_parameter(
        &db,
        "node",
        "demand",
        "runA",
        scalar_batch("n1", 3.0),
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap();
    let loaded = head_commit(&db).unwrap();
    let key = ObjectDeleteKey {
        kind: ObjectKind::Alternative,
        class: None,
        target: ObjectRef::Name("runA".into()),
    };
    let e = erase_alternative(&db, &key, &EraseOpts::default(), "tester").expect("erase");
    assert!(e.erased);
    let found = head_commit(&db).unwrap();
    assert!(found > loaded);
    assert_stale(write(&db, "n1", 2.0, expect(loaded)), loaded, found);
}
