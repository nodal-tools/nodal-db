//! Extract, compaction and erasure on a cell-diffed long history
//! (skip_unchanged_writes_PLAN §5, §6.3): a long value whose current cells
//! were written by different commits.
//!
//! - An as-of extract reproduces the source's cells at N verbatim (each
//!   with its own `valid_from`), and its tiling is clean with the horizon.
//! - Compaction preserves every commit's as-of state.
//! - Erasing the alternative leaves its marker, and an as-of read before it
//!   fails loudly, as for a whole-rewritten history.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::write_parameter;
use nodal_db_core::compaction::compact;
use nodal_db_core::erasure::{erase_alternative, EraseOpts};
use nodal_db_core::extract::{extract, ExtractOpts};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{ObjectDeleteKey, ObjectKind, ObjectRef, WriteOptions};

/// A rank-1 int long value of `entity` at coordinates 0..vals.len().
fn batch(entity: &str, vals: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; vals.len()])) as ArrayRef,
            Arc::new(Int64Array::from((0..vals.len() as i64).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(Int64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn write(db: &Path, alt: &str, entity: &str, vals: &[i64]) -> i64 {
    write_parameter(
        db,
        "node",
        "p",
        alt,
        batch(entity, vals),
        &[],
        WriteOptions::default(),
        "t",
        "w",
    )
    .unwrap()
    .commit_id
}

fn query(db: &Path, sql: &str) -> Result<Vec<String>, String> {
    let conn = Connection::open(db).unwrap();
    let out = (|| {
        let mut stmt = conn.prepare(sql)?;
        let n = stmt.column_count();
        let rows = stmt.query_map([], |r| {
            let cells: Vec<String> = (0..n)
                .map(|i| format!("{:?}", r.get::<_, duckdb::types::Value>(i).unwrap()))
                .collect();
            Ok(cells.join("|"))
        })?;
        rows.collect::<Result<Vec<_>, _>>()
    })()
    .map_err(|e| e.to_string());
    conn.close().unwrap();
    out.map(|mut v| {
        v.sort();
        v
    })
}

fn count(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

/// The cells of every long value as of `c` (verbatim: versions and spans).
fn long_asof(c: i64) -> String {
    format!(
        "SELECT entity_id, alternative_id, i1_pos, version, valid_from_commit, v_int \
         FROM value_long_asof({c})"
    )
}

/// A source with cell-diffed history on two alternatives; returns the db
/// and the commits.
fn fixture(dir: &Path) -> (PathBuf, Vec<i64>) {
    let db = dir.join("src.duckdb");
    init_schema_with_default(&db, Some("runA")).unwrap();
    let mut commits = vec![
        write(&db, "runA", "n1", &[1, 2, 3, 4, 5, 6]),
        write(&db, "runB", "n1", &[9, 9, 9]),
    ];
    // One cell at a time, so the current cells end up from four commits.
    for vals in [
        [1, 7, 3, 4, 5, 6],
        [1, 7, 3, 8, 5, 6],
        [1, 7, 3, 8, 5, 6], // identical: no commit
        [1, 7, 3, 8, 9, 6],
    ] {
        let c = write(&db, "runA", "n1", &vals);
        if c > 0 {
            commits.push(c);
        }
    }
    commits.push(write(&db, "runB", "n1", &[9, 1, 9]));
    (db, commits)
}

#[test]
fn diffed_history_has_cells_from_several_commits() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _) = fixture(dir.path());
    let froms = count(
        &db,
        "SELECT count(DISTINCT valid_from_commit) FROM value_long \
         WHERE valid_to_commit IS NULL AND alternative_id = \
           (SELECT id FROM alternative WHERE name = 'runA')",
    );
    assert_eq!(froms, 4, "the current cells come from four commits");
}

#[test]
fn asof_extract_of_a_diffed_history_reproduces_the_cells() {
    let dir = tempfile::tempdir().unwrap();
    let (db, commits) = fixture(dir.path());
    for &n in &commits {
        let dest = dir.path().join(format!("x{n}.duckdb"));
        let report = extract(
            &db,
            &dest,
            &ExtractOpts {
                as_of: Some(n),
                user: "t",
                note: None,
            },
        )
        .unwrap();
        assert_eq!(report.horizon_commit, n);
        assert_eq!(
            query(
                &dest,
                "SELECT entity_id, alternative_id, i1_pos, version, valid_from_commit, v_int \
                 FROM value_long"
            ),
            query(&db, &long_asof(n)),
            "as of {n}"
        );
        for audit in ["audit_history_tiling", "audit_long_value_coherence"] {
            assert_eq!(
                count(&dest, &format!("SELECT count(*) FROM {audit}")),
                0,
                "{audit} as of {n}"
            );
        }
    }
}

#[test]
fn compaction_preserves_every_asof_state_of_a_diffed_history() {
    let dir = tempfile::tempdir().unwrap();
    let (db, commits) = fixture(dir.path());
    let before: Vec<_> = commits.iter().map(|&c| query(&db, &long_asof(c))).collect();
    compact(&db).unwrap();
    let after: Vec<_> = commits.iter().map(|&c| query(&db, &long_asof(c))).collect();
    assert_eq!(before, after);
    assert_eq!(
        count(&db, "SELECT count(*) FROM audit_long_value_coherence"),
        0
    );
}

#[test]
fn erasing_a_diffed_alternative_leaves_its_marker_and_guards_asof() {
    let dir = tempfile::tempdir().unwrap();
    let (db, commits) = fixture(dir.path());
    let key = ObjectDeleteKey {
        kind: ObjectKind::Alternative,
        class: None,
        target: ObjectRef::Name("runA".into()),
    };
    let report = erase_alternative(&db, &key, &EraseOpts::default(), "t").unwrap();
    assert!(report.erased);
    assert_eq!(
        count(
            &db,
            "SELECT count(*) FROM erasure_marker WHERE kind = 'alternative'"
        ),
        1
    );
    let err = query(&db, &long_asof(commits[2])).unwrap_err();
    assert!(err.contains("unreliable"), "{err}");
    assert_eq!(
        count(
            &db,
            "SELECT count(*) FROM value_long WHERE alternative_id = \
               (SELECT id FROM alternative WHERE name = 'runA')"
        ),
        0
    );
}
