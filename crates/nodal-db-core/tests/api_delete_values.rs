//! End-to-end tests for the sanctioned delete + combined write/delete path
//! (`api::delete_values` / `api::commit_changes`, delete design
//! `specs/api_delete_design.md`). Each test builds a fresh schema DB via
//! `init_schema`, writes through the public write path, and deletes through the
//! new entry — exercising lookup-only name resolution (no auto-create), the
//! machine-readable UnknownReferent/NothingToDelete warnings, append-only
//! close-out + as-of retention, whole-invocation atomicity, and monotonic
//! versioning across write→delete→re-write on every tier.

use std::sync::Arc;

use duckdb::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{
    commit_changes, delete_values, read_parameter, write_parameter, DeleteKey, Segment,
};
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::types::{WarningKind, WriteOptions};

// --- batch builders --------------------------------------------------------

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

fn scalar_batch2(entities: &[&str], vals: &[f64]) -> RecordBatch {
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

/// Rank-1 integer-indexed float batch — full Cartesian ⇒ array tier.
fn indexed_float_batch(entity: &str, dim: &str, coords: &[i64], vals: &[f64]) -> RecordBatch {
    let n = coords.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new(dim, DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; n])) as ArrayRef,
            Arc::new(Int64Array::from(coords.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Rank-1 integer-indexed integer batch — int measure ⇒ long tier.
fn indexed_int_batch(entity: &str, dim: &str, coords: &[i64], vals: &[i64]) -> RecordBatch {
    let n = coords.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new(dim, DataType::Int64, false),
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

/// Rank-1 label batch WITHOUT any domain: canonicalization fails (MissingDomain)
/// only INSIDE the transaction — the mid-closure roll-back probe.
fn undomained_label_batch(entity: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("level", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity])) as ArrayRef,
            Arc::new(StringArray::from(vec!["low"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn seg(class: &str, parameter: &str, alternative: &str, batch: RecordBatch) -> Segment {
    Segment {
        class: class.into(),
        parameter: parameter.into(),
        alternative: alternative.into(),
        batches: vec![batch],
        domains: vec![],
    }
}

fn dk(class: &str, parameter: &str, alternative: &str, entity: &str) -> DeleteKey {
    DeleteKey {
        class: class.into(),
        parameter: parameter.into(),
        alternative: alternative.into(),
        entity: entity.into(),
    }
}

// --- fixture helpers -------------------------------------------------------

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("delete.duckdb");
    init_schema(&db).expect("schema");
    (dir, db)
}

fn count(db: &std::path::Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

/// Every scalar row live AS-OF `commit` (append-only history read): the
/// as-of predicate `valid_from <= c < valid_to`. Returns `(content_retained,
/// v_float)` per row so a retained value round-trips and a stub does not.
fn as_of_scalar(db: &std::path::Path, commit: i64) -> Vec<(bool, Option<f64>)> {
    let conn = Connection::open(db).unwrap();
    let rows = conn
        .prepare(
            "SELECT content_retained, v_float FROM value_scalar \
             WHERE valid_from_commit <= ? AND (valid_to_commit IS NULL OR valid_to_commit > ?) \
             ORDER BY version",
        )
        .unwrap()
        .query_map(duckdb::params![commit, commit], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    conn.close().unwrap();
    rows
}

fn audit_clean(db: &std::path::Path) {
    assert_eq!(
        count(db, "SELECT count(*) FROM audit_history_tiling"),
        0,
        "audit_history_tiling must stay empty after every step"
    );
}

fn o() -> WriteOptions {
    WriteOptions::default()
}

// --- tests -----------------------------------------------------------------

/// write a scalar → delete → gone from the read view, the history row is closed
/// but still queryable as-of the prior commit (retain_content = TRUE).
#[test]
fn scalar_delete_removes_current_and_stays_as_of_queryable() {
    let (_dir, db) = fresh_db();

    let w = write_parameter(&db, "node", "demand", "scen_a", scalar_batch("node_1", 42.5), &[], o(), "t", "v1")
        .expect("write");
    let c_write = w.commit_id;

    let r = delete_values(&db, vec![dk("node", "demand", "scen_a", "node_1")], "t", "del", o())
        .expect("delete");
    assert!(r.warnings.is_empty(), "clean delete has no warnings: {:?}", r.warnings);
    assert_eq!(r.rows_closed, 1, "one scalar row closed");
    assert_eq!(r.values_written, 1, "one value deleted");
    assert_eq!(r.rows_written, 0, "delete writes no new current row");
    assert!(r.tiers.is_empty(), "delete touches no tier");
    assert!(r.commit_id > c_write, "delete stamps a later commit");

    // Gone from the current read view.
    assert_eq!(read_parameter(&db, "node", "demand", "scen_a").unwrap().num_rows(), 0);

    // The physical row is closed out, not removed: content retained + still
    // live (and correct) AS-OF the write commit.
    assert_eq!(count(&db, "SELECT count(*) FROM value_scalar"), 1, "row retained, not deleted");
    assert_eq!(
        count(&db, "SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NOT NULL"),
        1,
        "the one row is closed out",
    );
    assert_eq!(
        as_of_scalar(&db, c_write),
        vec![(true, Some(42.5))],
        "the deleted value round-trips as-of the prior commit",
    );
    audit_clean(&db);
}

/// retain=FALSE negative: a retain_content=FALSE definition's deleted value is
/// stubbed — content dropped, so it does NOT round-trip even as-of.
#[test]
fn retain_false_delete_stubs_and_is_not_recoverable_as_of() {
    let (_dir, db) = fresh_db();

    // Pre-create a class + a retain_content=FALSE definition (a result class).
    {
        let conn = Connection::open(&db).unwrap();
        let cls: i64 = conn
            .query_row(
                "INSERT INTO entity_class (name, valid_from_commit) VALUES ('unit', 1) RETURNING id",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute(
            "INSERT INTO parameter_definition (entity_class_id, name, retain_content, valid_from_commit) \
             VALUES (?, 'result', FALSE, 1)",
            duckdb::params![cls],
        )
        .unwrap();
        conn.close().unwrap();
    }

    let w = write_parameter(&db, "unit", "result", "Base", scalar_batch("u1", 7.0), &[], o(), "t", "v1")
        .expect("write");
    let c_write = w.commit_id;

    delete_values(&db, vec![dk("unit", "result", "Base", "u1")], "t", "del", o()).expect("delete");

    // As-of the write commit the row exists but is a stub: content NOT retained,
    // payload NULL — the value is unrecoverable, never round-trips.
    assert_eq!(
        as_of_scalar(&db, c_write),
        vec![(false, None)],
        "retain_content=FALSE delete stubs the closed-out row",
    );
    audit_clean(&db);
}

/// delete a missing value (referents all resolve, but no live value) ⇒
/// NothingToDelete warning + NO value-state change.
#[test]
fn delete_missing_value_warns_nothing_to_delete() {
    let (_dir, db) = fresh_db();

    write_parameter(&db, "node", "demand", "Base", scalar_batch("node_1", 1.0), &[], o(), "t", "v1")
        .expect("write");
    // First delete closes it out.
    delete_values(&db, vec![dk("node", "demand", "Base", "node_1")], "t", "del1", o()).expect("del1");

    // Snapshot value state; the second delete must not touch it.
    let before: Vec<(i16, Option<i64>, Option<f64>)> = {
        let conn = Connection::open(&db).unwrap();
        let v = conn
            .prepare("SELECT version, valid_to_commit, v_float FROM value_scalar ORDER BY version")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        conn.close().unwrap();
        v
    };

    let r = delete_values(&db, vec![dk("node", "demand", "Base", "node_1")], "t", "del2", o())
        .expect("del2");
    assert_eq!(r.rows_closed, 0, "nothing was live to close");
    assert_eq!(r.values_written, 0, "no value counted for a no-op delete");
    assert_eq!(r.warnings.len(), 1, "exactly one warning");
    assert!(
        matches!(r.warnings[0].kind, WarningKind::NothingToDelete { key_index: 0 }),
        "unexpected: {:?}",
        r.warnings[0]
    );

    let after: Vec<(i16, Option<i64>, Option<f64>)> = {
        let conn = Connection::open(&db).unwrap();
        let v = conn
            .prepare("SELECT version, valid_to_commit, v_float FROM value_scalar ORDER BY version")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        conn.close().unwrap();
        v
    };
    assert_eq!(before, after, "no-op delete changed no value state");
    audit_clean(&db);
}

/// unknown class/param/alt/entity ⇒ one UnknownReferent{level} + skip, and the
/// short-circuit names the FIRST missing level.
#[test]
fn unknown_referent_warns_with_level_and_skips() {
    let (_dir, db) = fresh_db();
    // Establish node/demand/Base/node_1 as the resolvable baseline.
    write_parameter(&db, "node", "demand", "Base", scalar_batch("node_1", 1.0), &[], o(), "t", "v1")
        .expect("write");

    for (key, level) in [
        (dk("nope", "demand", "Base", "node_1"), "entity_class"),
        (dk("node", "nope", "Base", "node_1"), "parameter_definition"),
        (dk("node", "demand", "nope", "node_1"), "alternative"),
        (dk("node", "demand", "Base", "nope"), "entity"),
    ] {
        let r = delete_values(&db, vec![key], "t", "del", o()).expect("delete");
        assert_eq!(r.rows_closed, 0, "nothing deleted for {level}");
        assert_eq!(r.warnings.len(), 1, "one warning for {level}");
        match &r.warnings[0].kind {
            WarningKind::UnknownReferent { key_index, level: got } => {
                assert_eq!(*key_index, 0);
                assert_eq!(got, level, "level names the first missing referent");
            }
            other => panic!("expected UnknownReferent for {level}, got {other:?}"),
        }
    }
    // The baseline value was never touched by the skipped deletes.
    assert_eq!(read_parameter(&db, "node", "demand", "Base").unwrap().num_rows(), 1);
    audit_clean(&db);
}

/// an unknown key is skipped while every other key in the SAME call is applied.
#[test]
fn unknown_key_skipped_other_keys_applied() {
    let (_dir, db) = fresh_db();
    write_parameter(&db, "node", "demand", "Base", scalar_batch2(&["a", "b"], &[1.0, 2.0]), &[], o(), "t", "w")
        .expect("write");

    let r = delete_values(
        &db,
        vec![
            dk("ghost", "demand", "Base", "a"), // unknown class → skip
            dk("node", "demand", "Base", "a"),  // real → deleted
            dk("node", "demand", "Base", "b"),  // real → deleted
        ],
        "t",
        "mixed",
        o(),
    )
    .expect("delete");
    assert_eq!(r.rows_closed, 2, "the two resolvable values are deleted");
    assert_eq!(r.warnings.len(), 1, "only the ghost key warns");
    assert!(matches!(
        r.warnings[0].kind,
        WarningKind::UnknownReferent { key_index: 0, .. }
    ));
    assert_eq!(read_parameter(&db, "node", "demand", "Base").unwrap().num_rows(), 0, "both gone");
    audit_clean(&db);
}

/// multi-key atomic: several deletes in one call ⇒ exactly ONE new commit row.
#[test]
fn multi_key_delete_is_one_commit() {
    let (_dir, db) = fresh_db();
    write_parameter(&db, "node", "demand", "Base", scalar_batch2(&["a", "b"], &[1.0, 2.0]), &[], o(), "t", "w")
        .expect("write");
    let before = count(&db, "SELECT count(*) FROM commit");

    let r = delete_values(
        &db,
        vec![dk("node", "demand", "Base", "a"), dk("node", "demand", "Base", "b")],
        "t",
        "multi",
        o(),
    )
    .expect("delete");
    assert_eq!(r.rows_closed, 2);
    assert_eq!(count(&db, "SELECT count(*) FROM commit") - before, 1, "one commit for the whole batch");
    assert_eq!(read_parameter(&db, "node", "demand", "Base").unwrap().num_rows(), 0);
    audit_clean(&db);
}

/// combined `commit_changes`: a write + a delete in one call ⇒ both applied
/// under ONE commit row.
#[test]
fn combined_write_and_delete_one_commit() {
    let (_dir, db) = fresh_db();
    // node_1 exists so the delete has something to close.
    write_parameter(&db, "node", "demand", "Base", scalar_batch("node_1", 1.0), &[], o(), "t", "seed")
        .expect("seed");
    let before = count(&db, "SELECT count(*) FROM commit");

    let r = commit_changes(
        &db,
        vec![seg("node", "demand", "Base", scalar_batch("node_2", 9.0))], // write
        vec![dk("node", "demand", "Base", "node_1")],                     // delete
        "t",
        "atomic save",
        o(),
    )
    .expect("commit_changes");

    assert_eq!(count(&db, "SELECT count(*) FROM commit") - before, 1, "one commit for write+delete");
    assert_eq!(r.rows_written, 1, "the write added one current row");
    assert_eq!(r.rows_closed, 1, "the delete closed one row");

    let got = read_parameter(&db, "node", "demand", "Base").expect("read");
    // Only node_2 remains current; node_1 was deleted in the same commit.
    let names: Vec<String> = {
        let i = got.schema().index_of("entity").unwrap();
        let a = got.column(i).as_any().downcast_ref::<StringArray>().unwrap();
        (0..a.len()).map(|r| a.value(r).to_string()).collect()
    };
    assert_eq!(names, vec!["node_2".to_string()], "node_1 deleted, node_2 written, one commit");
    audit_clean(&db);
}

/// atomic rollback: a whole-invocation error mid-batch closes NOTHING and
/// persists NO commit row (the write's close-out AND the queued delete both
/// roll back).
#[test]
fn combined_error_rolls_back_whole_invocation() {
    let (_dir, db) = fresh_db();
    write_parameter(&db, "node", "demand", "Base", scalar_batch("node_1", 1.0), &[], o(), "t", "seed")
        .expect("seed");
    let commits_before = count(&db, "SELECT count(*) FROM commit");

    let err = commit_changes(
        &db,
        vec![
            // seg 1 overwrites node_1/demand → CLOSES version 1, writes version 2.
            seg("node", "demand", "Base", scalar_batch("node_1", 2.0)),
            // seg 2 fails at canonicalization (MissingDomain) mid-transaction.
            seg("node", "level_param", "Base", undomained_label_batch("node_1")),
        ],
        vec![dk("node", "demand", "Base", "node_1")], // queued delete, never reached
        "t",
        "must roll back",
        o(),
    )
    .expect_err("undomained segment must fail the whole invocation");
    assert!(err.to_string().contains("level_param"), "error names the failing segment: {err}");

    // Nothing persisted: no new commit; node_1/demand still its ORIGINAL value 1.0
    // (the mid-closure close-out of version 1 was rolled back); no version 2 row.
    assert_eq!(count(&db, "SELECT count(*) FROM commit"), commits_before, "no commit row persisted");
    assert_eq!(count(&db, "SELECT count(*) FROM value_scalar"), 1, "no version-2 row, close-out rolled back");
    assert_eq!(
        count(&db, "SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NOT NULL"),
        0,
        "no row was closed",
    );
    let got = read_parameter(&db, "node", "demand", "Base").expect("read");
    assert_eq!(got.num_rows(), 1, "the original value is intact");
    audit_clean(&db);
}

/// write→delete→re-write monotonic versions on the SCALAR tier, audit clean
/// after every step.
#[test]
fn write_delete_rewrite_monotonic_scalar() {
    let (_dir, db) = fresh_db();

    write_parameter(&db, "node", "demand", "Base", scalar_batch("node_1", 1.0), &[], o(), "t", "v1")
        .expect("v1");
    audit_clean(&db);
    delete_values(&db, vec![dk("node", "demand", "Base", "node_1")], "t", "del", o()).expect("del");
    audit_clean(&db);
    write_parameter(&db, "node", "demand", "Base", scalar_batch("node_1", 2.0), &[], o(), "t", "v2")
        .expect("v2");
    audit_clean(&db);

    let conn = Connection::open(&db).unwrap();
    let rows: Vec<(i16, bool)> = conn
        .prepare("SELECT version, valid_to_commit IS NULL FROM value_scalar ORDER BY version")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    conn.close().unwrap();
    // v1 written, v1 closed on delete, v2 on re-write: monotone 1 → 2, only v2 live.
    assert_eq!(rows, vec![(1, false), (2, true)], "monotone versions; only v2 current");
}

/// write→delete→re-write monotonic versions on the ARRAY tier.
#[test]
fn write_delete_rewrite_monotonic_array() {
    let (_dir, db) = fresh_db();

    let w = write_parameter(&db, "node", "profile", "Base", indexed_float_batch("node_1", "step", &[0, 1, 2], &[1.0, 2.0, 3.0]), &[], o(), "t", "v1")
        .expect("v1");
    assert_eq!(w.tiers, vec![nodal_db_core::types::Tier::Array], "array tier");
    audit_clean(&db);
    delete_values(&db, vec![dk("node", "profile", "Base", "node_1")], "t", "del", o()).expect("del");
    audit_clean(&db);
    write_parameter(&db, "node", "profile", "Base", indexed_float_batch("node_1", "step", &[0, 1, 2], &[4.0, 5.0, 6.0]), &[], o(), "t", "v2")
        .expect("v2");
    audit_clean(&db);

    // v1 lived, was moved to the _hist twin on delete, v2 is the current row.
    assert_eq!(count(&db, "SELECT count(*) FROM value_array"), 1, "one live array row");
    assert_eq!(count(&db, "SELECT version FROM value_array"), 2, "current row is version 2");
    assert_eq!(count(&db, "SELECT count(*) FROM value_array_hist"), 1, "the pre-image is retained");
    assert_eq!(count(&db, "SELECT version FROM value_array_hist"), 1, "history holds version 1");
}

/// write→delete→re-write monotonic versions on the LONG tier.
#[test]
fn write_delete_rewrite_monotonic_long() {
    let (_dir, db) = fresh_db();

    let w = write_parameter(&db, "node", "counts", "Base", indexed_int_batch("node_1", "step", &[10, 20, 30], &[100, 200, 300]), &[], o(), "t", "v1")
        .expect("v1");
    assert_eq!(w.tiers, vec![nodal_db_core::types::Tier::Long], "long tier");
    audit_clean(&db);
    delete_values(&db, vec![dk("node", "counts", "Base", "node_1")], "t", "del", o()).expect("del");
    audit_clean(&db);
    write_parameter(&db, "node", "counts", "Base", indexed_int_batch("node_1", "step", &[10, 20, 30], &[111, 222, 333]), &[], o(), "t", "v2")
        .expect("v2");
    audit_clean(&db);

    let conn = Connection::open(&db).unwrap();
    // Distinct (version, is_current) groupings: version-1 cells all closed,
    // version-2 cells all current.
    let rows: Vec<(i16, bool, i64)> = conn
        .prepare(
            "SELECT version, valid_to_commit IS NULL AS live, count(*) \
             FROM value_long GROUP BY version, live ORDER BY version",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    conn.close().unwrap();
    assert_eq!(
        rows,
        vec![(1, false, 3), (2, true, 3)],
        "3 cells at version 1 (closed) + 3 at version 2 (current); monotone",
    );
}

/// caller-contract errors: nothing-to-do, blank component, fold-colliding
/// distinct spellings — all rejected before any commit.
#[test]
fn caller_contract_errors_are_rejected() {
    let (_dir, db) = fresh_db();

    // Empty segments AND empty deletes ⇒ Api error.
    let err = delete_values(&db, vec![], "t", "m", o()).expect_err("empty must be rejected");
    assert!(err.to_string().contains("nothing to do"), "unexpected: {err}");

    // Blank component ⇒ Api error.
    let err = delete_values(&db, vec![dk("node", " ", "Base", "e")], "t", "m", o())
        .expect_err("blank component must be rejected");
    assert!(err.to_string().contains("parameter"), "unexpected: {err}");

    // Fold-colliding distinct spellings (case-alias) ⇒ Api error.
    let err = delete_values(
        &db,
        vec![dk("node", "demand", "Base", "N1"), dk("Node", "demand", "base", "n1")],
        "t",
        "m",
        o(),
    )
    .expect_err("case-aliased duplicate must be rejected");
    assert!(err.to_string().contains("same value"), "unexpected: {err}");

    // Blank user ⇒ Api error.
    let err = delete_values(&db, vec![dk("node", "demand", "Base", "e")], "  ", "m", o())
        .expect_err("blank user must be rejected");
    assert!(err.to_string().contains("user"), "unexpected: {err}");

    assert_eq!(count(&db, "SELECT count(*) FROM commit"), 1, "bootstrap commit only — no DB work");
}

/// identical keys in one call are silently deduped (not an error, not a
/// double-count).
#[test]
fn identical_keys_are_deduped() {
    let (_dir, db) = fresh_db();
    write_parameter(&db, "node", "demand", "Base", scalar_batch("node_1", 1.0), &[], o(), "t", "w")
        .expect("write");

    let r = delete_values(
        &db,
        vec![dk("node", "demand", "Base", "node_1"), dk("node", "demand", "Base", "node_1")],
        "t",
        "dupe",
        o(),
    )
    .expect("delete");
    assert_eq!(r.rows_closed, 1, "the identical second key was deduped, not double-applied");
    assert!(r.warnings.is_empty(), "no warning: the single delete succeeded");
    audit_clean(&db);
}
