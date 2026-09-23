//! End-to-end tests for ALTERNATIVE object deletion (object_delete plan §6,
//! P1). A fresh schema DB seeds `Base` explicitly (`init_schema_with_default`,
//! decision D3); values are written through the public write path; alternatives
//! are deleted through the new `commit_changes`/`delete_objects` entry. These
//! exercise the RESTRICT report, the cascade close-out (incl. array →
//! `value_array_hist`), lookup-only resolution, as-of retention, monotonic
//! versioning + clean tiling, idempotent re-delete ⇒ NotFound, whole-invocation
//! atomicity (incl. the §2 same-commit write+cascade guard), and the read-only
//! preview. Also pins the change-model ruling-2 audit fix: a `run` referencing a
//! deleted alternative is NOT flagged dangling.

use std::sync::Arc;

use duckdb::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{
    commit_changes, delete_objects, preview_object_deletes, read_parameter, write_parameter,
    DeleteKey, Segment,
};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    Blocker, CascadeCounts, ObjectDeleteKey, ObjectDeleteOpts, ObjectDeleteResult, ObjectKind,
    ObjectRef, TableCount, WriteOptions,
};

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

/// Rank-1 integer-indexed float batch — full Cartesian ⇒ array tier.
fn array_batch(entity: &str, coords: &[i64], vals: &[f64]) -> RecordBatch {
    let n = coords.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
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

// --- helpers ---------------------------------------------------------------

fn o() -> WriteOptions {
    WriteOptions::default()
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

/// An alternative object-delete key (P1's only exercised kind).
fn oalt(name: &str) -> ObjectDeleteKey {
    ObjectDeleteKey {
        kind: ObjectKind::Alternative,
        class: None,
        target: ObjectRef::Name(name.into()),
    }
}

fn cascade(c: bool) -> ObjectDeleteOpts {
    ObjectDeleteOpts { cascade: c }
}

/// A second object-delete request that is guaranteed to FORCE a genuine error
/// *inside* the commit transaction (mid-batch), for the atomic-rollback tests.
/// It addresses a 0-D class (`node`, present in every rollback test's seed) by an
/// element tuple, which resolution rejects ("class 'node' is 0-D") only after the
/// preceding valid delete has already been applied — proving the whole txn rolls
/// back. (Class delete is now implemented, so `ObjectKind::Class` no longer
/// serves as an unimplemented-kind vehicle.)
fn forced_txn_error_key_for(class: &str) -> ObjectDeleteKey {
    ObjectDeleteKey {
        kind: ObjectKind::Entity,
        class: Some(class.into()),
        // Element name is irrelevant: the 0-D-class rejection fires first.
        target: ObjectRef::Elements(vec!["x".into()]),
    }
}

fn forced_txn_error_key() -> ObjectDeleteKey {
    forced_txn_error_key_for("node")
}

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("objdel.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("schema");
    (dir, db)
}

fn count(db: &std::path::Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

fn exec(db: &std::path::Path, sql: &str) {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch(sql).unwrap();
    conn.close().unwrap();
}

fn audit_tiling_clean(db: &std::path::Path) {
    assert_eq!(
        count(db, "SELECT count(*) FROM audit_history_tiling"),
        0,
        "audit_history_tiling must stay empty after an object delete"
    );
}

fn per_table<'a>(cc: &'a CascadeCounts, table: &str) -> Option<&'a TableCount> {
    cc.per_table.iter().find(|t| t.table == table)
}

fn blocker<'a>(bs: &'a [Blocker], table: &str) -> Option<&'a Blocker> {
    bs.iter().find(|b| b.table == table)
}

/// Seed alternative `alt` (auto-created by the first write) with one dependent
/// in every alternative leg (§4a): value_scalar, value_array, value_long, plus
/// the raw-inserted entity_alternative / scenario_alternative / membership /
/// parameter_value_metadata rows. Returns nothing; counts are deterministic:
/// scalar=1, array=1, long=3, and the four raw legs = 1 each.
fn seed_full_deps(db: &std::path::Path, alt: &str) {
    // Value tiers, through the one write pipeline (auto-creates `alt`, node
    // class, node_1, and the three parameter definitions).
    write_parameter(db, "node", "demand", alt, scalar_batch("node_1", 1.0), &[], o(), "t", "s")
        .expect("scalar");
    write_parameter(db, "node", "profile", alt, array_batch("node_1", &[0, 1, 2], &[1.0, 2.0, 3.0]), &[], o(), "t", "a")
        .expect("array");
    write_parameter(db, "node", "counts", alt, long_batch("node_1", &[0, 1, 2], &[10, 20, 30]), &[], o(), "t", "l")
        .expect("long");
    // node_2 (a distinct entity, in Base) so membership has two members.
    write_parameter(db, "node", "demand", "Base", scalar_batch("node_2", 5.0), &[], o(), "t", "n2")
        .expect("node_2");

    // Raw dependents (tables the value write path never touches). Each attaches
    // to the latest commit and resolves ids by name via subqueries.
    exec(
        db,
        &format!(
            "INSERT INTO entity_alternative (entity_id, alternative_id, active, valid_from_commit)
             VALUES ((SELECT id FROM entity WHERE cur_name=lower('node_1')),
                     (SELECT id FROM alternative WHERE cur_name=lower('{alt}')),
                     TRUE, (SELECT max(id) FROM commit));

             INSERT INTO scenario (name, valid_from_commit)
             VALUES ('s1', (SELECT max(id) FROM commit));
             INSERT INTO scenario_alternative (scenario_id, alternative_id, rank, valid_from_commit)
             VALUES ((SELECT id FROM scenario WHERE cur_name=lower('s1')),
                     (SELECT id FROM alternative WHERE cur_name=lower('{alt}')),
                     1, (SELECT max(id) FROM commit));

             INSERT INTO membership (group_entity_id, member_entity_id, alternative_id, included, valid_from_commit)
             VALUES ((SELECT id FROM entity WHERE cur_name=lower('node_1')),
                     (SELECT id FROM entity WHERE cur_name=lower('node_2')),
                     (SELECT id FROM alternative WHERE cur_name=lower('{alt}')),
                     TRUE, (SELECT max(id) FROM commit));

             INSERT INTO metadata (name, value, valid_from_commit)
             VALUES ('note', 'x', (SELECT max(id) FROM commit));
             INSERT INTO parameter_value_metadata
               (entity_id, parameter_definition_id, alternative_id, metadata_id, valid_from_commit)
             VALUES ((SELECT id FROM entity WHERE cur_name=lower('node_1')),
                     (SELECT id FROM parameter_definition WHERE cur_name=lower('demand')),
                     (SELECT id FROM alternative WHERE cur_name=lower('{alt}')),
                     (SELECT id FROM metadata WHERE cur_name=lower('note')),
                     (SELECT max(id) FROM commit));"
        ),
    );
}

const DEP_TABLES: &[&str] = &[
    "value_scalar",
    "value_array",
    "value_long",
    "entity_alternative",
    "membership",
    "scenario_alternative",
    "parameter_value_metadata",
];

/// Total live rows referencing alternative id `a` across the seeded dependent
/// legs — used to prove RESTRICT closes NOTHING (count-identical before/after).
/// Takes the id (not the name) so it still works after the alternative's own row
/// is closed out (its `cur_name` then resolves to nothing).
fn live_dep_rows(db: &std::path::Path, a: i64) -> i64 {
    // value_array has no valid_to_commit; the rest are valid_to_commit IS NULL.
    count(db, &format!("SELECT count(*) FROM value_scalar WHERE alternative_id={a} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM value_long WHERE alternative_id={a} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM value_array WHERE alternative_id={a}"))
        + count(db, &format!("SELECT count(*) FROM entity_alternative WHERE alternative_id={a} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM membership WHERE alternative_id={a} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM scenario_alternative WHERE alternative_id={a} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM parameter_value_metadata WHERE alternative_id={a} AND valid_to_commit IS NULL"))
}

// --- test 1: RESTRICT blocks + reports -------------------------------------

#[test]
fn restrict_blocks_and_reports() {
    let (_dir, db) = fresh_db();
    seed_full_deps(&db, "victim");

    let alt_id = count(&db, "SELECT id FROM alternative WHERE cur_name=lower('victim')");
    let before_live = live_dep_rows(&db, alt_id);
    let commits_before = count(&db, "SELECT count(*) FROM commit");

    let r = delete_objects(&db, vec![(oalt("victim"), cascade(false))], "t", "restrict", o())
        .expect("commit");
    assert_eq!(r.object_deletes.len(), 1);
    let out = &r.object_deletes[0];
    assert_eq!(out.kind, ObjectKind::Alternative);
    let blockers = match &out.result {
        ObjectDeleteResult::Restricted { blockers } => blockers,
        other => panic!("expected Restricted, got {other:?}"),
    };
    // Every seeded dependent leg is reported.
    for table in DEP_TABLES {
        let b = blocker(blockers, table).unwrap_or_else(|| panic!("missing blocker for {table}"));
        assert!(b.count >= 1, "{table} blocker count");
        assert!(!b.sample.is_empty(), "{table} sample present");
    }
    assert_eq!(blocker(blockers, "value_long").unwrap().count, 3, "3 long cells");

    // RESTRICT closed NOTHING: object row still live, every dependent still live.
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM alternative WHERE id={alt_id} AND valid_to_commit IS NULL")),
        1,
        "object row still live"
    );
    assert_eq!(live_dep_rows(&db, alt_id), before_live, "no dependent closed");
    // A commit row IS created (the transaction ran), but no data changed. The
    // per-request contract is the report, not a refusal (D1).
    assert_eq!(
        count(&db, "SELECT count(*) FROM commit") - commits_before,
        1,
        "one commit for the restrict request"
    );
    audit_tiling_clean(&db);
}

// --- test 2: cascade closes + reports (incl. array → hist) -----------------

#[test]
fn cascade_closes_and_reports() {
    let (_dir, db) = fresh_db();
    seed_full_deps(&db, "victim");
    let alt_id = count(&db, "SELECT id FROM alternative WHERE cur_name=lower('victim')");
    let hist_before = count(&db, "SELECT count(*) FROM value_array_hist");

    let r = delete_objects(&db, vec![(oalt("victim"), cascade(true))], "t", "cascade", o())
        .expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted, got {other:?}"),
    };
    assert_eq!(cc.object_rows, 1, "the alternative row itself");
    assert_eq!(per_table(cc, "value_scalar").unwrap().rows, 1);
    assert_eq!(per_table(cc, "value_array").unwrap().rows, 1);
    assert_eq!(per_table(cc, "value_long").unwrap().rows, 3);
    assert_eq!(per_table(cc, "entity_alternative").unwrap().rows, 1);
    assert_eq!(per_table(cc, "membership").unwrap().rows, 1);
    assert_eq!(per_table(cc, "scenario_alternative").unwrap().rows, 1);
    assert_eq!(per_table(cc, "parameter_value_metadata").unwrap().rows, 1);
    assert_eq!(cc.total_rows, 1 + 1 + 1 + 3 + 1 + 1 + 1 + 1, "headline = object + all deps");

    // Object row + all versioned dependents now closed; array moved to hist.
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM alternative WHERE id={alt_id} AND valid_to_commit IS NULL")),
        0,
        "object row closed"
    );
    assert_eq!(live_dep_rows(&db, alt_id), 0, "all dependents closed");
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM value_array WHERE alternative_id={alt_id}")),
        0,
        "array row deleted from current table"
    );
    assert_eq!(
        count(&db, "SELECT count(*) FROM value_array_hist") - hist_before,
        1,
        "array pre-image moved to value_array_hist"
    );
    audit_tiling_clean(&db);
}

// --- test 3: bare delete (no dependents) -----------------------------------

#[test]
fn bare_delete_no_dependents() {
    let (_dir, db) = fresh_db();
    // Base exists from the seed and has no dependents.
    let base_id = count(&db, "SELECT id FROM alternative WHERE cur_name=lower('base')");
    let r = delete_objects(&db, vec![(oalt("Base"), cascade(false))], "t", "bare", o())
        .expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted(empty), got {other:?}"),
    };
    assert_eq!(cc.object_rows, 1);
    assert!(cc.per_table.is_empty(), "no dependent tables");
    assert_eq!(cc.total_rows, 1);
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM alternative WHERE id={base_id} AND valid_to_commit IS NULL")),
        0,
        "bare object row closed"
    );
    audit_tiling_clean(&db);
}

// --- test 4: as-of queryable after delete ----------------------------------

#[test]
fn as_of_queryable_after_delete() {
    let (_dir, db) = fresh_db();
    seed_full_deps(&db, "victim");
    let alt_id = count(&db, "SELECT id FROM alternative WHERE cur_name=lower('victim')");
    let c_before = count(&db, "SELECT max(id) FROM commit");

    let r = delete_objects(&db, vec![(oalt("victim"), cascade(true))], "t", "cascade", o())
        .expect("commit");
    let c_del = r.commit_id;

    // The closed-out alternative row is still visible AS-OF the pre-delete
    // commit (valid_from <= c < valid_to), and gone as-of the delete commit.
    assert_eq!(
        count(&db, &format!(
            "SELECT count(*) FROM alternative WHERE id={alt_id} \
             AND valid_from_commit <= {c_before} \
             AND (valid_to_commit IS NULL OR valid_to_commit > {c_before})"
        )),
        1,
        "alternative live as-of the pre-delete commit"
    );
    assert_eq!(
        count(&db, &format!(
            "SELECT count(*) FROM alternative WHERE id={alt_id} \
             AND valid_from_commit <= {c_del} \
             AND (valid_to_commit IS NULL OR valid_to_commit > {c_del})"
        )),
        0,
        "alternative gone as-of the delete commit"
    );
    // A retained scalar value round-trips as-of the pre-delete commit.
    assert_eq!(
        count(&db, &format!(
            "SELECT count(*) FROM value_scalar WHERE alternative_id={alt_id} \
             AND content_retained AND v_float IS NOT NULL \
             AND valid_from_commit <= {c_before} \
             AND (valid_to_commit IS NULL OR valid_to_commit > {c_before})"
        )),
        1,
        "value content retained + queryable as-of"
    );
}

// --- test 5: monotonic versions + tiling clean -----------------------------

#[test]
fn monotonic_versions_and_tiling_clean() {
    let (_dir, db) = fresh_db();
    seed_full_deps(&db, "victim");
    delete_objects(&db, vec![(oalt("victim"), cascade(true))], "t", "cascade", o()).expect("commit");
    audit_tiling_clean(&db);
    // The alternative row kept its version (a close-out sets valid_to only).
    assert_eq!(
        count(&db, "SELECT count(*) FROM alternative WHERE cur_name=lower('victim')"),
        0,
        "no current victim row after delete"
    );
    // No span-CHECK damage anywhere (valid_to strictly after valid_from).
    assert_eq!(
        count(&db, "SELECT count(*) FROM alternative WHERE valid_to_commit IS NOT NULL AND valid_to_commit <= valid_from_commit"),
        0,
        "closed alternative spans are coherent"
    );
}

// --- test 6: idempotent re-delete ⇒ NotFound -------------------------------

#[test]
fn idempotent_redelete_is_notfound() {
    let (_dir, db) = fresh_db();
    seed_full_deps(&db, "victim");
    let r1 = delete_objects(&db, vec![(oalt("victim"), cascade(true))], "t", "del1", o()).expect("del1");
    assert!(matches!(r1.object_deletes[0].result, ObjectDeleteResult::Deleted { .. }));

    let r2 = delete_objects(&db, vec![(oalt("victim"), cascade(true))], "t", "del2", o()).expect("del2");
    assert_eq!(
        r2.object_deletes[0].result,
        ObjectDeleteResult::NotFound,
        "second delete of an already-closed alternative is NotFound, not an error"
    );
}

// --- test 7: atomic rollback on forced mid-batch error ---------------------

#[test]
fn atomic_rollback_on_mid_batch_error() {
    let (_dir, db) = fresh_db();
    seed_full_deps(&db, "victim");
    let alt_id = count(&db, "SELECT id FROM alternative WHERE cur_name=lower('victim')");
    let before_live = live_dep_rows(&db, alt_id);
    let commits_before = count(&db, "SELECT count(*) FROM commit");

    // Batch: a valid alternative cascade FIRST (would close many rows), then a
    // request that forces a genuine error mid-txn (0-D class addressed by tuple).
    // The whole txn must roll back.
    let err = delete_objects(
        &db,
        vec![(oalt("victim"), cascade(true)), (forced_txn_error_key(), cascade(true))],
        "t",
        "must roll back",
        o(),
    )
    .expect_err("forced mid-batch error");
    assert!(err.to_string().contains("0-D"), "error names the cause: {err}");

    // Nothing persisted: first delete rolled back, no commit row.
    assert_eq!(count(&db, "SELECT count(*) FROM commit"), commits_before, "no commit persisted");
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM alternative WHERE id={alt_id} AND valid_to_commit IS NULL")),
        1,
        "victim still live (first delete rolled back)"
    );
    assert_eq!(live_dep_rows(&db, alt_id), before_live, "dependents untouched");
}

// --- test 8: combined atomicity (value write + value delete + object delete) -

#[test]
fn combined_atomicity_one_commit() {
    let (_dir, db) = fresh_db();
    // altA holds two scalar values; altB is a separate alternative with deps.
    write_parameter(&db, "node", "demand", "altA", scalar_batch("node_1", 1.0), &[], o(), "t", "a1").expect("a1");
    write_parameter(&db, "node", "demand", "altA", scalar_batch("node_2", 2.0), &[], o(), "t", "a2").expect("a2");
    seed_full_deps(&db, "altB");
    let altb_id = count(&db, "SELECT id FROM alternative WHERE cur_name=lower('altb')");
    let commits_before = count(&db, "SELECT count(*) FROM commit");

    // ONE commit: write node_3 in altA, delete node_1 in altA, cascade-delete altB.
    let r = commit_changes(
        &db,
        vec![seg("node", "demand", "altA", scalar_batch("node_3", 3.0))],
        vec![],
        vec![dk("node", "demand", "altA", "node_1")],
        vec![(oalt("altB"), cascade(true))],
        "t",
        "one atomic save",
        o(),
    )
    .expect("commit");

    assert_eq!(count(&db, "SELECT count(*) FROM commit") - commits_before, 1, "exactly one commit");
    assert_eq!(r.rows_written, 1, "node_3 written");
    assert_eq!(r.rows_closed, 1, "node_1 closed");
    assert!(matches!(r.object_deletes[0].result, ObjectDeleteResult::Deleted { .. }), "altB cascaded");

    // altA now has node_2 + node_3 current; node_1 gone.
    let got = read_parameter(&db, "node", "demand", "altA").expect("read");
    let names = entity_names(&got);
    assert!(names.contains(&"node_2".to_string()) && names.contains(&"node_3".to_string()));
    assert!(!names.contains(&"node_1".to_string()), "node_1 deleted");
    // altB fully closed.
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM alternative WHERE id={altb_id} AND valid_to_commit IS NULL")),
        0,
        "altB object row closed in the same commit"
    );
    audit_tiling_clean(&db);
}

// --- test 8b: the §2 same-commit write-then-cascade guard holds ------------

#[test]
fn same_commit_write_then_cascade_guard() {
    let (_dir, db) = fresh_db();
    let commits_before = count(&db, "SELECT count(*) FROM commit");

    // In ONE commit: write a value into a BRAND-NEW alternative `fresh` (which
    // this write auto-creates, valid_from = commit_id) AND cascade-delete it.
    // The guard (`valid_from < commit_id`) skips both the same-commit object row
    // and its same-commit value ⇒ NotFound, no span-CHECK trip, value survives.
    let r = commit_changes(
        &db,
        vec![seg("node", "demand", "fresh", scalar_batch("node_1", 7.0))],
        vec![],
        vec![],
        vec![(oalt("fresh"), cascade(true))],
        "t",
        "hazard",
        o(),
    )
    .expect("commit must not trip the span CHECK");

    assert_eq!(count(&db, "SELECT count(*) FROM commit") - commits_before, 1, "one commit");
    assert_eq!(
        r.object_deletes[0].result,
        ObjectDeleteResult::NotFound,
        "same-commit-created target is guarded ⇒ NotFound, not an error"
    );
    // The value (and its alternative) survived — the guard protected them.
    let got = read_parameter(&db, "node", "demand", "fresh").expect("read");
    assert_eq!(entity_names(&got), vec!["node_1".to_string()], "value survives");
    assert_eq!(
        count(&db, "SELECT count(*) FROM alternative WHERE cur_name=lower('fresh')"),
        1,
        "alternative still live"
    );
    audit_tiling_clean(&db);
}

// --- test 10: preview matches commit ---------------------------------------

#[test]
fn preview_matches_commit() {
    let (_dir, db) = fresh_db();
    seed_full_deps(&db, "victim");
    let commits_before = count(&db, "SELECT count(*) FROM commit");

    let previews = preview_object_deletes(&db, vec![oalt("victim")]).expect("preview");
    assert_eq!(previews.len(), 1);
    let p = &previews[0];
    assert!(p.found);
    // Preview mutates nothing.
    assert_eq!(count(&db, "SELECT count(*) FROM commit"), commits_before, "preview is a pure read");

    let r = delete_objects(&db, vec![(oalt("victim"), cascade(true))], "t", "cascade", o()).expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted, got {other:?}"),
    };

    // The preview's cascade framing equals what commit actually closed.
    assert_eq!(p.cascade.object_rows, cc.object_rows, "object_rows match");
    assert_eq!(p.cascade.total_rows, cc.total_rows, "total_rows match");
    for tc in &cc.per_table {
        let pc = per_table(&p.cascade, &tc.table)
            .unwrap_or_else(|| panic!("preview missing {}", tc.table));
        assert_eq!(pc.rows, tc.rows, "{} count matches", tc.table);
    }
    // And the blocker framing lists the same dependent tables.
    for tc in &cc.per_table {
        assert!(blocker(&p.blockers, &tc.table).is_some(), "blocker for {}", tc.table);
    }
}

// --- run provenance is not flagged dangling after ruling-2 audit fix -------

#[test]
fn run_referencing_deleted_alternative_not_flagged() {
    let (_dir, db) = fresh_db();
    // A tool entity + a result alternative, then a run pointing at the alt.
    write_parameter(&db, "tool", "noop", "results", scalar_batch("solver_x", 1.0), &[], o(), "t", "seed")
        .expect("seed");
    exec(
        &db,
        "INSERT INTO run (tool_entity_id, result_alternative_id, input_commit_id)
         VALUES ((SELECT id FROM entity WHERE cur_name=lower('solver_x')),
                 (SELECT id FROM alternative WHERE cur_name=lower('results')),
                 (SELECT max(id) FROM commit));",
    );
    // Delete the result alternative (cascade closes its value; run is immutable
    // and intentionally left pointing at the now-closed alternative).
    delete_objects(&db, vec![(oalt("results"), cascade(true))], "t", "del", o()).expect("del");

    // Ruling 2: a run leg to a CLOSED-OUT (not absent) alternative/entity is
    // coherent as-of and must NOT be reported dangling.
    assert_eq!(
        count(&db, "SELECT count(*) FROM audit_dangling_ref WHERE src_table='run'"),
        0,
        "run referencing a deleted (closed-out) alternative is not flagged dangling"
    );
}

// ===========================================================================
// PARAMETER object deletion (object_delete plan §4d, §6, P2).
//
// The parameter leg set exercises the two generalized leg shapes P1 flagged:
//   - indirect key: `parameter_type_dimension` via `parameter_type_id IN (…)`;
//   - filtered leg:  `semantic_annotation` where `object_type='parameter_definition'`.
// Identity legs (`parameter_type`, its dimensions, and the definition's OWN
// `definition_value_*` default/prefill mirrors) close WITH the definition and
// count in `object_rows`, never blocking — §4d amended: the mirrors are IDENTITY,
// and `definition_value_array` closes with a plain UPDATE (no `_hist` twin).
// ===========================================================================

/// A parameter object-delete key (per-class, name-addressed).
fn opar(class: &str, name: &str) -> ObjectDeleteKey {
    ObjectDeleteKey {
        kind: ObjectKind::Parameter,
        class: Some(class.into()),
        target: ObjectRef::Name(name.into()),
    }
}

/// Seed an ARRAY-tier parameter `node.profile` with a non-trivial leg set:
///   identity — `parameter_type` (1, rank-1) + `parameter_type_dimension` (1) +
///     a `definition_value_scalar` default + a `definition_value_array` prefill;
///   dependents — `value_array` entity values across TWO alternatives (altA/altB,
///     ⇒ move-to-`_hist` on cascade), `entity_parameter_unit`,
///     `parameter_value_metadata`, and a filtered `semantic_annotation`.
/// Deterministic counts: object_rows = 1 pd + 1 pt + 1 ptd + 1 dvs + 1 dva = 5;
/// dependents value_array=2, entity_parameter_unit=1, parameter_value_metadata=1,
/// semantic_annotation=1 ⇒ total_rows = 10.
fn seed_parameter_deps(db: &std::path::Path) -> i64 {
    write_parameter(db, "node", "profile", "altA", array_batch("node_1", &[0, 1, 2], &[1.0, 2.0, 3.0]), &[], o(), "t", "pa")
        .expect("profile altA");
    write_parameter(db, "node", "profile", "altB", array_batch("node_1", &[0, 1, 2], &[4.0, 5.0, 6.0]), &[], o(), "t", "pb")
        .expect("profile altB");

    exec(
        db,
        "INSERT INTO units_of_measure (name, valid_from_commit)
           VALUES ('MW', (SELECT max(id) FROM commit));
         INSERT INTO metadata (name, value, valid_from_commit)
           VALUES ('pnote', 'x', (SELECT max(id) FROM commit));

         -- Identity: definition-scoped default/prefill mirrors (two shapes).
         INSERT INTO definition_value_scalar
           (parameter_definition_id, slot, valid_from_commit, version, value_kind, v_float)
           VALUES ((SELECT id FROM parameter_definition WHERE cur_name=lower('profile')),
                   'default_value', (SELECT max(id) FROM commit), 1, 'float', 0.0);
         INSERT INTO definition_value_array
           (parameter_definition_id, slot, valid_from_commit, version, axis1_id, values)
           VALUES ((SELECT id FROM parameter_definition WHERE cur_name=lower('profile')),
                   'prefill_value', (SELECT max(id) FROM commit), 1,
                   (SELECT axis1_id FROM value_array LIMIT 1), [0.0, 0.0, 0.0]);

         -- Dependents: unit override, value metadata, semantic annotation.
         INSERT INTO entity_parameter_unit
           (entity_id, parameter_definition_id, unit_id, valid_from_commit)
           VALUES ((SELECT id FROM entity WHERE cur_name=lower('node_1')),
                   (SELECT id FROM parameter_definition WHERE cur_name=lower('profile')),
                   (SELECT id FROM units_of_measure WHERE cur_name=lower('MW')),
                   (SELECT max(id) FROM commit));
         INSERT INTO parameter_value_metadata
           (entity_id, parameter_definition_id, alternative_id, metadata_id, valid_from_commit)
           VALUES ((SELECT id FROM entity WHERE cur_name=lower('node_1')),
                   (SELECT id FROM parameter_definition WHERE cur_name=lower('profile')),
                   (SELECT id FROM alternative WHERE cur_name=lower('altA')),
                   (SELECT id FROM metadata WHERE cur_name=lower('pnote')),
                   (SELECT max(id) FROM commit));
         INSERT INTO semantic_annotation
           (object_type, object_id, term_iri, valid_from_commit)
           VALUES ('parameter_definition',
                   (SELECT id FROM parameter_definition WHERE cur_name=lower('profile')),
                   'http://example/term', (SELECT max(id) FROM commit));",
    );

    count(db, "SELECT id FROM parameter_definition WHERE cur_name=lower('profile')")
}

const PARAM_DEP_TABLES: &[&str] = &[
    "value_array",
    "entity_parameter_unit",
    "parameter_value_metadata",
    "semantic_annotation",
];

/// Live rows across the parameter's DEPENDENT legs (used to prove RESTRICT
/// closes nothing). value_array has no valid_to_commit.
fn live_param_deps(db: &std::path::Path, pd: i64) -> i64 {
    count(db, &format!("SELECT count(*) FROM value_array WHERE parameter_definition_id={pd}"))
        + count(db, &format!("SELECT count(*) FROM entity_parameter_unit WHERE parameter_definition_id={pd} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM parameter_value_metadata WHERE parameter_definition_id={pd} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM semantic_annotation WHERE object_id={pd} AND object_type='parameter_definition' AND valid_to_commit IS NULL"))
}

/// Live rows across the parameter's IDENTITY legs (object row + type + dims +
/// definition-value mirrors) — proves RESTRICT leaves identity untouched too.
fn live_param_identity(db: &std::path::Path, pd: i64) -> i64 {
    count(db, &format!("SELECT count(*) FROM parameter_definition WHERE id={pd} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM parameter_type WHERE parameter_definition_id={pd} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM parameter_type_dimension WHERE parameter_type_id IN (SELECT id FROM parameter_type WHERE parameter_definition_id={pd}) AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM definition_value_scalar WHERE parameter_definition_id={pd} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM definition_value_array WHERE parameter_definition_id={pd} AND valid_to_commit IS NULL"))
}

// --- param test 1: RESTRICT blocks + reports (identity is NOT a blocker) ----

#[test]
fn param_restrict_blocks_and_reports() {
    let (_dir, db) = fresh_db();
    let pd = seed_parameter_deps(&db);
    let before_deps = live_param_deps(&db, pd);
    let before_identity = live_param_identity(&db, pd);

    let r = delete_objects(&db, vec![(opar("node", "profile"), cascade(false))], "t", "restrict", o())
        .expect("commit");
    let blockers = match &r.object_deletes[0].result {
        ObjectDeleteResult::Restricted { blockers } => blockers,
        other => panic!("expected Restricted, got {other:?}"),
    };
    for table in PARAM_DEP_TABLES {
        let b = blocker(blockers, table).unwrap_or_else(|| panic!("missing blocker for {table}"));
        assert!(b.count >= 1, "{table} blocker count");
        assert!(!b.sample.is_empty(), "{table} sample present");
    }
    assert_eq!(blocker(blockers, "value_array").unwrap().count, 2, "2 array entity values");
    // Identity legs are NOT blockers (§4d): a param blocked ONLY by its own type
    // + default would be un-deletable; assert none leak into the blocker list.
    for id_table in ["parameter_type", "parameter_type_dimension", "definition_value_scalar", "definition_value_array"] {
        assert!(blocker(blockers, id_table).is_none(), "{id_table} must not block");
    }

    // RESTRICT closed NOTHING — dependents AND identity all still live.
    assert_eq!(live_param_deps(&db, pd), before_deps, "no dependent closed");
    assert_eq!(live_param_identity(&db, pd), before_identity, "no identity row closed");
    audit_tiling_clean(&db);
}

// --- param test 2: cascade closes + reports (identity object_rows + array→hist) -

#[test]
fn param_cascade_closes_and_reports() {
    let (_dir, db) = fresh_db();
    let pd = seed_parameter_deps(&db);
    let hist_before = count(&db, "SELECT count(*) FROM value_array_hist");

    let r = delete_objects(&db, vec![(opar("node", "profile"), cascade(true))], "t", "cascade", o())
        .expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted, got {other:?}"),
    };
    // Identity: pd + parameter_type + parameter_type_dimension + 2 definition mirrors.
    assert_eq!(cc.object_rows, 5, "object row + type + dim + 2 definition-value mirrors");
    // Dependents.
    assert_eq!(per_table(cc, "value_array").unwrap().rows, 2);
    assert_eq!(per_table(cc, "entity_parameter_unit").unwrap().rows, 1);
    assert_eq!(per_table(cc, "parameter_value_metadata").unwrap().rows, 1);
    assert_eq!(per_table(cc, "semantic_annotation").unwrap().rows, 1);
    assert!(per_table(cc, "value_scalar").is_none(), "no scalar values for this param");
    assert!(per_table(cc, "value_long").is_none(), "no long values for this param");
    assert_eq!(cc.total_rows, 5 + 2 + 1 + 1 + 1, "headline = identity + all deps");

    // Everything closed; value_array entity values moved to hist.
    assert_eq!(live_param_deps(&db, pd), 0, "all dependents closed");
    assert_eq!(live_param_identity(&db, pd), 0, "object row + identity closed");
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM value_array WHERE parameter_definition_id={pd}")),
        0,
        "array entity values deleted from current table"
    );
    assert_eq!(
        count(&db, "SELECT count(*) FROM value_array_hist") - hist_before,
        2,
        "both array entity values moved to value_array_hist"
    );
    // definition_value_array has NO `_hist` twin ⇒ it closes in-place (valid_to
    // set), it does NOT move to value_array_hist.
    assert_eq!(
        count(&db, &format!(
            "SELECT count(*) FROM definition_value_array WHERE parameter_definition_id={pd} AND valid_to_commit IS NOT NULL"
        )),
        1,
        "definition_value_array closed in place via plain UPDATE"
    );
    audit_tiling_clean(&db);
}

// --- param test 3: bare delete (identity only, no dependents) ---------------

#[test]
fn param_bare_delete_no_dependents() {
    let (_dir, db) = fresh_db();
    // Create the `node` class + node_1 via a throwaway write, then raw-insert a
    // dependent-free parameter carrying only identity (definition + type).
    write_parameter(&db, "node", "seedp", "altA", scalar_batch("node_1", 1.0), &[], o(), "t", "s").expect("seed class");
    exec(
        &db,
        "INSERT INTO parameter_definition (entity_class_id, name, valid_from_commit)
           VALUES ((SELECT id FROM entity_class WHERE cur_name=lower('node')), 'lonely',
                   (SELECT max(id) FROM commit));
         INSERT INTO parameter_type (parameter_definition_id, value_kind, rank, valid_from_commit)
           VALUES ((SELECT id FROM parameter_definition WHERE cur_name=lower('lonely')), 'float', 0,
                   (SELECT max(id) FROM commit));",
    );
    let pd = count(&db, "SELECT id FROM parameter_definition WHERE cur_name=lower('lonely')");

    let r = delete_objects(&db, vec![(opar("node", "lonely"), cascade(false))], "t", "bare", o())
        .expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted(identity only), got {other:?}"),
    };
    assert_eq!(cc.object_rows, 2, "definition + its parameter_type");
    assert!(cc.per_table.is_empty(), "no dependent tables");
    assert_eq!(cc.total_rows, 2);
    assert_eq!(live_param_identity(&db, pd), 0, "identity closed");
    audit_tiling_clean(&db);
}

// --- param test 4: as-of queryable after delete ----------------------------

#[test]
fn param_as_of_queryable_after_delete() {
    let (_dir, db) = fresh_db();
    let pd = seed_parameter_deps(&db);
    let c_before = count(&db, "SELECT max(id) FROM commit");

    let r = delete_objects(&db, vec![(opar("node", "profile"), cascade(true))], "t", "cascade", o())
        .expect("commit");
    let c_del = r.commit_id;

    // The closed-out definition row is visible as-of the pre-delete commit, gone
    // as-of the delete commit.
    assert_eq!(
        count(&db, &format!(
            "SELECT count(*) FROM parameter_definition WHERE id={pd} \
             AND valid_from_commit <= {c_before} \
             AND (valid_to_commit IS NULL OR valid_to_commit > {c_before})"
        )),
        1,
        "definition live as-of the pre-delete commit"
    );
    assert_eq!(
        count(&db, &format!(
            "SELECT count(*) FROM parameter_definition WHERE id={pd} \
             AND valid_from_commit <= {c_del} \
             AND (valid_to_commit IS NULL OR valid_to_commit > {c_del})"
        )),
        0,
        "definition gone as-of the delete commit"
    );
    // The array entity values live on in the hist twin, content retained.
    assert_eq!(
        count(&db, &format!(
            "SELECT count(*) FROM value_array_hist WHERE parameter_definition_id={pd} AND content_retained"
        )),
        2,
        "array pre-images retained in history"
    );
}

// --- param test 5: monotonic versions + tiling clean -----------------------

#[test]
fn param_monotonic_versions_and_tiling_clean() {
    let (_dir, db) = fresh_db();
    seed_parameter_deps(&db);
    delete_objects(&db, vec![(opar("node", "profile"), cascade(true))], "t", "cascade", o()).expect("commit");
    audit_tiling_clean(&db);
    assert_eq!(
        count(&db, "SELECT count(*) FROM parameter_definition WHERE cur_name=lower('profile')"),
        0,
        "no current profile definition after delete"
    );
    // No span-CHECK damage on the definition or its type rows.
    assert_eq!(
        count(&db, "SELECT count(*) FROM parameter_definition WHERE valid_to_commit IS NOT NULL AND valid_to_commit <= valid_from_commit"),
        0,
        "closed definition spans are coherent"
    );
    assert_eq!(
        count(&db, "SELECT count(*) FROM parameter_type WHERE valid_to_commit IS NOT NULL AND valid_to_commit <= valid_from_commit"),
        0,
        "closed parameter_type spans are coherent"
    );
}

// --- param test 6: idempotent re-delete ⇒ NotFound -------------------------

#[test]
fn param_idempotent_redelete_is_notfound() {
    let (_dir, db) = fresh_db();
    seed_parameter_deps(&db);
    let r1 = delete_objects(&db, vec![(opar("node", "profile"), cascade(true))], "t", "del1", o()).expect("del1");
    assert!(matches!(r1.object_deletes[0].result, ObjectDeleteResult::Deleted { .. }));

    let r2 = delete_objects(&db, vec![(opar("node", "profile"), cascade(true))], "t", "del2", o()).expect("del2");
    assert_eq!(
        r2.object_deletes[0].result,
        ObjectDeleteResult::NotFound,
        "second delete of an already-closed parameter is NotFound"
    );
    // A miss on the CLASS resolves to NotFound too (never auto-create, never error).
    let r3 = delete_objects(&db, vec![(opar("no_such_class", "profile"), cascade(true))], "t", "del3", o()).expect("del3");
    assert_eq!(r3.object_deletes[0].result, ObjectDeleteResult::NotFound, "unknown class ⇒ NotFound");
}

// --- param test 7: atomic rollback on forced mid-batch error ---------------

#[test]
fn param_atomic_rollback_on_mid_batch_error() {
    let (_dir, db) = fresh_db();
    let pd = seed_parameter_deps(&db);
    let before_deps = live_param_deps(&db, pd);
    let before_identity = live_param_identity(&db, pd);
    let commits_before = count(&db, "SELECT count(*) FROM commit");

    // Valid parameter cascade FIRST, then a request that forces a genuine error
    // mid-txn (0-D class addressed by tuple) ⇒ whole txn rolls back.
    let err = delete_objects(
        &db,
        vec![(opar("node", "profile"), cascade(true)), (forced_txn_error_key(), cascade(true))],
        "t",
        "must roll back",
        o(),
    )
    .expect_err("forced mid-batch error");
    assert!(err.to_string().contains("0-D"), "error names the cause: {err}");

    assert_eq!(count(&db, "SELECT count(*) FROM commit"), commits_before, "no commit persisted");
    assert_eq!(live_param_deps(&db, pd), before_deps, "dependents untouched");
    assert_eq!(live_param_identity(&db, pd), before_identity, "identity untouched");
}

// --- param test 8: combined atomicity (value write + delete + param delete) -

#[test]
fn param_combined_atomicity_one_commit() {
    let (_dir, db) = fresh_db();
    // `profile` is the deletable param; `keep` is a scalar param we mutate.
    let pd = seed_parameter_deps(&db);
    write_parameter(&db, "node", "keep", "altA", scalar_batch("node_1", 1.0), &[], o(), "t", "k1").expect("keep");
    let commits_before = count(&db, "SELECT count(*) FROM commit");

    // ONE commit: write keep/node_2, delete keep/node_1, cascade-delete profile.
    let r = commit_changes(
        &db,
        vec![seg("node", "keep", "altA", scalar_batch("node_2", 2.0))],
        vec![],
        vec![dk("node", "keep", "altA", "node_1")],
        vec![(opar("node", "profile"), cascade(true))],
        "t",
        "one atomic save",
        o(),
    )
    .expect("commit");

    assert_eq!(count(&db, "SELECT count(*) FROM commit") - commits_before, 1, "exactly one commit");
    assert_eq!(r.rows_written, 1, "keep/node_2 written");
    assert_eq!(r.rows_closed, 1, "keep/node_1 closed");
    assert!(matches!(r.object_deletes[0].result, ObjectDeleteResult::Deleted { .. }), "profile cascaded");

    let got = read_parameter(&db, "node", "keep", "altA").expect("read");
    let names = entity_names(&got);
    assert!(names.contains(&"node_2".to_string()), "node_2 present");
    assert!(!names.contains(&"node_1".to_string()), "node_1 deleted");
    assert_eq!(live_param_identity(&db, pd), 0, "profile closed in the same commit");
    audit_tiling_clean(&db);
}

// --- param test 10: preview matches commit ---------------------------------

#[test]
fn param_preview_matches_commit() {
    let (_dir, db) = fresh_db();
    seed_parameter_deps(&db);
    let commits_before = count(&db, "SELECT count(*) FROM commit");

    let previews = preview_object_deletes(&db, vec![opar("node", "profile")]).expect("preview");
    let p = &previews[0];
    assert!(p.found);
    assert_eq!(p.kind, ObjectKind::Parameter);
    assert_eq!(count(&db, "SELECT count(*) FROM commit"), commits_before, "preview is a pure read");

    let r = delete_objects(&db, vec![(opar("node", "profile"), cascade(true))], "t", "cascade", o()).expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted, got {other:?}"),
    };

    assert_eq!(p.cascade.object_rows, cc.object_rows, "identity object_rows match");
    assert_eq!(p.cascade.total_rows, cc.total_rows, "total_rows match");
    for tc in &cc.per_table {
        let pc = per_table(&p.cascade, &tc.table)
            .unwrap_or_else(|| panic!("preview missing {}", tc.table));
        assert_eq!(pc.rows, tc.rows, "{} count matches", tc.table);
    }
    for tc in &cc.per_table {
        assert!(blocker(&p.blockers, &tc.table).is_some(), "blocker for {}", tc.table);
    }
}

// --- shared read helper ----------------------------------------------------

fn entity_names(batch: &RecordBatch) -> Vec<String> {
    let i = batch.schema().index_of("entity").unwrap();
    let a = batch.column(i).as_any().downcast_ref::<StringArray>().unwrap();
    (0..a.len()).map(|r| a.value(r).to_string()).collect()
}

// ===========================================================================
// ENTITY object deletion (object_delete plan §4b/§4c, §6, §10 ruling 1, P3).
//
// Stage 1 exercises 0-D entities (flat legs, no recursion): value tiers +
// entity_location / entity_alternative / membership (BOTH group and member
// legs) / entity_parameter_unit / entity_metadata / parameter_value_metadata /
// filtered semantic_annotation. run.tool_entity_id is EXCLUDED (immutable).
// Stage 2 adds the N-D `Elements` tuple resolver (§4c, critique C6). Stage 3
// adds the B1 recursive cascade: deleting an entity used as an element of a
// live N-D container recurses into that container (§10 ruling 1).
// ===========================================================================

/// An entity object-delete key addressed by (per-class-unique) name.
fn oent(class: &str, name: &str) -> ObjectDeleteKey {
    ObjectDeleteKey {
        kind: ObjectKind::Entity,
        class: Some(class.into()),
        target: ObjectRef::Name(name.into()),
    }
}

/// An entity object-delete key addressed by its element tuple (§4c).
fn oent_elems(class: &str, elems: &[&str]) -> ObjectDeleteKey {
    ObjectDeleteKey {
        kind: ObjectKind::Entity,
        class: Some(class.into()),
        target: ObjectRef::Elements(elems.iter().map(|e| e.to_string()).collect()),
    }
}

const ENT_DEP_TABLES: &[&str] = &[
    "value_scalar",
    "value_long",
    "value_array",
    "entity_location",
    "entity_alternative",
    "membership",
    "entity_parameter_unit",
    "entity_metadata",
    "parameter_value_metadata",
    "semantic_annotation",
];

/// Seed 0-D entity `node_1` with one live dependent in every entity leg (§4b),
/// including BOTH membership legs (node_1 as group of node_2, and as member of
/// node_2). Deterministic dependent counts: value_scalar=1, value_long=3,
/// value_array=1, entity_location=1, entity_alternative=1, membership=2 (group +
/// member, merged), entity_parameter_unit=1, entity_metadata=1,
/// parameter_value_metadata=1, semantic_annotation=1 ⇒ dep total 13; object_rows
/// = 1 (0-D has no identity tuple) ⇒ total_rows 14. Returns node_1's id.
fn seed_entity_deps(db: &std::path::Path) -> i64 {
    write_parameter(db, "node", "demand", "Base", scalar_batch("node_1", 1.0), &[], o(), "t", "s")
        .expect("scalar");
    write_parameter(db, "node", "counts", "Base", long_batch("node_1", &[0, 1, 2], &[10, 20, 30]), &[], o(), "t", "l")
        .expect("long");
    write_parameter(db, "node", "profile", "Base", array_batch("node_1", &[0, 1, 2], &[1.0, 2.0, 3.0]), &[], o(), "t", "a")
        .expect("array");
    write_parameter(db, "node", "demand", "Base", scalar_batch("node_2", 5.0), &[], o(), "t", "n2")
        .expect("node_2");

    exec(
        db,
        "INSERT INTO units_of_measure (name, valid_from_commit)
           VALUES ('MW', (SELECT max(id) FROM commit));
         INSERT INTO metadata (name, value, valid_from_commit)
           VALUES ('enote', 'x', (SELECT max(id) FROM commit));

         INSERT INTO entity_location (entity_id, lat, lon, valid_from_commit)
           VALUES ((SELECT id FROM entity WHERE cur_name=lower('node_1')), 1.0, 2.0,
                   (SELECT max(id) FROM commit));

         INSERT INTO entity_alternative (entity_id, alternative_id, active, valid_from_commit)
           VALUES ((SELECT id FROM entity WHERE cur_name=lower('node_1')),
                   (SELECT id FROM alternative WHERE cur_name=lower('Base')),
                   TRUE, (SELECT max(id) FROM commit));

         -- node_1 as GROUP of node_2, and node_1 as MEMBER of node_2.
         INSERT INTO membership (group_entity_id, member_entity_id, alternative_id, included, valid_from_commit)
           VALUES ((SELECT id FROM entity WHERE cur_name=lower('node_1')),
                   (SELECT id FROM entity WHERE cur_name=lower('node_2')),
                   NULL, TRUE, (SELECT max(id) FROM commit)),
                  ((SELECT id FROM entity WHERE cur_name=lower('node_2')),
                   (SELECT id FROM entity WHERE cur_name=lower('node_1')),
                   NULL, TRUE, (SELECT max(id) FROM commit));

         INSERT INTO entity_parameter_unit (entity_id, parameter_definition_id, unit_id, valid_from_commit)
           VALUES ((SELECT id FROM entity WHERE cur_name=lower('node_1')),
                   (SELECT id FROM parameter_definition WHERE cur_name=lower('demand')),
                   (SELECT id FROM units_of_measure WHERE cur_name=lower('MW')),
                   (SELECT max(id) FROM commit));

         INSERT INTO entity_metadata (entity_id, metadata_id, valid_from_commit)
           VALUES ((SELECT id FROM entity WHERE cur_name=lower('node_1')),
                   (SELECT id FROM metadata WHERE cur_name=lower('enote')),
                   (SELECT max(id) FROM commit));

         INSERT INTO parameter_value_metadata
           (entity_id, parameter_definition_id, alternative_id, metadata_id, valid_from_commit)
           VALUES ((SELECT id FROM entity WHERE cur_name=lower('node_1')),
                   (SELECT id FROM parameter_definition WHERE cur_name=lower('demand')),
                   (SELECT id FROM alternative WHERE cur_name=lower('Base')),
                   (SELECT id FROM metadata WHERE cur_name=lower('enote')),
                   (SELECT max(id) FROM commit));

         INSERT INTO semantic_annotation (object_type, object_id, term_iri, valid_from_commit)
           VALUES ('entity', (SELECT id FROM entity WHERE cur_name=lower('node_1')),
                   'http://example.org/node', (SELECT max(id) FROM commit));",
    );

    count(db, "SELECT id FROM entity WHERE cur_name=lower('node_1')")
}

/// Live rows referencing entity `e` across the seeded entity legs (both
/// membership legs). Takes the id so it survives the entity's own close-out.
fn live_entity_deps(db: &std::path::Path, e: i64) -> i64 {
    count(db, &format!("SELECT count(*) FROM value_scalar WHERE entity_id={e} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM value_long WHERE entity_id={e} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM value_array WHERE entity_id={e}"))
        + count(db, &format!("SELECT count(*) FROM entity_location WHERE entity_id={e} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM entity_alternative WHERE entity_id={e} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM membership WHERE group_entity_id={e} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM membership WHERE member_entity_id={e} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM entity_parameter_unit WHERE entity_id={e} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM entity_metadata WHERE entity_id={e} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM parameter_value_metadata WHERE entity_id={e} AND valid_to_commit IS NULL"))
        + count(db, &format!("SELECT count(*) FROM semantic_annotation WHERE object_id={e} AND object_type='entity' AND valid_to_commit IS NULL"))
}

// --- entity test 1: RESTRICT blocks + reports (0-D) ------------------------

#[test]
fn entity_restrict_blocks_and_reports() {
    let (_dir, db) = fresh_db();
    let e = seed_entity_deps(&db);
    let before = live_entity_deps(&db, e);

    let r = delete_objects(&db, vec![(oent("node", "node_1"), cascade(false))], "t", "restrict", o())
        .expect("commit");
    let blockers = match &r.object_deletes[0].result {
        ObjectDeleteResult::Restricted { blockers } => blockers,
        other => panic!("expected Restricted, got {other:?}"),
    };
    for table in ENT_DEP_TABLES {
        let b = blocker(blockers, table).unwrap_or_else(|| panic!("missing blocker for {table}"));
        assert!(b.count >= 1, "{table} blocker count");
    }
    // Nothing closed.
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity WHERE id={e} AND valid_to_commit IS NULL")),
        1, "entity row still live"
    );
    assert_eq!(live_entity_deps(&db, e), before, "no dependent closed");
    audit_tiling_clean(&db);
}

// --- entity test 2: cascade closes + reports (0-D, incl. array → hist) ------

#[test]
fn entity_cascade_closes_and_reports() {
    let (_dir, db) = fresh_db();
    let e = seed_entity_deps(&db);
    let hist_before = count(&db, "SELECT count(*) FROM value_array_hist");

    let r = delete_objects(&db, vec![(oent("node", "node_1"), cascade(true))], "t", "cascade", o())
        .expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted, got {other:?}"),
    };
    assert_eq!(cc.object_rows, 1, "0-D entity has no identity tuple");
    assert_eq!(per_table(cc, "value_scalar").unwrap().rows, 1);
    assert_eq!(per_table(cc, "value_long").unwrap().rows, 3);
    assert_eq!(per_table(cc, "value_array").unwrap().rows, 1);
    assert_eq!(per_table(cc, "entity_location").unwrap().rows, 1);
    assert_eq!(per_table(cc, "entity_alternative").unwrap().rows, 1);
    assert_eq!(per_table(cc, "membership").unwrap().rows, 2, "group + member legs merged");
    assert_eq!(per_table(cc, "entity_parameter_unit").unwrap().rows, 1);
    assert_eq!(per_table(cc, "entity_metadata").unwrap().rows, 1);
    assert_eq!(per_table(cc, "parameter_value_metadata").unwrap().rows, 1);
    assert_eq!(per_table(cc, "semantic_annotation").unwrap().rows, 1);
    assert_eq!(cc.total_rows, 1 + 13, "object + all deps");

    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity WHERE id={e} AND valid_to_commit IS NULL")),
        0, "entity row closed"
    );
    assert_eq!(live_entity_deps(&db, e), 0, "all dependents closed");
    assert_eq!(count(&db, "SELECT count(*) FROM value_array_hist") - hist_before, 1, "array pre-image to hist");
    audit_tiling_clean(&db);
}

// --- entity test 3: bare delete (no dependents, 0-D) -----------------------

#[test]
fn entity_bare_delete_no_dependents() {
    let (_dir, db) = fresh_db();
    // A lone entity with no dependents (write then delete the value first? No —
    // create a standalone entity via a value then clear it). Simplest: seed a
    // fresh entity `solo` by a value write, then delete THAT value, leaving the
    // entity row dependent-free.
    write_parameter(&db, "node", "demand", "Base", scalar_batch("solo", 1.0), &[], o(), "t", "s").expect("solo");
    delete_objects(&db, vec![], "t", "noop", o()).ok();
    // Close the only value so `solo` has no dependents.
    commit_changes(&db, vec![], vec![], vec![dk("node", "demand", "Base", "solo")], vec![], "t", "clear", o())
        .expect("clear value");
    let e = count(&db, "SELECT id FROM entity WHERE cur_name=lower('solo')");

    let r = delete_objects(&db, vec![(oent("node", "solo"), cascade(false))], "t", "bare", o())
        .expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted(empty), got {other:?}"),
    };
    assert_eq!(cc.object_rows, 1);
    assert!(cc.per_table.is_empty(), "no dependent tables");
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity WHERE id={e} AND valid_to_commit IS NULL")),
        0, "bare entity row closed"
    );
    audit_tiling_clean(&db);
}

// --- entity test 6: idempotent re-delete ⇒ NotFound (0-D) ------------------

#[test]
fn entity_idempotent_redelete_is_notfound() {
    let (_dir, db) = fresh_db();
    seed_entity_deps(&db);
    delete_objects(&db, vec![(oent("node", "node_1"), cascade(true))], "t", "first", o()).expect("first");
    let r = delete_objects(&db, vec![(oent("node", "node_1"), cascade(true))], "t", "second", o())
        .expect("second");
    assert!(matches!(r.object_deletes[0].result, ObjectDeleteResult::NotFound), "second ⇒ NotFound");
}

// --- entity test 5: monotonic versions + clean tiling ----------------------

#[test]
fn entity_monotonic_versions_and_tiling_clean() {
    let (_dir, db) = fresh_db();
    seed_entity_deps(&db);
    delete_objects(&db, vec![(oent("node", "node_1"), cascade(true))], "t", "cascade", o()).expect("commit");
    audit_tiling_clean(&db);
}

// --- entity test 7: atomic rollback on forced mid-batch error --------------

#[test]
fn entity_atomic_rollback_on_mid_batch_error() {
    let (_dir, db) = fresh_db();
    let e = seed_entity_deps(&db);
    let before = live_entity_deps(&db, e);
    let commits_before = count(&db, "SELECT count(*) FROM commit");
    // Valid entity cascade FIRST, then a forced mid-txn error ⇒ rollback.
    let err = delete_objects(&db, vec![(oent("node", "node_1"), cascade(true)), (forced_txn_error_key(), cascade(true))], "t", "roll", o())
        .expect_err("forced mid-batch error");
    assert!(err.to_string().contains("0-D"), "names cause: {err}");
    assert_eq!(count(&db, "SELECT count(*) FROM commit"), commits_before, "no commit persisted");
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity WHERE id={e} AND valid_to_commit IS NULL")),
        1, "entity still live"
    );
    assert_eq!(live_entity_deps(&db, e), before, "dependents untouched");
}

// --- entity test 8: combined atomicity (value write + delete + entity delete)

#[test]
fn entity_combined_atomicity_one_commit() {
    let (_dir, db) = fresh_db();
    seed_entity_deps(&db);
    let commits_before = count(&db, "SELECT count(*) FROM commit");
    // ONE commit: write demand/node_3 (new), delete the pre-existing demand/node_2,
    // cascade-delete node_1 — three effects, one commit.
    let r = commit_changes(
        &db,
        vec![seg("node", "demand", "Base", scalar_batch("node_3", 9.0))],
        vec![],
        vec![dk("node", "demand", "Base", "node_2")],
        vec![(oent("node", "node_1"), cascade(true))],
        "t", "one atomic save", o(),
    ).expect("commit");
    assert_eq!(count(&db, "SELECT count(*) FROM commit") - commits_before, 1, "exactly one commit");
    assert!(matches!(r.object_deletes[0].result, ObjectDeleteResult::Deleted { .. }), "node_1 cascaded");
    assert_eq!(
        count(&db, "SELECT count(*) FROM entity WHERE cur_name=lower('node_1')"),
        0, "node_1 closed"
    );
}

// --- entity test 10: preview matches commit (0-D) --------------------------

#[test]
fn entity_preview_matches_commit() {
    let (_dir, db) = fresh_db();
    seed_entity_deps(&db);
    let commits_before = count(&db, "SELECT count(*) FROM commit");
    let previews = preview_object_deletes(&db, vec![oent("node", "node_1")]).expect("preview");
    assert_eq!(count(&db, "SELECT count(*) FROM commit"), commits_before, "preview mutates nothing");
    let p = &previews[0];
    assert!(p.found);

    let r = delete_objects(&db, vec![(oent("node", "node_1"), cascade(true))], "t", "cascade", o()).expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted, got {other:?}"),
    };
    assert_eq!(p.cascade.object_rows, cc.object_rows, "object_rows match");
    assert_eq!(p.cascade.total_rows, cc.total_rows, "total_rows match");
    for tc in &cc.per_table {
        let pc = per_table(&p.cascade, &tc.table).unwrap_or_else(|| panic!("preview missing {}", tc.table));
        assert_eq!(pc.rows, tc.rows, "{} count matches", tc.table);
    }
}

// ---------------------------------------------------------------------------
// Stage 2 + 3: N-D tuple resolution + B1 recursive cascade.
// ---------------------------------------------------------------------------

/// Create element entities node_a / node_b (0-D, class `node`), a 2-D class
/// `link` typed (node, node), and the N-D entity `link_ab` with tuple
/// (node_a, node_b). Returns link_ab's id.
fn seed_nd_link(db: &std::path::Path) -> i64 {
    write_parameter(db, "node", "demand", "Base", scalar_batch("node_a", 1.0), &[], o(), "t", "a").expect("node_a");
    write_parameter(db, "node", "demand", "Base", scalar_batch("node_b", 2.0), &[], o(), "t", "b").expect("node_b");
    exec(
        db,
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
                   (SELECT id FROM entity WHERE cur_name=lower('node_a')), (SELECT max(id) FROM commit)),
                  ((SELECT id FROM entity WHERE cur_name=lower('link_ab')), 2,
                   (SELECT id FROM entity WHERE cur_name=lower('node_b')), (SELECT max(id) FROM commit));",
    );
    count(db, "SELECT id FROM entity WHERE cur_name=lower('link_ab')")
}

// --- entity test 9: N-D dual channel (Name vs Elements ⇒ identical) --------

#[test]
fn entity_nd_dual_channel_identical() {
    // By Name.
    let (_d1, db1) = fresh_db();
    seed_nd_link(&db1);
    let r1 = delete_objects(&db1, vec![(oent("link", "link_ab"), cascade(true))], "t", "byname", o()).expect("c1");
    let cc1 = match &r1.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out.clone(),
        other => panic!("byname expected Deleted, got {other:?}"),
    };
    // link_ab is N-D: object_rows = 1 entity + 2 identity element rows.
    assert_eq!(cc1.object_rows, 3, "N-D object_rows = entity + tuple");

    // By Elements (fresh DB).
    let (_d2, db2) = fresh_db();
    seed_nd_link(&db2);
    let r2 = delete_objects(&db2, vec![(oent_elems("link", &["node_a", "node_b"]), cascade(true))], "t", "byelem", o())
        .expect("c2");
    let cc2 = match &r2.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out.clone(),
        other => panic!("byelem expected Deleted, got {other:?}"),
    };
    assert_eq!(cc1, cc2, "Name and Elements resolve the same row ⇒ identical counts");
}

// --- N-D ambiguity is rejected --------------------------------------------

#[test]
fn entity_nd_ambiguous_tuple_rejected() {
    let (_dir, db) = fresh_db();
    seed_nd_link(&db);
    // A second link entity with the SAME tuple (node_a, node_b) — no schema
    // constraint forbids it (one_current_entity_element is on (entity_id,pos)).
    exec(
        &db,
        "INSERT INTO entity (entity_class_id, name, valid_from_commit)
           VALUES ((SELECT id FROM entity_class WHERE cur_name=lower('link')), 'link_ab_dup', (SELECT max(id) FROM commit));
         INSERT INTO entity_element (entity_id, position, element_entity_id, valid_from_commit)
           VALUES ((SELECT id FROM entity WHERE cur_name=lower('link_ab_dup')), 1,
                   (SELECT id FROM entity WHERE cur_name=lower('node_a')), (SELECT max(id) FROM commit)),
                  ((SELECT id FROM entity WHERE cur_name=lower('link_ab_dup')), 2,
                   (SELECT id FROM entity WHERE cur_name=lower('node_b')), (SELECT max(id) FROM commit));",
    );
    let err = delete_objects(&db, vec![(oent_elems("link", &["node_a", "node_b"]), cascade(true))], "t", "amb", o())
        .expect_err("ambiguous tuple");
    assert!(err.to_string().contains("ambiguous"), "names ambiguity: {err}");
}

// --- Stage 3: B1 recursion — deleting an element cascades into its container -

#[test]
fn entity_cascade_recurses_into_container() {
    let (_dir, db) = fresh_db();
    let link_id = seed_nd_link(&db);
    let a_id = count(&db, "SELECT id FROM entity WHERE cur_name=lower('node_a')");

    // Delete node_a cascade:true ⇒ recurses into link_ab (which lists node_a).
    let r = delete_objects(&db, vec![(oent("node", "node_a"), cascade(true))], "t", "recurse", o()).expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted, got {other:?}"),
    };
    // object_rows: node_a (1) + link_ab entity (1) + link_ab's 2 identity element
    // rows (its whole tuple closes with it) = 4.
    assert_eq!(cc.object_rows, 4, "node_a + container entity + container tuple");
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity WHERE id={a_id} AND valid_to_commit IS NULL")),
        0, "node_a closed"
    );
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity WHERE id={link_id} AND valid_to_commit IS NULL")),
        0, "container link_ab recursively closed"
    );
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity_element WHERE entity_id={link_id} AND valid_to_commit IS NULL")),
        0, "container tuple closed"
    );
    audit_tiling_clean(&db);
}

// --- Stage 3: two-level container chain ------------------------------------

#[test]
fn entity_cascade_recurses_two_levels() {
    let (_dir, db) = fresh_db();
    let link_id = seed_nd_link(&db);
    // A super-container `chain` (1-D over link) whose single element is link_ab.
    exec(
        &db,
        "INSERT INTO entity_class (name, valid_from_commit)
           VALUES ('chain', (SELECT max(id) FROM commit));
         INSERT INTO entity_class_dimension (entity_class_id, position, dimension_class_id, valid_from_commit)
           VALUES ((SELECT id FROM entity_class WHERE cur_name=lower('chain')), 1,
                   (SELECT id FROM entity_class WHERE cur_name=lower('link')), (SELECT max(id) FROM commit));
         INSERT INTO entity (entity_class_id, name, valid_from_commit)
           VALUES ((SELECT id FROM entity_class WHERE cur_name=lower('chain')), 'chain_x', (SELECT max(id) FROM commit));
         INSERT INTO entity_element (entity_id, position, element_entity_id, valid_from_commit)
           VALUES ((SELECT id FROM entity WHERE cur_name=lower('chain_x')), 1,
                   (SELECT id FROM entity WHERE cur_name=lower('link_ab')), (SELECT max(id) FROM commit));",
    );
    let chain_id = count(&db, "SELECT id FROM entity WHERE cur_name=lower('chain_x')");

    // Deleting node_a recurses node_a → link_ab → chain_x.
    let r = delete_objects(&db, vec![(oent("node", "node_a"), cascade(true))], "t", "recurse2", o()).expect("commit");
    assert!(matches!(r.object_deletes[0].result, ObjectDeleteResult::Deleted { .. }));
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity WHERE id={link_id} AND valid_to_commit IS NULL")),
        0, "level-1 container closed"
    );
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity WHERE id={chain_id} AND valid_to_commit IS NULL")),
        0, "level-2 container closed"
    );
    audit_tiling_clean(&db);
}

// --- Stage 3: cascade:false reports the container as a blocker -------------

#[test]
fn entity_restrict_reports_container_blocker() {
    let (_dir, db) = fresh_db();
    let link_id = seed_nd_link(&db);
    let a_id = count(&db, "SELECT id FROM entity WHERE cur_name=lower('node_a')");

    let r = delete_objects(&db, vec![(oent("node", "node_a"), cascade(false))], "t", "restrict", o()).expect("commit");
    let blockers = match &r.object_deletes[0].result {
        ObjectDeleteResult::Restricted { blockers } => blockers,
        other => panic!("expected Restricted, got {other:?}"),
    };
    let cb = blocker(blockers, "entity").expect("container blocker on `entity`");
    assert_eq!(cb.column, "element_entity_id");
    assert!(cb.count >= 1, "at least one container");
    assert!(cb.sample.iter().any(|s| s == "link_ab"), "container name sampled: {:?}", cb.sample);
    // Closed NOTHING.
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity WHERE id={a_id} AND valid_to_commit IS NULL")),
        1, "node_a still live"
    );
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity WHERE id={link_id} AND valid_to_commit IS NULL")),
        1, "container still live"
    );
    audit_tiling_clean(&db);
}

// --- Stage 3: cycle-safety (visited-set) -----------------------------------

#[test]
fn entity_cascade_cycle_is_safe() {
    let (_dir, db) = fresh_db();
    write_parameter(&db, "node", "demand", "Base", scalar_batch("seed", 1.0), &[], o(), "t", "s").expect("seed");
    // Two 1-D `pair` entities that contain EACH OTHER (p1 ∈ p2, p2 ∈ p1).
    exec(
        &db,
        "INSERT INTO entity_class (name, valid_from_commit)
           VALUES ('pair', (SELECT max(id) FROM commit));
         INSERT INTO entity_class_dimension (entity_class_id, position, dimension_class_id, valid_from_commit)
           VALUES ((SELECT id FROM entity_class WHERE cur_name=lower('pair')), 1,
                   (SELECT id FROM entity_class WHERE cur_name=lower('pair')), (SELECT max(id) FROM commit));
         INSERT INTO entity (entity_class_id, name, valid_from_commit)
           VALUES ((SELECT id FROM entity_class WHERE cur_name=lower('pair')), 'p1', (SELECT max(id) FROM commit)),
                  ((SELECT id FROM entity_class WHERE cur_name=lower('pair')), 'p2', (SELECT max(id) FROM commit));
         INSERT INTO entity_element (entity_id, position, element_entity_id, valid_from_commit)
           VALUES ((SELECT id FROM entity WHERE cur_name=lower('p1')), 1,
                   (SELECT id FROM entity WHERE cur_name=lower('p2')), (SELECT max(id) FROM commit)),
                  ((SELECT id FROM entity WHERE cur_name=lower('p2')), 1,
                   (SELECT id FROM entity WHERE cur_name=lower('p1')), (SELECT max(id) FROM commit));",
    );
    let p1 = count(&db, "SELECT id FROM entity WHERE cur_name=lower('p1')");
    let p2 = count(&db, "SELECT id FROM entity WHERE cur_name=lower('p2')");

    // Must terminate (visited-set) and close BOTH.
    let r = delete_objects(&db, vec![(oent("pair", "p1"), cascade(true))], "t", "cycle", o()).expect("commit");
    assert!(matches!(r.object_deletes[0].result, ObjectDeleteResult::Deleted { .. }));
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity WHERE id IN ({p1},{p2}) AND valid_to_commit IS NULL")),
        0, "both cycle members closed"
    );
    audit_tiling_clean(&db);
}

// --- Stage 3: preview surfaces the recursive blast radius ------------------

#[test]
fn entity_preview_matches_recursive_commit() {
    let (_dir, db) = fresh_db();
    seed_nd_link(&db);
    let p = preview_object_deletes(&db, vec![oent("node", "node_a")]).expect("preview");
    let pv = &p[0];
    // Preview must include the container link_ab (blast radius) AND report it as
    // a blocker.
    assert!(blocker(&pv.blockers, "entity").is_some(), "container in blockers");

    let r = delete_objects(&db, vec![(oent("node", "node_a"), cascade(true))], "t", "cascade", o()).expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted, got {other:?}"),
    };
    assert_eq!(pv.cascade.object_rows, cc.object_rows, "recursive object_rows match");
    assert_eq!(pv.cascade.total_rows, cc.total_rows, "recursive total_rows match");
}

// ===========================================================================
// CLASS object deletion (object_delete plan §4e, §9 C7, §10 ruling 1, P4).
//
// The highest blast radius. Deleting a class composes CROSS-KIND cascades: its
// child ENTITIES (each running its own §4b/§4c cascade, incl. B1 into their
// containers), its child PARAMETER definitions (each its own §4d cascade), and
// the B2 recursive edge — every class that types a dimension on this one
// (`entity_class_dimension.dimension_class_id`) is closed with its own §4e
// cascade. Flat class legs (§4e/C7): identity `entity_class_dimension`;
// dependents `class_set_member`, `entity_class_display_mode`, and
// `semantic_annotation` filtered to BOTH 'entity_class' and 'class_dimension'.
// ===========================================================================

/// A class object-delete key (globally name-addressed; `class` is None).
fn oclass(name: &str) -> ObjectDeleteKey {
    ObjectDeleteKey { kind: ObjectKind::Class, class: None, target: ObjectRef::Name(name.into()) }
}

/// Seed class `node` with a full cross-kind dependent set:
///   - 2 child entities node_1, node_2 (node_1 carries scalar + long + array
///     values; node_2 a scalar) ⇒ across the class value_scalar=2, value_long=3,
///     value_array=1;
///   - 3 child parameter definitions demand (scalar), counts (long), profile
///     (array);
///   - flat class legs: class_set_member (node ∈ set `grp`),
///     entity_class_display_mode (mode `dm`), and semantic_annotation ×2 on node
///     (object_type 'entity_class' AND 'class_dimension') ⇒ semantic_annotation=2;
///   - a SECOND class `link` typed (node, node) — the B2 edge — with the N-D
///     entity link_ab = (node_1, node_2).
/// Returns class node's id.
fn seed_class_deps(db: &std::path::Path) -> i64 {
    write_parameter(db, "node", "demand", "Base", scalar_batch("node_1", 1.0), &[], o(), "t", "s")
        .expect("scalar node_1");
    write_parameter(db, "node", "demand", "Base", scalar_batch("node_2", 2.0), &[], o(), "t", "s2")
        .expect("scalar node_2");
    write_parameter(db, "node", "counts", "Base", long_batch("node_1", &[0, 1, 2], &[10, 20, 30]), &[], o(), "t", "l")
        .expect("long node_1");
    write_parameter(db, "node", "profile", "Base", array_batch("node_1", &[0, 1, 2], &[1.0, 2.0, 3.0]), &[], o(), "t", "a")
        .expect("array node_1");

    exec(
        db,
        "INSERT INTO class_set (name, valid_from_commit)
           VALUES ('grp', (SELECT max(id) FROM commit));
         INSERT INTO class_set_member (class_set_id, member_class_id, valid_from_commit)
           VALUES ((SELECT id FROM class_set WHERE cur_name=lower('grp')),
                   (SELECT id FROM entity_class WHERE cur_name=lower('node')),
                   (SELECT max(id) FROM commit));

         INSERT INTO display_mode (name, valid_from_commit)
           VALUES ('dm', (SELECT max(id) FROM commit));
         INSERT INTO entity_class_display_mode (display_mode_id, entity_class_id, valid_from_commit)
           VALUES ((SELECT id FROM display_mode WHERE cur_name=lower('dm')),
                   (SELECT id FROM entity_class WHERE cur_name=lower('node')),
                   (SELECT max(id) FROM commit));

         INSERT INTO semantic_annotation (object_type, object_id, term_iri, valid_from_commit)
           VALUES ('entity_class', (SELECT id FROM entity_class WHERE cur_name=lower('node')),
                   'http://example.org/node_class', (SELECT max(id) FROM commit));
         INSERT INTO semantic_annotation (object_type, object_id, object_subkey, term_iri, valid_from_commit)
           VALUES ('class_dimension', (SELECT id FROM entity_class WHERE cur_name=lower('node')),
                   '1', 'http://example.org/node_dim', (SELECT max(id) FROM commit));

         -- SECOND class `link` typed (node, node): the B2 dependent class.
         INSERT INTO entity_class (name, valid_from_commit)
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

    count(db, "SELECT id FROM entity_class WHERE cur_name=lower('node')")
}

/// Live class-scoped value rows across the whole class (both child entities).
fn live_class_values(db: &std::path::Path) -> i64 {
    count(db, "SELECT count(*) FROM value_scalar vs JOIN entity e ON e.id=vs.entity_id \
               WHERE e.entity_class_id=(SELECT id FROM entity_class WHERE cur_name=lower('node')) AND vs.valid_to_commit IS NULL")
        + count(db, "SELECT count(*) FROM value_long vl JOIN entity e ON e.id=vl.entity_id \
               WHERE e.entity_class_id=(SELECT id FROM entity_class WHERE cur_name=lower('node')) AND vl.valid_to_commit IS NULL")
        + count(db, "SELECT count(*) FROM value_array va JOIN entity e ON e.id=va.entity_id \
               WHERE e.entity_class_id=(SELECT id FROM entity_class WHERE cur_name=lower('node'))")
}

// --- class test 1: RESTRICT blocks + reports -------------------------------

#[test]
fn class_restrict_blocks_and_reports() {
    let (_dir, db) = fresh_db();
    seed_class_deps(&db);
    let values_before = live_class_values(&db);

    let r = delete_objects(&db, vec![(oclass("node"), cascade(false))], "t", "restrict", o())
        .expect("commit");
    let blockers = match &r.object_deletes[0].result {
        ObjectDeleteResult::Restricted { blockers } => blockers,
        other => panic!("expected Restricted, got {other:?}"),
    };
    // Cross-kind edges surface as blockers on their REAL tables.
    let ent = blocker(blockers, "entity").expect("child-entity blocker");
    assert_eq!(ent.column, "entity_class_id");
    assert!(ent.count >= 2, "node_1 + node_2 children");
    let pd = blocker(blockers, "parameter_definition").expect("child-parameter blocker");
    assert!(pd.count >= 3, "demand + counts + profile");
    let dep_cls = blocker(blockers, "entity_class").expect("B2 dependent-class blocker");
    assert_eq!(dep_cls.column, "dimension_class_id");
    assert!(dep_cls.sample.iter().any(|s| s == "link"), "B2 class sampled: {:?}", dep_cls.sample);
    // Flat class legs also block.
    assert!(blocker(blockers, "class_set_member").is_some());
    assert!(blocker(blockers, "entity_class_display_mode").is_some());
    assert!(blocker(blockers, "semantic_annotation").is_some());

    // Closed NOTHING.
    assert_eq!(count(&db, "SELECT count(*) FROM entity_class WHERE cur_name=lower('node')"), 1, "node still live");
    assert_eq!(count(&db, "SELECT count(*) FROM entity_class WHERE cur_name=lower('link')"), 1, "link still live");
    assert_eq!(live_class_values(&db), values_before, "no value closed");
    audit_tiling_clean(&db);
}

// --- class test 2: cascade closes + reports (cross-kind aggregate) ----------

#[test]
fn class_cascade_closes_and_reports() {
    let (_dir, db) = fresh_db();
    let node_id = seed_class_deps(&db);
    let hist_before = count(&db, "SELECT count(*) FROM value_array_hist");

    let r = delete_objects(&db, vec![(oclass("node"), cascade(true))], "t", "cascade", o())
        .expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted, got {other:?}"),
    };
    // Value counts AGGREGATE across both child entities (node_1 + node_2 scalars).
    assert_eq!(per_table(cc, "value_scalar").unwrap().rows, 2, "node_1 + node_2 scalars");
    assert_eq!(per_table(cc, "value_long").unwrap().rows, 3);
    assert_eq!(per_table(cc, "value_array").unwrap().rows, 1);
    // Flat class legs.
    assert_eq!(per_table(cc, "class_set_member").unwrap().rows, 1);
    assert_eq!(per_table(cc, "entity_class_display_mode").unwrap().rows, 1);
    assert_eq!(per_table(cc, "semantic_annotation").unwrap().rows, 2, "entity_class + class_dimension");

    // The class row, the B2 dependent class, every child entity + parameter are
    // all closed.
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity_class WHERE id={node_id} AND valid_to_commit IS NULL")),
        0, "class node closed"
    );
    assert_eq!(count(&db, "SELECT count(*) FROM entity_class WHERE cur_name=lower('link')"), 0, "B2 class link closed");
    assert_eq!(count(&db, "SELECT count(*) FROM entity WHERE cur_name IN (lower('node_1'),lower('node_2'),lower('link_ab'))"), 0, "child + container entities closed");
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM parameter_definition WHERE entity_class_id={node_id} AND valid_to_commit IS NULL")),
        0, "all child parameter definitions closed"
    );
    assert_eq!(live_class_values(&db), 0, "all class values closed");
    assert_eq!(count(&db, "SELECT count(*) FROM value_array_hist") - hist_before, 1, "array pre-image to hist");
    // object_rows covers class + link + 3 entities + 3 params + their identity.
    assert!(cc.object_rows >= 1 + 1 + 3 + 3, "object rows span all cascaded objects: {}", cc.object_rows);
    audit_tiling_clean(&db);
}

// --- class test 3: bare delete (empty class, no dependents) -----------------

#[test]
fn class_bare_delete_empty() {
    let (_dir, db) = fresh_db();
    exec(&db, "INSERT INTO entity_class (name, valid_from_commit) VALUES ('empty', (SELECT max(id) FROM commit));");
    let e = count(&db, "SELECT id FROM entity_class WHERE cur_name=lower('empty')");

    let r = delete_objects(&db, vec![(oclass("empty"), cascade(false))], "t", "bare", o())
        .expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted(empty), got {other:?}"),
    };
    assert_eq!(cc.object_rows, 1, "just the class row (0-D class has no identity dims)");
    assert!(cc.per_table.is_empty(), "no dependent tables");
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity_class WHERE id={e} AND valid_to_commit IS NULL")),
        0, "empty class row closed"
    );
    audit_tiling_clean(&db);
}

// --- class test 4: as-of queryable after delete ----------------------------

#[test]
fn class_as_of_queryable_after_delete() {
    let (_dir, db) = fresh_db();
    let node_id = seed_class_deps(&db);
    let c_before = count(&db, "SELECT max(id) FROM commit");

    let r = delete_objects(&db, vec![(oclass("node"), cascade(true))], "t", "cascade", o()).expect("commit");
    let c_del = r.commit_id;

    assert_eq!(
        count(&db, &format!(
            "SELECT count(*) FROM entity_class WHERE id={node_id} \
             AND valid_from_commit <= {c_before} AND (valid_to_commit IS NULL OR valid_to_commit > {c_before})")),
        1, "class live as-of the pre-delete commit"
    );
    assert_eq!(
        count(&db, &format!(
            "SELECT count(*) FROM entity_class WHERE id={node_id} \
             AND valid_from_commit <= {c_del} AND (valid_to_commit IS NULL OR valid_to_commit > {c_del})")),
        0, "class gone as-of the delete commit"
    );
    // A child value is content-retained + queryable as-of the pre-delete commit.
    assert_eq!(
        count(&db, &format!(
            "SELECT count(*) FROM value_long WHERE content_retained \
             AND valid_from_commit <= {c_before} AND (valid_to_commit IS NULL OR valid_to_commit > {c_before})")),
        3, "child values retained + queryable as-of"
    );
}

// --- class test 5: monotonic versions + clean tiling -----------------------

#[test]
fn class_monotonic_and_tiling_clean() {
    let (_dir, db) = fresh_db();
    seed_class_deps(&db);
    delete_objects(&db, vec![(oclass("node"), cascade(true))], "t", "cascade", o()).expect("commit");
    audit_tiling_clean(&db);
    assert_eq!(count(&db, "SELECT count(*) FROM entity_class WHERE cur_name=lower('node')"), 0, "no current node row");
    assert_eq!(
        count(&db, "SELECT count(*) FROM entity_class WHERE valid_to_commit IS NOT NULL AND valid_to_commit <= valid_from_commit"),
        0, "closed class spans are coherent"
    );
}

// --- class test 6: idempotent re-delete ⇒ NotFound -------------------------

#[test]
fn class_idempotent_redelete_notfound() {
    let (_dir, db) = fresh_db();
    seed_class_deps(&db);
    delete_objects(&db, vec![(oclass("node"), cascade(true))], "t", "first", o()).expect("first");
    let r = delete_objects(&db, vec![(oclass("node"), cascade(true))], "t", "second", o()).expect("second");
    assert!(matches!(r.object_deletes[0].result, ObjectDeleteResult::NotFound), "second ⇒ NotFound");
}

// --- class test 7: atomic rollback on forced mid-batch error ---------------

#[test]
fn class_atomic_rollback_on_mid_batch_error() {
    let (_dir, db) = fresh_db();
    let node_id = seed_class_deps(&db);
    // An independent 0-D class that node's cascade does NOT touch — the forced
    // error must survive the first (valid) class delete.
    exec(&db, "INSERT INTO entity_class (name, valid_from_commit) VALUES ('keeper', (SELECT max(id) FROM commit));");
    let commits_before = count(&db, "SELECT count(*) FROM commit");

    // Valid class cascade FIRST, then a forced mid-txn error ⇒ whole txn rolls back.
    let err = delete_objects(
        &db,
        vec![(oclass("node"), cascade(true)), (forced_txn_error_key_for("keeper"), cascade(true))],
        "t", "roll", o(),
    ).expect_err("forced mid-batch error");
    assert!(err.to_string().contains("0-D"), "names cause: {err}");

    assert_eq!(count(&db, "SELECT count(*) FROM commit"), commits_before, "no commit persisted");
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM entity_class WHERE id={node_id} AND valid_to_commit IS NULL")),
        1, "node still live (first delete rolled back)"
    );
    assert_eq!(count(&db, "SELECT count(*) FROM entity_class WHERE cur_name=lower('link')"), 1, "B2 class untouched");
}

// --- class test 8: combined atomicity (value write + delete + class delete) -

#[test]
fn class_combined_atomicity_one_commit() {
    let (_dir, db) = fresh_db();
    seed_class_deps(&db);
    // A SEPARATE class `gen` we mutate in the same commit (independent of node).
    write_parameter(&db, "gen", "cap", "Base", scalar_batch("g1", 1.0), &[], o(), "t", "g1").expect("g1");
    let commits_before = count(&db, "SELECT count(*) FROM commit");

    // ONE commit: write gen/g2, delete gen/g1, cascade-delete class node.
    let r = commit_changes(
        &db,
        vec![seg("gen", "cap", "Base", scalar_batch("g2", 2.0))],
        vec![],
        vec![dk("gen", "cap", "Base", "g1")],
        vec![(oclass("node"), cascade(true))],
        "t", "one atomic save", o(),
    ).expect("commit");

    assert_eq!(count(&db, "SELECT count(*) FROM commit") - commits_before, 1, "exactly one commit");
    assert_eq!(r.rows_written, 1, "gen/g2 written");
    assert_eq!(r.rows_closed, 1, "gen/g1 closed");
    assert!(matches!(r.object_deletes[0].result, ObjectDeleteResult::Deleted { .. }), "node cascaded");
    assert_eq!(count(&db, "SELECT count(*) FROM entity_class WHERE cur_name=lower('node')"), 0, "node closed in same commit");
    // gen survived with g2 current, g1 gone.
    let got = read_parameter(&db, "gen", "cap", "Base").expect("read");
    let names = entity_names(&got);
    assert!(names.contains(&"g2".to_string()) && !names.contains(&"g1".to_string()));
    audit_tiling_clean(&db);
}

// --- class test 10: preview matches commit (full cross-kind blast radius) ---

#[test]
fn class_preview_matches_commit() {
    let (_dir, db) = fresh_db();
    seed_class_deps(&db);
    let commits_before = count(&db, "SELECT count(*) FROM commit");

    let previews = preview_object_deletes(&db, vec![oclass("node")]).expect("preview");
    let p = &previews[0];
    assert!(p.found);
    assert_eq!(p.kind, ObjectKind::Class);
    assert_eq!(count(&db, "SELECT count(*) FROM commit"), commits_before, "preview mutates nothing");
    // Immediate cross-kind dependents surface as blockers.
    assert!(blocker(&p.blockers, "entity").is_some(), "child entities in blockers");
    assert!(blocker(&p.blockers, "parameter_definition").is_some(), "child params in blockers");
    assert!(blocker(&p.blockers, "entity_class").is_some(), "B2 class in blockers");

    let r = delete_objects(&db, vec![(oclass("node"), cascade(true))], "t", "cascade", o()).expect("commit");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted, got {other:?}"),
    };
    // The dedup preview counter must match the commit rowcounts EXACTLY, across
    // kinds (a value keyed by both a child entity and a child parameter counted
    // once), proving no drift.
    assert_eq!(p.cascade.object_rows, cc.object_rows, "object_rows match");
    assert_eq!(p.cascade.total_rows, cc.total_rows, "total_rows match");
    for tc in &cc.per_table {
        let pc = per_table(&p.cascade, &tc.table).unwrap_or_else(|| panic!("preview missing {}", tc.table));
        assert_eq!(pc.rows, tc.rows, "{} count matches", tc.table);
    }
    assert_eq!(p.cascade.per_table.len(), cc.per_table.len(), "no extra preview tables");
}

// --- class test: B2 mutual-reference cycle terminates + closes both --------

#[test]
fn class_b2_cycle_is_safe() {
    let (_dir, db) = fresh_db();
    write_parameter(&db, "node", "demand", "Base", scalar_batch("seed", 1.0), &[], o(), "t", "s").expect("seed");
    // Classes `ca` and `cb` type a dimension on EACH OTHER (mutual B2).
    exec(
        &db,
        "INSERT INTO entity_class (name, valid_from_commit)
           VALUES ('ca', (SELECT max(id) FROM commit)), ('cb', (SELECT max(id) FROM commit));
         INSERT INTO entity_class_dimension (entity_class_id, position, dimension_class_id, valid_from_commit)
           VALUES ((SELECT id FROM entity_class WHERE cur_name=lower('ca')), 1,
                   (SELECT id FROM entity_class WHERE cur_name=lower('cb')), (SELECT max(id) FROM commit)),
                  ((SELECT id FROM entity_class WHERE cur_name=lower('cb')), 1,
                   (SELECT id FROM entity_class WHERE cur_name=lower('ca')), (SELECT max(id) FROM commit));",
    );
    // Must terminate (visited-set on (kind,id)) and close BOTH classes.
    let r = delete_objects(&db, vec![(oclass("ca"), cascade(true))], "t", "cycle", o()).expect("commit");
    assert!(matches!(r.object_deletes[0].result, ObjectDeleteResult::Deleted { .. }));
    assert_eq!(
        count(&db, "SELECT count(*) FROM entity_class WHERE cur_name IN (lower('ca'),lower('cb'))"),
        0, "both mutually-referencing classes closed"
    );
    audit_tiling_clean(&db);
}

// ===========================================================================
// Final-review fixes (review of P1–P4).
// ===========================================================================

/// Seed a 2-D class `gap` whose `entity_class_dimension` positions are
/// NON-contiguous (1 and 3 — the schema only requires `position >= 1` with
/// per-class uniqueness, NOT contiguity), plus its N-D entity `gap_ab` whose
/// tuple lives at those same positions (node_a@1, node_b@3).
fn seed_gap_class(db: &std::path::Path) {
    write_parameter(db, "node", "demand", "Base", scalar_batch("node_a", 1.0), &[], o(), "t", "a").expect("node_a");
    write_parameter(db, "node", "demand", "Base", scalar_batch("node_b", 2.0), &[], o(), "t", "b").expect("node_b");
    exec(
        db,
        "INSERT INTO entity_class (name, valid_from_commit)
           VALUES ('gap', (SELECT max(id) FROM commit));
         INSERT INTO entity_class_dimension (entity_class_id, position, dimension_class_id, valid_from_commit)
           VALUES ((SELECT id FROM entity_class WHERE cur_name=lower('gap')), 1,
                   (SELECT id FROM entity_class WHERE cur_name=lower('node')), (SELECT max(id) FROM commit)),
                  ((SELECT id FROM entity_class WHERE cur_name=lower('gap')), 3,
                   (SELECT id FROM entity_class WHERE cur_name=lower('node')), (SELECT max(id) FROM commit));
         INSERT INTO entity (entity_class_id, name, valid_from_commit)
           VALUES ((SELECT id FROM entity_class WHERE cur_name=lower('gap')), 'gap_ab', (SELECT max(id) FROM commit));
         INSERT INTO entity_element (entity_id, position, element_entity_id, valid_from_commit)
           VALUES ((SELECT id FROM entity WHERE cur_name=lower('gap_ab')), 1,
                   (SELECT id FROM entity WHERE cur_name=lower('node_a')), (SELECT max(id) FROM commit)),
                  ((SELECT id FROM entity WHERE cur_name=lower('gap_ab')), 3,
                   (SELECT id FROM entity WHERE cur_name=lower('node_b')), (SELECT max(id) FROM commit));",
    );
}

// --- Fix 1: N-D resolver binds the REAL position, not i+1 ------------------

#[test]
fn entity_nd_noncontiguous_positions_resolve() {
    // By Name — the baseline.
    let (_d1, db1) = fresh_db();
    seed_gap_class(&db1);
    let r1 = delete_objects(&db1, vec![(oent("gap", "gap_ab"), cascade(true))], "t", "byname", o()).expect("c1");
    let cc1 = match &r1.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out.clone(),
        other => panic!("byname expected Deleted, got {other:?}"),
    };
    assert_eq!(cc1.object_rows, 3, "N-D object_rows = entity + 2 tuple rows");

    // By Elements at NON-contiguous positions (node_a@1, node_b@3). Under the old
    // `i+1` HAVING this spuriously resolved to NotFound (it looked at positions
    // 1,2 instead of 1,3); with the real position it must resolve identically.
    let (_d2, db2) = fresh_db();
    seed_gap_class(&db2);
    let r2 = delete_objects(&db2, vec![(oent_elems("gap", &["node_a", "node_b"]), cascade(true))], "t", "byelem", o())
        .expect("c2");
    let cc2 = match &r2.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out.clone(),
        other => panic!("byelem expected Deleted (NotFound under the i+1 bug), got {other:?}"),
    };
    assert_eq!(cc1, cc2, "Name and Elements resolve the same entity ⇒ identical CascadeCounts");
}

// --- Fix 2: preview validates keys exactly like commit ---------------------

#[test]
fn preview_rejects_malformed_alternative_key() {
    let (_dir, db) = fresh_db();
    // An Alternative key carrying `class: Some(..)` — commit rejects this via
    // validate_object_key; preview must reject it with the SAME error class,
    // never silently resolve it.
    let bad = ObjectDeleteKey {
        kind: ObjectKind::Alternative,
        class: Some("node".into()),
        target: ObjectRef::Name("Base".into()),
    };
    let err = preview_object_deletes(&db, vec![bad]).expect_err("preview must reject the malformed key");
    assert!(err.to_string().contains("class must be None"), "same error as commit: {err}");
}

// --- Fix 3a: overlapping value-delete + object-cascade counts once (D5) -----

#[test]
fn overlapping_value_delete_and_object_cascade_counts_once() {
    let (_dir, db) = fresh_db();
    // Two scalar coords in alternative `victim`: node_1 and node_2.
    write_parameter(&db, "node", "demand", "victim", scalar_batch("node_1", 1.0), &[], o(), "t", "s1").expect("s1");
    write_parameter(&db, "node", "demand", "victim", scalar_batch("node_2", 2.0), &[], o(), "t", "s2").expect("s2");
    let alt_id = count(&db, "SELECT id FROM alternative WHERE cur_name=lower('victim')");

    // ONE commit: value-delete node_1's coord AND cascade-delete the whole victim
    // alternative — which ALSO covers node_1's coord. The value delete closes it
    // first; the object cascade's live-guard then finds it already closed and does
    // NOT re-close or re-count it (§2/§8.4 D5).
    let r = commit_changes(
        &db,
        vec![],
        vec![],
        vec![dk("node", "demand", "victim", "node_1")],
        vec![(oalt("victim"), cascade(true))],
        "t", "overlap", o(),
    ).expect("commit");

    assert_eq!(r.rows_closed, 1, "value delete closed node_1's coord once");
    let cc = match &r.object_deletes[0].result {
        ObjectDeleteResult::Deleted { closed_out } => closed_out,
        other => panic!("expected Deleted, got {other:?}"),
    };
    // The cascade counts ONLY the still-live coord (node_2), not the already-closed
    // node_1 — no double-count.
    assert_eq!(
        per_table(cc, "value_scalar").unwrap().rows, 1,
        "cascade counts the still-live coord only, not the already-closed one"
    );

    // Reality matches the report: both coords closed at this commit, each exactly
    // once (none carries a double / re-stamped valid_to), none left live.
    let cid = r.commit_id;
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM value_scalar WHERE alternative_id={alt_id} AND valid_to_commit={cid}")),
        2, "both coords closed exactly once at this commit"
    );
    assert_eq!(
        count(&db, &format!("SELECT count(*) FROM value_scalar WHERE alternative_id={alt_id} AND valid_to_commit IS NULL")),
        0, "no live coord remains"
    );
    audit_tiling_clean(&db);
}

// --- Fix 3b: same-commit ARRAY write + cascade guard (move-to-hist) ---------

#[test]
fn same_commit_array_write_then_cascade_guard() {
    let (_dir, db) = fresh_db();
    let hist_before = count(&db, "SELECT count(*) FROM value_array_hist");

    // In ONE commit: write an ARRAY value into a BRAND-NEW alternative `afresh`
    // (valid_from = commit_id) AND cascade-delete it. The move-to-hist guard
    // (`valid_from_commit < commit_id`) must skip the same-commit array row, so
    // NOTHING is inserted into value_array_hist (whose span CHECK has no NULL
    // branch) ⇒ no trip, result NotFound, and the array value survives.
    let r = commit_changes(
        &db,
        vec![seg("node", "profile", "afresh", array_batch("node_1", &[0, 1, 2], &[1.0, 2.0, 3.0]))],
        vec![],
        vec![],
        vec![(oalt("afresh"), cascade(true))],
        "t", "array hazard", o(),
    ).expect("commit must not trip the value_array_hist span CHECK");

    assert_eq!(
        r.object_deletes[0].result,
        ObjectDeleteResult::NotFound,
        "same-commit-created target is guarded ⇒ NotFound, not an error"
    );
    assert_eq!(
        count(&db, "SELECT count(*) FROM value_array_hist") - hist_before,
        0, "no same-commit array row moved to hist"
    );
    // The array value (and its alternative) survived — the guard protected them.
    let got = read_parameter(&db, "node", "profile", "afresh").expect("read");
    assert_eq!(entity_names(&got), vec!["node_1".to_string(); 3], "array value survives");
    assert_eq!(
        count(&db, "SELECT count(*) FROM alternative WHERE cur_name=lower('afresh')"),
        1, "alternative still live"
    );
    audit_tiling_clean(&db);
}
