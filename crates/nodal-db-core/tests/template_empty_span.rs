//! Empty migration spans (`steps: []`) — a declared version bump with no
//! chain steps (template_ir_plan §4, finale-plan D-F1): such a span parses,
//! validates, chains, and version-stamps. The runner walks a stamped DB
//! through consecutive empty spans to head and certifies convergence; the
//! linear-chain rules (`from != to`, unique `from`) still apply.
//!
//! Generic, self-contained templates only. The production-scale ladder
//! (empty spans interleaved with hint and `run:` spans) is exercised by
//! `template_synthetic_ladder.rs`.

use std::path::Path;

use duckdb::Connection;

use nodal_db_core::schema_init::init_schema;
use nodal_db_core::template::{apply, parse_str, validate, ApplyOptions, MigrationRegistry};
use nodal_db_core::verbs::stamp_template_identity;

fn meta(db: &Path, key: &str) -> Option<String> {
    let conn = Connection::open(db).unwrap();
    conn.query_row(
        "SELECT value FROM metadata WHERE cur_name = lower(?) ",
        [key],
        |r| r.get::<_, String>(0),
    )
    .ok()
}

#[test]
fn empty_span_parses_and_validates() {
    let t = parse_str(
        "template: {name: t, version: '3'}\n\
         classes: [{name: c}]\n\
         migrations:\n\
         - {from: '1', to: '2', steps: []}\n\
         - {from: '2', to: '3', steps: []}\n",
    )
    .unwrap();
    let chk = validate(&t).expect("empty steps are a declared version bump (D-F1)");
    assert_eq!(chk.template.migrations.len(), 2);
    assert!(chk.template.migrations.iter().all(|m| m.steps.is_empty()));
}

#[test]
fn empty_span_still_rejects_from_eq_to_and_duplicate_from() {
    let bad = parse_str(
        "template: {name: t, version: '2'}\nclasses: [{name: c}]\n\
         migrations: [{from: '1', to: '1', steps: []}]\n",
    )
    .unwrap();
    assert!(validate(&bad).is_err(), "from == to stays an error");
    let dup = parse_str(
        "template: {name: t, version: '3'}\nclasses: [{name: c}]\n\
         migrations:\n - {from: '1', to: '2', steps: []}\n - {from: '1', to: '3', steps: []}\n",
    )
    .unwrap();
    assert!(validate(&dup).is_err(), "duplicate from stays an error (linear chain)");
}

/// The runner walk: a DB stamped mid-chain walks consecutive EMPTY spans to
/// the template's version — each span version-stamps in its own commit, the
/// final reconcile certifies, and a re-apply is the no-op short-circuit.
#[test]
fn empty_span_chain_stamps_through_and_certifies() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();

    let v1 = "template: {name: t, version: '1'}\nclasses: [{name: c}]\n";
    let t1 = parse_str(v1).unwrap();
    let out =
        apply(&db, &t1, &MigrationRegistry::new(), "tester", "", &ApplyOptions::default())
            .unwrap();
    assert!(out.converged);
    assert_eq!(meta(&db, "template_version").as_deref(), Some("1"));

    // Head template v4: same vocabulary, three empty spans 1→2→3→4.
    let v4 = "template: {name: t, version: '4'}\n\
              classes: [{name: c}]\n\
              migrations:\n\
              - {from: '1', to: '2', steps: []}\n\
              - {from: '2', to: '3', steps: []}\n\
              - {from: '3', to: '4', steps: []}\n";
    let t4 = parse_str(v4).unwrap();
    let out =
        apply(&db, &t4, &MigrationRegistry::new(), "tester", "", &ApplyOptions::default())
            .unwrap();
    assert!(out.converged, "empty chain converges: {:?}", out.plan.skipped);
    assert!(!out.no_op);
    // One version-stamp commit per span (empty spans burn no step commits)
    // plus the final reconcile commit (identity re-stamp: the walked-to
    // version's content hash must be re-certified for THIS template).
    assert_eq!(out.commits.len(), 4, "three span stamps + the identity re-stamp");
    assert_eq!(meta(&db, "template_version").as_deref(), Some("4"));
    assert!(
        meta(&db, "template_content_hash").is_some(),
        "converged walk certifies the content hash"
    );
    // Substamp cleared (span stamps clear it; empty spans never wrote one).
    assert_eq!(meta(&db, "template_migration_step"), None);

    // Idempotency: the walked DB short-circuits.
    let again =
        apply(&db, &t4, &MigrationRegistry::new(), "tester", "", &ApplyOptions::default())
            .unwrap();
    assert!(again.no_op, "re-apply after the walk is the hash short-circuit");

    // A mid-chain entrant (stamped "3") is selected by pending_spans and
    // walks exactly the remaining span.
    let db2 = dir.path().join("t2.duckdb");
    init_schema(&db2).unwrap();
    let out = apply(&db2, &t1, &MigrationRegistry::new(), "tester", "", &ApplyOptions::default())
        .unwrap();
    assert!(out.converged);
    stamp_template_identity(&db2, "t", "3", true, "tester").unwrap();
    let out =
        apply(&db2, &t4, &MigrationRegistry::new(), "tester", "", &ApplyOptions::default())
            .unwrap();
    assert!(out.converged);
    assert_eq!(
        out.commits.len(),
        2,
        "one pending span (3→4) version stamp + the identity re-stamp"
    );
    assert_eq!(meta(&db2, "template_version").as_deref(), Some("4"));
}
