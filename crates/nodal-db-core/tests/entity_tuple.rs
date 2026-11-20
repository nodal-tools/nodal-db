//! The element-tuple rule (entity_rename_PLAN E2, §5.2-§5.4): no two live
//! entities of one class hold the same element tuple. Every row of the
//! create table, `ensure_entity`, files that already hold duplicates (the
//! `audit_entity_tuple_dup` view), the per-call tuple index's statement
//! count and scope, the conflict retry, template validation and export.

use std::path::{Path, PathBuf};

use duckdb::Connection;

use nodal_db_core::api::{commit_changes, commit_structural};
use nodal_db_core::entity_tuple::test_hooks;
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::template::export::export;
use nodal_db_core::template::{parse_str, validate};
use nodal_db_core::types::{
    ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, StructuralChange as SC,
    StructuralEffect as SE, WarningKind, WriteOptions, WriteReport,
};
use nodal_db_core::verbs::ensure_entity;

fn cs(db: &Path, changes: Vec<SC>) -> Result<WriteReport, DbError> {
    commit_structural(db, changes, "t", "m", WriteOptions::default())
}

fn ok(db: &Path, changes: Vec<SC>) -> WriteReport {
    cs(db, changes).unwrap()
}

fn api_err<T: std::fmt::Debug>(r: Result<T, DbError>) -> String {
    match r {
        Err(DbError::Api(m)) => m,
        other => panic!("expected DbError::Api, got {other:?}"),
    }
}

fn q1(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn class(name: &str, dims: &[&str]) -> SC {
    SC::CreateEntityClass {
        name: name.into(),
        dims: dims.iter().map(|d| d.to_string()).collect(),
        description: None,
    }
}

fn ent(class: &str, name: &str) -> SC {
    SC::CreateEntity {
        class: class.into(),
        name: Some(name.into()),
        elements: vec![],
        description: None,
    }
}

fn nd(class: &str, name: Option<&str>, elements: &[&str]) -> SC {
    SC::CreateEntity {
        class: class.into(),
        name: name.map(Into::into),
        elements: elements.iter().map(|e| e.to_string()).collect(),
        description: None,
    }
}

/// `unit` (u1, u2), `node` (n1, n2, bus) and the 2-D `unit__node`, holding
/// `u1__n1` over (u1, n1).
fn fixture(dir: &tempfile::TempDir) -> PathBuf {
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    ok(
        &db,
        vec![
            class("unit", &[]),
            class("node", &[]),
            class("unit__node", &["unit", "node"]),
            ent("unit", "u1"),
            ent("unit", "u2"),
            ent("node", "n1"),
            ent("node", "n2"),
            ent("node", "bus"),
            nd("unit__node", None, &["u1", "n1"]),
        ],
    );
    db
}

fn live_nd(db: &Path) -> i64 {
    q1(
        db,
        "SELECT count(*) FROM entity e JOIN entity_class c ON c.id = e.entity_class_id \
         AND c.valid_to_commit IS NULL AND c.name = 'unit__node' WHERE e.valid_to_commit IS NULL",
    )
}

// ---------------------------------------------------------------------------
// The create table (§5.2)
// ---------------------------------------------------------------------------

#[test]
fn explicit_name_hit_with_its_tuple_is_a_noop_and_another_tuple_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let r = ok(&db, vec![nd("unit__node", Some("u1__n1"), &["u1", "n1"])]);
    assert_eq!(r.structural[0].effect, SE::NoOp);
    assert_eq!(r.commit_id, 0);
    let m = api_err(cs(&db, vec![nd("unit__node", Some("u1__n1"), &["u2", "n1"])]));
    assert!(m.contains("already exists with elements [u1, n1], not [u2, n1]"), "{m}");
}

#[test]
fn explicit_new_name_over_a_held_tuple_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let m = api_err(cs(&db, vec![nd("unit__node", Some("link1"), &["u1", "n1"])]));
    assert!(
        m.contains("elements [u1, n1] already exist as \"u1__n1\"")
            && m.contains("duplicate element tuples are not allowed"),
        "{m}"
    );
    assert_eq!(live_nd(&db), 1);
    // A free tuple under an explicit name is created.
    let r = ok(&db, vec![nd("unit__node", Some("link1"), &["u1", "n2"])]);
    assert_eq!(r.structural[0].effect, SE::Created);
}

#[test]
fn derived_name_holder_is_a_noop_with_a_folded_spelling_warned() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let id = q1(&db, "SELECT id FROM entity WHERE cur_name = 'u1__n1'");
    let r = ok(&db, vec![nd("unit__node", None, &["u1", "n1"])]);
    assert_eq!(r.structural[0].effect, SE::NoOp);
    assert_eq!(r.structural[0].object_id, id);
    // Another spelling of the elements derives another spelling of the
    // name: the holder answers, and the fold is reported.
    let r = ok(&db, vec![nd("unit__node", None, &["U1", "N1"])]);
    assert_eq!(r.structural[0].effect, SE::NoOp);
    assert_eq!(r.structural[0].object_id, id);
    let folded: Vec<(String, String)> = r
        .warnings
        .iter()
        .filter_map(|w| match &w.kind {
            WarningKind::NameFolded { spellings, .. } => Some(spellings.clone()),
            _ => None,
        })
        .flatten()
        .map(|n| (n.written, n.stored))
        .collect();
    assert!(
        folded.contains(&("U1__N1".to_string(), "u1__n1".to_string())),
        "{folded:?}"
    );
}

#[test]
fn derived_create_over_a_tuple_held_under_another_name_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    ok(&db, vec![nd("unit__node", Some("link"), &["u2", "n2"])]);
    let m = api_err(cs(&db, vec![nd("unit__node", None, &["u2", "n2"])]));
    assert!(
        m.contains("elements [u2, n2] already exist as \"link\"")
            && m.contains("address it by that name"),
        "{m}"
    );
}

#[test]
fn derived_name_taken_by_another_tuple_asks_for_an_explicit_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    // `u2__n2` names the tuple (u2, n1).
    ok(&db, vec![nd("unit__node", Some("u2__n2"), &["u2", "n1"])]);
    let m = api_err(cs(&db, vec![nd("unit__node", None, &["u2", "n2"])]));
    assert!(
        m.contains("the derived name \"u2__n2\" is taken by an entity with elements [u2, n1]")
            && m.contains("give an explicit name"),
        "{m}"
    );
    // A free derived name over a free tuple is created.
    let r = ok(&db, vec![nd("unit__node", None, &["u1", "bus"])]);
    assert_eq!(r.structural[0].effect, SE::Created);
    assert_eq!(
        q1(&db, "SELECT count(*) FROM entity WHERE cur_name = 'u1__bus'"),
        1
    );
}

#[test]
fn an_unresolved_element_still_errors_first() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let m = api_err(cs(&db, vec![nd("unit__node", None, &["u1", "nope"])]));
    assert!(m.contains("element \"nope\" at position 2"), "{m}");
}

#[test]
fn zero_d_creates_are_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let r = ok(&db, vec![ent("node", "N1"), ent("node", "n3")]);
    assert_eq!(r.structural[0].effect, SE::NoOp);
    assert_eq!(r.structural[1].effect, SE::Created);
}

// ---------------------------------------------------------------------------
// Within one call: the index follows the call's own creates
// ---------------------------------------------------------------------------

#[test]
fn two_creates_of_one_tuple_in_a_call() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    // Two names: the second is refused (the first create entered the index).
    let m = api_err(cs(
        &db,
        vec![
            nd("unit__node", Some("a"), &["u2", "bus"]),
            nd("unit__node", Some("b"), &["u2", "bus"]),
        ],
    ));
    assert!(m.contains("already exist as \"a\""), "{m}");
    // The same derived create twice: one entity, then a NoOp.
    let r = ok(
        &db,
        vec![
            nd("unit__node", None, &["u2", "bus"]),
            nd("unit__node", None, &["u2", "bus"]),
        ],
    );
    assert_eq!(r.structural[0].effect, SE::Created);
    assert_eq!(r.structural[1].effect, SE::NoOp);
    assert_eq!(r.structural[0].object_id, r.structural[1].object_id);
}

#[test]
fn elements_created_in_the_call_are_free() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let r = ok(
        &db,
        vec![
            nd("unit__node", None, &["u1", "n2"]),
            ent("unit", "u3"),
            nd("unit__node", None, &["u3", "n1"]),
            nd("unit__node", Some("other"), &["u1", "bus"]),
        ],
    );
    assert!(r
        .structural
        .iter()
        .filter(|o| o.change_index != 1)
        .all(|o| o.effect == SE::Created));
    assert_eq!(live_nd(&db), 4);
}

#[test]
fn a_delete_and_a_re_create_of_a_tuple_in_one_call_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let r = commit_changes(
        &db,
        vec![],
        vec![nd("unit__node", Some("again"), &["u1", "n1"])],
        vec![],
        vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Entity,
                class: Some("unit__node".into()),
                target: ObjectRef::Name("u1__n1".into()),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        "t",
        "m",
        WriteOptions::default(),
    );
    let m = api_err(r);
    assert!(m.contains("already exist as \"u1__n1\""), "{m}");
}

#[test]
fn a_conflict_retry_rebuilds_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    nodal_db_core::api::test_hooks::inject_conflicts(1);
    let r = ok(
        &db,
        vec![
            nd("unit__node", None, &["u2", "n1"]),
            nd("unit__node", None, &["u2", "n2"]),
        ],
    );
    assert!(r.structural.iter().all(|o| o.effect == SE::Created));
    assert_eq!(live_nd(&db), 3);
    assert_eq!(
        q1(&db, "SELECT count(*) FROM audit_entity_tuple_dup"),
        0
    );
}

// ---------------------------------------------------------------------------
// ensure_entity
// ---------------------------------------------------------------------------

#[test]
fn ensure_entity_follows_the_rule_and_its_fast_path_falls_through() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let id = q1(&db, "SELECT id FROM entity WHERE cur_name = 'u1__n1'");
    let els = |a: &str, b: &str| vec![a.to_string(), b.to_string()];
    // Fast path: a by-name hit whose tuple verifies.
    assert_eq!(
        ensure_entity(&db, "unit__node", None, &els("u1", "n1"), "t").unwrap(),
        id
    );
    // A held tuple under another name.
    let m = api_err(ensure_entity(
        &db,
        "unit__node",
        Some("x"),
        &els("u1", "n1"),
        "t",
    ));
    assert!(m.contains("already exist as \"u1__n1\""), "{m}");
    // The derived name taken by another tuple: the fast path's mismatch
    // falls through to the transaction's message.
    ensure_entity(&db, "unit__node", Some("u2__n2"), &els("u2", "n1"), "t").unwrap();
    let m = api_err(ensure_entity(&db, "unit__node", None, &els("u2", "n2"), "t"));
    assert!(m.contains("give an explicit name"), "{m}");
}

// ---------------------------------------------------------------------------
// Files that already hold duplicates (§5.4)
// ---------------------------------------------------------------------------

/// Plant a second holder of (u1, n1), named `dup`, by direct SQL.
fn plant_duplicate(db: &Path) -> (i64, i64) {
    let conn = Connection::open(db).unwrap();
    let c: i64 = conn
        .query_row(
            "INSERT INTO commit (user, message) VALUES ('sql', 'dup') RETURNING id",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let orig: i64 = conn
        .query_row("SELECT id FROM entity WHERE cur_name = 'u1__n1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    let dup: i64 = conn
        .query_row(
            "INSERT INTO entity (entity_class_id, name, valid_from_commit) \
             SELECT entity_class_id, 'dup', ? FROM entity WHERE id = ? RETURNING id",
            duckdb::params![c, orig],
            |r| r.get(0),
        )
        .unwrap();
    conn.execute(
        "INSERT INTO entity_element (entity_id, position, element_entity_id, valid_from_commit) \
         SELECT ?, position, element_entity_id, ? FROM entity_element WHERE cur_e = ?",
        duckdb::params![dup, c, orig],
    )
    .unwrap();
    (orig, dup)
}

#[test]
fn existing_duplicates_are_audited_left_alone_and_block_new_ones() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let (orig, dup) = plant_duplicate(&db);
    let conn = Connection::open(&db).unwrap();
    let row: (String, String, i64, String, String) = conn
        .query_row(
            "SELECT class, element_names::VARCHAR, n, entity_ids::VARCHAR, \
                    entity_names::VARCHAR FROM audit_entity_tuple_dup",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();
    drop(conn);
    assert_eq!(
        row,
        (
            "unit__node".to_string(),
            "[u1, n1]".to_string(),
            2,
            format!("[{orig}, {dup}]"),
            "[u1__n1, dup]".to_string()
        )
    );
    // Other ops on them work.
    let r = ok(
        &db,
        vec![
            SC::CreateAlternative {
                name: "Base".into(),
                description: None,
            },
            SC::SetEntityActivation {
                class: "unit__node".into(),
                entity: "dup".into(),
                alternative: "Base".into(),
                active: Some(false),
            },
            nd("unit__node", Some("dup"), &["u1", "n1"]),
        ],
    );
    assert_eq!(r.structural[2].effect, SE::NoOp);
    // A new create of the tuple lists every holder.
    let m = api_err(cs(&db, vec![nd("unit__node", Some("third"), &["u1", "n1"])]));
    assert!(m.contains("already exist as \"u1__n1\", \"dup\""), "{m}");
    // `name: None` is a NoOp: one holder has the derived name.
    let r = ok(&db, vec![nd("unit__node", None, &["u1", "n1"])]);
    assert_eq!(r.structural[0].effect, SE::NoOp);
    assert_eq!(r.structural[0].object_id, orig);
}

// ---------------------------------------------------------------------------
// The guard's cost (§5.3)
// ---------------------------------------------------------------------------

/// Tuple-guard statements and tuples loaded for one call of `n` N-D creates
/// over `n` distinct units on a file holding `n` + `extra` tuples.
fn guard_cost(n: usize, extra: usize) -> (u64, u64) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    let mut setup = vec![
        class("unit", &[]),
        class("node", &[]),
        class("unit__node", &["unit", "node"]),
        ent("node", "n1"),
        ent("node", "n2"),
    ];
    for i in 0..n + extra {
        setup.push(ent("unit", &format!("u{i}")));
    }
    for i in 0..n + extra {
        setup.push(nd("unit__node", None, &[&format!("u{i}"), "n1"]));
    }
    ok(&db, setup);
    let creates: Vec<SC> = (0..n)
        .map(|i| nd("unit__node", None, &[&format!("u{i}"), "n2"]))
        .collect();
    let (s0, l0) = (test_hooks::statements(), test_hooks::tuples_loaded());
    let r = ok(&db, creates);
    assert!(r.structural.iter().all(|o| o.effect == SE::Created));
    (
        test_hooks::statements() - s0,
        test_hooks::tuples_loaded() - l0,
    )
}

#[test]
fn guard_statements_do_not_depend_on_the_number_of_creates() {
    let (one, one_loaded) = guard_cost(1, 300);
    let (many, many_loaded) = guard_cost(2_000, 0);
    // One create: one fenced name lookup and one load. Many: one set
    // lookup and one load.
    assert_eq!(one, 2);
    assert_eq!(many, 2);
    // The load is scoped to the call's position-1 elements: one sibling
    // tuple per unit, not the class.
    assert_eq!(one_loaded, 1);
    assert_eq!(many_loaded, 2_000);
}

// ---------------------------------------------------------------------------
// Templates: validation and export (§6.3)
// ---------------------------------------------------------------------------

const DUP_SEEDS: &str = r#"
template: {name: t, version: "1"}
classes:
  - name: unit
  - name: node
  - name: unit__node
    dimensions: [unit, node]
seed:
  entities:
    - {class: unit, name: u1}
    - {class: node, name: n1}
    - {class: unit__node, name: a, elements: [u1, n1]}
    - {class: unit__node, name: b, elements: [U1, n1]}
"#;

#[test]
fn template_seeds_with_one_tuple_fail_validation() {
    let t = parse_str(DUP_SEEDS).unwrap();
    let m = match validate(&t) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("duplicate seed tuples validated"),
    };
    assert!(
        m.contains("seeds \"a\" and \"b\" have the same elements [U1, n1]"),
        "{m}"
    );
}

#[test]
fn export_of_a_file_with_duplicates_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    export(&db).unwrap();
    plant_duplicate(&db);
    let m = api_err(export(&db));
    assert!(
        m.contains("unit__node: elements [u1, n1] held by \"dup\", \"u1__n1\"")
            && m.contains("audit_entity_tuple_dup"),
        "{m}"
    );
}
