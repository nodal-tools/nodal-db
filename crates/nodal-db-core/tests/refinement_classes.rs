//! Refinement classes (pre-release review 2, OD-1): a class is a refinement
//! class only when the core lists it (`verbs::set_class_refinement`, the
//! core property `refinement_classes`), never because of its name. A
//! refinement link entity (element 1 coarse, element 2 fine) makes an
//! explicit deactivation of the coarse entity propagate to the fine one in
//! every scenario (`realizes_edge`, `entity_activation_resolved`).

use std::path::Path;

use duckdb::Connection;

use nodal_db_core::error::DbError;
use nodal_db_core::verbs::{
    ensure_alternative, ensure_class, ensure_entity, set_class_refinement, set_entity_activation,
    set_scenario_alternatives,
};
use nodal_db_core::{api::head_commit, schema_init::init_schema};

const U: &str = "tester";

/// plant p1 and gen g1, linked by a 2-D class named `realizes_plant` (the
/// old magic prefix), p1 switched off in scenario s1.
fn fixture(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("r.duckdb");
    init_schema(&db).unwrap();
    ensure_class(&db, "plant", &[], U).unwrap();
    ensure_class(&db, "gen", &[], U).unwrap();
    ensure_class(&db, "realizes_plant", &["plant".into(), "gen".into()], U).unwrap();
    ensure_entity(&db, "plant", Some("p1"), &[], U).unwrap();
    ensure_entity(&db, "gen", Some("g1"), &[], U).unwrap();
    ensure_entity(&db, "realizes_plant", None, &["p1".into(), "g1".into()], U).unwrap();
    ensure_alternative(&db, "Base", None, U).unwrap();
    ensure_alternative(&db, "off", None, U).unwrap();
    set_scenario_alternatives(&db, "s1", &["Base".into(), "off".into()], U).unwrap();
    set_entity_activation(&db, "plant", "p1", "off", Some(false), U).unwrap();
    db
}

/// (active, active_source) of `entity` in scenario s1, and the edge count.
fn g1_state(db: &Path) -> ((bool, String), i64) {
    let conn = Connection::open(db).unwrap();
    let state = conn
        .query_row(
            "SELECT r.active, r.active_source FROM entity_activation_resolved r \
             JOIN entity e ON e.id = r.entity_id AND e.valid_to_commit IS NULL \
             JOIN scenario s ON s.id = r.scenario_id AND s.valid_to_commit IS NULL \
             WHERE e.name = 'g1' AND s.name = 's1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let edges = conn
        .query_row("SELECT count(*) FROM realizes_edge", [], |r| r.get(0))
        .unwrap();
    conn.close().unwrap();
    (state, edges)
}

fn stored(db: &Path) -> Option<String> {
    let conn = Connection::open(db).unwrap();
    let v = conn
        .query_row(
            "SELECT value FROM database_property_current WHERE key = 'refinement_classes'",
            [],
            |r| r.get(0),
        )
        .ok();
    conn.close().unwrap();
    v
}

#[test]
fn a_class_named_realizes_is_not_a_refinement_class() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    assert_eq!(
        g1_state(&db),
        ((true, "default".to_string()), 0),
        "the name prefix `realizes` has no meaning"
    );
}

#[test]
fn a_listed_class_propagates_deactivation_and_unlisting_stops_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    set_class_refinement(&db, "REALIZES_PLANT", true, U).unwrap();
    assert_eq!(g1_state(&db), ((false, "propagated".to_string()), 1));
    let conn = Connection::open(&db).unwrap();
    let cid: i64 = conn
        .query_row(
            "SELECT id FROM entity_class_current WHERE name = 'realizes_plant'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    conn.close().unwrap();
    assert_eq!(stored(&db), Some(format!("[{cid}]")));

    // Idempotent: no commit for no change.
    let head = head_commit(&db).unwrap();
    set_class_refinement(&db, "realizes_plant", true, U).unwrap();
    assert_eq!(head_commit(&db).unwrap(), head);

    // Any name works; two classes are listed in ascending id order.
    ensure_class(&db, "refines", &["plant".into(), "gen".into()], U).unwrap();
    set_class_refinement(&db, "refines", true, U).unwrap();
    let v = stored(&db).unwrap();
    assert!(v.starts_with(&format!("[{cid},")), "{v}");

    set_class_refinement(&db, "realizes_plant", false, U).unwrap();
    set_class_refinement(&db, "refines", false, U).unwrap();
    assert_eq!(stored(&db), None, "an empty list clears the key");
    assert_eq!(g1_state(&db), ((true, "default".to_string()), 0));
}

#[test]
fn a_refinement_class_has_two_dimensions() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let err = set_class_refinement(&db, "plant", true, U).unwrap_err();
    assert!(
        matches!(&err, DbError::Api(m) if m.contains("exactly two")),
        "{err}"
    );
    let err = set_class_refinement(&db, "nope", true, U).unwrap_err();
    assert!(
        matches!(&err, DbError::Api(m) if m.contains("not found")),
        "{err}"
    );
}

#[test]
fn the_metadata_channel_cannot_write_the_list() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = Connection::open(&db).unwrap();
    // The DDL classifies the key as core: a user/tool row is refused.
    conn.execute(
        "INSERT INTO database_property (key, value, owner, valid_from_commit) \
         VALUES ('refinement_classes', '[1]', 'user', 1)",
        [],
    )
    .expect_err("refinement_classes is a core key");
    conn.close().unwrap();
}

fn class_id(db: &Path, name: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let id = conn
        .query_row(
            "SELECT id FROM entity_class_current WHERE name = ?",
            [name],
            |r| r.get(0),
        )
        .unwrap();
    conn.close().unwrap();
    id
}

fn delete_class(db: &Path, name: &str) -> i64 {
    nodal_db_core::api::delete_objects(
        db,
        vec![(
            nodal_db_core::types::ObjectDeleteKey {
                kind: nodal_db_core::types::ObjectKind::Class,
                class: None,
                target: nodal_db_core::types::ObjectRef::Name(name.into()),
            },
            nodal_db_core::types::ObjectDeleteOpts { cascade: true },
        )],
        U,
        "drop a class",
        nodal_db_core::types::WriteOptions::default(),
    )
    .unwrap()
    .commit_id
}

/// The mark is keyed by class id: a rename keeps it, and the links keep
/// propagating.
#[test]
fn the_mark_survives_a_class_rename() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    set_class_refinement(&db, "realizes_plant", true, U).unwrap();
    let cid = class_id(&db, "realizes_plant");
    nodal_db_core::api::commit_structural(
        &db,
        vec![nodal_db_core::types::StructuralChange::RenameClass {
            current: "realizes_plant".into(),
            new_name: "plant_generator".into(),
            description: None,
            allow_on_template: false,
        }],
        U,
        "rename",
        nodal_db_core::types::WriteOptions::default(),
    )
    .unwrap();
    assert_eq!(class_id(&db, "plant_generator"), cid);
    assert_eq!(stored(&db), Some(format!("[{cid}]")));
    assert_eq!(g1_state(&db), ((false, "propagated".to_string()), 1));
}

/// Deleting a refinement class removes its id from the list in the same
/// commit; the other listed classes stay, and the last one clears the key.
#[test]
fn deleting_a_refinement_class_unlists_it_in_the_same_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    ensure_class(&db, "refines", &["plant".into(), "gen".into()], U).unwrap();
    set_class_refinement(&db, "realizes_plant", true, U).unwrap();
    set_class_refinement(&db, "refines", true, U).unwrap();
    let keep = class_id(&db, "refines");
    let c = delete_class(&db, "realizes_plant");
    assert_eq!(stored(&db), Some(format!("[{keep}]")));
    let conn = Connection::open(&db).unwrap();
    let from: i64 = conn
        .query_row(
            "SELECT valid_from_commit FROM database_property_current \
             WHERE key = 'refinement_classes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    conn.close().unwrap();
    assert_eq!(from, c, "in the delete's own commit");
    assert_eq!(g1_state(&db), ((true, "default".to_string()), 0));
    delete_class(&db, "refines");
    assert_eq!(stored(&db), None);
}
