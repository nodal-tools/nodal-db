//! `RenameEntity` (entity_rename_PLAN §2-§4, §8): the versioned rename and
//! description revision of an entity, the one revision site, uniqueness
//! (per class and across class sets), the in-call rules (chains, swaps,
//! creates), the old-name refusals (OD5) of every channel, deletes and
//! purges in the same call, the `entity_byname` view, and history.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{commit, ChangeSet, DeleteKey, Segment};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::template::ir::TypeDecl;
use nodal_db_core::types::{
    EntityAddr, LinkSpec, MetadataChange, ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef,
    PairRef, PurgeRequest, PurgeScope, RoleSel, StructuralChange as SC, StructuralEffect as SE,
    StructuralKind, Subject, Target, ValueAddr, WriteOptions, WriteReport,
};

fn q1(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn qs(db: &Path, sql: &str) -> Vec<String> {
    let conn = Connection::open(db).unwrap();
    let mut stmt = conn.prepare(sql).unwrap();
    let rows = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    rows
}

fn set(db: &Path, s: ChangeSet) -> Result<WriteReport, DbError> {
    commit(db, s, "t", "m", WriteOptions::default())
}

fn st(db: &Path, changes: Vec<SC>) -> Result<WriteReport, DbError> {
    set(
        db,
        ChangeSet {
            structural: changes,
            ..Default::default()
        },
    )
}

fn ok(db: &Path, changes: Vec<SC>) -> WriteReport {
    st(db, changes).unwrap()
}

fn api_err(r: Result<WriteReport, DbError>) -> String {
    match r {
        Err(DbError::Api(m)) => m,
        other => panic!("expected DbError::Api, got {other:?}"),
    }
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

fn ren(class: &str, current: &str, new_name: &str) -> SC {
    SC::RenameEntity {
        class: class.into(),
        current: current.into(),
        new_name: new_name.into(),
        description: None,
    }
}

fn desc(class: &str, current: &str, description: &str) -> SC {
    SC::RenameEntity {
        class: class.into(),
        current: current.into(),
        new_name: current.into(),
        description: Some(description.into()),
    }
}

fn float_param(class: &str, name: &str) -> SC {
    SC::CreateParameter {
        class: class.into(),
        name: name.into(),
        types: vec![TypeDecl {
            kind: "float".into(),
            dims: None,
        }],
        value_list: None,
        description: None,
    }
}

fn seg(class: &str, parameter: &str, entity: &str, v: f64) -> Segment {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity])) as ArrayRef,
            Arc::new(Float64Array::from(vec![v])) as ArrayRef,
        ],
    )
    .unwrap();
    Segment {
        class: class.into(),
        parameter: parameter.into(),
        alternative: "Base".into(),
        batches: vec![batch],
        domains: vec![],
    }
}

fn entity_key(class: &str, target: ObjectRef) -> (ObjectDeleteKey, ObjectDeleteOpts) {
    (
        ObjectDeleteKey {
            kind: ObjectKind::Entity,
            class: Some(class.into()),
            target,
        },
        ObjectDeleteOpts { cascade: true },
    )
}

fn by_name(class: &str, name: &str) -> (ObjectDeleteKey, ObjectDeleteOpts) {
    entity_key(class, ObjectRef::Name(name.into()))
}

/// `node` (n1, n2; `demand` 5.0 on n1 in Base), `unit` (u1) and the 2-D
/// `unit__node` holding `u1__n1` over (u1, n1); `u1` active in Base and a
/// member of group n2.
fn fixture(dir: &tempfile::TempDir) -> PathBuf {
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    ok(
        &db,
        vec![
            class("node", &[]),
            class("unit", &[]),
            class("unit__node", &["unit", "node"]),
            float_param("node", "demand"),
            SC::CreateAlternative {
                name: "Base".into(),
                description: None,
            },
            ent("node", "n1"),
            ent("node", "n2"),
            ent("unit", "u1"),
            nd("unit__node", None, &["u1", "n1"]),
            SC::SetEntityActivation {
                class: "unit".into(),
                entity: "u1".into(),
                alternative: "Base".into(),
                active: Some(true),
            },
            SC::SetMembership {
                group: EntityAddr {
                    class: "node".into(),
                    name: "n2".into(),
                },
                member: EntityAddr {
                    class: "unit".into(),
                    name: "u1".into(),
                },
                alternative: None,
                included: Some(true),
            },
        ],
    );
    set(
        &db,
        ChangeSet {
            segments: vec![seg("node", "demand", "n1", 5.0)],
            ..Default::default()
        },
    )
    .unwrap();
    db
}

fn id_of(db: &Path, name: &str) -> i64 {
    q1(
        db,
        &format!("SELECT id FROM entity WHERE cur_name = lower('{name}')"),
    )
}

fn demand(db: &Path, entity: &str) -> Vec<String> {
    qs(
        db,
        &format!(
            "SELECT entity || '=' || CAST(v_float AS VARCHAR) FROM \
             read_values('node', 'demand', ['{entity}'])"
        ),
    )
}

// ---------------------------------------------------------------------------
// The basic contract (tests 1-2)
// ---------------------------------------------------------------------------

#[test]
fn a_rename_keeps_the_id_and_everything_follows() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let n1 = id_of(&db, "n1");
    let u1 = id_of(&db, "u1");
    let before = q1(&db, "SELECT max(id) FROM commit");
    let r = ok(
        &db,
        vec![ren("node", "n1", "bus"), ren("unit", "u1", "gen")],
    );
    assert_eq!(r.structural[0].kind, StructuralKind::Entity);
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(r.structural[0].object_id, n1);
    assert_eq!(id_of(&db, "bus"), n1);
    assert_eq!(
        q1(
            &db,
            &format!("SELECT version FROM entity WHERE cur_id = {n1}")
        ),
        2
    );
    assert_eq!(
        q1(
            &db,
            &format!("SELECT count(*) FROM entity WHERE id = {n1} AND valid_to_commit IS NOT NULL")
        ),
        1
    );
    // As of the commit before, the old names.
    assert_eq!(
        qs(
            &db,
            &format!("SELECT name FROM entity_asof({before}) WHERE id IN ({n1}, {u1}) ORDER BY id")
        ),
        vec!["n1", "u1"]
    );
    // Values, activations, memberships and elements follow the id.
    assert_eq!(demand(&db, "bus"), vec!["bus=5.0"]);
    assert!(demand(&db, "n1").is_empty());
    assert_eq!(
        q1(
            &db,
            &format!(
                "SELECT count(*) FROM entity_alternative WHERE entity_id = {u1} \
                 AND valid_to_commit IS NULL"
            )
        ),
        1
    );
    assert_eq!(
        q1(
            &db,
            &format!(
                "SELECT count(*) FROM membership WHERE member_entity_id = {u1} \
                 AND valid_to_commit IS NULL"
            )
        ),
        1
    );
    // The N-D entity keeps its name over the renamed elements (E1).
    assert_eq!(
        qs(
            &db,
            "SELECT entity || ':' || array_to_string(element_names, ',') || ':' || \
             array_to_string(element_classes, ',') FROM entity_byname WHERE class = 'unit__node'"
        ),
        vec!["u1__n1:gen,bus:unit,node"]
    );
}

#[test]
fn case_and_form_renames_and_description_edits() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    // Case-only: a real rename (R4).
    let r = ok(&db, vec![ren("node", "n1", "N1")]);
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(
        qs(&db, "SELECT name FROM entity WHERE cur_name = 'n1'"),
        vec!["N1"]
    );
    // An NFD spelling of the stored NFC bytes is the same name: NoOp.
    ok(&db, vec![ren("node", "n2", "caf\u{e9}")]);
    let r = ok(&db, vec![ren("node", "caf\u{e9}", "cafe\u{301}")]);
    assert_eq!(r.structural[0].effect, SE::NoOp);
    assert_eq!(r.commit_id, 0);
    // Description only: Updated; equal, None and Some("") on NULL: NoOp.
    let r = ok(&db, vec![desc("node", "N1", "a bus")]);
    assert_eq!(r.structural[0].effect, SE::Updated);
    assert_eq!(
        qs(&db, "SELECT description FROM entity WHERE cur_name = 'n1'"),
        vec!["a bus"]
    );
    let r = ok(
        &db,
        vec![
            desc("node", "N1", "a bus"),
            ren("node", "N1", "N1"),
            desc("unit", "u1", ""),
        ],
    );
    assert!(r.structural.iter().all(|o| o.effect == SE::NoOp));
    assert_eq!(r.commit_id, 0);
    // Some("") clears.
    let r = ok(&db, vec![desc("node", "N1", "")]);
    assert_eq!(r.structural[0].effect, SE::Updated);
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity WHERE cur_name = 'n1' AND description IS NULL"
        ),
        1
    );
}

// ---------------------------------------------------------------------------
// Uniqueness and in-call rules (tests 3-4)
// ---------------------------------------------------------------------------

#[test]
fn uniqueness_per_class_and_across_class_sets() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let m = api_err(st(&db, vec![ren("node", "n1", "N2")]));
    assert!(
        m.contains("already names a different entity of class \"node\""),
        "{m}"
    );
    // The same name in another class is allowed (no class set joins them).
    let r = ok(&db, vec![ren("unit", "u1", "n1")]);
    assert_eq!(r.structural[0].effect, SE::Renamed);
    // A class set over node and unit: a unit may not take a node's name.
    let conn = Connection::open(&db).unwrap();
    let c: i64 = conn
        .query_row(
            "INSERT INTO commit (user, message) VALUES ('sql', 'set') RETURNING id",
            [],
            |r| r.get(0),
        )
        .unwrap();
    conn.execute(
        "INSERT INTO class_set (name, valid_from_commit) VALUES ('site', ?)",
        [c],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO class_set_member (class_set_id, member_class_id, valid_from_commit) \
         SELECT s.id, ec.id, ? FROM class_set s, entity_class ec \
         WHERE s.name = 'site' AND ec.name IN ('node', 'unit')",
        [c],
    )
    .unwrap();
    drop(conn);
    let m = api_err(st(&db, vec![ren("unit", "n1", "n2")]));
    assert!(
        m.contains("ambiguous across class set \"site\", where node:n2 exists"),
        "{m}"
    );
    // A case-only rename collides with nothing new.
    ok(&db, vec![ren("unit", "n1", "N1")]);
}

#[test]
fn create_then_rename_in_one_call_edits_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let r = ok(&db, vec![ent("node", "n3"), ren("node", "n3", "n4")]);
    assert_eq!(r.structural[1].effect, SE::Renamed);
    assert_eq!(
        q1(&db, "SELECT version FROM entity WHERE cur_name = 'n4'"),
        1
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity WHERE lower(name) IN ('n3', 'n4')"
        ),
        1
    );
}

#[test]
fn chains_swaps_and_contradictions() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    // A chain is one version bump.
    let r = ok(&db, vec![ren("node", "n1", "a"), ren("node", "a", "b")]);
    assert_eq!(r.structural[1].effect, SE::Renamed);
    assert_eq!(
        q1(&db, "SELECT version FROM entity WHERE cur_name = 'b'"),
        2
    );
    // A swap and a round trip in one call are refused.
    let m = api_err(st(
        &db,
        vec![
            ren("node", "b", "t"),
            ren("node", "n2", "b"),
            ren("node", "t", "n2"),
        ],
    ));
    assert!(m.contains("was renamed away"), "{m}");
    let m = api_err(st(&db, vec![ren("node", "b", "c"), ren("node", "c", "b")]));
    assert!(m.contains("was renamed away"), "{m}");
    // A swap in three commits (a freed name is free in the next one).
    ok(&db, vec![ren("node", "b", "t")]);
    ok(&db, vec![ren("node", "n2", "b")]);
    ok(&db, vec![ren("node", "t", "n2")]);
    assert_eq!(
        qs(
            &db,
            "SELECT name FROM entity_current WHERE entity_class_id = \
        (SELECT id FROM entity_class WHERE name = 'node') ORDER BY id"
        ),
        vec!["n2", "b"]
    );
    // Two renames of one entity to different names: refused up front.
    let m = api_err(st(&db, vec![ren("node", "b", "x"), ren("node", "B", "y")]));
    assert!(m.contains("contradictory renames"), "{m}");
    // A later revision by a name renamed away earlier in the call.
    let m = api_err(st(&db, vec![ren("node", "b", "x"), desc("node", "b", "d")]));
    assert!(
        m.contains("not found (entity \"b\" was renamed to \"x\" earlier in this call"),
        "{m}"
    );
}

#[test]
fn rename_then_create_of_the_old_name_is_a_new_entity() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let n1 = id_of(&db, "n1");
    let r = ok(&db, vec![ren("node", "n1", "bus"), ent("node", "n1")]);
    assert_eq!(r.structural[1].effect, SE::Created);
    assert_ne!(id_of(&db, "n1"), n1);
    assert_eq!(id_of(&db, "bus"), n1);
}

#[test]
fn class_and_entity_renames_in_one_call() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let n1 = id_of(&db, "n1");
    ok(
        &db,
        vec![
            SC::RenameEntityClass {
                current: "node".into(),
                new_name: "bus".into(),
                description: None,
                allow_on_template: false,
            },
            ren("bus", "n1", "b1"),
        ],
    );
    assert_eq!(id_of(&db, "b1"), n1);
}

// ---------------------------------------------------------------------------
// The old name in every channel (test 5, OD5)
// ---------------------------------------------------------------------------

#[test]
fn the_old_name_is_refused_in_every_channel() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let renamed = || vec![ren("node", "n1", "bus")];
    let check = |what: &str, s: ChangeSet| {
        let m = api_err(set(&db, s));
        assert!(
            m.contains("\"n1\" was renamed to \"bus\" earlier in this call"),
            "{what}: {m}"
        );
    };
    // A 0-D segment label: no auto-create of an empty `n1`.
    check(
        "segment",
        ChangeSet {
            structural: renamed(),
            segments: vec![seg("node", "demand", "n1", 6.0)],
            ..Default::default()
        },
    );
    check(
        "value delete",
        ChangeSet {
            structural: renamed(),
            deletes: vec![DeleteKey {
                class: "node".into(),
                parameter: "demand".into(),
                alternative: "Base".into(),
                entity: "n1".into(),
            }],
            ..Default::default()
        },
    );
    // An object delete by the old name: the name-level guard (M2) refuses
    // it before the transaction.
    let m = api_err(set(
        &db,
        ChangeSet {
            structural: renamed(),
            object_deletes: vec![by_name("node", "n1")],
            ..Default::default()
        },
    ));
    assert!(
        m.contains("both structurally referenced and object-deleted"),
        "{m}"
    );
    check(
        "object delete by elements",
        ChangeSet {
            structural: renamed(),
            object_deletes: vec![entity_key(
                "unit__node",
                ObjectRef::Elements(vec!["u1".into(), "n1".into()]),
            )],
            ..Default::default()
        },
    );
    let mut s = renamed();
    s.push(SC::SetEntityActivation {
        class: "node".into(),
        entity: "n1".into(),
        alternative: "Base".into(),
        active: Some(false),
    });
    check(
        "activation",
        ChangeSet {
            structural: s,
            ..Default::default()
        },
    );
    let mut s = renamed();
    s.push(SC::SetMembership {
        group: EntityAddr {
            class: "node".into(),
            name: "n1".into(),
        },
        member: EntityAddr {
            class: "unit".into(),
            name: "u1".into(),
        },
        alternative: None,
        included: Some(true),
    });
    check(
        "membership",
        ChangeSet {
            structural: s,
            ..Default::default()
        },
    );
    let mut s = renamed();
    s.push(nd("unit__node", Some("x"), &["u1", "n1"]));
    check(
        "element",
        ChangeSet {
            structural: s,
            ..Default::default()
        },
    );
    check(
        "metadata subject",
        ChangeSet {
            structural: renamed(),
            metadata: vec![
                MetadataChange::CreatePair {
                    name: "k".into(),
                    value: "v".into(),
                },
                MetadataChange::Link {
                    subject: Subject::Entity {
                        class: "node".into(),
                        name: "n1".into(),
                    },
                    link: LinkSpec {
                        target: Target::Pair(PairRef::Pair {
                            name: "k".into(),
                            value: "v".into(),
                        }),
                        role: RoleSel::NoRole,
                    },
                },
            ],
            ..Default::default()
        },
    );
    check(
        "metadata value address",
        ChangeSet {
            structural: renamed(),
            metadata: vec![
                MetadataChange::CreatePair {
                    name: "k".into(),
                    value: "v".into(),
                },
                MetadataChange::LinkValue {
                    value: ValueAddr {
                        class: "node".into(),
                        parameter: "demand".into(),
                        alternative: "Base".into(),
                        entity: "n1".into(),
                    },
                    link: LinkSpec {
                        target: Target::Pair(PairRef::Pair {
                            name: "k".into(),
                            value: "v".into(),
                        }),
                        role: RoleSel::NoRole,
                    },
                },
            ],
            ..Default::default()
        },
    );
    // After a same-call re-create the old name resolves again (allowed).
    let mut s = renamed();
    s.push(ent("node", "n1"));
    set(
        &db,
        ChangeSet {
            structural: s,
            segments: vec![seg("node", "demand", "n1", 6.0)],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(demand(&db, "n1"), vec!["n1=6.0"]);
    assert_eq!(demand(&db, "bus"), vec!["bus=5.0"]);
    // A case-only rename frees no name.
    set(
        &db,
        ChangeSet {
            structural: vec![ren("node", "bus", "BUS")],
            segments: vec![seg("node", "demand", "bus", 7.0)],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(demand(&db, "BUS"), vec!["BUS=7.0"]);
}

// ---------------------------------------------------------------------------
// Object deletes and purges in the same call (tests 6-7)
// ---------------------------------------------------------------------------

#[test]
fn a_renamed_entity_cannot_be_deleted_in_the_same_call() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let refused = |s: ChangeSet| {
        let m = api_err(set(&db, s));
        assert!(
            m.contains("creates or revises") || m.contains("structurally referenced"),
            "{m}"
        );
    };
    // By the new name, the class, an element cascading to it.
    refused(ChangeSet {
        structural: vec![ren("node", "n1", "bus")],
        object_deletes: vec![by_name("node", "bus")],
        ..Default::default()
    });
    refused(ChangeSet {
        structural: vec![ren("unit__node", "u1__n1", "link")],
        object_deletes: vec![by_name("node", "n1")],
        ..Default::default()
    });
    refused(ChangeSet {
        structural: vec![ren("node", "n1", "bus")],
        object_deletes: vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Class,
                class: None,
                target: ObjectRef::Name("node".into()),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        ..Default::default()
    });
    // A container of a renamed element and another entity stay deletable.
    let r = set(
        &db,
        ChangeSet {
            structural: vec![ren("node", "n1", "bus")],
            object_deletes: vec![by_name("unit__node", "u1__n1"), by_name("node", "n2")],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(r.object_deletes.len(), 2);
    assert_eq!(id_of(&db, "bus"), r.structural[0].object_id);
}

#[test]
fn a_renamed_entity_survives_a_purge_of_its_class() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let r = set(
        &db,
        ChangeSet {
            structural: vec![ren("node", "n1", "bus")],
            purges: vec![PurgeRequest {
                allow_outside_scope: true,
                ..PurgeRequest::new(PurgeScope::Entities {
                    class: Some("node".into()),
                })
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(
        qs(
            &db,
            "SELECT e.name FROM entity_current e JOIN entity_class c ON c.id = e.entity_class_id \
             WHERE c.name = 'node'"
        ),
        vec!["bus"]
    );
    // Its unstated values close (true replace).
    assert!(demand(&db, "bus").is_empty());
}

// ---------------------------------------------------------------------------
// Conflict retry (test 8)
// ---------------------------------------------------------------------------

#[test]
fn a_conflict_retry_rebuilds_the_call_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    nodal_db_core::api::test_hooks::inject_conflicts(1);
    let r = set(
        &db,
        ChangeSet {
            structural: vec![
                ren("node", "n1", "bus"),
                nd("unit__node", None, &["u1", "n2"]),
            ],
            segments: vec![seg("node", "demand", "bus", 8.0)],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(r.structural[1].effect, SE::Created);
    assert_eq!(demand(&db, "bus"), vec!["bus=8.0"]);
    assert_eq!(
        q1(&db, "SELECT version FROM entity WHERE cur_name = 'bus'"),
        2
    );
}

// ---------------------------------------------------------------------------
// N-D names and tuples after an element rename (§5.1, §5.2)
// ---------------------------------------------------------------------------

#[test]
fn an_element_rename_keeps_n_d_names_and_tuples() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let link = id_of(&db, "u1__n1");
    // Rename n1 -> bus, then create the tuple (u1, bus) by derivation in
    // the same call: `u1__n1` holds it (by element id), so the create is
    // refused, naming it.
    let m = api_err(st(
        &db,
        vec![
            ren("node", "n1", "bus"),
            nd("unit__node", None, &["u1", "bus"]),
        ],
    ));
    assert!(
        m.contains("elements [u1, bus] already exist as \"u1__n1\""),
        "{m}"
    );
    ok(&db, vec![ren("node", "n1", "bus")]);
    // After a new `n1`, the derived name `u1__n1` is taken by the tuple
    // (u1, bus): an explicit name is asked for.
    ok(&db, vec![ent("node", "n1")]);
    let m = api_err(st(&db, vec![nd("unit__node", None, &["u1", "n1"])]));
    assert!(
        m.contains("the derived name \"u1__n1\" is taken by an entity with elements [u1, bus]"),
        "{m}"
    );
    let r = ok(
        &db,
        vec![nd("unit__node", Some("u1__n1_new"), &["u1", "n1"])],
    );
    assert_eq!(r.structural[0].effect, SE::Created);
    assert_eq!(id_of(&db, "u1__n1"), link);
    // A renamed N-D entity is found by its new name; its tuple stays.
    ok(&db, vec![ren("unit__node", "u1__n1", "link")]);
    assert_eq!(
        qs(
            &db,
            "SELECT entity || ':' || array_to_string(element_names, ',') FROM entity_byname \
             WHERE class = 'unit__node' ORDER BY entity_id"
        ),
        vec!["link:u1,bus", "u1__n1_new:u1,n1"]
    );
    // 0-D rows of the view have empty lists.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity_byname WHERE class = 'node' \
             AND dimension_count = 0 AND len(element_names) = 0 AND len(element_classes) = 0"
        ),
        3
    );
}

// ---------------------------------------------------------------------------
// History through compaction (test 13)
// ---------------------------------------------------------------------------

#[test]
fn compaction_keeps_the_version_and_the_closed_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let n1 = id_of(&db, "n1");
    ok(&db, vec![ren("node", "n1", "bus")]);
    ok(&db, vec![desc("node", "bus", "renamed")]);
    let rows = |db: &Path| {
        qs(
            db,
            &format!(
                "SELECT name || ':' || version || ':' || (valid_to_commit IS NULL) FROM entity \
                 WHERE id = {n1} ORDER BY version"
            ),
        )
    };
    let before = rows(&db);
    assert_eq!(before, vec!["n1:1:false", "bus:2:false", "bus:3:true"]);
    nodal_db_core::compaction::compact(&db).unwrap();
    assert_eq!(rows(&db), before);
    assert_eq!(id_of(&db, "bus"), n1);
}

// ---------------------------------------------------------------------------
// The set path for runs of renames (§3.2, test 11)
// ---------------------------------------------------------------------------

use nodal_db_core::entity_rename_set::{test_hooks as set_hooks, RENAME_SET_MIN};

/// A file with `n` node entities `e0..` (auto-created by one segment).
fn many(dir: &tempfile::TempDir, n: usize) -> PathBuf {
    let db = dir.path().join(format!("m{n}.duckdb"));
    init_schema(&db).unwrap();
    ok(&db, vec![class("node", &[]), float_param("node", "demand")]);
    let names: Vec<String> = (0..n).map(|i| format!("e{i}")).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0; n])) as ArrayRef,
        ],
    )
    .unwrap();
    set(
        &db,
        ChangeSet {
            segments: vec![Segment {
                class: "node".into(),
                parameter: "demand".into(),
                alternative: "Base".into(),
                batches: vec![batch],
                domains: vec![],
            }],
            ..Default::default()
        },
    )
    .unwrap();
    db
}

/// Every entity row, history included, without commit ids' absolute values.
fn entity_rows(db: &Path) -> Vec<String> {
    qs(
        db,
        "SELECT id || '|' || entity_class_id || '|' || name || '|' || \
                coalesce(description, '<null>') || '|' || version || '|' || \
                valid_from_commit || '|' || coalesce(CAST(valid_to_commit AS VARCHAR), '-') \
         FROM entity ORDER BY id, version",
    )
}

/// A mixed run: chains, a case-only rename, description edits (one that
/// returns to the original), a create-then-rename of an entity of this
/// call, and a rename after a per-item rename of the same entity.
fn mixed_call() -> Vec<SC> {
    // A per-item rename (a run of one), then the set run.
    let mut v = vec![
        ent("node", "fresh"),
        ren("node", "e0", "first"),
        ent("node", "sep"),
    ];
    v.push(ren("node", "first", "second"));
    v.push(ren("node", "fresh", "fresh2"));
    v.push(ren("node", "e1", "E1"));
    v.push(desc("node", "e2", "x"));
    v.push(desc("node", "e2", ""));
    v.push(ren("node", "e3", "a"));
    v.push(ren("node", "a", "b"));
    v.push(desc("node", "b", "d"));
    for i in 4..30 {
        v.push(ren("node", &format!("e{i}"), &format!("r{i}")));
    }
    v.push(ent("node", "e4")); // the old name, re-created after the run
    v
}

#[test]
fn the_set_path_and_the_per_item_path_write_the_same_rows() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let a = many(&d1, 40);
    let b = many(&d2, 40);
    let s0 = set_hooks::statements();
    let ra = ok(&a, mixed_call());
    assert!(set_hooks::statements() > s0, "the run took the set path");
    let rb = set_hooks::without_set_path(|| ok(&b, mixed_call()));
    assert_eq!(ra.structural, rb.structural);
    assert_eq!(ra.warnings, rb.warnings);
    assert_eq!(entity_rows(&a), entity_rows(&b));
    // Both NoOp-only and real outcomes are present.
    assert!(ra.structural.iter().any(|o| o.effect == SE::Updated));
    assert!(ra.structural.iter().any(|o| o.effect == SE::Renamed));
}

#[test]
fn the_set_path_reports_the_first_offending_item_as_the_per_item_path() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let a = many(&d1, 40);
    let b = many(&d2, 40);
    let call = |bad: SC| {
        let mut v: Vec<SC> = (0..30)
            .map(|i| ren("node", &format!("e{i}"), &format!("r{i}")))
            .collect();
        v[17] = bad;
        v
    };
    for bad in [
        ren("node", "e17", "E35"),      // taken by another entity
        ren("node", "nope", "x"),       // not found
        ren("node", "e5", "y"),         // renamed earlier in the run
        ren("node", "e17", "e3"),       // a name freed earlier in the run
        ren("ghost_class", "e17", "y"), // unknown class
    ] {
        let ma = api_err(st(&a, call(bad.clone())));
        let mb = set_hooks::without_set_path(|| api_err(st(&b, call(bad.clone()))));
        assert_eq!(ma, mb, "{bad:?}");
    }
    assert_eq!(entity_rows(&a), entity_rows(&b));
}

#[test]
fn set_path_statements_do_not_depend_on_the_number_of_renames() {
    let count = |n: usize| -> u64 {
        let dir = tempfile::tempdir().unwrap();
        let db = many(&dir, n);
        let renames: Vec<SC> = (0..n)
            .map(|i| ren("node", &format!("e{i}"), &format!("n{i}")))
            .collect();
        let s0 = set_hooks::statements();
        let r = ok(&db, renames);
        assert!(r.structural.iter().all(|o| o.effect == SE::Renamed));
        assert_eq!(
            q1(
                &db,
                "SELECT count(*) FROM entity WHERE valid_to_commit IS NULL AND name LIKE 'n%'"
            ),
            n as i64
        );
        assert_eq!(
            demand(&db, &format!("n{}", n - 1)),
            vec![format!("n{}=1.0", n - 1)]
        );
        set_hooks::statements() - s0
    };
    assert_eq!(count(RENAME_SET_MIN), count(10_000));
}

#[test]
fn the_set_path_checks_class_sets_as_the_per_item_path() {
    let setup = |dir: &tempfile::TempDir| {
        let db = many(dir, 30);
        ok(&db, vec![class("unit", &[]), ent("unit", "u7")]);
        let conn = Connection::open(&db).unwrap();
        let c: i64 = conn
            .query_row(
                "INSERT INTO commit (user, message) VALUES ('sql', 'set') RETURNING id",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute(
            "INSERT INTO class_set (name, valid_from_commit) VALUES ('site', ?)",
            [c],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO class_set_member (class_set_id, member_class_id, valid_from_commit) \
             SELECT s.id, ec.id, ? FROM class_set s, entity_class ec \
             WHERE s.name = 'site' AND ec.name IN ('node', 'unit')",
            [c],
        )
        .unwrap();
        db
    };
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let (a, b) = (setup(&d1), setup(&d2));
    let call = || {
        let mut v: Vec<SC> = (0..25)
            .map(|i| ren("node", &format!("e{i}"), &format!("r{i}")))
            .collect();
        v[20] = ren("node", "e20", "U7");
        v
    };
    let ma = api_err(st(&a, call()));
    let mb = set_hooks::without_set_path(|| api_err(st(&b, call())));
    assert_eq!(ma, mb);
    assert!(
        ma.contains("ambiguous across class set \"site\", where unit:u7 exists"),
        "{ma}"
    );
}

// ---------------------------------------------------------------------------
// The swap guard across a class set (review fix 1)
// ---------------------------------------------------------------------------

/// Make `classes` the members of a new class set `name` (direct SQL; the
/// structural channel has no class-set op).
fn add_class_set(db: &Path, name: &str, classes: &[&str]) {
    let conn = Connection::open(db).unwrap();
    let c: i64 = conn
        .query_row(
            "INSERT INTO commit (user, message) VALUES ('sql', 'set') RETURNING id",
            [],
            |r| r.get(0),
        )
        .unwrap();
    conn.execute(
        "INSERT INTO class_set (name, valid_from_commit) VALUES (?, ?)",
        duckdb::params![name, c],
    )
    .unwrap();
    let list = classes
        .iter()
        .map(|c| format!("'{c}'"))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute(
        &format!(
            "INSERT INTO class_set_member (class_set_id, member_class_id, valid_from_commit) \
             SELECT s.id, ec.id, ? FROM class_set s, entity_class ec \
             WHERE s.name = ? AND ec.name IN ({list})"
        ),
        duckdb::params![c, name],
    )
    .unwrap();
}

#[test]
fn a_name_freed_in_another_class_of_a_set_cannot_be_taken_in_the_call() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    add_class_set(&db, "site", &["node", "unit"]);
    // node:n1 -> x2 frees `n1` in the set; unit:u1 -> n1 would make a later
    // set-typed element `n1` bind to the unit.
    let m = api_err(st(
        &db,
        vec![ren("node", "n1", "x2"), ren("unit", "u1", "n1")],
    ));
    assert!(
        m.contains("was renamed away from node (to \"x2\") earlier in this call")
            && m.contains("shares class set \"site\""),
        "{m}"
    );
    // Two commits are fine.
    ok(&db, vec![ren("node", "n1", "x2")]);
    ok(&db, vec![ren("unit", "u1", "n1")]);
    // A class in no set is unaffected: the same swap across node and a
    // class outside the set passes in one call.
    ok(&db, vec![class("other", &[]), ent("other", "o1")]);
    ok(&db, vec![ren("node", "n2", "y"), ren("other", "o1", "n2")]);
}

#[test]
fn the_batch_path_refuses_an_in_set_swap_as_the_per_item_path() {
    let setup = |dir: &tempfile::TempDir| {
        let db = many(dir, 30);
        ok(&db, vec![class("unit", &[]), ent("unit", "u7")]);
        add_class_set(&db, "site", &["node", "unit"]);
        db
    };
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let (a, b) = (setup(&d1), setup(&d2));
    let call = || {
        let mut v: Vec<SC> = (0..25)
            .map(|i| ren("node", &format!("e{i}"), &format!("r{i}")))
            .collect();
        v[21] = ren("unit", "u7", "e3");
        v
    };
    let ma = api_err(st(&a, call()));
    let mb = set_hooks::without_set_path(|| api_err(st(&b, call())));
    assert_eq!(ma, mb);
    assert!(
        ma.contains("was renamed away from node (to \"r3\")"),
        "{ma}"
    );
}

// ---------------------------------------------------------------------------
// Batch vs per item across every channel (review fix 3)
// ---------------------------------------------------------------------------

/// `many(40)` plus `unit` (u0..u4), the 2-D `unit__node` (`u1__e1` over
/// (u1, e1)) and a `spare` class (x0, x1) for the purge.
fn rich(dir: &tempfile::TempDir) -> PathBuf {
    let db = many(dir, 40);
    let mut v = vec![
        class("unit", &[]),
        class("unit__node", &["unit", "node"]),
        class("spare", &[]),
        ent("spare", "x0"),
        ent("spare", "x1"),
    ];
    for i in 0..5 {
        v.push(ent("unit", &format!("u{i}")));
    }
    v.push(nd("unit__node", None, &["u1", "e1"]));
    ok(&db, v);
    db
}

fn pair_link() -> LinkSpec {
    LinkSpec {
        target: Target::Pair(PairRef::Pair {
            name: "src".into(),
            value: "survey".into(),
        }),
        role: RoleSel::NoRole,
    }
}

/// One call over every channel around a 30-item rename run.
fn every_channel() -> ChangeSet {
    let mut s = vec![
        ren("node", "e0", "a0"),
        ren("node", "a0", "b0"),            // chain
        ren("node", "e1", "E1"),            // case-only
        ren("unit", "u0", "uu0"),           // another class, inside the run
        ren("unit__node", "u1__e1", "lnk"), // an N-D entity
        desc("node", "e2", "two"),
    ];
    for i in 3..28 {
        s.push(ren("node", &format!("e{i}"), &format!("r{i}")));
    }
    s.push(nd("unit__node", Some("link_new"), &["uu0", "r5"])); // N-D create, new name
    s.push(ent("node", "e4")); // re-create of an old name
    ChangeSet {
        structural: s,
        segments: vec![seg2("r5", 2.0, "e4", 3.0)],
        deletes: vec![DeleteKey {
            class: "node".into(),
            parameter: "demand".into(),
            alternative: "Base".into(),
            entity: "e38".into(),
        }],
        object_deletes: vec![by_name("node", "e39")],
        purges: vec![PurgeRequest::new(PurgeScope::Entities {
            class: Some("spare".into()),
        })],
        metadata: vec![
            MetadataChange::CreatePair {
                name: "src".into(),
                value: "survey".into(),
            },
            MetadataChange::Link {
                subject: Subject::Entity {
                    class: "node".into(),
                    name: "r6".into(),
                },
                link: pair_link(),
            },
            MetadataChange::LinkValue {
                value: ValueAddr {
                    class: "node".into(),
                    parameter: "demand".into(),
                    alternative: "Base".into(),
                    entity: "r7".into(),
                },
                link: pair_link(),
            },
        ],
    }
}

/// One `node.demand` segment in `Base` with two entities.
fn seg2(e1: &str, v1: f64, e2: &str, v2: f64) -> Segment {
    let mut s = seg("node", "demand", e1, v1);
    let schema = s.batches[0].schema();
    s.batches = vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![e1, e2])) as ArrayRef,
            Arc::new(Float64Array::from(vec![v1, v2])) as ArrayRef,
        ],
    )
    .unwrap()];
    s
}

fn all_rows(db: &Path) -> Vec<String> {
    let mut out = entity_rows(db);
    for sql in [
        "SELECT 'el|' || entity_id || '|' || position || '|' || element_entity_id || '|' || \
         valid_from_commit || '|' || coalesce(CAST(valid_to_commit AS VARCHAR), '-') \
         FROM entity_element ORDER BY ALL",
        "SELECT 'v|' || entity_id || '|' || parameter_definition_id || '|' || alternative_id \
         || '|' || CAST(v_float AS VARCHAR) || '|' || version || '|' || valid_from_commit || \
         '|' || coalesce(CAST(valid_to_commit AS VARCHAR), '-') FROM value_scalar ORDER BY ALL",
        "SELECT 'ol|' || object_type || '|' || object_id || '|' || target_kind || '|' || \
         target_id || '|' || valid_from_commit FROM object_metadata_link ORDER BY ALL",
        "SELECT 'vl|' || entity_id || '|' || target_kind || '|' || target_id || '|' || \
         valid_from_commit FROM value_metadata_link ORDER BY ALL",
    ] {
        out.extend(qs(db, sql));
    }
    out
}

#[test]
fn the_set_path_matches_the_per_item_path_across_every_channel() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let (a, b) = (rich(&d1), rich(&d2));
    let s0 = set_hooks::statements();
    let ra = set(&a, every_channel()).unwrap();
    assert!(set_hooks::statements() > s0, "the run took the set path");
    let rb = set_hooks::without_set_path(|| set(&b, every_channel())).unwrap();
    assert_eq!(ra, rb);
    assert_eq!(all_rows(&a), all_rows(&b));
    // What the call did, on the batch side.
    assert_eq!(demand(&a, "r5"), vec!["r5=2.0"]);
    assert_eq!(demand(&a, "e4"), vec!["e4=3.0"]);
    assert!(demand(&a, "e38").is_empty());
    assert_eq!(id_of(&a, "lnk"), id_of(&b, "lnk"));
    assert_eq!(
        q1(
            &a,
            "SELECT count(*) FROM entity_current WHERE name IN ('x0', 'x1', 'e39')"
        ),
        0
    );
    assert_eq!(q1(&a, "SELECT count(*) FROM object_metadata_link"), 1);
    assert_eq!(q1(&a, "SELECT count(*) FROM value_metadata_link"), 1);

    // The old-name refusals are the same on both paths.
    let d3 = tempfile::tempdir().unwrap();
    let d4 = tempfile::tempdir().unwrap();
    let (c, d) = (rich(&d3), rich(&d4));
    let stale = |s: ChangeSet, what: &str| {
        let mc = api_err(set(&c, s.clone()));
        let md = set_hooks::without_set_path(|| api_err(set(&d, s)));
        assert_eq!(mc, md, "{what}");
        assert!(mc.contains("renamed to"), "{what}: {mc}");
    };
    let base = || {
        let mut s = every_channel();
        s.segments.clear();
        s.deletes.clear();
        s.object_deletes.clear();
        s.metadata.clear();
        s.structural.truncate(31); // the run only
        s
    };
    let mut s = base();
    s.segments = vec![seg("node", "demand", "e10", 9.0)];
    stale(s, "segment");
    let mut s = base();
    s.deletes = vec![DeleteKey {
        class: "node".into(),
        parameter: "demand".into(),
        alternative: "Base".into(),
        entity: "e11".into(),
    }];
    stale(s, "value delete");
    let mut s = base();
    s.object_deletes = vec![entity_key(
        "unit__node",
        ObjectRef::Elements(vec!["u0".into(), "e1".into()]),
    )];
    stale(s, "object delete by elements");
    let mut s = base();
    s.metadata = vec![MetadataChange::Link {
        subject: Subject::Entity {
            class: "node".into(),
            name: "e12".into(),
        },
        link: pair_link(),
    }];
    stale(s, "metadata");
    let mut s = base();
    s.structural.push(SC::SetEntityActivation {
        class: "node".into(),
        entity: "e13".into(),
        alternative: "Base".into(),
        active: Some(false),
    });
    stale(s, "activation");
    assert_eq!(all_rows(&c), all_rows(&d));
}

#[test]
fn the_renamed_to_note_names_the_rename_in_the_elements_class() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    ok(&db, vec![ent("node", "q"), ent("unit", "q")]);
    // `q` was renamed in both classes; the element at the node position
    // names the node's rename.
    let m = api_err(st(
        &db,
        vec![
            ren("unit", "q", "uq"),
            ren("node", "q", "nq"),
            nd("unit__node", None, &["u1", "q"]),
        ],
    ));
    assert!(
        m.contains("entity \"q\" was renamed to \"nq\" earlier in this call")
            && !m.contains("\"uq\""),
        "{m}"
    );
}
