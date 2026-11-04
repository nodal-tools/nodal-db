//! Class and parameter descriptions and renames (`CreateEntityClass`'s
//! description, `RenameEntityClass`, `RenameParameter`), the description
//! rule shared by every rename op (a stored description is never `""`), and
//! the empty-description rule of the template IR.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{commit_changes, commit_structural, DeleteKey, Segment};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::template::ir::TypeDecl;
use nodal_db_core::template::{apply, parse_str, ApplyOptions, MigrationRegistry};
use nodal_db_core::types::{
    ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, StructuralChange as SC,
    StructuralEffect as SE, StructuralKind, WriteOptions, WriteReport,
};

fn fresh_db(dir: &tempfile::TempDir) -> PathBuf {
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    db
}

fn cs(db: &Path, changes: Vec<SC>) -> WriteReport {
    commit_structural(db, changes, "t", "m", WriteOptions::default()).unwrap()
}

fn q1(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn qopt(db: &Path, sql: &str) -> Option<String> {
    let conn = Connection::open(db).unwrap();
    conn.query_row(sql, [], |r| r.get::<_, Option<String>>(0))
        .unwrap()
}

fn class_d(name: &str, description: Option<&str>) -> SC {
    SC::CreateEntityClass {
        name: name.into(),
        dims: vec![],
        description: description.map(Into::into),
    }
}

// ---------------------------------------------------------------------------
// CreateEntityClass.description (P1)
// ---------------------------------------------------------------------------

#[test]
fn create_entity_class_stores_its_description_on_create_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    let r = cs(&db, vec![class_d("node", Some("A bus."))]);
    assert_eq!(r.structural[0].effect, SE::Created);
    assert_eq!(
        qopt(
            &db,
            "SELECT description FROM entity_class WHERE cur_name = 'node'"
        ),
        Some("A bus.".into())
    );
    // A re-create with another description is a NoOp and keeps the first.
    let r = cs(&db, vec![class_d("NODE", Some("Something else."))]);
    assert_eq!(r.structural[0].effect, SE::NoOp);
    assert_eq!(r.commit_id, 0);
    assert_eq!(
        qopt(
            &db,
            "SELECT description FROM entity_class WHERE cur_name = 'node'"
        ),
        Some("A bus.".into())
    );
    // Some("") is no description: stored NULL, never "".
    cs(&db, vec![class_d("unit", Some(""))]);
    assert_eq!(
        qopt(
            &db,
            "SELECT description FROM entity_class WHERE cur_name = 'unit'"
        ),
        None
    );
}

#[test]
fn empty_descriptions_are_never_stored_by_creates() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(
        &db,
        vec![
            SC::CreateAlternative {
                name: "a".into(),
                description: Some("".into()),
            },
            SC::CreateScenario {
                name: "s".into(),
                description: Some("".into()),
            },
            class_d("node", None),
            SC::CreateParameter {
                class: "node".into(),
                name: "p".into(),
                types: vec![],
                value_list: None,
                description: Some("".into()),
            },
        ],
    );
    for t in ["alternative", "scenario", "parameter_definition"] {
        assert_eq!(
            q1(
                &db,
                &format!("SELECT count(*) FROM {t} WHERE description IS NOT NULL")
            ),
            0,
            "{t}"
        );
    }
}

#[test]
fn template_empty_description_is_no_description() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    let yaml = |version: &str| {
        format!(
            "template: {{name: t, version: \"{version}\"}}\n\
             classes:\n  - name: node\n    description: \"\"\n    parameters:\n      \
             - {{name: p, description: \"\"}}\n"
        )
    };
    let reg = MigrationRegistry::new();
    let opts = ApplyOptions::default();
    apply(&db, &parse_str(&yaml("1")).unwrap(), &reg, "t", "", &opts).unwrap();
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity_class WHERE description IS NOT NULL"
        ),
        0
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM parameter_definition WHERE description IS NOT NULL"
        ),
        0
    );
    // A changed template (new version) declaring "" again finds nothing to
    // revise: the template's "" and the database's NULL are one value.
    let out = apply(&db, &parse_str(&yaml("2")).unwrap(), &reg, "t", "", &opts).unwrap();
    assert!(
        !out.plan
            .actions
            .iter()
            .any(|a| format!("{a}").starts_with("update")),
        "{:?}",
        out.plan.actions
    );
    assert_eq!(q1(&db, "SELECT max(version) FROM entity_class"), 1);
}

// ---------------------------------------------------------------------------
// Helpers for the rename tests
// ---------------------------------------------------------------------------

fn class(name: &str) -> SC {
    class_d(name, None)
}

fn nd_class(name: &str, dims: &[&str]) -> SC {
    SC::CreateEntityClass {
        name: name.into(),
        dims: dims.iter().map(|d| d.to_string()).collect(),
        description: None,
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

fn entity(class: &str, name: &str) -> SC {
    SC::CreateEntity {
        class: class.into(),
        name: Some(name.into()),
        elements: vec![],
    }
}

fn ren_class(current: &str, new_name: &str) -> SC {
    SC::RenameEntityClass {
        current: current.into(),
        new_name: new_name.into(),
        description: None,
        allow_on_template: false,
    }
}

fn desc_class(current: &str, description: &str) -> SC {
    SC::RenameEntityClass {
        current: current.into(),
        new_name: current.into(),
        description: Some(description.into()),
        allow_on_template: false,
    }
}

fn ren_param(class: &str, current: &str, new_name: &str) -> SC {
    SC::RenameParameter {
        class: class.into(),
        current: current.into(),
        new_name: new_name.into(),
        description: None,
        allow_on_template: false,
    }
}

fn desc_param(class: &str, current: &str, description: &str) -> SC {
    SC::RenameParameter {
        class: class.into(),
        current: current.into(),
        new_name: current.into(),
        description: Some(description.into()),
        allow_on_template: false,
    }
}

fn allowed(mut ch: SC) -> SC {
    match &mut ch {
        SC::RenameEntityClass {
            allow_on_template, ..
        }
        | SC::RenameParameter {
            allow_on_template, ..
        } => *allow_on_template = true,
        _ => unreachable!(),
    }
    ch
}

fn ren_alt(current: &str, new_name: &str) -> SC {
    SC::RenameAlternative {
        current: current.into(),
        new_name: new_name.into(),
        description: None,
    }
}

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

fn seg(class: &str, parameter: &str, alternative: &str, entity: &str, v: f64) -> Segment {
    Segment {
        class: class.into(),
        parameter: parameter.into(),
        alternative: alternative.into(),
        batches: vec![scalar_batch(entity, v)],
        domains: vec![],
    }
}

fn okey(kind: ObjectKind, class: Option<&str>, name: &str) -> (ObjectDeleteKey, ObjectDeleteOpts) {
    (
        ObjectDeleteKey {
            kind,
            class: class.map(Into::into),
            target: ObjectRef::Name(name.into()),
        },
        ObjectDeleteOpts { cascade: true },
    )
}

/// One `commit_changes` call over every channel.
fn call(
    db: &Path,
    segments: Vec<Segment>,
    structural: Vec<SC>,
    deletes: Vec<DeleteKey>,
    object_deletes: Vec<(ObjectDeleteKey, ObjectDeleteOpts)>,
) -> Result<WriteReport, DbError> {
    commit_changes(
        db,
        segments,
        structural,
        deletes,
        object_deletes,
        "t",
        "m",
        WriteOptions::default(),
    )
}

fn api_err(r: Result<WriteReport, DbError>) -> String {
    match r {
        Err(DbError::Api(m)) => m,
        other => panic!("expected DbError::Api, got {other:?}"),
    }
}

fn commits(db: &Path) -> i64 {
    q1(db, "SELECT count(*) FROM commit")
}

fn closed(db: &Path, table: &str) -> i64 {
    q1(
        db,
        &format!("SELECT count(*) FROM {table} WHERE valid_to_commit IS NOT NULL"),
    )
}

/// `node` (with `demand`, value 5.0 on `n1` in `Base`) and `unit`.
fn seeded(dir: &tempfile::TempDir) -> PathBuf {
    let db = fresh_db(dir);
    cs(
        &db,
        vec![class("node"), class("unit"), float_param("node", "demand")],
    );
    call(
        &db,
        vec![seg("node", "demand", "Base", "n1", 5.0)],
        vec![],
        vec![],
        vec![],
    )
    .unwrap();
    db
}

fn class_id(db: &Path, name: &str) -> i64 {
    q1(
        db,
        &format!("SELECT id FROM entity_class WHERE cur_name = lower('{name}')"),
    )
}

fn param_id(db: &Path, name: &str) -> i64 {
    q1(
        db,
        &format!("SELECT id FROM parameter_definition WHERE cur_name = lower('{name}')"),
    )
}

// ---------------------------------------------------------------------------
// RenameEntityClass / RenameParameter: the basic contract (tests 2-11)
// ---------------------------------------------------------------------------

#[test]
fn class_rename_keeps_the_id_and_history() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    let id = class_id(&db, "node");
    let before = q1(&db, "SELECT max(id) FROM commit");
    let r = cs(&db, vec![ren_class("node", "bus")]);
    assert_eq!(r.structural[0].kind, StructuralKind::EntityClass);
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(r.structural[0].object_id, id);
    assert_eq!(class_id(&db, "bus"), id);
    assert_eq!(
        q1(
            &db,
            "SELECT version FROM entity_class WHERE cur_name = 'bus'"
        ),
        2
    );
    assert_eq!(closed(&db, "entity_class"), 1);
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity_class WHERE cur_name = 'node'"
        ),
        0
    );
    // The old name is visible as of the commit before the rename.
    assert_eq!(
        qopt(
            &db,
            &format!("SELECT name FROM entity_class_asof({before}) WHERE id = {id}")
        ),
        Some("node".into())
    );
}

#[test]
fn parameter_rename_keeps_values_and_reads_follow() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    let pid = param_id(&db, "demand");
    let r = cs(&db, vec![ren_param("node", "demand", "load")]);
    assert_eq!(r.structural[0].kind, StructuralKind::Parameter);
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(r.structural[0].object_id, pid);
    assert_eq!(param_id(&db, "load"), pid);
    assert_eq!(
        q1(
            &db,
            "SELECT version FROM parameter_definition WHERE cur_name = 'load'"
        ),
        2
    );
    // The value stays on the same definition id; reads by the new name see
    // it, the old name returns no rows (not an error).
    assert_eq!(
        q1(
            &db,
            &format!(
                "SELECT count(*) FROM value_scalar WHERE parameter_definition_id = {pid} \
                 AND valid_to_commit IS NULL"
            )
        ),
        1
    );
    assert_eq!(
        q1(&db, "SELECT count(*) FROM read_parameter('node', 'load')"),
        1
    );
    assert_eq!(
        q1(&db, "SELECT count(*) FROM read_parameter('node', 'demand')"),
        0
    );
    // A class rename: reads follow it too.
    cs(&db, vec![ren_class("node", "bus")]);
    assert_eq!(
        q1(&db, "SELECT count(*) FROM read_parameter('bus', 'load')"),
        1
    );
    assert_eq!(
        q1(&db, "SELECT count(*) FROM read_parameter('node', 'load')"),
        0
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NOT NULL"
        ),
        0
    );
}

#[test]
fn case_only_rename_is_real_and_not_a_self_collision() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    let r = cs(
        &db,
        vec![
            ren_class("node", "Node"),
            ren_param("Node", "demand", "Demand"),
        ],
    );
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(r.structural[1].effect, SE::Renamed);
    assert_eq!(
        qopt(&db, "SELECT name FROM entity_class WHERE cur_name = 'node'"),
        Some("Node".into())
    );
    assert_eq!(
        q1(
            &db,
            "SELECT version FROM parameter_definition WHERE cur_name = 'demand'"
        ),
        2
    );
}

#[test]
fn nfd_spelling_of_the_stored_nfc_name_is_a_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(
        &db,
        vec![class("caf\u{e9}"), float_param("caf\u{e9}", "w\u{e4}rme")],
    );
    let r = cs(
        &db,
        vec![
            ren_class("caf\u{e9}", "cafe\u{301}"),
            ren_param("caf\u{e9}", "w\u{e4}rme", "wa\u{308}rme"),
        ],
    );
    assert_eq!(r.structural[0].effect, SE::NoOp);
    assert_eq!(r.structural[1].effect, SE::NoOp);
    assert_eq!(r.commit_id, 0);
}

#[test]
fn description_only_revision_is_updated_and_clear_maps_before_compare() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    let c0 = commits(&db);
    // Some("") on an absent description: NoOp, no version bump, no commit.
    let r = cs(
        &db,
        vec![desc_class("node", ""), desc_param("node", "demand", "")],
    );
    assert!(r.structural.iter().all(|o| o.effect == SE::NoOp));
    assert_eq!(r.commit_id, 0);
    assert_eq!(commits(&db), c0);
    // Set: Updated, version bumps, name unchanged.
    let r = cs(
        &db,
        vec![
            desc_class("node", "Buses."),
            desc_param("node", "demand", "MW"),
        ],
    );
    assert!(r.structural.iter().all(|o| o.effect == SE::Updated));
    assert_eq!(
        qopt(
            &db,
            "SELECT name || ':' || description FROM entity_class WHERE cur_name = 'node'"
        ),
        Some("node:Buses.".into())
    );
    assert_eq!(
        q1(
            &db,
            "SELECT version FROM parameter_definition WHERE cur_name = 'demand'"
        ),
        2
    );
    // The same description again, and None: NoOp.
    let r = cs(
        &db,
        vec![
            desc_class("node", "Buses."),
            SC::RenameEntityClass {
                current: "node".into(),
                new_name: "node".into(),
                description: None,
                allow_on_template: false,
            },
        ],
    );
    assert!(r.structural.iter().all(|o| o.effect == SE::NoOp));
    // Some("") on a set description clears it (NULL, never "").
    let r = cs(
        &db,
        vec![desc_class("node", ""), desc_param("node", "demand", "")],
    );
    assert!(r.structural.iter().all(|o| o.effect == SE::Updated));
    assert_eq!(
        q1(&db, "SELECT count(*) FROM entity_class WHERE valid_to_commit IS NULL AND description IS NOT NULL"),
        0
    );
    assert_eq!(
        q1(&db, "SELECT count(*) FROM parameter_definition WHERE valid_to_commit IS NULL AND description IS NOT NULL"),
        0
    );
    // A rename plus a description in one item: one bump, Renamed.
    let r = cs(
        &db,
        vec![SC::RenameEntityClass {
            current: "node".into(),
            new_name: "bus".into(),
            description: Some("Buses.".into()),
            allow_on_template: false,
        }],
    );
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(
        q1(
            &db,
            "SELECT version FROM entity_class WHERE cur_name = 'bus'"
        ),
        4
    );
}

#[test]
fn alternative_and_scenario_description_rule_and_effects() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(
        &db,
        vec![
            SC::CreateAlternative {
                name: "a".into(),
                description: None,
            },
            SC::CreateScenario {
                name: "s".into(),
                description: Some("x".into()),
            },
        ],
    );
    let r = cs(
        &db,
        vec![
            SC::RenameAlternative {
                current: "a".into(),
                new_name: "a".into(),
                description: Some("".into()),
            },
            SC::RenameScenario {
                current: "s".into(),
                new_name: "s".into(),
                description: Some("y".into()),
            },
        ],
    );
    assert_eq!(r.structural[0].effect, SE::NoOp);
    assert_eq!(r.structural[1].effect, SE::Updated);
    let r = cs(
        &db,
        vec![SC::RenameScenario {
            current: "s".into(),
            new_name: "s".into(),
            description: Some("".into()),
        }],
    );
    assert_eq!(r.structural[0].effect, SE::Updated);
    assert_eq!(
        qopt(&db, "SELECT description FROM scenario WHERE cur_name = 's'"),
        None
    );
}

#[test]
fn rename_collisions_are_refused_and_write_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    cs(
        &db,
        vec![float_param("node", "cost"), float_param("unit", "load")],
    );
    let c0 = commits(&db);
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_class("node", "UNIT")],
        vec![],
        vec![],
    ));
    assert!(m.contains("already names a different class"), "{m}");
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_param("node", "demand", "Cost")],
        vec![],
        vec![],
    ));
    assert!(m.contains("already names a different parameter"), "{m}");
    assert_eq!(commits(&db), c0);
    assert_eq!(
        closed(&db, "entity_class") + closed(&db, "parameter_definition"),
        0
    );
    // The same name on ANOTHER class is free: unique per class.
    let r = cs(&db, vec![ren_param("node", "demand", "load")]);
    assert_eq!(r.structural[0].effect, SE::Renamed);
}

#[test]
fn renames_carry_every_other_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(&db, vec![class("node"), float_param("node", "demand")]);
    {
        let conn = Connection::open(&db).unwrap();
        // Direct column edits only to give the carried columns non-default
        // values for the test (the rows are this file's own).
        conn.execute_batch(
            "UPDATE entity_class SET active_by_default = FALSE, required = TRUE, display_order = 7; \
             UPDATE parameter_definition SET required = TRUE, display_order = 3, retain_content = TRUE;",
        )
        .unwrap();
    }
    cs(
        &db,
        vec![ren_class("node", "bus"), ren_param("bus", "demand", "load")],
    );
    let conn = Connection::open(&db).unwrap();
    let ec: (bool, bool, i64) = conn
        .query_row(
            "SELECT active_by_default, required, display_order FROM entity_class \
             WHERE cur_name = 'bus'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(ec, (false, true, 7));
    let pd: (bool, i64, bool) = conn
        .query_row(
            "SELECT required, display_order, retain_content FROM parameter_definition \
             WHERE cur_name = 'load'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(pd, (true, 3, true));
    // The type member stays on the id.
    let pid: i64 = conn
        .query_row(
            "SELECT id FROM parameter_definition WHERE cur_name = 'load'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let members: i64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM parameter_type WHERE parameter_definition_id = {pid} \
                 AND valid_to_commit IS NULL"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(members, 1);
}

#[test]
fn nd_dimensions_and_class_sets_follow_a_class_rename() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(
        &db,
        vec![
            class("unit"),
            class("node"),
            nd_class("unit__node", &["unit", "node"]),
        ],
    );
    cs(&db, vec![ren_class("node", "bus")]);
    // The existing N-D class now reads [unit, bus]: re-declaring it with the
    // old dimension name fails, with the new one is a NoOp.
    let m = api_err(call(
        &db,
        vec![],
        vec![nd_class("unit__node", &["unit", "node"])],
        vec![],
        vec![],
    ));
    assert!(
        m.contains("dimension class \"node\" does not exist") || m.contains("[unit, bus]"),
        "{m}"
    );
    let r = cs(&db, vec![nd_class("unit__node", &["unit", "bus"])]);
    assert_eq!(r.structural[0].effect, SE::NoOp);
    // An N-D entity over the renamed class still resolves its elements.
    let r = cs(
        &db,
        vec![
            entity("unit", "u1"),
            entity("bus", "b1"),
            SC::CreateEntity {
                class: "unit__node".into(),
                name: None,
                elements: vec!["u1".into(), "b1".into()],
            },
        ],
    );
    assert_eq!(r.structural[2].effect, SE::Created);
}

#[test]
fn missing_source_and_bad_new_name_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_class("nope", "x")],
        vec![],
        vec![],
    ));
    assert!(m.contains("does not exist"), "{m}");
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_param("node", "nope", "x")],
        vec![],
        vec![],
    ));
    assert!(m.contains("does not exist"), "{m}");
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_param("nope", "demand", "x")],
        vec![],
        vec![],
    ));
    assert!(m.contains("not found"), "{m}");
    for bad in [" bus", "bus ", "b\nus", ""] {
        let m = api_err(call(
            &db,
            vec![],
            vec![ren_class("node", bad)],
            vec![],
            vec![],
        ));
        assert!(m.contains("must be non-empty"), "{bad:?}: {m}");
        let m = api_err(call(
            &db,
            vec![],
            vec![ren_param("node", "demand", bad)],
            vec![],
            vec![],
        ));
        assert!(m.contains("must be non-empty"), "{bad:?}: {m}");
    }
}

#[test]
fn set_class_active_by_default_after_a_rename() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    cs(&db, vec![ren_class("node", "Bus")]);
    nodal_db_core::verbs::set_class_active_by_default(&db, "bus", false, "t").unwrap();
    assert_eq!(
        qopt(&db, "SELECT name || ':' || version || ':' || active_by_default FROM entity_class WHERE cur_name = 'bus'"),
        Some("Bus:3:false".into())
    );
}

#[test]
fn class_set_members_follow_a_class_rename() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    let yaml = "template: {name: t, version: \"1\"}\n\
                class_sets:\n  - {name: endpoint, members: [node, unit]}\n\
                classes:\n  - name: node\n  - name: unit\n  - name: flow\n    dimensions: [{class_set: endpoint}]\n";
    apply(
        &db,
        &parse_str(yaml).unwrap(),
        &MigrationRegistry::new(),
        "t",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
    cs(&db, vec![allowed(ren_class("node", "bus"))]);
    let conn = Connection::open(&db).unwrap();
    let members: Vec<String> = conn
        .prepare(
            "SELECT ec.name FROM class_set_member m JOIN entity_class ec \
             ON ec.cur_id = m.member_class_id WHERE m.valid_to_commit IS NULL ORDER BY 1",
        )
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(members, vec!["bus".to_string(), "unit".to_string()]);
    // An entity of the renamed member class is a valid element of the
    // class-set-typed dimension.
    let r = cs(
        &db,
        vec![
            entity("bus", "b1"),
            SC::CreateEntity {
                class: "flow".into(),
                name: None,
                elements: vec!["b1".into()],
            },
        ],
    );
    assert_eq!(r.structural[1].effect, SE::Created);
}

// ---------------------------------------------------------------------------
// Ordering within one call (tests 12-15, 20, 21)
// ---------------------------------------------------------------------------

#[test]
fn create_then_rename_in_one_call_is_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    let r = cs(
        &db,
        vec![
            class("a"),
            float_param("a", "p"),
            ren_class("a", "b"),
            ren_param("b", "p", "q"),
            class("c"),
            ren_class("c", "C"),
        ],
    );
    assert_eq!(r.structural[2].effect, SE::Renamed);
    assert_eq!(r.structural[5].effect, SE::Renamed);
    assert_eq!(q1(&db, "SELECT max(version) FROM entity_class"), 1);
    assert_eq!(q1(&db, "SELECT max(version) FROM parameter_definition"), 1);
    assert_eq!(
        closed(&db, "entity_class") + closed(&db, "parameter_definition"),
        0
    );
    assert_eq!(
        qopt(&db, "SELECT name FROM entity_class WHERE cur_name = 'c'"),
        Some("C".into())
    );
}

#[test]
fn rename_then_create_the_old_name_makes_a_new_object() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    let id = class_id(&db, "node");
    let r = cs(&db, vec![ren_class("node", "bus"), class("node")]);
    assert_eq!(r.structural[1].effect, SE::Created);
    assert_ne!(class_id(&db, "node"), id);
    assert_eq!(class_id(&db, "bus"), id);
    assert_eq!(
        q1(&db, "SELECT count(*) FROM read_parameter('bus', 'demand')"),
        1
    );
    // Reverse order (S3): the create is a NoOp on the existing class, and
    // the rename then renames it — the editor emits renames first.
    let r = cs(&db, vec![class("unit"), ren_class("unit", "gen")]);
    assert_eq!(r.structural[0].effect, SE::NoOp);
    assert_eq!(r.structural[1].effect, SE::Renamed);
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity_class WHERE cur_name = 'unit'"
        ),
        0
    );
}

#[test]
fn in_call_swaps_and_round_trips_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    cs(&db, vec![float_param("node", "cost")]);
    let c0 = commits(&db);
    // The direct swap collides with the live other name.
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_class("node", "unit"), ren_class("unit", "node")],
        vec![],
        vec![],
    ));
    assert!(m.contains("already names a different class"), "{m}");
    // Three steps through a temporary name: refused at the second item.
    let m = api_err(call(
        &db,
        vec![],
        vec![
            ren_class("node", "t"),
            ren_class("unit", "node"),
            ren_class("t", "unit"),
        ],
        vec![],
        vec![],
    ));
    assert!(m.contains("renamed away"), "{m}");
    // A round trip.
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_class("node", "t"), ren_class("t", "NODE")],
        vec![],
        vec![],
    ));
    assert!(m.contains("renamed away"), "{m}");
    // Parameters, per class.
    let m = api_err(call(
        &db,
        vec![],
        vec![
            ren_param("node", "demand", "t"),
            ren_param("node", "cost", "demand"),
        ],
        vec![],
        vec![],
    ));
    assert!(m.contains("renamed away"), "{m}");
    // Alternatives.
    cs(
        &db,
        vec![
            SC::CreateAlternative {
                name: "x".into(),
                description: None,
            },
            SC::CreateAlternative {
                name: "y".into(),
                description: None,
            },
        ],
    );
    let c1 = commits(&db);
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_alt("x", "t"), ren_alt("y", "x"), ren_alt("t", "y")],
        vec![],
        vec![],
    ));
    assert!(m.contains("renamed away"), "{m}");
    assert_eq!(commits(&db), c1);
    assert_eq!(c1, c0 + 1);
    // The two-commit swap succeeds, and so does a chain.
    let (n, u) = (class_id(&db, "node"), class_id(&db, "unit"));
    cs(&db, vec![ren_class("node", "t1"), ren_class("unit", "t2")]);
    cs(&db, vec![ren_class("t1", "unit"), ren_class("t2", "node")]);
    assert_eq!((class_id(&db, "unit"), class_id(&db, "node")), (n, u));
    let r = cs(&db, vec![ren_class("node", "b"), ren_class("b", "c")]);
    assert_eq!(r.structural[1].effect, SE::Renamed);
    assert_eq!(
        q1(
            &db,
            &format!("SELECT version FROM entity_class WHERE cur_id = {u}")
        ),
        4
    );
}

#[test]
fn contradictory_renames_are_refused_at_prepare() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_class("node", "b"), ren_class("NODE", "c")],
        vec![],
        vec![],
    ));
    assert!(m.contains("contradictory renames"), "{m}");
    let m = api_err(call(
        &db,
        vec![],
        vec![
            ren_param("node", "demand", "a"),
            ren_param("Node", "DEMAND", "b"),
        ],
        vec![],
        vec![],
    ));
    assert!(m.contains("contradictory renames"), "{m}");
}

#[test]
fn class_and_parameter_renames_address_the_class_as_of_their_position() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    // Class first: the parameter item names the new class.
    cs(
        &db,
        vec![ren_class("node", "bus"), ren_param("bus", "demand", "load")],
    );
    // Parameter first: by the old class name.
    cs(
        &db,
        vec![ren_param("bus", "load", "demand"), ren_class("bus", "grid")],
    );
    assert_eq!(
        q1(&db, "SELECT count(*) FROM read_parameter('grid', 'demand')"),
        1
    );
    // The wrong spelling fails strictly.
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_class("grid", "net"), ren_param("grid", "demand", "x")],
        vec![],
        vec![],
    ));
    assert!(m.contains("class \"grid\" not found"), "{m}");
}

#[test]
fn a_call_of_only_noop_renames_burns_no_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    let c0 = commits(&db);
    let r = cs(
        &db,
        vec![
            ren_class("node", "node"),
            ren_param("node", "demand", "demand"),
        ],
    );
    assert_eq!(r.commit_id, 0);
    assert_eq!(commits(&db), c0);
}

#[test]
fn a_conflict_retry_rebuilds_the_rename_state() {
    use nodal_db_core::api::test_hooks::inject_conflicts;
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    inject_conflicts(1);
    let r = call(
        &db,
        vec![seg("bus", "load", "Base", "n1", 6.0)],
        vec![ren_class("node", "bus"), ren_param("bus", "demand", "load")],
        vec![],
        vec![],
    );
    inject_conflicts(0);
    let r = r.expect("the retry commits");
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(
        q1(
            &db,
            "SELECT version FROM entity_class WHERE cur_name = 'bus'"
        ),
        2
    );
    assert_eq!(closed(&db, "entity_class"), 1);
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM read_parameter('bus', 'load') WHERE v_float = 6.0"
        ),
        1
    );
}

// ---------------------------------------------------------------------------
// Rename plus object delete (tests 16-18, OD6)
// ---------------------------------------------------------------------------

#[test]
fn rename_plus_delete_of_the_same_object_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    cs(&db, vec![class("gone")]);
    let c0 = commits(&db);
    let cases: Vec<(SC, (ObjectDeleteKey, ObjectDeleteOpts))> = vec![
        (
            ren_class("node", "bus"),
            okey(ObjectKind::Class, None, "node"),
        ),
        (
            ren_class("node", "bus"),
            okey(ObjectKind::Class, None, "BUS"),
        ),
        (
            ren_param("node", "demand", "load"),
            okey(ObjectKind::Parameter, Some("node"), "demand"),
        ),
        (
            ren_param("node", "demand", "load"),
            okey(ObjectKind::Parameter, Some("node"), "load"),
        ),
        (
            ren_param("node", "demand", "load"),
            okey(ObjectKind::Class, None, "node"),
        ),
        // A rename to a name a same-call delete frees.
        (
            ren_class("node", "gone"),
            okey(ObjectKind::Class, None, "gone"),
        ),
        (
            desc_class("node", "d"),
            okey(ObjectKind::Class, None, "node"),
        ),
    ];
    for (rename, del) in cases {
        let m = api_err(call(
            &db,
            vec![],
            vec![rename.clone()],
            vec![],
            vec![del.clone()],
        ));
        assert!(m.contains("live orphan"), "{rename:?} + {del:?}: {m}");
    }
    assert_eq!(commits(&db), c0);
}

#[test]
fn rename_plus_delete_reaching_it_by_id_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(
        &db,
        vec![
            class("d"),
            class("x"),
            nd_class("r", &["d", "x"]),
            class("a"),
            float_param("a", "p"),
        ],
    );
    let c0 = commits(&db);
    // Rename the N-D class, delete its dimension class: the cascade reaches
    // the renamed class (the name check cannot see it).
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_class("r", "r2")],
        vec![],
        vec![okey(ObjectKind::Class, None, "d")],
    ));
    assert!(m.contains("creates or revises"), "{m}");
    // S5: a parameter description revision under `a`, then `a -> b`, then
    // a delete of `b.p`: the prepare layer keys the delete by `b`, the
    // revision by `a`; the id layer refuses it.
    let m = api_err(call(
        &db,
        vec![],
        vec![desc_param("a", "p", "d"), ren_class("a", "b")],
        vec![],
        vec![okey(ObjectKind::Parameter, Some("b"), "p")],
    ));
    assert!(m.contains("creates or revises"), "{m}");
    assert_eq!(commits(&db), c0);
}

#[test]
fn deleting_another_object_under_a_renamed_class_is_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    cs(&db, vec![entity("node", "e"), float_param("node", "cost")]);
    let r = call(
        &db,
        vec![],
        vec![ren_class("node", "bus")],
        vec![],
        vec![
            okey(ObjectKind::Entity, Some("bus"), "e"),
            okey(ObjectKind::Parameter, Some("bus"), "cost"),
        ],
    )
    .unwrap();
    assert_eq!(r.object_deletes.len(), 2);
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM parameter_definition WHERE cur_name = 'cost'"
        ),
        0
    );
}

// ---------------------------------------------------------------------------
// References to a renamed-away name (test 19, OD5 + S4)
// ---------------------------------------------------------------------------

#[test]
fn references_to_a_renamed_away_name_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    cs(&db, vec![entity("node", "e2")]);
    let c0 = commits(&db);
    let dk = |class: &str, parameter: &str, alternative: &str| DeleteKey {
        class: class.into(),
        parameter: parameter.into(),
        alternative: alternative.into(),
        entity: "n1".into(),
    };
    // Segments.
    for (s, what) in [
        (seg("node", "demand", "Base", "n1", 1.0), "class \"node\""),
        (seg("node", "load", "Base", "n1", 1.0), "class \"node\""),
    ] {
        let m = api_err(call(
            &db,
            vec![s],
            vec![ren_class("node", "bus")],
            vec![],
            vec![],
        ));
        assert!(m.contains(what) && m.contains("renamed to \"bus\""), "{m}");
    }
    let m = api_err(call(
        &db,
        vec![seg("node", "demand", "Base", "n1", 1.0)],
        vec![ren_param("node", "demand", "load")],
        vec![],
        vec![],
    ));
    assert!(
        m.contains("parameter \"demand\" was renamed to \"load\""),
        "{m}"
    );
    let m = api_err(call(
        &db,
        vec![seg("node", "demand", "Base", "n1", 1.0)],
        vec![ren_alt("Base", "base2")],
        vec![],
        vec![],
    ));
    assert!(m.contains("alternative \"Base\""), "{m}");
    // Value deletes.
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_class("node", "bus")],
        vec![dk("node", "demand", "Base")],
        vec![],
    ));
    assert!(m.contains("delete key 0"), "{m}");
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_param("node", "demand", "load")],
        vec![dk("node", "demand", "Base")],
        vec![],
    ));
    assert!(m.contains("delete key 0"), "{m}");
    // Object deletes (an entity under the old class name, an alternative).
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_class("node", "bus")],
        vec![],
        vec![okey(ObjectKind::Entity, Some("node"), "e2")],
    ));
    assert!(m.contains("object-delete key 0"), "{m}");
    let m = api_err(call(
        &db,
        vec![],
        vec![ren_alt("Base", "base2")],
        vec![],
        vec![okey(ObjectKind::Alternative, None, "Base")],
    ));
    assert!(m.contains("live orphan") || m.contains("renamed to"), "{m}");
    assert_eq!(commits(&db), c0);

    // A same-call re-create of the old name: the references address the
    // new object, so they are allowed.
    let r = call(
        &db,
        vec![seg("node", "demand", "Base", "n9", 2.0)],
        vec![
            ren_class("node", "bus"),
            class("node"),
            float_param("node", "demand"),
        ],
        vec![],
        vec![],
    )
    .unwrap();
    assert_eq!(r.structural[1].effect, SE::Created);
    assert_eq!(
        q1(&db, "SELECT count(*) FROM read_parameter('bus', 'demand')"),
        1
    );
    assert_eq!(
        q1(&db, "SELECT count(*) FROM read_parameter('node', 'demand')"),
        1
    );
    // A case-only rename frees no name: the old spelling still resolves.
    call(
        &db,
        vec![seg("bus", "demand", "Base", "n1", 3.0)],
        vec![ren_class("bus", "BUS")],
        vec![],
        vec![],
    )
    .unwrap();
}

#[test]
fn set_scenario_alternatives_does_not_recreate_a_renamed_away_scenario() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(
        &db,
        vec![
            SC::CreateAlternative {
                name: "a".into(),
                description: None,
            },
            SC::CreateScenario {
                name: "s".into(),
                description: None,
            },
        ],
    );
    let m = api_err(call(
        &db,
        vec![],
        vec![
            SC::RenameScenario {
                current: "s".into(),
                new_name: "t".into(),
                description: None,
            },
            SC::SetScenarioAlternatives {
                scenario: "s".into(),
                alternatives: vec!["a".into()],
            },
        ],
        vec![],
        vec![],
    ));
    assert!(m.contains("was renamed to \"t\""), "{m}");
    // By the new name it works.
    let r = cs(
        &db,
        vec![
            SC::RenameScenario {
                current: "s".into(),
                new_name: "t".into(),
                description: None,
            },
            SC::SetScenarioAlternatives {
                scenario: "t".into(),
                alternatives: vec!["a".into()],
            },
        ],
    );
    assert_eq!(r.structural[1].effect, SE::Updated);
}

// ---------------------------------------------------------------------------
// Template-stamped databases (test 22, OD7)
// ---------------------------------------------------------------------------

fn stamp(db: &Path) {
    nodal_db_core::verbs::stamp_template_identity(db, "flex", "7", false, "t").unwrap();
}

#[test]
fn renames_on_a_stamped_db_need_the_flag() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    cs(
        &db,
        vec![float_param("unit", "cap"), class("gen"), class("store")],
    );
    stamp(&db);
    let c0 = commits(&db);
    let err = call(
        &db,
        vec![],
        vec![
            ren_class("node", "bus"),
            ren_param("unit", "cap", "capacity"),
            desc_class("gen", "Generators."),
            ren_class("store", "STORE"),
        ],
        vec![],
        vec![],
    )
    .unwrap_err();
    let DbError::TemplateStampedRename(d) = err else {
        panic!("expected TemplateStampedRename, got {err:?}");
    };
    assert_eq!(d.template_name, "flex");
    assert_eq!(d.template_version.as_deref(), Some("7"));
    // The class rename and the parameter rename; not the description-only
    // revision nor the case-only rename.
    let got: Vec<(usize, StructuralKind, Option<String>, String, String)> = d
        .renames
        .iter()
        .map(|r| {
            (
                r.change_index,
                r.kind,
                r.class.clone(),
                r.current.clone(),
                r.new_name.clone(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (
                0,
                StructuralKind::EntityClass,
                None,
                "node".into(),
                "bus".into()
            ),
            (
                1,
                StructuralKind::Parameter,
                Some("unit".into()),
                "cap".into(),
                "capacity".into()
            ),
        ]
    );
    assert!(d.message.contains("allow_on_template"), "{}", d.message);
    assert_eq!(commits(&db), c0);
    assert_eq!(closed(&db, "entity_class"), 0);

    // Resent with the flag on each item: goes through.
    let r = cs(
        &db,
        vec![
            allowed(ren_class("node", "bus")),
            allowed(ren_param("unit", "cap", "capacity")),
            desc_class("gen", "Generators."),
            ren_class("store", "STORE"),
        ],
    );
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(r.structural[3].effect, SE::Renamed);
    // One flagged, one not: only the unflagged one is listed.
    let err = call(
        &db,
        vec![],
        vec![
            allowed(ren_class("bus", "grid")),
            ren_param("unit", "capacity", "cap"),
        ],
        vec![],
        vec![],
    )
    .unwrap_err();
    let DbError::TemplateStampedRename(d) = err else {
        panic!()
    };
    assert_eq!(d.renames.len(), 1);
    assert_eq!(d.renames[0].change_index, 1);
    // A class created in the same call needs no flag.
    let r = cs(&db, vec![class("tmp"), ren_class("tmp", "fresh")]);
    assert_eq!(r.structural[1].effect, SE::Renamed);
}

#[test]
fn renames_on_an_unstamped_db_need_no_flag() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    let r = cs(&db, vec![ren_class("node", "bus")]);
    assert_eq!(r.structural[0].effect, SE::Renamed);
}
