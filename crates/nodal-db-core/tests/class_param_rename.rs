//! Class and parameter descriptions and renames (`CreateEntityClass`'s
//! description, `RenameEntityClass`, `RenameParameter`), the description
//! rule shared by every rename op (a stored description is never `""`), and
//! the empty-description rule of the template IR.

use std::path::{Path, PathBuf};

use duckdb::Connection;

use nodal_db_core::api::commit_structural;
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::template::{apply, parse_str, ApplyOptions, MigrationRegistry};
use nodal_db_core::types::{
    StructuralChange as SC, StructuralEffect as SE, WriteOptions, WriteReport,
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
