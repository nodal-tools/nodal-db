//! The removed-locally blocker (entity_descriptions_purge_PLAN §3.8, V-OD1,
//! V-S1) and the content-hash clear (V-B2): a changed or uncertified
//! template apply refuses to silently re-create a declared class, parameter
//! or value list that a local commit (an object delete, `drop_value_list`,
//! a purge) closed, until `recreate_removed`; closes made by template
//! commits (migration steps, `run:` programs, applies) never block. Every
//! template commit writes the reserved `template_applied_commit` key.

use std::path::{Path, PathBuf};

use duckdb::Connection;

use nodal_db_core::api::delete_objects;
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::template::{apply, parse_str, ApplyOptions, ApplyOutcome, MigrationRegistry};
use nodal_db_core::types::{
    ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, WriteOptions,
};
use nodal_db_core::verbs::drop_value_list;

/// Version `v` of template `t`: list L, classes node (parameter demand) and
/// unit, plus `extra` YAML appended (migrations).
fn tpl(v: &str, extra: &str) -> String {
    format!(
        "template: {{name: t, version: \"{v}\"}}\n\
         value_lists:\n  - name: L\n    members: [{{value: a}}, {{value: b}}]\n\
         classes:\n  - name: node\n    parameters:\n      - name: demand\n        types: [{{kind: float}}]\n  - name: unit\n{extra}"
    )
}

fn apply_str(db: &Path, yaml: &str, opts: ApplyOptions) -> Result<ApplyOutcome, DbError> {
    apply(
        db,
        &parse_str(yaml).unwrap(),
        &MigrationRegistry::new(),
        "t",
        "",
        &opts,
    )
}

fn recreate() -> ApplyOptions {
    ApplyOptions {
        recreate_removed: true,
        ..Default::default()
    }
}

fn loaded(dir: &tempfile::TempDir) -> PathBuf {
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    apply_str(&db, &tpl("1", ""), ApplyOptions::default()).unwrap();
    db
}

fn q(db: &Path, sql: &str) -> i64 {
    Connection::open(db)
        .unwrap()
        .query_row(sql, [], |r| r.get(0))
        .unwrap()
}

fn has_hash(db: &Path) -> bool {
    q(
        db,
        "SELECT count(*) FROM metadata WHERE cur_name = 'template_content_hash'",
    ) == 1
}

fn delete(db: &Path, kind: ObjectKind, class: Option<&str>, name: &str) {
    delete_objects(
        db,
        vec![(
            ObjectDeleteKey {
                kind,
                class: class.map(Into::into),
                target: ObjectRef::Name(name.into()),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        "t",
        "local delete",
        WriteOptions::default(),
    )
    .unwrap();
}

#[test]
fn every_template_commit_is_stamped_applied() {
    let dir = tempfile::tempdir().unwrap();
    let db = loaded(&dir);
    let head = q(&db, "SELECT max(id) FROM commit");
    assert_eq!(
        q(
            &db,
            "SELECT CAST(value AS BIGINT) FROM metadata \
             WHERE cur_name = 'template_applied_commit'"
        ),
        head
    );
}

#[test]
fn deleted_declared_class_blocks_the_same_template_until_recreate_removed() {
    let dir = tempfile::tempdir().unwrap();
    let db = loaded(&dir);
    assert!(has_hash(&db));
    let unit_id = q(&db, "SELECT id FROM entity_class WHERE cur_name = 'unit'");
    delete(&db, ObjectKind::Class, None, "unit");
    let removed_at = q(&db, "SELECT max(id) FROM commit");
    // V-B2: the delete cleared the certified hash, so the same template
    // replans instead of short-circuiting...
    assert!(!has_hash(&db));
    // ...and meets the blocker (no live hash row: no time anchor needed).
    let err = apply_str(&db, &tpl("1", ""), ApplyOptions::default())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(&format!(
            "class `unit` was removed locally in commit {removed_at}: re-create it with \
             `recreate_removed`, or drop it from the template"
        )),
        "{err}"
    );
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM entity_class WHERE cur_name = 'unit'"
        ),
        0
    );
    // recreate_removed: an ordinary create with a new id, noted.
    let out = apply_str(&db, &tpl("1", ""), recreate()).unwrap();
    assert!(out.converged);
    assert!(
        out.plan.notes.iter().any(|n| n.contains("re-created")),
        "{:?}",
        out.plan.notes
    );
    let new_id = q(&db, "SELECT id FROM entity_class WHERE cur_name = 'unit'");
    assert_ne!(new_id, unit_id);
    // Converged again: the next apply is a no-op.
    assert!(
        apply_str(&db, &tpl("1", ""), ApplyOptions::default())
            .unwrap()
            .no_op
    );
}

#[test]
fn deleted_declared_parameter_and_dropped_list_block() {
    let dir = tempfile::tempdir().unwrap();
    let db = loaded(&dir);
    delete(&db, ObjectKind::Parameter, Some("node"), "demand");
    drop_value_list(&db, "L", "t").unwrap();
    assert!(!has_hash(&db));
    let err = apply_str(&db, &tpl("1", ""), ApplyOptions::default())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("parameter `node.demand` was removed locally"),
        "{err}"
    );
    assert!(err.contains("value list `L` was removed locally"), "{err}");
    apply_str(&db, &tpl("1", ""), recreate()).unwrap();
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM parameter_definition WHERE cur_name = 'demand'"
        ),
        1
    );
}

#[test]
fn a_list_a_migration_step_dropped_is_redeclared_without_a_blocker() {
    let dir = tempfile::tempdir().unwrap();
    let db = loaded(&dir);
    // v3's chain: v1 -> v2 drops L, v2 -> v3 has no steps; v3 declares L.
    let v3 = tpl(
        "3",
        "migrations:\n  - from: \"1\"\n    to: \"2\"\n    steps:\n      - drop_value_list: {list: L}\n  - from: \"2\"\n    to: \"3\"\n    steps: []\n",
    );
    let out = apply_str(&db, &v3, ApplyOptions::default()).unwrap();
    assert!(out.converged);
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM parameter_value_list WHERE cur_name = 'l'"
        ),
        1
    );
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM parameter_value_list WHERE lower(name) = 'l' \
             AND valid_to_commit IS NOT NULL"
        ),
        1,
        "closed by the step, re-created by the reconcile"
    );
}

#[test]
fn a_class_a_run_program_closes_never_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let db = loaded(&dir);
    let v2 = tpl(
        "2",
        "migrations:\n  - from: \"1\"\n    to: \"2\"\n    steps:\n      - run: drop_unit\n",
    );
    let mut registry = MigrationRegistry::new();
    registry.register("drop_unit", |db: &Path, user: &str| {
        delete_objects(
            db,
            vec![(
                ObjectDeleteKey {
                    kind: ObjectKind::Class,
                    class: None,
                    target: ObjectRef::Name("unit".into()),
                },
                ObjectDeleteOpts { cascade: true },
            )],
            user,
            "program",
            WriteOptions::default(),
        )
        .map(|_| ())
    });
    let out = apply(
        &db,
        &parse_str(&v2).unwrap(),
        &registry,
        "t",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
    assert!(out.converged);
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM entity_class WHERE cur_name = 'unit'"
        ),
        1
    );
}
