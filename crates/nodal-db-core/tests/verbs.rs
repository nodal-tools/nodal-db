//! The G8 migration verbs (flextool_port_finale_plan.md §1, D-F4): core
//! acceptance for the eight-verb surface — idempotency (double-run =
//! single-run, no-op burns NO commit), close-out-protocol citizenship
//! (versions bump, history rows close), strict resolution errors, the
//! stamp guard — plus the §1.6 close-out+recreate RETYPE idiom proven
//! through existing verbs (no new surface needed).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{delete_objects, write_parameter};
use nodal_db_core::canon::LabelDomain;
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::template::ir::{DimSpec, TypeDecl};
use nodal_db_core::template::{apply, parse_str, ApplyOptions, MigrationRegistry};
use nodal_db_core::types::{
    ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, WriteOptions,
};
use nodal_db_core::verbs::{
    ensure_alternative, ensure_entity, ensure_entity_class, ensure_parameter_definition,
    set_class_active_by_default, set_entity_activation, set_scenario_alternatives,
    stamp_template_identity,
};

fn fresh_db(dir: &tempfile::TempDir) -> PathBuf {
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    db
}

fn q1(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn qs(db: &Path, sql: &str) -> Option<String> {
    let conn = Connection::open(db).unwrap();
    conn.query_row(sql, [], |r| r.get::<_, String>(0)).ok()
}

fn commits(db: &Path) -> i64 {
    q1(db, "SELECT count(*) FROM commit")
}

fn scalar(kind: &str) -> TypeDecl {
    TypeDecl { kind: kind.into(), dims: None }
}

fn indexed(dims: &[(&str, &str)]) -> TypeDecl {
    TypeDecl {
        kind: "indexed".into(),
        dims: Some(
            dims.iter()
                .map(|(n, k)| DimSpec { name: (*n).into(), kind: (*k).into() })
                .collect(),
        ),
    }
}

// ---------------------------------------------------------------------------
// ensure_alternative (1.1)
// ---------------------------------------------------------------------------

#[test]
fn ensure_alternative_creates_once_with_description() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    let id = ensure_alternative(&db, "floor", Some("the v38 floor"), "tester").unwrap();
    assert!(id > 0);
    assert_eq!(
        qs(&db, "SELECT description FROM alternative WHERE cur_name = 'floor'").as_deref(),
        Some("the v38 floor")
    );
    let after_create = commits(&db);
    // Idempotent: same id, NO new commit, description untouched.
    let id2 = ensure_alternative(&db, "Floor", Some("other text"), "tester").unwrap();
    assert_eq!(id, id2, "case-insensitive resolution");
    assert_eq!(commits(&db), after_create, "no-op burns no commit");
    assert!(ensure_alternative(&db, "  ", None, "tester").is_err());
    assert!(ensure_alternative(&db, "x", None, " ").is_err(), "user required");
}

// ---------------------------------------------------------------------------
// set_scenario_alternatives (1.1) — whole-list declarative replace
// ---------------------------------------------------------------------------

#[test]
fn set_scenario_alternatives_replaces_whole_list_order_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    for a in ["base", "high", "floor"] {
        ensure_alternative(&db, a, None, "tester").unwrap();
    }
    let sid = set_scenario_alternatives(
        &db,
        "s1",
        &["base".into(), "high".into()],
        "tester",
    )
    .unwrap();
    assert!(sid > 0);
    let order = |db: &Path| -> Vec<(String, i64)> {
        let conn = Connection::open(db).unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT a.name, sa.rank FROM scenario_alternative sa \
                 JOIN alternative a ON a.cur_id = sa.alternative_id \
                 WHERE sa.valid_to_commit IS NULL ORDER BY sa.rank",
            )
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    assert_eq!(order(&db), vec![("base".into(), 1), ("high".into(), 2)]);

    // No-op: same order, no commit.
    let n = commits(&db);
    set_scenario_alternatives(&db, "s1", &["base".into(), "high".into()], "tester").unwrap();
    assert_eq!(commits(&db), n);

    // The v38 shape: read → prepend the floor → whole-list replace. Ranks
    // renumber 1..=n (order-only semantics) and versions carry per pair.
    set_scenario_alternatives(
        &db,
        "s1",
        &["floor".into(), "base".into(), "high".into()],
        "tester",
    )
    .unwrap();
    assert_eq!(
        order(&db),
        vec![("floor".into(), 1), ("base".into(), 2), ("high".into(), 3)]
    );
    assert_eq!(
        q1(&db, "SELECT max(version) FROM scenario_alternative WHERE valid_to_commit IS NULL"),
        2,
        "pre-existing (scenario, alternative) pairs revise, not restart"
    );
    // History: the replaced ranking's rows are closed, not erased.
    assert_eq!(
        q1(&db, "SELECT count(*) FROM scenario_alternative WHERE valid_to_commit IS NOT NULL"),
        2
    );

    // Strict resolution + caller-contract checks.
    let err = set_scenario_alternatives(&db, "s1", &["ghost".into()], "tester").unwrap_err();
    assert!(format!("{err}").contains("ghost"));
    assert!(set_scenario_alternatives(
        &db,
        "s1",
        &["base".into(), "BASE".into()],
        "tester"
    )
    .is_err());

    // Empty list clears the ranking (declarative replace).
    set_scenario_alternatives(&db, "s1", &[], "tester").unwrap();
    assert_eq!(order(&db), vec![]);
}

// ---------------------------------------------------------------------------
// set_entity_activation + set_class_active_by_default (1.2)
// ---------------------------------------------------------------------------

#[test]
fn activation_verbs_write_update_close_and_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    ensure_alternative(&db, "base", None, "tester").unwrap();
    ensure_entity_class(&db, "constraint", &[], "tester").unwrap();
    ensure_entity(&db, "constraint", Some("c1"), &[], "tester").unwrap();

    // Strict resolution: entity/class/alternative must exist.
    assert!(set_entity_activation(&db, "constraint", "ghost", "base", Some(true), "tester")
        .is_err());
    assert!(set_entity_activation(&db, "ghost", "c1", "base", Some(true), "tester").is_err());
    assert!(set_entity_activation(&db, "constraint", "c1", "ghost", Some(true), "tester")
        .is_err());

    set_entity_activation(&db, "constraint", "c1", "base", Some(true), "tester").unwrap();
    assert_eq!(q1(&db, "SELECT count(*) FROM entity_alternative WHERE valid_to_commit IS NULL AND active"), 1);
    let n = commits(&db);
    // Equal state: no commit.
    set_entity_activation(&db, "constraint", "c1", "base", Some(true), "tester").unwrap();
    assert_eq!(commits(&db), n);
    // Flip: close-out + successor (version 2).
    set_entity_activation(&db, "constraint", "c1", "base", Some(false), "tester").unwrap();
    assert_eq!(
        q1(&db, "SELECT version FROM entity_alternative WHERE valid_to_commit IS NULL"),
        2
    );
    // None: close the row (v56 Batch F's entity_alternative deletion).
    set_entity_activation(&db, "constraint", "c1", "base", None, "tester").unwrap();
    assert_eq!(q1(&db, "SELECT count(*) FROM entity_alternative WHERE valid_to_commit IS NULL"), 0);
    assert!(q1(&db, "SELECT count(*) FROM entity_alternative WHERE valid_to_commit IS NOT NULL") >= 2);
    let n = commits(&db);
    set_entity_activation(&db, "constraint", "c1", "base", None, "tester").unwrap();
    assert_eq!(commits(&db), n, "closing an absent row is a no-op");
}

#[test]
fn set_class_active_by_default_is_a_versioned_header_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    ensure_entity_class(&db, "reserve_cls", &[], "tester").unwrap();
    assert_eq!(q1(&db, "SELECT CAST(active_by_default AS BIGINT) FROM entity_class WHERE cur_name = 'reserve_cls'"), 1);
    let n = commits(&db);
    // Equal flag: no commit.
    set_class_active_by_default(&db, "reserve_cls", true, "tester").unwrap();
    assert_eq!(commits(&db), n);
    // Flip (the v56 Batch F False→True direction is the same mechanics).
    set_class_active_by_default(&db, "reserve_cls", false, "tester").unwrap();
    assert_eq!(q1(&db, "SELECT CAST(active_by_default AS BIGINT) FROM entity_class WHERE cur_name = 'reserve_cls'"), 0);
    assert_eq!(
        q1(&db, "SELECT version FROM entity_class WHERE cur_name = 'reserve_cls'"),
        2,
        "revise_* family: close-out + successor, same id"
    );
    assert!(set_class_active_by_default(&db, "ghost", true, "tester").is_err());
}

// ---------------------------------------------------------------------------
// ensure_entity_class / ensure_parameter_definition / ensure_entity (1.3)
// ---------------------------------------------------------------------------

#[test]
fn ensure_entity_class_0d_and_nd_idempotent_with_dim_guard() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    // The v58 shape: 0-D flowGroup, then the 3-D carve-out class.
    let fg = ensure_entity_class(&db, "flowGroup", &[], "tester").unwrap();
    ensure_entity_class(&db, "unit", &[], "tester").unwrap();
    ensure_entity_class(&db, "node", &[], "tester").unwrap();
    let nd = ensure_entity_class(
        &db,
        "flowGroup__unit__node",
        &["flowGroup".into(), "unit".into(), "node".into()],
        "tester",
    )
    .unwrap();
    assert_eq!(
        q1(&db, &format!("SELECT count(*) FROM entity_class_dimension WHERE cur_c = {nd}")),
        3
    );
    let n = commits(&db);
    // Idempotent (matching dims): same ids, no commits.
    assert_eq!(ensure_entity_class(&db, "flowGroup", &[], "tester").unwrap(), fg);
    assert_eq!(
        ensure_entity_class(
            &db,
            "flowGroup__unit__node",
            &["flowGroup".into(), "unit".into(), "node".into()],
            "tester"
        )
        .unwrap(),
        nd
    );
    assert_eq!(commits(&db), n);
    // Dim mismatch: the reshape door stays shut.
    let err = ensure_entity_class(
        &db,
        "flowGroup__unit__node",
        &["unit".into(), "node".into()],
        "tester",
    )
    .unwrap_err();
    assert!(format!("{err}").contains("reshape"));
    // Missing dimension class: strict.
    assert!(ensure_entity_class(&db, "x__y", &["ghost".into()], "tester").is_err());
}

#[test]
fn ensure_parameter_definition_types_inference_and_no_widen() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    ensure_entity_class(&db, "solve", &[], "tester").unwrap();

    // Explicitly typed: float + a 1-D labeled map member.
    let pd = ensure_parameter_definition(
        &db,
        "solve",
        "knob",
        &[scalar("float"), indexed(&[("key", "label")])],
        None,
        Some("a knob"),
        "tester",
    )
    .unwrap();
    assert_eq!(
        q1(&db, &format!(
            "SELECT count(*) FROM parameter_type WHERE parameter_definition_id = {pd} \
             AND valid_to_commit IS NULL"
        )),
        2
    );
    // Existing: returned AS-IS — never re-declared, never widened (D-F4).
    let n = commits(&db);
    let pd2 = ensure_parameter_definition(
        &db,
        "solve",
        "knob",
        &[scalar("string")],
        None,
        None,
        "tester",
    )
    .unwrap();
    assert_eq!(pd, pd2);
    assert_eq!(commits(&db), n, "existing definition is a no-op, no widen door");
    assert_eq!(
        q1(&db, &format!(
            "SELECT count(*) FROM parameter_type WHERE parameter_definition_id = {pd} \
             AND valid_to_commit IS NULL"
        )),
        2,
        "declared set unchanged"
    );

    // Value-list binding with §3.5 inference (the v56 Batch F is_enabled
    // shape): the list's uniform member kind becomes the rank-0 member.
    let t = parse_str(
        "template: {name: seedlist, version: '1'}\n\
         value_lists: [{name: yes_no, members: [{value: 'yes'}, {value: 'no'}]}]\n\
         classes: [{name: seedcls}]\n",
    )
    .unwrap();
    apply(&db, &t, &MigrationRegistry::new(), "tester", "", &ApplyOptions::default()).unwrap();
    let pd3 = ensure_parameter_definition(
        &db,
        "solve",
        "is_enabled",
        &[],
        Some("yes_no"),
        None,
        "tester",
    )
    .unwrap();
    assert_eq!(
        qs(&db, &format!(
            "SELECT value_kind FROM parameter_type WHERE parameter_definition_id = {pd3} \
             AND valid_to_commit IS NULL"
        ))
        .as_deref(),
        Some("string")
    );
    assert!(
        q1(&db, &format!(
            "SELECT parameter_value_list_id FROM parameter_definition WHERE cur_id = {pd3}"
        )) > 0
    );

    // Typeless creation (auto-materializes on first write).
    let pd4 =
        ensure_parameter_definition(&db, "solve", "freeform", &[], None, None, "tester").unwrap();
    assert_eq!(
        q1(&db, &format!(
            "SELECT count(*) FROM parameter_type WHERE parameter_definition_id = {pd4}"
        )),
        0
    );

    // Validation: indistinguishable members, bad kinds, missing class/list.
    assert!(ensure_parameter_definition(
        &db, "solve", "dup", &[scalar("float"), scalar("float")], None, None, "tester"
    )
    .is_err());
    assert!(ensure_parameter_definition(
        &db, "solve", "bad", &[scalar("floaty")], None, None, "tester"
    )
    .is_err());
    assert!(
        ensure_parameter_definition(&db, "ghost", "p", &[], None, None, "tester").is_err()
    );
    assert!(ensure_parameter_definition(
        &db, "solve", "p", &[], Some("ghost_list"), None, "tester"
    )
    .is_err());
}

#[test]
fn ensure_entity_0d_nd_default_name_and_tuple_guard() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    ensure_entity_class(&db, "flowGroup", &[], "tester").unwrap();
    ensure_entity_class(&db, "unit", &[], "tester").unwrap();
    ensure_entity_class(&db, "node", &[], "tester").unwrap();
    ensure_entity_class(
        &db,
        "flowGroup__unit__node",
        &["flowGroup".into(), "unit".into(), "node".into()],
        "tester",
    )
    .unwrap();
    let g = ensure_entity(&db, "flowGroup", Some("g1"), &[], "tester").unwrap();
    ensure_entity(&db, "unit", Some("u1"), &[], "tester").unwrap();
    ensure_entity(&db, "node", Some("n1"), &[], "tester").unwrap();
    // N-D with the derived name (elements joined with __).
    let e = ensure_entity(
        &db,
        "flowGroup__unit__node",
        None,
        &["g1".into(), "u1".into(), "n1".into()],
        "tester",
    )
    .unwrap();
    assert_eq!(
        qs(&db, &format!("SELECT name FROM entity WHERE cur_id = {e}")).as_deref(),
        Some("g1__u1__n1")
    );
    assert_eq!(
        q1(&db, &format!("SELECT count(*) FROM entity_element WHERE cur_e = {e}")),
        3
    );
    let n = commits(&db);
    // Idempotent, including element verification.
    assert_eq!(
        ensure_entity(
            &db,
            "flowGroup__unit__node",
            Some("g1__u1__n1"),
            &["g1".into(), "u1".into(), "n1".into()],
            "tester"
        )
        .unwrap(),
        e
    );
    assert_eq!(ensure_entity(&db, "flowGroup", Some("g1"), &[], "tester").unwrap(), g);
    assert_eq!(commits(&db), n);
    // Same name over a DIFFERENT tuple: loud, never a silent alias.
    ensure_entity(&db, "node", Some("n2"), &[], "tester").unwrap();
    assert!(ensure_entity(
        &db,
        "flowGroup__unit__node",
        Some("g1__u1__n1"),
        &["g1".into(), "u1".into(), "n2".into()],
        "tester"
    )
    .is_err());
    // Arity and missing-element guards.
    assert!(ensure_entity(&db, "flowGroup__unit__node", None, &["g1".into()], "tester")
        .is_err());
    assert!(ensure_entity(
        &db,
        "flowGroup__unit__node",
        None,
        &["g1".into(), "u1".into(), "ghost".into()],
        "tester"
    )
    .is_err());
    // 0-D: no name, no elements → error; elements on a 0-D class → error.
    assert!(ensure_entity(&db, "flowGroup", None, &[], "tester").is_err());
    assert!(ensure_entity(&db, "flowGroup", Some("g2"), &["x".into()], "tester").is_err());
}

// ---------------------------------------------------------------------------
// stamp_template_identity (1.7)
// ---------------------------------------------------------------------------

#[test]
fn stamp_template_identity_guards_and_restamps() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    let meta = |k: &str| qs(&db, &format!("SELECT value FROM metadata WHERE cur_name = '{k}'"));

    // Fresh stamp (the importer's non-optional final step).
    stamp_template_identity(&db, "flextool", "40", false, "tester").unwrap();
    assert_eq!(meta("template_name").as_deref(), Some("flextool"));
    assert_eq!(meta("template_version").as_deref(), Some("40"));

    // Identical restamp: idempotent, no commit (name folds).
    let n = commits(&db);
    stamp_template_identity(&db, "FlexTool", "40", false, "tester").unwrap();
    assert_eq!(commits(&db), n);

    // A DIFFERENT stamp refuses without the explicit opt-in (mirrors
    // allow_retarget's posture) — version and name alike.
    assert!(stamp_template_identity(&db, "flextool", "41", false, "tester").is_err());
    assert!(stamp_template_identity(&db, "ines", "40", false, "tester").is_err());
    assert_eq!(meta("template_version").as_deref(), Some("40"), "refusal touched nothing");

    // Opt-in restamp works and closes any stale certification.
    stamp_template_identity(&db, "flextool", "41", true, "tester").unwrap();
    assert_eq!(meta("template_version").as_deref(), Some("41"));

    assert!(stamp_template_identity(&db, "", "1", true, "tester").is_err());
    assert!(stamp_template_identity(&db, "t", " ", true, "tester").is_err());
}

/// The stamp is what arms the chain: an apply on a stamped DB SELECTS the
/// pending spans (an unstamped DB would silently reconcile to head — the M4
/// silent-skip this verb exists to close).
#[test]
fn stamp_then_apply_enters_the_chain() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    let head = parse_str(
        "template: {name: t, version: '3'}\nclasses: [{name: c}]\n\
         migrations:\n - {from: '1', to: '2', steps: []}\n - {from: '2', to: '3', steps: []}\n",
    )
    .unwrap();
    // Unstamped: plain reconcile to head, zero spans.
    let out = apply(&db, &head, &MigrationRegistry::new(), "tester", "",
        &ApplyOptions::default()).unwrap();
    assert!(out.plan.pending_migrations.is_empty());

    // Stamped mid-version: the chain walks.
    let dir2 = tempfile::tempdir().unwrap();
    let db2 = fresh_db(&dir2);
    stamp_template_identity(&db2, "t", "1", false, "tester").unwrap();
    let out = apply(&db2, &head, &MigrationRegistry::new(), "tester", "",
        &ApplyOptions::default()).unwrap();
    assert_eq!(out.commits.len(), 3, "two span stamps + reconcile");
    assert_eq!(
        qs(&db2, "SELECT value FROM metadata WHERE cur_name = 'template_version'").as_deref(),
        Some("3")
    );
}

// ---------------------------------------------------------------------------
// The §1.6 close-out + recreate RETYPE idiom (the sanctioned mid-chain
// retype; v56 C.1's shape) — through existing verbs only.
// ---------------------------------------------------------------------------

fn array_batch(entity: &str, pairs: &[(i64, f64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("idx", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; pairs.len()])) as ArrayRef,
            Arc::new(Int64Array::from(pairs.iter().map(|p| p.0).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(Float64Array::from(pairs.iter().map(|p| p.1).collect::<Vec<_>>()))
                as ArrayRef,
        ],
    )
    .unwrap()
}

fn map_batch(entity: &str, pairs: &[(&str, f64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; pairs.len()])) as ArrayRef,
            Arc::new(StringArray::from(pairs.iter().map(|p| p.0).collect::<Vec<_>>()))
                as ArrayRef,
            Arc::new(Float64Array::from(pairs.iter().map(|p| p.1).collect::<Vec<_>>()))
                as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn retype_idiom_close_out_recreate_through_existing_verbs() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    ensure_entity_class(&db, "solve", &[], "tester").unwrap();
    ensure_entity(&db, "solve", Some("s1"), &[], "tester").unwrap();

    // The imported state: solver_arguments declared ARRAY-shaped (integer
    // axis), holding values.
    let old_pd = ensure_parameter_definition(
        &db,
        "solve",
        "solver_arguments",
        &[indexed(&[("idx", "integer")])],
        None,
        None,
        "tester",
    )
    .unwrap();
    write_parameter(
        &db,
        "solve",
        "solver_arguments",
        "base",
        array_batch("s1", &[(1, 2.0), (2, 7.0)]),
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap();

    // A map-shaped batch is REJECTED against the declared set — the write
    // path never widens (the wall that makes the idiom necessary, B3).
    let err = write_parameter(
        &db,
        "solve",
        "solver_arguments",
        "base",
        map_batch("s1", &[("presolve", 1.0)]),
        &[LabelDomain { position: 1, labels: vec!["presolve".into()] }],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap_err();
    assert!(format!("{err}").contains("never widens"));

    // THE IDIOM: (1) read the old values (a program uses query()) — elided
    // here; (2) close out the definition, values cascade, versioned;
    let report = delete_objects(
        &db,
        vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Parameter,
                class: Some("solve".into()),
                target: ObjectRef::Name("solver_arguments".into()),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        "tester",
        "v56 C.1 retype: close out the array-shaped definition",
        WriteOptions::default(),
    )
    .unwrap();
    assert_eq!(report.object_deletes.len(), 1);
    // (3) recreate with the 1d_map declaration;
    let new_pd = ensure_parameter_definition(
        &db,
        "solve",
        "solver_arguments",
        &[indexed(&[("key", "label")])],
        None,
        None,
        "tester",
    )
    .unwrap();
    assert_ne!(old_pd, new_pd, "row identity changes — the idiom's accepted cost");
    // (4) write the converted values.
    write_parameter(
        &db,
        "solve",
        "solver_arguments",
        "base",
        map_batch("s1", &[("presolve", 1.0), ("threads", 4.0)]),
        &[LabelDomain { position: 1, labels: vec!["presolve".into(), "threads".into()] }],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap();

    // End state: one live definition, map-typed, with the converted values;
    // the old array rows are closed-out history, not current data.
    assert_eq!(
        q1(&db, "SELECT count(*) FROM parameter_definition WHERE cur_name = 'solver_arguments'"),
        1
    );
    assert_eq!(
        qs(
            &db,
            &format!(
                "SELECT d.kind FROM parameter_type_dimension d \
                 JOIN parameter_type pt ON pt.id = d.parameter_type_id \
                 WHERE pt.parameter_definition_id = {new_pd} AND d.valid_to_commit IS NULL"
            )
        )
        .as_deref(),
        Some("label")
    );
}
