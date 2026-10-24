//! Reconcile v1 mechanics (template_ir_plan §4, §8.1.8 shape): the additive
//! diff (Layer 1), every Layer-2 hint incl. `rename_list_member` with its
//! value rewrite, the `run:` escape-hatch runner (Layer 3, in sequence,
//! through the write API), narrowing flags, the reshape blocker, and
//! plan-then-apply agreement.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::write_parameter;
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::template::{apply, parse_str, plan, ApplyOptions, MigrationRegistry, Template};
use nodal_db_core::types::WriteOptions;

const SAMPLE: &str = include_str!("fixtures/template_sample.yaml");

fn fresh_loaded_db(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    let t = parse_str(SAMPLE).unwrap();
    apply(
        &db,
        &t,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
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

fn string_batch(entity: &str, v: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity])) as ArrayRef,
            Arc::new(StringArray::from(vec![v])) as ArrayRef,
        ],
    )
    .unwrap()
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

/// The v4 template: renamed per the migration hints (efficiency->eta,
/// constant_efficiency->const_eff), one added class, one added parameter,
/// one widened type set, one changed default, one escape-hatch step.
fn v4_template() -> Template {
    let mut t = parse_str(SAMPLE).unwrap();
    let v4_extra = r#"
template: {name: sample, version: "4"}
classes:
  - name: storage
    parameters:
      - name: capacity
        types: [{kind: float}]
migrations:
  - from: "3"
    to: "4"
    steps:
      - rename_parameter: {class: unit, from: efficiency, to: eta}
      - rename_list_member: {list: method, from: constant_efficiency, to: const_eff,
                             rewrite_values: true}
      - run: mark_step
"#;
    let extra = parse_str(v4_extra).unwrap();
    t.template.version = "4".into();
    t.migrations = extra.migrations;
    t.classes.extend(extra.classes);
    // Post-migration names in the v4 vocabulary:
    {
        let unit = t.classes.iter_mut().find(|c| c.name == "unit").unwrap();
        let eff = unit
            .parameters
            .iter_mut()
            .find(|p| p.name == "efficiency")
            .unwrap();
        eff.name = "eta".into();
        // …plus a brand-new parameter (the additive majority, 8.1.8a):
        let newp = parse_str(
            "template: {name: x, version: \"1\"}
classes:
  - name: c
    parameters:
      - name: startup_cost
        types: [{kind: float}]
        default: {kind: float, value: 0.0}
",
        )
        .unwrap();
        unit.parameters.push(newp.classes[0].parameters[0].clone());
        let cm = unit
            .parameters
            .iter_mut()
            .find(|p| p.name == "conversion_method")
            .unwrap();
        cm.default = nodal_db_core::template::ir::DefaultSpec::Value(
            nodal_db_core::template::ir::DefaultValue {
                kind: "string".into(),
                value: nodal_db_core::template::ir::Lit::Str("const_eff".into()),
            },
        );
    }
    {
        let list = t
            .value_lists
            .iter_mut()
            .find(|l| l.name == "method")
            .unwrap();
        let m = list
            .members
            .iter_mut()
            .find(|m| matches!(&m.value, nodal_db_core::template::ir::Lit::Str(s) if s == "constant_efficiency"))
            .unwrap();
        m.value = nodal_db_core::template::ir::Lit::Str("const_eff".into());
        // A new list member inserted MID-LIST: ordinal is semantic, so the
        // reconcile must renumber the existing members to template order
        // (review item 13).
        list.members.insert(
            1,
            nodal_db_core::template::ir::ListMember {
                value: nodal_db_core::template::ir::Lit::Str("piecewise".into()),
                kind: None,
                description: None,
            },
        );
    }
    // Widen node.demand with a period-indexed member (D2 widening).
    {
        let node = t.classes.iter_mut().find(|c| c.name == "node").unwrap();
        let demand = node
            .parameters
            .iter_mut()
            .find(|p| p.name == "demand")
            .unwrap();
        demand
            .types
            .as_mut()
            .unwrap()
            .push(nodal_db_core::template::ir::TypeDecl {
                kind: "indexed".into(),
                dims: Some(vec![nodal_db_core::template::ir::DimSpec {
                    name: "period".into(),
                    kind: "period".into(),
                }]),
            });
        // Changed default (Layer 1 "changed defaults").
        demand.default = nodal_db_core::template::ir::DefaultSpec::Value(
            nodal_db_core::template::ir::DefaultValue {
                kind: "float".into(),
                value: nodal_db_core::template::ir::Lit::Float(1.0),
            },
        );
    }
    // The audit rule keys must track the renames too.
    {
        let rules = t.audit_rules.as_mut().unwrap();
        let r = &mut rules.conditional_requirements[0];
        let clause = r.when.remove("constant_efficiency").unwrap();
        let mut clause = clause;
        clause.require = vec!["eta".into()];
        r.when.insert("const_eff".into(), clause);
    }
    t
}

#[test]
fn migration_chain_hints_rewrite_and_escape_hatch_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);

    // Stored data the rewrite must touch — and one value it must NOT.
    write_parameter(
        &db,
        "unit",
        "conversion_method",
        "alt",
        string_batch("u1", "constant_efficiency"),
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap();
    write_parameter(
        &db,
        "unit",
        "conversion_method",
        "alt",
        string_batch("u2", "two_point_efficiency"),
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap();

    // The escape-hatch program observes execution order and writes through
    // the PUBLIC API (Layer 3's whole point): it writes unit.eta — the
    // post-rename name, proving the declarative rename ran first.
    let order: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = MigrationRegistry::new();
    {
        let order = order.clone();
        let calls = calls.clone();
        registry.register("mark_step", move |db: &Path, user: &str| {
            order.lock().unwrap().push("mark_step");
            calls.fetch_add(1, Ordering::SeqCst);
            write_parameter(
                db,
                "unit",
                "eta",
                "alt",
                scalar_batch("u1", 0.99),
                &[],
                WriteOptions::default(),
                user,
                "escape hatch: set eta",
            )
            .map(|_| ())
        });
    }

    let t4 = v4_template();
    // Plan first (dry run): the chain and the diff are both visible.
    let p = plan(&db, &t4).unwrap();
    assert_eq!(p.pending_migrations.len(), 1);
    assert_eq!(p.pending_migrations[0].steps.len(), 3);
    assert!(!p.actions.is_empty());
    assert!(p.blocked.is_empty(), "unexpected blockers: {:?}", p.blocked);
    let rendered = format!("{p}");
    assert!(
        rendered.contains("rename_parameter unit.efficiency -> eta"),
        "{rendered}"
    );
    assert!(rendered.contains("run mark_step"), "{rendered}");

    let out = apply(&db, &t4, &registry, "tester", "", &ApplyOptions::default()).unwrap();
    assert!(!out.no_op);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "escape hatch ran exactly once"
    );
    assert_eq!(order.lock().unwrap().as_slice(), &["mark_step"]);

    // Hint results: the definition rename kept its id/history; the list
    // member renamed; the stored value + the default were rewritten; the
    // unrelated value untouched.
    assert_eq!(
        qs(
            &db,
            "SELECT cur_name FROM parameter_definition WHERE cur_name = 'eta'"
        )
        .as_deref(),
        Some("eta")
    );
    assert!(qs(
        &db,
        "SELECT cur_name FROM parameter_definition WHERE cur_name = 'efficiency'"
    )
    .is_none());
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM list_value lv \
                 JOIN parameter_value_list l ON l.id = lv.parameter_value_list_id \
                 WHERE l.cur_name = 'method' AND lv.valid_to_commit IS NULL \
                 AND lv.v_str = 'const_eff'"
        ),
        1
    );
    // Ordinals track TEMPLATE order after the mid-list insertion (item 13):
    // const_eff=1, piecewise=2, two_point_efficiency=3.
    for (val, ord) in [
        ("const_eff", 1),
        ("piecewise", 2),
        ("two_point_efficiency", 3),
    ] {
        assert_eq!(
            q1(
                &db,
                &format!(
                    "SELECT lv.ordinal FROM list_value lv \
                 JOIN parameter_value_list l ON l.id = lv.parameter_value_list_id \
                 WHERE l.cur_name = 'method' AND lv.valid_to_commit IS NULL \
                 AND lv.v_str = '{val}'"
                )
            ),
            ord,
            "{val} ordinal"
        );
    }
    assert_eq!(
        qs(
            &db,
            "SELECT v_str FROM value_scalar vs JOIN entity e ON e.id = vs.entity_id \
                 WHERE e.cur_name = 'u1' AND vs.valid_to_commit IS NULL \
                 AND vs.value_kind = 'string' AND vs.v_str LIKE '%eff%'"
        )
        .as_deref(),
        Some("const_eff"),
        "stored value rewritten"
    );
    assert_eq!(
        qs(
            &db,
            "SELECT v_str FROM value_scalar vs JOIN entity e ON e.id = vs.entity_id \
                 WHERE e.cur_name = 'u2' AND vs.valid_to_commit IS NULL \
                 AND vs.value_kind = 'string'"
        )
        .as_deref(),
        Some("two_point_efficiency"),
        "unrelated value untouched"
    );
    // History kept the pre-rename value (close-out, not overwrite).
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NOT NULL \
                 AND v_str = 'constant_efficiency'"
        ),
        1
    );
    // Default rewritten by the SAME hint (the defaults leg).
    assert_eq!(
        qs(
            &db,
            "SELECT dv.v_str FROM definition_value_scalar dv \
                 JOIN parameter_definition pd ON pd.id = dv.parameter_definition_id \
                 WHERE pd.cur_name = 'conversion_method' AND dv.valid_to_commit IS NULL"
        )
        .as_deref(),
        Some("const_eff")
    );
    // Escape-hatch write landed through the API.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM value_scalar vs \
                 JOIN parameter_definition pd ON pd.id = vs.parameter_definition_id \
                 WHERE pd.cur_name = 'eta' AND vs.valid_to_commit IS NULL"
        ),
        1
    );

    // Layer-1 additions: new class, new parameter + default, widened set,
    // new list member, version restamped.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity_class WHERE cur_name = 'storage'"
        ),
        1
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM parameter_definition WHERE cur_name = 'startup_cost'"
        ),
        1
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM parameter_type pt \
                 JOIN parameter_definition pd ON pd.id = pt.parameter_definition_id \
                 WHERE pt.valid_to_commit IS NULL AND pd.cur_name = 'demand'"
        ),
        3,
        "widened to three members"
    );
    assert_eq!(
        qs(
            &db,
            "SELECT value FROM metadata WHERE cur_name = 'template_version'"
        )
        .as_deref(),
        Some("4")
    );
    // Changed default: demand now 1.0 current, 0.0 in history.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM definition_value_scalar dv \
                 JOIN parameter_definition pd ON pd.id = dv.parameter_definition_id \
                 WHERE pd.cur_name = 'demand' AND dv.valid_to_commit IS NULL \
                 AND dv.v_float = 1.0"
        ),
        1
    );

    // Re-applying v4 is now a no-op.
    let again = apply(&db, &t4, &registry, "tester", "", &ApplyOptions::default()).unwrap();
    assert!(again.no_op);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "no-op must not re-run programs"
    );
}

#[test]
fn unregistered_run_program_fails_before_any_step() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);
    let t4 = v4_template();
    let commits = q1(&db, "SELECT max(id) FROM commit");
    let err = apply(
        &db,
        &t4,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("mark_step"), "{err}");
    assert!(err.to_string().contains("not registered"), "{err}");
    // Nothing of the span ran (the registry is checked before step 1).
    assert_eq!(q1(&db, "SELECT max(id) FROM commit"), commits);
}

#[test]
fn narrowing_closes_unused_members_and_keeps_used_ones() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);

    // Write data at demand's datetime member so IT is in use; the float
    // member of max_share stays unused.
    write_parameter(
        &db,
        "node",
        "demand",
        "alt",
        scalar_batch("n1", 2.0),
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap();

    let mut t = parse_str(SAMPLE).unwrap();
    t.template.version = "3n".into();
    {
        // Narrow demand to ONLY the datetime member (drops the float one —
        // but a float value binds it: kept, warned).
        let node = t.classes.iter_mut().find(|c| c.name == "node").unwrap();
        let demand = node
            .parameters
            .iter_mut()
            .find(|p| p.name == "demand")
            .unwrap();
        demand
            .types
            .as_mut()
            .unwrap()
            .retain(|m| m.kind == "indexed");
        demand.default = nodal_db_core::template::ir::DefaultSpec::Unspecified; // no opinion (D12)
                                                                                // Narrow max_share to only float (drops unused indexed member: closed).
        let r4 = t
            .classes
            .iter_mut()
            .find(|c| c.name == "reserve__upDown__unit__node")
            .unwrap();
        let ms = r4
            .parameters
            .iter_mut()
            .find(|p| p.name == "max_share")
            .unwrap();
        ms.types.as_mut().unwrap().retain(|m| m.kind == "float");
    }
    let out = apply(
        &db,
        &t,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
    // The kept member is a SKIP: the apply is PARTIAL, convergence withheld
    // (review item 14).
    assert!(
        out.plan
            .skipped
            .iter()
            .any(|s| s.contains("narrowing node.demand") && s.contains("kept")),
        "expected a kept-member skip, got {:?}",
        out.plan.skipped
    );
    assert!(!out.converged, "a skipped narrow must withhold convergence");
    // template_content_hash is withheld (absent), so the no-op short-circuit
    // cannot certify this state: a re-apply REPLANS (not a no-op).
    assert!(
        qs(
            &db,
            "SELECT value FROM metadata WHERE cur_name = 'template_content_hash'"
        )
        .is_none(),
        "content hash must be withheld on a partial apply"
    );
    assert_eq!(
        qs(
            &db,
            "SELECT value FROM metadata WHERE cur_name = 'template_version'"
        )
        .as_deref(),
        Some("3n"),
        "name/version still stamp on a partial apply"
    );
    let again = apply(
        &db,
        &t,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
    assert!(!again.no_op, "partial state must keep replanning");
    assert!(!again.converged);
    // demand: float member survived (in use), datetime member still there.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM parameter_type pt \
                 JOIN parameter_definition pd ON pd.id = pt.parameter_definition_id \
                 WHERE pt.valid_to_commit IS NULL AND pd.cur_name = 'demand'"
        ),
        2
    );
    // max_share: narrowed to one member.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM parameter_type pt \
                 JOIN parameter_definition pd ON pd.id = pt.parameter_definition_id \
                 WHERE pt.valid_to_commit IS NULL AND pd.cur_name = 'max_share'"
        ),
        1
    );
}

#[test]
fn dimension_change_is_blocked_reshape() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);
    let mut t = parse_str(SAMPLE).unwrap();
    t.template.version = "3r".into();
    {
        let c = t
            .classes
            .iter_mut()
            .find(|c| c.name == "node__to_unit")
            .unwrap();
        c.dimensions.reverse(); // (node, unit) -> (unit, node)
                                // Its seed entity would now mismatch too; drop it to isolate the check.
        t.seed
            .as_mut()
            .unwrap()
            .entities
            .retain(|e| e.class != "node__to_unit");
        t.seed
            .as_mut()
            .unwrap()
            .entities
            .retain(|e| e.class != "unit_flow__unit_flow");
    }
    let p = plan(&db, &t).unwrap();
    assert!(
        p.blocked
            .iter()
            .any(|b| b.contains("node__to_unit") && b.contains("reshape")),
        "expected a reshape blocker, got {:?}",
        p.blocked
    );
    let err = apply(
        &db,
        &t,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("reshape"), "{err}");
}

#[test]
fn move_parameter_guards_values_and_moves_empty_definitions() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);

    // v5: move unit.efficiency to storage (a new class introduced same
    // template) — legal because efficiency has no values.
    let mut t = parse_str(SAMPLE).unwrap();
    t.template.version = "5".into();
    let mig = parse_str(
        "template: {name: x, version: \"1\"}
classes: [{name: c}]
migrations:
  - from: \"3\"
    to: \"5\"
    steps:
      - move_parameter: {name: efficiency, from_class: unit, to_class: storage}
",
    )
    .unwrap();
    t.migrations = mig.migrations;
    // v5 vocabulary: storage exists and owns efficiency; unit no longer has it.
    let storage = parse_str(
        "template: {name: x, version: \"1\"}
classes:
  - name: storage
    parameters:
      - name: efficiency
        types: [{kind: float}]
",
    )
    .unwrap();
    {
        let unit = t.classes.iter_mut().find(|c| c.name == "unit").unwrap();
        unit.parameters.retain(|p| p.name != "efficiency");
        // The audit rule requires efficiency on unit — drop the rule.
        t.audit_rules = None;
    }
    t.classes.extend(storage.classes);

    // But storage doesn't exist when the migration step runs (chain runs
    // BEFORE the diff creates classes) — so the step must fail cleanly…
    let err = apply(
        &db,
        &t,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("storage"), "{err}");

    // …teaching the real ordering: additive prerequisites ship in an earlier
    // version. Apply a v4 that only adds the class, then the v5 chain
    // (migrations from \"4\") works.
    let mut t4 = parse_str(SAMPLE).unwrap();
    t4.template.version = "4".into();
    t4.classes.extend(
        parse_str("template: {name: x, version: \"1\"}\nclasses: [{name: storage}]")
            .unwrap()
            .classes,
    );
    apply(
        &db,
        &t4,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();

    let mut t5 = t.clone();
    t5.migrations[0].from = "4".into();
    apply(
        &db,
        &t5,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM parameter_definition pd \
                 JOIN entity_class c ON c.id = pd.cur_cls \
                 WHERE pd.cur_name = 'efficiency' AND c.cur_name = 'storage'"
        ),
        1
    );

    // Guard: moving a definition WITH values errors, naming the reshape door.
    write_parameter(
        &db,
        "storage",
        "efficiency",
        "alt",
        scalar_batch("s1", 0.8),
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap();
    let mut t6 = t5.clone();
    t6.template.version = "6".into();
    t6.migrations = parse_str(
        "template: {name: x, version: \"1\"}
classes: [{name: c}]
migrations:
  - from: \"5\"
    to: \"6\"
    steps:
      - move_parameter: {name: efficiency, from_class: storage, to_class: unit}
",
    )
    .unwrap()
    .migrations;
    let err = apply(
        &db,
        &t6,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("current value row"), "{err}");
}

#[test]
fn rename_class_hint_preserves_identity_and_dimensions() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);
    let old_id = q1(
        &db,
        "SELECT id FROM entity_class WHERE cur_name = 'reserve'",
    );

    let mut t = parse_str(SAMPLE).unwrap();
    t.template.version = "7".into();
    t.migrations = parse_str(
        "template: {name: x, version: \"1\"}
classes: [{name: c}]
migrations:
  - from: \"3\"
    to: \"7\"
    steps:
      - rename_class: {from: reserve, to: reserve_product}
      - rename_value_list: {from: yes_no, to: yesno}
",
    )
    .unwrap()
    .migrations;
    // v7 vocabulary reflects both renames.
    for c in &mut t.classes {
        if c.name == "reserve" {
            c.name = "reserve_product".into();
        }
        for d in &mut c.dimensions {
            if let nodal_db_core::template::ir::DimRef::Class(n) = d {
                if n == "reserve" {
                    *n = "reserve_product".into();
                }
            }
        }
        for p in &mut c.parameters {
            if p.value_list.as_deref() == Some("yes_no") {
                p.value_list = Some("yesno".into());
            }
        }
    }
    for l in &mut t.value_lists {
        if l.name == "yes_no" {
            l.name = "yesno".into();
        }
    }
    if let Some(seed) = &mut t.seed {
        for e in &mut seed.entities {
            if e.class == "reserve" {
                e.class = "reserve_product".into();
            }
        }
    }

    let out = apply(
        &db,
        &t,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
    assert!(!out.no_op);
    // Same id across the rename (from/to versioning, not delete+add) —
    // dimension rows referencing the class id stay valid untouched.
    let new_id = q1(
        &db,
        "SELECT id FROM entity_class WHERE cur_name = 'reserve_product'",
    );
    assert_eq!(old_id, new_id);
    // The 4-D class's dimension row still points at the same class id.
    assert_eq!(
        q1(
            &db,
            &format!(
                "SELECT count(*) FROM entity_class_dimension \
             WHERE valid_to_commit IS NULL AND dimension_class_id = {old_id}"
            )
        ),
        1
    );
    // The list rename carried its members (same list id, members intact).
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM list_value lv \
                 JOIN parameter_value_list l ON l.id = lv.parameter_value_list_id \
                 WHERE l.cur_name = 'yesno' AND lv.valid_to_commit IS NULL"
        ),
        2
    );
    // Referential audits stay clean across both renames.
    assert_eq!(q1(&db, "SELECT count(*) FROM audit_dangling_ref"), 0);
    assert_eq!(
        q1(&db, "SELECT count(*) FROM audit_value_list_membership"),
        0
    );
    // And re-applying v7 is a no-op (idempotency after hints).
    let again = apply(
        &db,
        &t,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
    assert!(again.no_op);
}

// ---------------------------------------------------------------------------
// Chain-integrity guards (review items 1, 2, 16) + partial-apply honesty
// ---------------------------------------------------------------------------

#[test]
fn retarget_refuses_by_default_and_needs_the_explicit_opt_in() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);
    let other =
        parse_str("template: {name: other, version: \"1\"}\nclasses: [{name: unrelated}]\n")
            .unwrap();
    // Default: typed refusal, before anything runs (no commit burned).
    let commits = q1(&db, "SELECT max(id) FROM commit");
    let err = apply(
        &db,
        &other,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("retarget"), "{err}");
    assert_eq!(q1(&db, "SELECT max(id) FROM commit"), commits);
    // Plan surfaces the same refusal as a blocker (read-only).
    let p = plan(&db, &other).unwrap();
    assert!(
        p.blocked.iter().any(|b| b.contains("retarget")),
        "plan must surface the retarget refusal: {:?}",
        p.blocked
    );
    // The explicit opt-in applies and restamps, with a warning.
    let opts = ApplyOptions {
        allow_retarget: true,
        ..Default::default()
    };
    let out = apply(&db, &other, &MigrationRegistry::new(), "tester", "", &opts).unwrap();
    assert!(
        out.warnings.iter().any(|w| w.contains("retargeting")),
        "{:?}",
        out.warnings
    );
    assert_eq!(
        qs(
            &db,
            "SELECT value FROM metadata WHERE cur_name = 'template_name'"
        )
        .as_deref(),
        Some("other")
    );
}

#[test]
fn downgrade_and_unreachable_chains_refuse_loudly() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);
    // Move the DB to version 4 first (additive bump, no migrations).
    let mut t4 = parse_str(SAMPLE).unwrap();
    t4.template.version = "4".into();
    apply(
        &db,
        &t4,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();

    // Downgrade: a v3 template whose own chain knows 3 -> 4 sees the DB (at
    // 4) AHEAD of itself — refused by default.
    let mut t3 = parse_str(SAMPLE).unwrap();
    t3.migrations = parse_str(
        "template: {name: x, version: \"1\"}
classes: [{name: c}]
migrations:
  - {from: \"3\", to: \"4\", steps: [{rename_class: {from: a, to: b}}]}
",
    )
    .unwrap()
    .migrations;
    let err = apply(
        &db,
        &t3,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("downgrade"), "{err}");

    // Unreachable: a v6 template whose chain only covers 5 -> 6 cannot walk
    // from the stamped 4 — refused loudly (never a silent gap-bridge when
    // the template DOES declare a chain).
    let mut t6 = parse_str(SAMPLE).unwrap();
    t6.template.version = "6".into();
    t6.migrations = parse_str(
        "template: {name: x, version: \"1\"}
classes: [{name: c}]
migrations:
  - {from: \"5\", to: \"6\", steps: [{rename_class: {from: a, to: b}}]}
",
    )
    .unwrap()
    .migrations;
    let err = apply(
        &db,
        &t6,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("no path"), "{err}");

    // The no-migrations-at-all case still bridges (the §4.2 L1 majority):
    let mut t5 = parse_str(SAMPLE).unwrap();
    t5.template.version = "5".into();
    apply(
        &db,
        &t5,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
    assert_eq!(
        qs(
            &db,
            "SELECT value FROM metadata WHERE cur_name = 'template_version'"
        )
        .as_deref(),
        Some("5")
    );
}

#[test]
fn interrupted_chain_resumes_from_the_progress_substamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);

    // First attempt: the declarative renames succeed (committed, each with
    // its same-commit substamp), then the run: program FAILS.
    let t4 = v4_template();
    let mut failing = MigrationRegistry::new();
    failing.register("mark_step", |_db: &Path, _u: &str| {
        Err(nodal_db_core::error::DbError::Api("simulated crash".into()))
    });
    let err = apply(&db, &t4, &failing, "tester", "", &ApplyOptions::default()).unwrap_err();
    assert!(err.to_string().contains("simulated crash"), "{err}");
    // The chain is mid-span: version NOT bumped, substamp present.
    assert_eq!(
        qs(
            &db,
            "SELECT value FROM metadata WHERE cur_name = 'template_version'"
        )
        .as_deref(),
        Some("3")
    );
    assert_eq!(
        qs(
            &db,
            "SELECT value FROM metadata WHERE cur_name = 'template_migration_step'"
        )
        .as_deref(),
        Some("3->4:1"),
        "substamp records the last completed step"
    );

    // Second attempt with a working program: the completed renames are
    // SKIPPED (re-running rename_parameter would fail — 'efficiency' is
    // gone), the program runs, the span stamps, the reconcile lands.
    let mut working = MigrationRegistry::new();
    working.register("mark_step", |_db: &Path, _u: &str| Ok(()));
    let out = apply(&db, &t4, &working, "tester", "", &ApplyOptions::default()).unwrap();
    assert!(
        out.converged,
        "resumed apply must converge: {:?}",
        out.plan.skipped
    );
    assert_eq!(
        qs(
            &db,
            "SELECT value FROM metadata WHERE cur_name = 'template_version'"
        )
        .as_deref(),
        Some("4")
    );
    assert!(
        qs(
            &db,
            "SELECT value FROM metadata WHERE cur_name = 'template_migration_step'"
        )
        .is_none(),
        "span stamp clears the substamp"
    );
    // The rename happened exactly once (one closed + one current row).
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM parameter_definition WHERE name = 'eta'"
        ),
        1
    );
}

#[test]
fn excluded_membership_is_respected_and_withholds_convergence() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);

    // A user excludes the seeded base membership (close + excluded row) —
    // raw SQL here stands in for the editor's membership override.
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "BEGIN;
             INSERT INTO commit (user, message) VALUES ('user', 'exclude membership');
             UPDATE membership SET valid_to_commit = currval('seq_commit')
               WHERE valid_to_commit IS NULL AND alternative_id IS NULL;
             INSERT INTO membership (group_entity_id, member_entity_id, alternative_id,
                                     included, valid_from_commit, version)
               SELECT group_entity_id, member_entity_id, NULL, FALSE,
                      currval('seq_commit'), version + 1
               FROM membership WHERE valid_to_commit = currval('seq_commit');
             COMMIT;",
        )
        .unwrap();
    }

    // Re-apply at a bumped version: the exclusion is respected (no
    // resurrection, no unique-index collision), the apply is PARTIAL.
    let mut t = parse_str(SAMPLE).unwrap();
    t.template.version = "3x".into();
    let out = apply(
        &db,
        &t,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
    assert!(
        out.plan.skipped.iter().any(|s| s.contains("EXCLUDED")),
        "expected the exclusion skip, got {:?}",
        out.plan.skipped
    );
    assert!(!out.converged);
    // The excluded row is untouched (still the one current base row, FALSE).
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM membership WHERE valid_to_commit IS NULL \
                 AND alternative_id IS NULL AND NOT included"
        ),
        1
    );
    assert!(
        qs(
            &db,
            "SELECT value FROM metadata WHERE cur_name = 'template_content_hash'"
        )
        .is_none(),
        "hash withheld while the exclusion diverges from the template"
    );
}

#[test]
fn stale_plan_is_refused_via_expect_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);
    let mut t = parse_str(SAMPLE).unwrap();
    t.template.version = "3s".into();

    let p = plan(&db, &t).unwrap();
    // Something else commits in between (any write moves the cookie).
    write_parameter(
        &db,
        "node",
        "demand",
        "alt",
        scalar_batch("n9", 1.0),
        &[],
        WriteOptions::default(),
        "someone_else",
        "",
    )
    .unwrap();

    let opts = ApplyOptions {
        expect_commit: Some(p.db_commit),
        ..Default::default()
    };
    let err = apply(&db, &t, &MigrationRegistry::new(), "tester", "", &opts).unwrap_err();
    assert!(
        matches!(err, nodal_db_core::error::DbError::Stale { expected, .. } if expected == p.db_commit),
        "{err:?}"
    );

    // Re-plan → apply with the fresh commit id succeeds.
    let p2 = plan(&db, &t).unwrap();
    let opts2 = ApplyOptions {
        expect_commit: Some(p2.db_commit),
        ..Default::default()
    };
    apply(&db, &t, &MigrationRegistry::new(), "tester", "", &opts2).unwrap();
}

#[test]
fn d12_default_omission_keeps_and_explicit_null_clears() {
    use nodal_db_core::template::ir::DefaultSpec;
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);
    let has_default = |db: &Path| {
        q1(
            db,
            "SELECT count(*) FROM definition_value_scalar dv \
                JOIN parameter_definition pd ON pd.id = dv.parameter_definition_id \
                WHERE pd.cur_name = 'demand' AND dv.valid_to_commit IS NULL",
        )
    };
    assert_eq!(has_default(&db), 1, "sample seeds demand's default");

    // OMISSION = no opinion (D12 KEEP): the DB default survives, no action,
    // no noise, and the apply CONVERGES.
    let mut keep = parse_str(SAMPLE).unwrap();
    keep.template.version = "3k".into();
    {
        let node = keep.classes.iter_mut().find(|c| c.name == "node").unwrap();
        let demand = node
            .parameters
            .iter_mut()
            .find(|p| p.name == "demand")
            .unwrap();
        demand.default = DefaultSpec::Unspecified;
    }
    let out = apply(
        &db,
        &keep,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
    assert!(out.converged);
    assert!(
        !out.plan
            .actions
            .iter()
            .any(|a| a.to_string().contains("default of node.demand")),
        "omission must emit NO default action: {:?}",
        out.plan
            .actions
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(has_default(&db), 1, "omission keeps the database's default");

    // EXPLICIT `default: null` clears — and is idempotent (the instruction
    // is not state: the second apply is a hash-certified no-op).
    let mut clear = parse_str(SAMPLE).unwrap();
    clear.template.version = "3c".into();
    {
        let node = clear.classes.iter_mut().find(|c| c.name == "node").unwrap();
        let demand = node
            .parameters
            .iter_mut()
            .find(|p| p.name == "demand")
            .unwrap();
        demand.default = DefaultSpec::Clear;
    }
    let out = apply(
        &db,
        &clear,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
    assert!(out.converged);
    assert!(
        out.plan
            .actions
            .iter()
            .any(|a| a.to_string().contains("clear default of node.demand")),
        "{:?}",
        out.plan
            .actions
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(has_default(&db), 0, "explicit null clears");
    let again = apply(
        &db,
        &clear,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
    assert!(again.no_op, "clearing template re-applies as a no-op");

    // The parse-level spelling drives the same behavior end-to-end: the
    // YAML `default: null` document also hashes like the omitting one.
    assert_eq!(
        nodal_db_core::template::template_content_hash(&clear),
        {
            let mut omitted = parse_str(SAMPLE).unwrap();
            omitted.template.version = "3c".into();
            let node = omitted
                .classes
                .iter_mut()
                .find(|c| c.name == "node")
                .unwrap();
            let d = node
                .parameters
                .iter_mut()
                .find(|p| p.name == "demand")
                .unwrap();
            d.default = DefaultSpec::Unspecified;
            nodal_db_core::template::template_content_hash(&omitted)
        },
        "clear instruction is hash-invisible (D12)"
    );
}

#[test]
fn d12_explicit_null_respects_the_nonscalar_guard() {
    use nodal_db_core::template::ir::DefaultSpec;
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);

    // Manufacture a default the v1 IR cannot express (a date_time scalar —
    // dbvocab treats any non-literal kind as non-scalar-representable).
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "BEGIN;
             INSERT INTO commit (user, message) VALUES ('user', 'exotic default');
             INSERT INTO definition_value_scalar
               (parameter_definition_id, slot, valid_from_commit, version, value_kind,
                v_datetime)
               SELECT id, 'default_value', currval('seq_commit'), 1, 'date_time',
                      TIMESTAMP '2026-01-01 00:00:00'
               FROM parameter_definition WHERE cur_name = 'efficiency';
             COMMIT;",
        )
        .unwrap();
    }

    let mut t = parse_str(SAMPLE).unwrap();
    t.template.version = "3g".into();
    {
        let unit = t.classes.iter_mut().find(|c| c.name == "unit").unwrap();
        let eff = unit
            .parameters
            .iter_mut()
            .find(|p| p.name == "efficiency")
            .unwrap();
        eff.default = DefaultSpec::Clear;
    }
    let out = apply(
        &db,
        &t,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();
    assert!(
        out.plan
            .skipped
            .iter()
            .any(|s| s.contains("cannot clear a NON-SCALAR default")),
        "expected the item-6 guard skip, got {:?}",
        out.plan.skipped
    );
    assert!(!out.converged, "guarded clear withholds convergence");
    // The exotic default is untouched.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM definition_value_scalar dv \
                 JOIN parameter_definition pd ON pd.id = dv.parameter_definition_id \
                 WHERE pd.cur_name = 'efficiency' AND dv.valid_to_commit IS NULL \
                 AND dv.value_kind = 'date_time'"
        ),
        1
    );
}

/// Nested-self-apply guard (py-skin review): a `run:` program calling `apply`
/// on the database it is migrating would re-execute its own pending step
/// (its substamp lands only after it returns) and recurse unboundedly. The
/// core refuses the inner apply with a typed error naming the reentrancy;
/// the outer chain aborts mid-span and remains resumable. Process-local by
/// design — cross-process is any other concurrent-writer race.
#[test]
fn nested_self_apply_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_loaded_db(&dir);

    let t4 = v4_template();
    let mut registry = MigrationRegistry::new();
    registry.register("mark_step", move |db: &Path, user: &str| {
        // The program tries to re-apply its own in-flight template.
        let inner = v4_template();
        let e = apply(
            db,
            &inner,
            &MigrationRegistry::new(),
            user,
            "",
            &ApplyOptions::default(),
        )
        .unwrap_err();
        Err(e)
    });
    let err = apply(&db, &t4, &registry, "tester", "", &ApplyOptions::default()).unwrap_err();
    assert!(
        err.to_string().contains("already in flight"),
        "expected the in-flight refusal, got: {err}"
    );

    // The guard entry was released on error: a fresh (non-nested) apply of
    // the same template succeeds afterwards.
    let mut working = MigrationRegistry::new();
    working.register("mark_step", |_db: &Path, _u: &str| Ok(()));
    let out = apply(&db, &t4, &working, "tester", "", &ApplyOptions::default()).unwrap();
    assert!(out.converged);
}
