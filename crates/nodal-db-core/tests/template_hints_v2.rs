//! Hint vocabulary v2 (template_ir_plan §4.2 as revised by D14/D15,
//! jkiviluo 2026-09-22) — chain-expressibility acceptance against the
//! FlexTool classification's concrete targets
//! (`specs/flextool_migration_classification.md` §3, G1–G7):
//!
//! - v28/v66 — `rename_parameter` + `on_collision: drop_source` (G2)
//! - v41     — `merge_parameter` fill-if-absent (G3)
//! - v52     — `rename_value_list` + `merge_list_member` glpsol→highs (G4)
//! - v55     — `rename_list_member` with the D15 rewrite-by-default (G7)
//! - v56     — `drop_parameter` with cascade counts + orphan
//!             `drop_value_list` (D14/G1)
//! - v62     — `rename_list_member` rewrite + `drop_parameter` batch
//! - v65     — `rename_parameter` + `map_values` translation table (G6)
//!
//! Plus: `drop_class` cascade with asserted blast radius, the
//! `drop_list_member` stranded-value SKIP (convergence withheld, D14), the
//! `drop_value_list` bound-list refusal (plan blocker + apply error), parse
//! tests for every new step kind incl. duplicate-key rejection (UniqueMap),
//! and the migrations-stay-hash-excluded confirmation.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::write_parameter;
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::template::ir::{MigrationStep, OnCollision, UnmappedValues};
use nodal_db_core::template::{
    apply, parse_str, plan, template_content_hash, validate, ApplyOptions, MigrationRegistry,
};
use nodal_db_core::types::WriteOptions;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn fresh_db_with(dir: &tempfile::TempDir, base_yaml: &str) -> PathBuf {
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    let t = parse_str(base_yaml).unwrap();
    let out =
        apply(&db, &t, &MigrationRegistry::new(), "tester", "", &ApplyOptions::default()).unwrap();
    assert!(out.converged, "base template must converge: {:?}", out.plan.skipped);
    db
}

fn apply_v2(db: &Path, yaml: &str) -> nodal_db_core::template::ApplyOutcome {
    let t = parse_str(yaml).unwrap();
    apply(db, &t, &MigrationRegistry::new(), "tester", "", &ApplyOptions::default()).unwrap()
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

fn float_batch(entity: &str, v: f64) -> RecordBatch {
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

fn write_str(db: &Path, class: &str, param: &str, entity: &str, v: &str) {
    write_parameter(db, class, param, "alt", string_batch(entity, v), &[],
        WriteOptions::default(), "tester", "").unwrap();
}

fn write_f(db: &Path, class: &str, param: &str, entity: &str, v: f64) {
    write_parameter(db, class, param, "alt", float_batch(entity, v), &[],
        WriteOptions::default(), "tester", "").unwrap();
}

/// Current scalar value (string) for (class, param, entity), if any.
fn cur_str(db: &Path, param: &str, entity: &str) -> Option<String> {
    qs(db, &format!(
        "SELECT v.v_str FROM value_scalar v \
         JOIN parameter_definition pd ON pd.id = v.parameter_definition_id \
         JOIN entity e ON e.id = v.entity_id \
         WHERE v.valid_to_commit IS NULL AND pd.cur_name = '{param}' \
           AND e.cur_name = '{entity}'"
    ))
}

fn cur_float(db: &Path, param: &str, entity: &str) -> Option<f64> {
    let conn = Connection::open(db).unwrap();
    conn.query_row(
        &format!(
            "SELECT v.v_float FROM value_scalar v \
             JOIN parameter_definition pd ON pd.id = v.parameter_definition_id \
             JOIN entity e ON e.id = v.entity_id \
             WHERE v.valid_to_commit IS NULL AND pd.cur_name = '{param}' \
               AND e.cur_name = '{entity}'"
        ),
        [],
        |r| r.get::<_, f64>(0),
    )
    .ok()
}

/// Count of CURRENT rows in `table` matching a predicate.
fn live(db: &Path, table: &str, pred: &str) -> i64 {
    q1(db, &format!(
        "SELECT count(*) FROM {table} WHERE valid_to_commit IS NULL AND {pred}"
    ))
}

// ---------------------------------------------------------------------------
// Parse tests: every new step kind, defaults, duplicate-key rejection
// ---------------------------------------------------------------------------

#[test]
fn parse_v2_step_kinds_and_defaults() {
    let t = parse_str(
        r#"
template: {name: t, version: "2"}
classes: [{name: c}]
migrations:
  - from: "1"
    to: "2"
    steps:
      - rename_parameter: {class: c, from: a, to: b}
      - rename_parameter: {class: c, from: a, to: b, on_collision: drop_source}
      - rename_parameter: {class: c, from: a, to: b,
                           map_values: {yes: manual}, unmapped_values: drop}
      - rename_list_member: {list: l, from: a, to: b}
      - rename_list_member: {list: l, from: a, to: b, rewrite_values: false}
      - merge_parameter: {class: c, from: a, to: b}
      - merge_list_member: {list: l, from: a, to: b}
      - drop_parameter: {class: c, parameter: a}
      - drop_class: {class: c}
      - drop_value_list: {list: l}
      - drop_list_member: {list: l, value: a}
"#,
    )
    .unwrap();
    let steps = &t.migrations[0].steps;
    assert!(matches!(&steps[0],
        MigrationStep::RenameParameter { on_collision: OnCollision::Error,
            map_values: None, unmapped_values: UnmappedValues::Keep, .. }));
    assert!(matches!(&steps[1],
        MigrationStep::RenameParameter { on_collision: OnCollision::DropSource, .. }));
    match &steps[2] {
        MigrationStep::RenameParameter { map_values: Some(m), unmapped_values, .. } => {
            // YAML 1.2: `yes` stays a string key.
            assert_eq!(m.get("yes").map(String::as_str), Some("manual"));
            assert_eq!(*unmapped_values, UnmappedValues::Drop);
        }
        other => panic!("expected map_values rename, got {other:?}"),
    }
    // D15: rewrite defaults ON; the explicit opt-out is honored.
    assert!(matches!(&steps[3],
        MigrationStep::RenameListMember { rewrite_values: true, .. }));
    assert!(matches!(&steps[4],
        MigrationStep::RenameListMember { rewrite_values: false, .. }));
    assert!(matches!(&steps[5], MigrationStep::MergeParameter { .. }));
    assert!(matches!(&steps[6], MigrationStep::MergeListMember { .. }));
    assert!(matches!(&steps[7], MigrationStep::DropParameter { .. }));
    assert!(matches!(&steps[8], MigrationStep::DropClass { .. }));
    assert!(matches!(&steps[9], MigrationStep::DropValueList { .. }));
    assert!(matches!(&steps[10], MigrationStep::DropListMember { .. }));
    // The one-key contract names the v2 kinds.
    let err = parse_str(
        "template: {name: t, version: \"2\"}\nclasses: [{name: c}]\nmigrations:\n  - from: \"1\"\n    to: \"2\"\n    steps:\n      - drop_class: {class: c}\n        drop_value_list: {list: l}\n",
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("ONE-key") && err.contains("drop_value_list"), "{err}");
}

#[test]
fn parse_rejects_duplicate_keys_in_new_steps() {
    // Duplicate field key inside a step body (UniqueMap under StepDef).
    let err = parse_str(
        "template: {name: t, version: \"2\"}\nclasses: [{name: c}]\nmigrations:\n  - from: \"1\"\n    to: \"2\"\n    steps:\n      - drop_parameter: {class: a, class: b, parameter: p}\n",
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("duplicate"), "{err}");
    // Duplicate translation key inside map_values (UniqueMap).
    let err = parse_str(
        "template: {name: t, version: \"2\"}\nclasses: [{name: c}]\nmigrations:\n  - from: \"1\"\n    to: \"2\"\n    steps:\n      - rename_parameter: {class: c, from: a, to: b, map_values: {x: y, x: z}}\n",
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("duplicate key") && err.contains('x'), "{err}");
}

fn expect_validate_err(yaml: &str, needle: &str) {
    let t = parse_str(yaml).unwrap_or_else(|e| panic!("parse failed ({e}) for:\n{yaml}"));
    let msg = validate(&t).err().map(|e| e.to_string()).unwrap_or_default();
    assert!(msg.contains(needle), "expected {needle:?} in:\n{msg}");
}

#[test]
fn validation_checks_v2_step_shapes() {
    let head = "template: {name: t, version: \"2\"}\nclasses: [{name: c}]\nmigrations:\n  - from: \"1\"\n    to: \"2\"\n    steps:\n";
    // unmapped_values without a translation table.
    expect_validate_err(
        &format!("{head}      - rename_parameter: {{class: c, from: a, to: b, unmapped_values: drop}}\n"),
        "unmapped_values requires a map_values table",
    );
    // Chained translations are ambiguous.
    expect_validate_err(
        &format!("{head}      - rename_parameter: {{class: c, from: a, to: b, map_values: {{a: b, b: c}}}}\n"),
        "chained translations are ambiguous",
    );
    // The identity entry gets its own explanation (review small item).
    expect_validate_err(
        &format!("{head}      - rename_parameter: {{class: c, from: a, to: b, map_values: {{x: x}}}}\n"),
        "maps to itself",
    );
    // Empty map.
    expect_validate_err(
        &format!("{head}      - rename_parameter: {{class: c, from: a, to: b, map_values: {{}}}}\n"),
        "map_values must be non-empty",
    );
    // Merge from == to.
    expect_validate_err(
        &format!("{head}      - merge_parameter: {{class: c, from: a, to: A}}\n"),
        "merge_parameter from == to",
    );
    expect_validate_err(
        &format!("{head}      - merge_list_member: {{list: l, from: a, to: a}}\n"),
        "merge_list_member from == to",
    );
    // Empty names on drops.
    expect_validate_err(
        &format!("{head}      - drop_parameter: {{class: \"\", parameter: p}}\n"),
        "class must be non-empty",
    );
    expect_validate_err(
        &format!("{head}      - drop_list_member: {{list: l, value: \"\"}}\n"),
        "value must be non-empty",
    );
}

#[test]
fn v2_steps_stay_out_of_the_content_hash() {
    // The hash-exclusion rule (ir.rs module docs) must hold for the new
    // vocabulary too: chain-history edits never move the identity hash.
    let with = parse_str(
        r#"
template: {name: t, version: "2"}
classes: [{name: c}]
migrations:
  - from: "1"
    to: "2"
    steps:
      - drop_class: {class: x}
      - merge_parameter: {class: c, from: a, to: b}
      - rename_parameter: {class: c, from: p, to: q, on_collision: drop_source,
                           map_values: {yes: manual}, unmapped_values: drop}
"#,
    )
    .unwrap();
    let without =
        parse_str("template: {name: t, version: \"2\"}\nclasses: [{name: c}]").unwrap();
    assert_eq!(template_content_hash(&with), template_content_hash(&without));
}

// ---------------------------------------------------------------------------
// v28 / v66 — rename collision policy (G2)
// ---------------------------------------------------------------------------

const V28_BASE: &str = r#"
template: {name: v28, version: "1"}
classes:
  - name: node
    parameters:
      - {name: interest_rate, types: [{kind: float}]}
      - {name: discount_rate, types: [{kind: float}]}
      - {name: energy_margin, types: [{kind: float}]}
seed:
  entities:
    - {class: node, name: n1}
    - {class: node, name: n2}
"#;

const V28_V2: &str = r#"
template: {name: v28, version: "2"}
classes:
  - name: node
    parameters:
      - {name: discount_rate, types: [{kind: float}]}
      - {name: energy_margin_multiplier, types: [{kind: float}]}
migrations:
  - from: "1"
    to: "2"
    steps:
      # v28 shape: target already live -> the stale source closes, cascade.
      - rename_parameter: {class: node, from: interest_rate, to: discount_rate,
                           on_collision: drop_source}
      # v66 shape: same policy, target absent -> a plain rename.
      - rename_parameter: {class: node, from: energy_margin,
                           to: energy_margin_multiplier, on_collision: drop_source}
"#;

#[test]
fn v28_collision_drops_stale_source_v66_renames_plain() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, V28_BASE);
    write_f(&db, "node", "interest_rate", "n1", 0.05); // stale source value
    write_f(&db, "node", "discount_rate", "n1", 0.07); // survivor's value
    write_f(&db, "node", "energy_margin", "n2", 2.0);

    // The plan carries the D14 blast radius for the collision drop.
    let p = plan(&db, &parse_str(V28_V2).unwrap()).unwrap();
    let rendered = format!("{p}");
    assert!(rendered.contains("drop stale source"), "{rendered}");
    assert!(rendered.contains("would cover") || rendered.contains("row(s)"), "{rendered}");

    let out = apply_v2(&db, V28_V2);
    assert!(out.converged, "skips: {:?}", out.plan.skipped);
    // Stale source closed (versioned — history rows remain, nothing erased).
    assert_eq!(live(&db, "parameter_definition", "name = 'interest_rate'"), 0);
    assert!(q1(&db, "SELECT count(*) FROM parameter_definition WHERE name = 'interest_rate'") > 0,
        "close-out, not erasure");
    // Its value closed with it; the survivor's value intact.
    assert_eq!(cur_float(&db, "interest_rate", "n1"), None);
    assert_eq!(cur_float(&db, "discount_rate", "n1"), Some(0.07));
    // v66 path: renamed with value carried (same definition id).
    assert_eq!(cur_float(&db, "energy_margin_multiplier", "n2"), Some(2.0));
    assert_eq!(live(&db, "parameter_definition", "name = 'energy_margin'"), 0);
    assert!(out.notes.iter().any(|n| n.contains("dropping stale source")
            || n.contains("closed")), "notes: {:?}", out.notes);
}

// ---------------------------------------------------------------------------
// v41 — same-class parameter merge (G3)
// ---------------------------------------------------------------------------

const V41_BASE: &str = r#"
template: {name: v41, version: "1"}
classes:
  - name: node
    parameters:
      - {name: storate_state_end, types: [{kind: float}]}
      - {name: storage_state_end, types: [{kind: float}]}
  - name: solve
    parameters:
      - {name: old_name, types: [{kind: float}]}
seed:
  entities:
    - {class: node, name: n1}
    - {class: node, name: n2}
    - {class: solve, name: s1}
"#;

const V41_V2: &str = r#"
template: {name: v41, version: "2"}
classes:
  - name: node
    parameters:
      - {name: storage_state_end, types: [{kind: float}]}
  - name: solve
    parameters:
      - {name: new_name, types: [{kind: float}]}
migrations:
  - from: "1"
    to: "2"
    steps:
      # v41: typo merge, target exists -> fill-if-absent then close source.
      - merge_parameter: {class: node, from: storate_state_end, to: storage_state_end}
      # target absent -> create-by-rename (the G3 ordering case).
      - merge_parameter: {class: solve, from: old_name, to: new_name}
"#;

#[test]
fn v41_merge_parameter_fills_empty_slots_and_closes_source() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, V41_BASE);
    write_f(&db, "node", "storate_state_end", "n1", 1.0); // collides with target
    write_f(&db, "node", "storate_state_end", "n2", 2.0); // fills empty slot
    write_f(&db, "node", "storage_state_end", "n1", 5.0); // occupied target slot
    write_f(&db, "solve", "old_name", "s1", 9.0);

    let out = apply_v2(&db, V41_V2);
    assert!(out.converged, "skips: {:?}", out.plan.skipped);
    // Fill-if-absent: the occupied slot kept its value; the empty one filled.
    assert_eq!(cur_float(&db, "storage_state_end", "n1"), Some(5.0));
    assert_eq!(cur_float(&db, "storage_state_end", "n2"), Some(2.0));
    // The typo definition and its values are closed (versioned).
    assert_eq!(live(&db, "parameter_definition", "name = 'storate_state_end'"), 0);
    assert_eq!(cur_float(&db, "storate_state_end", "n1"), None);
    // Create-by-rename: value carried under the new name, same definition.
    assert_eq!(cur_float(&db, "new_name", "s1"), Some(9.0));
    assert!(out.notes.iter().any(|n| n.contains("carried 1 value(s)")),
        "notes: {:?}", out.notes);
}

// --- merge_parameter guards (2026-09-22 review, MEDIUM) ---

const MERGE_GUARD_BASE: &str = r#"
template: {name: mg, version: "1"}
value_lists:
  - name: yes_no
    members: [{value: "yes"}, {value: "no"}]
  - name: on_off
    members: [{value: "on_"}, {value: "off_"}]
classes:
  - name: node
    parameters:
      - {name: src_listed, value_list: yes_no}
      - {name: tgt_otherlist, value_list: on_off}
      - {name: tgt_samelist, value_list: yes_no}
      - {name: src_string, types: [{kind: string}]}
      - {name: tgt_float, types: [{kind: float}]}
seed:
  entities:
    - {class: node, name: n1}
    - {class: node, name: n2}
"#;

fn merge_guard_v2(step: &str) -> String {
    format!(
        r#"
template: {{name: mg, version: "2"}}
value_lists:
  - name: yes_no
    members: [{{value: "yes"}}, {{value: "no"}}]
  - name: on_off
    members: [{{value: "on_"}}, {{value: "off_"}}]
classes:
  - name: node
    parameters:
      - {{name: tgt_otherlist, value_list: on_off}}
      - {{name: tgt_samelist, value_list: yes_no}}
      - {{name: tgt_float, types: [{{kind: float}}]}}
migrations:
  - from: "1"
    to: "2"
    steps:
      - {step}
"#
    )
}

#[test]
fn merge_parameter_refuses_cross_list_targets() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, MERGE_GUARD_BASE);
    write_str(&db, "node", "src_listed", "n1", "yes");
    let v2 = merge_guard_v2("merge_parameter: {class: node, from: src_listed, to: tgt_otherlist}");
    let err = apply(&db, &parse_str(&v2).unwrap(), &MigrationRegistry::new(), "tester", "",
        &ApplyOptions::default()).unwrap_err().to_string();
    // The typed error names BOTH lists and points at run: as the escape.
    assert!(err.contains("yes_no") && err.contains("on_off") && err.contains("run:"), "{err}");
    assert_eq!(live(&db, "parameter_definition", "name = 'src_listed'"), 1,
        "refusal must close nothing");
}

#[test]
fn merge_parameter_refuses_kind_without_target_member() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, MERGE_GUARD_BASE);
    write_str(&db, "node", "src_string", "n1", "hello");
    let v2 = merge_guard_v2("merge_parameter: {class: node, from: src_string, to: tgt_float}");
    let err = apply(&db, &parse_str(&v2).unwrap(), &MigrationRegistry::new(), "tester", "",
        &ApplyOptions::default()).unwrap_err().to_string();
    // Hard error toward run: naming the uncovered kind — never the
    // NULL-binding fallback.
    assert!(err.contains("string") && err.contains("run:"), "{err}");
    assert_eq!(cur_str(&db, "src_string", "n1").as_deref(), Some("hello"));
}

#[test]
fn merge_parameter_same_list_carries_with_target_binding() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, MERGE_GUARD_BASE);
    write_str(&db, "node", "src_listed", "n2", "no"); // fills the empty slot
    write_str(&db, "node", "tgt_samelist", "n1", "yes"); // occupied slot
    let v2 = merge_guard_v2("merge_parameter: {class: node, from: src_listed, to: tgt_samelist}");
    let out = apply(&db, &parse_str(&v2).unwrap(), &MigrationRegistry::new(), "tester", "",
        &ApplyOptions::default()).unwrap();
    assert!(out.converged, "skips: {:?}", out.plan.skipped);
    assert_eq!(cur_str(&db, "tgt_samelist", "n1").as_deref(), Some("yes"));
    assert_eq!(cur_str(&db, "tgt_samelist", "n2").as_deref(), Some("no"));
    // The carried row binds the TARGET's own rank-0 string member (D11) —
    // never NULL, never the source's member.
    assert_eq!(q1(&db,
        "SELECT count(*) FROM value_scalar v \
         JOIN parameter_type pt ON pt.id = v.parameter_type_id \
          AND pt.parameter_definition_id = v.parameter_definition_id \
          AND pt.value_kind = 'string' AND pt.rank = 0 \
         JOIN parameter_definition pd ON pd.id = v.parameter_definition_id \
         WHERE v.valid_to_commit IS NULL AND pd.cur_name = 'tgt_samelist'"), 2);
    assert_eq!(live(&db, "parameter_definition", "name = 'src_listed'"), 0);
}

// ---------------------------------------------------------------------------
// v52 — value-list rename + list-member merge onto an existing target (G4)
// ---------------------------------------------------------------------------

const V52_BASE: &str = r#"
template: {name: v52, version: "1"}
value_lists:
  - name: solver
    members: [{value: "glpsol"}, {value: "highs"}]
classes:
  - name: solve
    parameters:
      - {name: solver, value_list: solver}
seed:
  entities:
    - {class: solve, name: s1}
    - {class: solve, name: s2}
"#;

const V52_V2: &str = r#"
template: {name: v52, version: "2"}
value_lists:
  - name: solvers
    members: [{value: "highs"}, {value: "cplex"}, {value: "gurobi"}]
classes:
  - name: solve
    parameters:
      - {name: solver, value_list: solvers}
migrations:
  - from: "1"
    to: "2"
    steps:
      - merge_list_member: {list: solver, from: glpsol, to: highs}
      - rename_value_list: {from: solver, to: solvers}
"#;

#[test]
fn v52_member_merge_rewrites_values_and_list_renames() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, V52_BASE);
    write_str(&db, "solve", "solver", "s1", "glpsol");
    write_str(&db, "solve", "solver", "s2", "highs");

    let out = apply_v2(&db, V52_V2);
    assert!(out.converged, "skips: {:?}", out.plan.skipped);
    // Stored values rewritten onto the surviving member.
    assert_eq!(cur_str(&db, "solver", "s1").as_deref(), Some("highs"));
    assert_eq!(cur_str(&db, "solver", "s2").as_deref(), Some("highs"));
    // The glpsol member is closed; the list renamed; new members added.
    assert_eq!(live(&db, "list_value", "v_str = 'glpsol'"), 0);
    assert_eq!(live(&db, "parameter_value_list", "name = 'solvers'"), 1);
    assert_eq!(live(&db, "parameter_value_list", "name = 'solver'"), 0);
    assert_eq!(live(&db, "list_value", "v_str = 'cplex'"), 1);
}

// ---------------------------------------------------------------------------
// v55 / v62 — rename_list_member rewrites by DEFAULT (D15/G7) + drops
// ---------------------------------------------------------------------------

const V55_BASE: &str = r#"
template: {name: v55, version: "1"}
value_lists:
  - name: constraint_method
    members: [{value: "bind_within_timeset"}, {value: "lagrangian"}]
classes:
  - name: group
    parameters:
      - {name: constraint_method, value_list: constraint_method}
      - {name: lagrangian_gap, types: [{kind: float}]}
seed:
  entities:
    - {class: group, name: g1}
    - {class: group, name: g2}
"#;

const V55_V2: &str = r#"
template: {name: v55, version: "2"}
value_lists:
  - name: constraint_method
    members: [{value: "bind_within_timeblock"}, {value: "benders"}]
classes:
  - name: group
    parameters:
      - {name: constraint_method, value_list: constraint_method}
migrations:
  - from: "1"
    to: "2"
    steps:
      # v55 shape: NO rewrite_values key -> D15 default ON.
      - rename_list_member: {list: constraint_method,
                             from: bind_within_timeset, to: bind_within_timeblock}
      # v62 shape: member rename with rewrite + definition drops.
      - rename_list_member: {list: constraint_method, from: lagrangian, to: benders}
      - drop_parameter: {class: group, parameter: lagrangian_gap}
"#;

#[test]
fn v55_v62_rewrite_by_default_and_drop_batch() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, V55_BASE);
    write_str(&db, "group", "constraint_method", "g1", "bind_within_timeset");
    write_str(&db, "group", "constraint_method", "g2", "lagrangian");
    write_f(&db, "group", "lagrangian_gap", "g1", 0.01);

    let out = apply_v2(&db, V55_V2);
    assert!(out.converged, "skips: {:?}", out.plan.skipped);
    // D15: values rewritten WITHOUT any rewrite_values in the YAML.
    assert_eq!(cur_str(&db, "constraint_method", "g1").as_deref(),
        Some("bind_within_timeblock"));
    assert_eq!(cur_str(&db, "constraint_method", "g2").as_deref(), Some("benders"));
    // v62 drop leg: definition + value closed.
    assert_eq!(live(&db, "parameter_definition", "name = 'lagrangian_gap'"), 0);
    assert_eq!(cur_float(&db, "lagrangian_gap", "g1"), None);
}

#[test]
fn rename_list_member_opt_out_still_honored() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, V55_BASE);
    write_str(&db, "group", "constraint_method", "g1", "bind_within_timeset");
    let v2 = r#"
template: {name: v55, version: "2"}
value_lists:
  - name: constraint_method
    members: [{value: "bind_within_timeblock"}, {value: "lagrangian"}]
classes:
  - name: group
    parameters:
      - {name: constraint_method, value_list: constraint_method}
      - {name: lagrangian_gap, types: [{kind: float}]}
migrations:
  - from: "1"
    to: "2"
    steps:
      - rename_list_member: {list: constraint_method,
                             from: bind_within_timeset, to: bind_within_timeblock,
                             rewrite_values: false}
"#;
    let out = apply_v2(&db, v2);
    assert!(out.converged);
    // The explicit opt-out strands the stored string — the author's call.
    assert_eq!(cur_str(&db, "constraint_method", "g1").as_deref(),
        Some("bind_within_timeset"));
    assert_eq!(live(&db, "list_value", "v_str = 'bind_within_timeblock'"), 1);
}

// ---------------------------------------------------------------------------
// v56 — drop_parameter cascade + orphan drop_value_list (D14/G1)
// ---------------------------------------------------------------------------

const V56_BASE: &str = r#"
template: {name: v56, version: "1"}
value_lists:
  - name: debug_levels
    members: [{value: "off"}, {value: "verbose"}]
classes:
  - name: solve
    parameters:
      - {name: debug, value_list: debug_levels}
      - {name: keeper, types: [{kind: float}]}
seed:
  entities:
    - {class: solve, name: s1}
    - {class: solve, name: s2}
"#;

const V56_V2: &str = r#"
template: {name: v56, version: "2"}
classes:
  - name: solve
    parameters:
      - {name: keeper, types: [{kind: float}]}
migrations:
  - from: "1"
    to: "2"
    steps:
      - drop_parameter: {class: solve, parameter: debug}
      - drop_value_list: {list: debug_levels}
"#;

#[test]
fn v56_drop_parameter_cascades_then_orphan_list_drops() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, V56_BASE);
    write_str(&db, "solve", "debug", "s1", "verbose");
    write_str(&db, "solve", "debug", "s2", "off");
    write_f(&db, "solve", "keeper", "s1", 1.0);

    // Plan: D14 blast radius — counts before apply, from the delete preview.
    let p = plan(&db, &parse_str(V56_V2).unwrap()).unwrap();
    let rendered = format!("{p}");
    assert!(rendered.contains("drop_parameter solve.debug"), "{rendered}");
    assert!(rendered.contains("value_scalar 2"), "expected blast counts:\n{rendered}");
    assert!(p.blocked.is_empty(), "{:?}", p.blocked);

    let out = apply_v2(&db, V56_V2);
    assert!(out.converged, "skips: {:?}", out.plan.skipped);
    // Cascade counts reported in the apply notes (D14 record).
    assert!(out.notes.iter().any(|n| n.contains("drop_parameter solve.debug")
            && n.contains("value_scalar 2")), "notes: {:?}", out.notes);
    // Definition + values closed; unrelated value untouched.
    assert_eq!(live(&db, "parameter_definition", "name = 'debug'"), 0);
    assert_eq!(cur_str(&db, "debug", "s1"), None);
    assert_eq!(cur_float(&db, "keeper", "s1"), Some(1.0));
    // The now-orphan list and its member rows closed — versioned, not erased.
    assert_eq!(live(&db, "parameter_value_list", "name = 'debug_levels'"), 0);
    assert_eq!(live(&db, "list_value", "v_str IN ('off', 'verbose')"), 0);
    assert!(q1(&db, "SELECT count(*) FROM list_value WHERE v_str = 'verbose'") > 0);
}

#[test]
fn drop_value_list_refuses_while_bound() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, V56_BASE);
    let v2 = r#"
template: {name: v56, version: "2"}
classes:
  - name: solve
    parameters:
      - {name: debug, value_list: debug_levels}
      - {name: keeper, types: [{kind: float}]}
value_lists:
  - name: debug_levels
    members: [{value: "off"}, {value: "verbose"}]
migrations:
  - from: "1"
    to: "2"
    steps:
      - drop_value_list: {list: debug_levels}
"#;
    // The plan predicts the refusal (simulated over the prior steps)…
    let p = plan(&db, &parse_str(v2).unwrap()).unwrap();
    assert!(p.blocked.iter().any(|b| b.contains("drop_value_list")), "{:?}", p.blocked);
    // …and apply refuses before any span commits.
    let err = apply(&db, &parse_str(v2).unwrap(), &MigrationRegistry::new(), "tester", "",
        &ApplyOptions::default()).unwrap_err().to_string();
    assert!(err.contains("drop_value_list"), "{err}");
    assert_eq!(live(&db, "parameter_value_list", "name = 'debug_levels'"), 1);
}

// ---------------------------------------------------------------------------
// v65 — rename with a value-translation table (G6)
// ---------------------------------------------------------------------------

const V65_BASE: &str = r#"
template: {name: v65, version: "1"}
value_lists:
  - name: yes_no
    members: [{value: "yes"}, {value: "no"}]
classes:
  - name: group
    parameters:
      - {name: has_capacity_margin, value_list: yes_no}
seed:
  entities:
    - {class: group, name: g1}
    - {class: group, name: g2}
"#;

const V65_V2: &str = r#"
template: {name: v65, version: "2"}
value_lists:
  - name: yes_no
    members: [{value: "yes"}, {value: "no"}]
  - name: capacity_margin_methods
    members: [{value: "none"}, {value: "manual"}]
classes:
  - name: group
    parameters:
      - name: capacity_margin_method
        value_list: capacity_margin_methods
        default: {kind: string, value: "none"}
migrations:
  - from: "1"
    to: "2"
    steps:
      - rename_parameter: {class: group, from: has_capacity_margin,
                           to: capacity_margin_method,
                           map_values: {yes: manual}, unmapped_values: drop}
"#;

#[test]
fn v65_translation_table_maps_yes_and_drops_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, V65_BASE);
    write_str(&db, "group", "has_capacity_margin", "g1", "yes");
    write_str(&db, "group", "has_capacity_margin", "g2", "no");

    let out = apply_v2(&db, V65_V2);
    assert!(out.converged, "skips: {:?}", out.plan.skipped);
    // yes -> manual under the renamed definition; 'no' dropped to the default.
    assert_eq!(cur_str(&db, "capacity_margin_method", "g1").as_deref(), Some("manual"));
    assert_eq!(cur_str(&db, "capacity_margin_method", "g2"), None);
    assert_eq!(live(&db, "parameter_definition", "name = 'has_capacity_margin'"), 0);
    // The dropped value is a versioned close-out, not erasure.
    assert!(q1(&db, "SELECT count(*) FROM value_scalar WHERE v_str = 'no'") > 0);
    // Reconcile rebound the definition to the new list + set the default.
    assert_eq!(qs(&db,
        "SELECT l.name FROM parameter_definition pd \
         JOIN parameter_value_list l ON l.id = pd.parameter_value_list_id \
           AND l.valid_to_commit IS NULL \
         WHERE pd.cur_name = 'capacity_margin_method'").as_deref(),
        Some("capacity_margin_methods"));
    assert!(out.notes.iter().any(|n| n.contains("translated 1 stored value(s)")),
        "notes: {:?}", out.notes);
}

#[test]
fn v65_unmapped_error_policy_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, V65_BASE);
    write_str(&db, "group", "has_capacity_margin", "g2", "no");
    let v2 = V65_V2.replace("unmapped_values: drop", "unmapped_values: error");
    let err = apply(&db, &parse_str(&v2).unwrap(), &MigrationRegistry::new(), "tester", "",
        &ApplyOptions::default()).unwrap_err().to_string();
    assert!(err.contains("unmapped_values: error"), "{err}");
    // Nothing renamed — the step's commit rolled back.
    assert_eq!(live(&db, "parameter_definition", "name = 'has_capacity_margin'"), 1);
}

// ---------------------------------------------------------------------------
// drop_class — full cascade with asserted blast radius (D14)
// ---------------------------------------------------------------------------

const DROP_CLASS_BASE: &str = r#"
template: {name: dc, version: "1"}
classes:
  - name: group
    parameters:
      - {name: x, types: [{kind: float}]}
  - name: node
seed:
  entities:
    - {class: group, name: g1}
    - {class: group, name: g2}
    - {class: node, name: n1}
"#;

const DROP_CLASS_V2: &str = r#"
template: {name: dc, version: "2"}
classes:
  - name: node
migrations:
  - from: "1"
    to: "2"
    steps:
      - drop_class: {class: group}
"#;

#[test]
fn drop_class_cascades_entities_parameters_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, DROP_CLASS_BASE);
    write_f(&db, "group", "x", "g1", 1.0);
    write_f(&db, "group", "x", "g2", 2.0);

    let p = plan(&db, &parse_str(DROP_CLASS_V2).unwrap()).unwrap();
    let rendered = format!("{p}");
    assert!(rendered.contains("drop_class group") && rendered.contains("value_scalar 2"),
        "expected class blast radius:\n{rendered}");

    let out = apply_v2(&db, DROP_CLASS_V2);
    assert!(out.converged, "skips: {:?}", out.plan.skipped);
    assert_eq!(live(&db, "entity_class", "name = 'group'"), 0);
    assert_eq!(live(&db, "entity", "name IN ('g1','g2')"), 0);
    assert_eq!(live(&db, "parameter_definition", "name = 'x'"), 0);
    assert_eq!(cur_float(&db, "x", "g1"), None);
    // Versioned close-out, never physical erasure (D14 altitude constraint).
    assert!(q1(&db, "SELECT count(*) FROM entity WHERE name = 'g1'") > 0);
    assert_eq!(live(&db, "entity", "name = 'n1'"), 1, "unrelated class untouched");
}

// ---------------------------------------------------------------------------
// drop_list_member — the stranded-value SKIP withholds convergence (D14)
// ---------------------------------------------------------------------------

const DLM_BASE: &str = r#"
template: {name: dlm, version: "1"}
value_lists:
  - name: methods
    members: [{value: "keepme"}, {value: "stranded"}, {value: "unused"}]
classes:
  - name: solve
    parameters:
      - {name: method, value_list: methods}
seed:
  entities:
    - {class: solve, name: s1}
"#;

const DLM_V2: &str = r#"
template: {name: dlm, version: "2"}
value_lists:
  - name: methods
    members: [{value: "keepme"}]
classes:
  - name: solve
    parameters:
      - {name: method, value_list: methods}
migrations:
  - from: "1"
    to: "2"
    steps:
      - drop_list_member: {list: methods, value: stranded}
      - drop_list_member: {list: methods, value: unused}
"#;

#[test]
fn drop_list_member_stranded_pauses_chain_then_retry_converges() {
    // The reviewer's probe (2026-09-22 review, HIGH): a skipped drop must
    // NOT substamp, its span must NOT version-stamp, a re-apply must NOT
    // certify while blocked and must RETRY the paused step; once unblocked,
    // the retry completes the chain and converges.
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, DLM_BASE);
    write_str(&db, "solve", "method", "s1", "stranded");

    // apply#1 — pauses at step 0.
    let out = apply_v2(&db, DLM_V2);
    assert!(!out.converged);
    assert!(out.warnings.iter().any(|w| w.contains("PAUSED") && w.contains("RETRIES")),
        "warnings: {:?}", out.warnings);
    assert!(out.plan.skipped.iter().any(|s| s.contains("drop_list_member")
            && s.contains("unblock")), "skips: {:?}", out.plan.skipped);
    // Member kept, value untouched; the step AFTER the pause never ran.
    assert_eq!(live(&db, "list_value", "v_str = 'stranded'"), 1);
    assert_eq!(cur_str(&db, "method", "s1").as_deref(), Some("stranded"));
    assert_eq!(live(&db, "list_value", "v_str = 'unused'"), 1,
        "steps after the paused one must not execute");
    // Version did NOT advance; no substamp written; no certified hash.
    assert_eq!(qs(&db, "SELECT value FROM metadata WHERE cur_name = 'template_version'")
        .as_deref(), Some("1"));
    assert_eq!(qs(&db, "SELECT value FROM metadata WHERE cur_name = 'template_migration_step'"),
        None, "a skipped step must not substamp");
    assert_eq!(qs(&db, "SELECT value FROM metadata WHERE cur_name = 'template_content_hash'"),
        None, "no certification behind a paused chain");

    // apply#2, still blocked — retries, pauses again, still not certified.
    let out2 = apply_v2(&db, DLM_V2);
    assert!(!out2.converged);
    assert!(out2.warnings.iter().any(|w| w.contains("PAUSED")), "{:?}", out2.warnings);
    assert_eq!(qs(&db, "SELECT value FROM metadata WHERE cur_name = 'template_version'")
        .as_deref(), Some("1"), "version stays until the blocker clears");
    assert_eq!(qs(&db, "SELECT value FROM metadata WHERE cur_name = 'template_content_hash'"),
        None, "apply#2 must NOT certify while the drop stays blocked");
    assert_eq!(live(&db, "list_value", "v_str = 'stranded'"), 1);

    // Mop up the stranding value, then apply#3 — the RETRY completes the
    // chain (both drops), stamps the version, and certifies.
    nodal_db_core::api::delete_values(
        &db,
        vec![nodal_db_core::api::DeleteKey {
            class: "solve".into(),
            parameter: "method".into(),
            alternative: "alt".into(),
            entity: "s1".into(),
        }],
        "tester",
        "mop up the stranding value",
        WriteOptions::default(),
    )
    .unwrap();
    let out3 = apply_v2(&db, DLM_V2);
    assert!(out3.converged, "skips: {:?}", out3.plan.skipped);
    assert_eq!(live(&db, "list_value", "v_str IN ('stranded', 'unused')"), 0);
    assert_eq!(qs(&db, "SELECT value FROM metadata WHERE cur_name = 'template_version'")
        .as_deref(), Some("2"));
    assert!(qs(&db, "SELECT value FROM metadata WHERE cur_name = 'template_content_hash'")
        .is_some());
}

#[test]
fn drop_list_member_clean_case_closes_the_member() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db_with(&dir, DLM_BASE);
    // No values at all: both drops execute, apply converges, hash certifies.
    let out = apply_v2(&db, DLM_V2);
    assert!(out.converged, "skips: {:?}", out.plan.skipped);
    assert_eq!(live(&db, "list_value", "v_str IN ('stranded', 'unused')"), 0);
    assert_eq!(live(&db, "list_value", "v_str = 'keepme'"), 1);
    assert!(qs(&db, "SELECT value FROM metadata WHERE cur_name = 'template_content_hash'")
        .is_some());
}
