//! Pure IR tests (template_ir_plan §2): YAML 1.2 parsing (the Norway problem
//! stays closed in every serde context), JSON through the same parser,
//! anchors, strict unknown-field rejection, load-time validation errors, and
//! normalization/content-hash semantics (§4.1).

use nodal_db_core::template::ir::{
    self, normalized_model, parse_str, template_content_hash, validate, DimRef, Lit,
};

const SAMPLE: &str = include_str!("fixtures/template_sample.yaml");

fn minimal(extra: &str) -> String {
    format!(
        "template: {{name: t, version: \"1\"}}\nclasses:\n  - name: c\n{extra}"
    )
}

#[test]
fn norway_problem_is_closed_everywhere() {
    // Unquoted yes/no/on/off resolve as STRINGS under the YAML 1.2 core
    // schema — including inside the untagged Lit enum (deserialize_any), the
    // context where 1.1-flavored parsers regress (the reason serde-saphyr was
    // rejected; ir.rs module docs).
    let y = "
template: {name: t, version: \"1\"}
value_lists:
  - name: l
    members:
      - {value: yes}
      - {value: no}
      - {value: on}
      - {value: off}
classes:
  - name: c
    parameters:
      - name: p
        value_list: l
        default: {kind: string, value: yes}
";
    let t = parse_str(y).unwrap();
    let vals: Vec<&Lit> = t.value_lists[0].members.iter().map(|m| &m.value).collect();
    for (i, expect) in ["yes", "no", "on", "off"].iter().enumerate() {
        assert_eq!(vals[i], &Lit::Str(expect.to_string()), "member {i}");
    }
    let dv = t.classes[0].parameters[0].default.as_ref().unwrap();
    assert_eq!(dv.value, Lit::Str("yes".into()));
    // And true/false ARE booleans (core schema keeps them).
    let t2 = parse_str(
        "template: {name: t, version: \"1\"}\nvalue_lists:\n  - name: l\n    members: [{value: true}]\nclasses: [{name: c}]",
    )
    .unwrap();
    assert_eq!(t2.value_lists[0].members[0].value, Lit::Bool(true));
}

#[test]
fn version_number_coerces_to_string_not_type_flaps() {
    // `version: 69` and `version: "69"` load IDENTICALLY (the serde_norway
    // coercion unifies the type-flap; ir.rs module docs deviation note).
    let a = parse_str("template: {name: t, version: 69}\nclasses: [{name: c}]").unwrap();
    let b = parse_str("template: {name: t, version: \"69\"}\nclasses: [{name: c}]").unwrap();
    assert_eq!(a.template.version, "69");
    assert_eq!(template_content_hash(&a), template_content_hash(&b));
}

#[test]
fn unknown_fields_are_rejected_except_unit_registry_extras() {
    // Typo'd keys fail at parse (strict-typing mitigation (b), §2.1)…
    let bad = minimal("    parameters:\n      - {name: p, typez: []}\n");
    assert!(parse_str(&bad).is_err());
    let bad2 = "template: {name: t, version: \"1\", versoin: 2}\nclasses: [{name: c}]";
    assert!(parse_str(bad2).is_err());
    // …but unit registry extras are tolerated, warned, and dropped (D7).
    let y = "template: {name: t, version: \"1\"}\nunits:\n  - {name: u, ucum: kW, factors: {a: 1}}\nclasses: [{name: c}]";
    let t = parse_str(y).unwrap();
    let chk = validate(&t).unwrap();
    assert_eq!(t.units[0].registry_extras.len(), 2);
    assert!(
        chk.warnings.iter().any(|w| w.contains("registry fields") && w.contains("ucum")),
        "expected a dropped-registry-fields warning, got {:?}",
        chk.warnings
    );
    // …and the extras never reach the normalized model (hash-invisible).
    let plain = parse_str(
        "template: {name: t, version: \"1\"}\nunits: [{name: u}]\nclasses: [{name: c}]",
    )
    .unwrap();
    assert_eq!(template_content_hash(&t), template_content_hash(&plain));
}

#[test]
fn json_parses_through_the_same_loader() {
    let json = r#"{
      "template": {"name": "t", "version": "1"},
      "classes": [
        {"name": "a"},
        {"name": "b", "dimensions": ["a", {"class_set": "s"}]}
      ],
      "class_sets": [{"name": "s", "members": ["a"]}]
    }"#;
    let t = parse_str(json).unwrap();
    assert_eq!(t.classes[1].dimensions[0], DimRef::Class("a".into()));
    assert_eq!(t.classes[1].dimensions[1], DimRef::Set { class_set: "s".into() });
    validate(&t).unwrap();
}

#[test]
fn anchors_expand_and_never_affect_the_hash() {
    let t = parse_str(SAMPLE).unwrap();
    validate(&t).unwrap();
    // The anchored types block expanded into both parameters.
    let node = t.classes.iter().find(|c| c.name == "node").unwrap();
    let r4 = t.classes.iter().find(|c| c.name == "reserve__upDown__unit__node").unwrap();
    let demand = &node.parameters[0];
    let max_share = r4.parameters.iter().find(|p| p.name == "max_share").unwrap();
    assert_eq!(demand.types, max_share.types);
    // Round-trip through the emitter (anchors gone) is hash-identical (§8.1.7).
    let emitted = ir::to_yaml(&t).unwrap();
    assert!(!emitted.contains("&float_or_t"), "anchors must not survive re-emission");
    let back = parse_str(&emitted).unwrap();
    assert_eq!(template_content_hash(&t), template_content_hash(&back));
    assert_eq!(normalized_model(&t), normalized_model(&back));
}

#[test]
fn normalization_is_order_and_literal_insensitive() {
    let a = parse_str(
        "template: {name: t, version: \"1\"}
classes:
  - name: b
    parameters:
      - name: p
        types:
          - {kind: indexed, dims: [{name: t, kind: datetime}]}
          - {kind: float}
        default: {kind: float, value: 1}
  - name: a
",
    )
    .unwrap();
    let b = parse_str(
        "template: {name: t, version: \"1\"}
classes:
  - name: a
  - name: b
    parameters:
      - name: p
        types:
          - {kind: float}
          - {kind: indexed, dims: [{name: t, kind: datetime}]}
        default: {kind: float, value: 1.0}
",
    )
    .unwrap();
    // Class order, types order (no semantics, §3.1), and int-vs-float default
    // spelling (unified under the declared kind) all normalize away.
    assert_eq!(normalized_model(&a), normalized_model(&b));
    assert_eq!(template_content_hash(&a), template_content_hash(&b));
}

#[test]
fn migrations_block_is_excluded_from_the_hash() {
    let with = parse_str(
        "template: {name: t, version: \"2\"}
classes: [{name: c}]
migrations:
  - {from: \"1\", to: \"2\", steps: [{rename_class: {from: x, to: c}}]}
",
    )
    .unwrap();
    let without =
        parse_str("template: {name: t, version: \"2\"}\nclasses: [{name: c}]").unwrap();
    validate(&with).unwrap();
    assert_eq!(template_content_hash(&with), template_content_hash(&without));
}

// --- load-time validation errors (§2.2: caught before any write) ---

fn expect_err(yaml: &str, needle: &str) {
    let t = parse_str(yaml).unwrap_or_else(|e| panic!("parse failed ({e}) for:\n{yaml}"));
    let err = validate(&t).err().unwrap_or_else(|| panic!("expected error {needle:?}"));
    let msg = err.to_string();
    assert!(msg.contains(needle), "expected {needle:?} in:\n{msg}");
}

#[test]
fn validation_catches_dangling_and_malformed_references() {
    expect_err(
        "template: {name: t, version: \"1\"}\nclasses: [{name: c, dimensions: [ghost]}]",
        "is not a declared class",
    );
    expect_err(
        "template: {name: t, version: \"1\"}\nclasses: [{name: c, dimensions: [{class_set: ghost}]}]",
        "is not a declared class_set",
    );
    expect_err(
        "template: {name: t, version: \"1\"}\nclasses: [{name: c, parameters: [{name: p, value_list: ghost}]}]",
        "is not declared",
    );
    expect_err(
        "template: {name: t, version: \"1\"}\nclasses: [{name: c, parameters: [{name: p, unit: ghost}]}]",
        "is not a declared unit",
    );
    expect_err(
        "template: {name: t, version: \"1\"}\nclasses: [{name: c, parameters: [{name: p, group: ghost}]}]",
        "is not a declared parameter_group",
    );
    expect_err(
        "template: {name: t, version: \"1\"}\nclasses: [{name: c}, {name: C}]",
        "duplicate class name",
    );
    expect_err(
        "template: {name: t, version: \"1\"}\nclasses: [{name: \"bad name\"}]",
        "must match [A-Za-z0-9_]+",
    );
}

#[test]
fn validation_refuses_indistinguishable_type_sets() {
    // Same (kind, dim names, dim kinds) twice — the D2a refusal, at load.
    expect_err(
        "template: {name: t, version: \"1\"}
classes:
  - name: c
    parameters:
      - name: p
        types:
          - {kind: indexed, dims: [{name: T, kind: datetime}]}
          - {kind: indexed, dims: [{name: t, kind: datetime}]}
",
        "indistinguishable",
    );
    // Distinguishable-by-name duplicates of one rank are LEGAL (§3.1).
    let ok = parse_str(
        "template: {name: t, version: \"1\"}
classes:
  - name: c
    parameters:
      - name: p
        types:
          - {kind: indexed, dims: [{name: period, kind: period}]}
          - {kind: indexed, dims: [{name: t, kind: datetime}]}
",
    )
    .unwrap();
    validate(&ok).unwrap();
}

#[test]
fn validation_checks_defaults_and_lists() {
    // Default outside its own value list (§3.3.4 — fail fast at load).
    expect_err(
        "template: {name: t, version: \"1\"}
value_lists:
  - name: l
    members: [{value: \"a\"}, {value: \"b\"}]
classes:
  - name: c
    parameters:
      - name: p
        value_list: l
        default: {kind: string, value: \"zzz\"}
",
        "is not a member of value_list",
    );
    // Default kind must instantiate a declared rank-0 member.
    expect_err(
        "template: {name: t, version: \"1\"}
classes:
  - name: c
    parameters:
      - name: p
        types: [{kind: indexed, dims: [{name: t, kind: datetime}]}]
        default: {kind: float, value: 1.0}
",
        "instantiates no declared rank-0",
    );
    // Mixed-kind list cannot infer a parameter kind (§3.5).
    expect_err(
        "template: {name: t, version: \"1\"}
value_lists:
  - name: l
    members: [{value: \"a\"}, {value: 3}]
classes:
  - name: c
    parameters:
      - name: p
        value_list: l
",
        "members mix kinds",
    );
    // Boolean-where-string fails loudly (the type-flap guard, §2.1 (b)).
    expect_err(
        "template: {name: t, version: \"1\"}
value_lists:
  - name: l
    members: [{value: true, kind: string}]
classes: [{name: c}]
",
        "does not instantiate declared kind",
    );
}

#[test]
fn validation_checks_seed_shape_and_set_scoped_names() {
    // Arity mismatch.
    expect_err(
        "template: {name: t, version: \"1\"}
classes:
  - {name: a}
  - {name: b, dimensions: [a, a]}
seed:
  entities:
    - {class: a, name: x}
    - {class: b, elements: [x]}
",
        "1 elements for a 2-D class",
    );
    // Element not a declared seed entity (D6 closure).
    expect_err(
        "template: {name: t, version: \"1\"}
classes:
  - {name: a}
  - {name: b, dimensions: [a]}
seed:
  entities:
    - {class: b, elements: [ghost]}
",
        "does not name a declared seed entity",
    );
    // Cross-member name duplication within a class-set
    // (audit_class_set_entity_name_dup, at load).
    expect_err(
        "template: {name: t, version: \"1\"}
classes: [{name: a}, {name: b}]
class_sets: [{name: s, members: [a, b]}]
seed:
  entities:
    - {class: a, name: x}
    - {class: b, name: x}
",
        "set-scoped names must be unique",
    );
}

#[test]
fn validation_checks_audit_rules_references() {
    expect_err(
        "template: {name: t, version: \"1\"}
value_lists:
  - name: l
    members: [{value: \"m1\"}]
classes:
  - name: c
    parameters:
      - {name: sel, value_list: l}
audit_rules:
  conditional_requirements:
    - class: c
      selector: sel
      when:
        m1: {require: [ghost]}
",
        "is not a parameter of class",
    );
}

#[test]
fn class_set_shadowing_warns_but_loads() {
    let t = parse_str(
        "template: {name: t, version: \"1\"}
classes: [{name: s}, {name: c, dimensions: [s]}]
class_sets: [{name: s, members: [c]}]
",
    )
    .unwrap();
    let chk = validate(&t).unwrap();
    assert!(
        chk.warnings.iter().any(|w| w.contains("shadows a class")),
        "expected shadow warning, got {:?}",
        chk.warnings
    );
}
