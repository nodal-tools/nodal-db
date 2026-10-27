//! Round-trip acceptance (template_ir_plan §8.1.7 / §8.2.5): load → export →
//! the exported document is SEMANTICALLY equal to the source under the
//! normalized object model (the same normalization that feeds
//! `template_content_hash`); loading the export into a second fresh DB yields
//! the same content hash and an immediate no-op on re-apply.

use nodal_db_core::schema_init::init_schema;
use nodal_db_core::template::ir::normalized_model;
use nodal_db_core::template::{
    apply, export, parse_str, template_content_hash, to_json, to_yaml, ApplyOptions,
    MigrationRegistry,
};

const SAMPLE: &str = include_str!("fixtures/template_sample.yaml");

fn opts() -> ApplyOptions {
    ApplyOptions::default()
}

#[test]
fn load_export_reload_is_semantically_identical() {
    let dir = tempfile::tempdir().unwrap();
    let db1 = dir.path().join("a.duckdb");
    init_schema(&db1).unwrap();

    let source = parse_str(SAMPLE).unwrap();
    apply(
        &db1,
        &source,
        &MigrationRegistry::new(),
        "tester",
        "",
        &opts(),
    )
    .unwrap();

    // Export re-derives the IR purely from the database. The only expected
    // warning is the ALWAYS-on seed-block honesty notice (review item 15).
    let (exported, warnings) = export(&db1).unwrap();
    assert_eq!(warnings.len(), 1, "unexpected warnings: {warnings:?}");
    assert!(
        warnings[0].contains("seed block carries ALL"),
        "unexpected warning: {}",
        warnings[0]
    );

    // Semantic equality: normalized object models match (NOT textual —
    // anchors are parse-time, ordering is normalized; §8.1.7).
    assert_eq!(
        normalized_model(&source),
        normalized_model(&exported),
        "normalized models differ:\nsource:\n{}\nexport:\n{}",
        serde_json::to_string_pretty(&normalized_model(&source)).unwrap(),
        serde_json::to_string_pretty(&normalized_model(&exported)).unwrap(),
    );
    assert_eq!(
        template_content_hash(&source),
        template_content_hash(&exported)
    );

    // The emitted YAML and JSON both re-parse to the same model (one loader,
    // both surfaces — D1).
    let yaml = to_yaml(&exported).unwrap();
    let json = to_json(&exported).unwrap();
    let from_yaml = parse_str(&yaml).unwrap();
    let from_json = parse_str(&json).unwrap();
    assert_eq!(normalized_model(&from_yaml), normalized_model(&source));
    assert_eq!(normalized_model(&from_json), normalized_model(&source));

    // Load the export into a SECOND fresh DB: same stamped hash, and
    // re-applying the ORIGINAL source is a no-op there (full circle).
    let db2 = dir.path().join("b.duckdb");
    init_schema(&db2).unwrap();
    apply(
        &db2,
        &from_yaml,
        &MigrationRegistry::new(),
        "tester",
        "",
        &opts(),
    )
    .unwrap();
    let out = apply(
        &db2,
        &source,
        &MigrationRegistry::new(),
        "tester",
        "",
        &opts(),
    )
    .unwrap();
    assert!(
        out.no_op,
        "source must no-op on a DB loaded from its own export"
    );

    // And the two databases export identically.
    let (e1, _) = export(&db1).unwrap();
    let (e2, _) = export(&db2).unwrap();
    assert_eq!(normalized_model(&e1), normalized_model(&e2));
}

/// A value list whose name is in the former drift set (final sigma): the
/// template binds it by a case variant that only DuckDB's `lower()`, now
/// also the core's fold, makes equal (`ΟΔΟΣ` = `οδοσ`; Rust's
/// `to_lowercase` kept them apart). It loads, the export finds the list by
/// the same fold (`export.rs` probes the vocabulary map keyed by
/// `types::fold`) and elides the inferable types, and the export reloads to
/// the same model (name_fold_identity_PLAN K2/K9).
#[test]
fn drift_set_value_list_name_round_trips() {
    let yaml = "template: {name: t, version: \"1\"}
value_lists:
  - name: ΟΔΟΣ
    members:
      - {value: a}
      - {value: b}
classes:
  - name: c
    parameters:
      - name: p
        value_list: οδοσ
";
    let dir = tempfile::tempdir().unwrap();
    let db1 = dir.path().join("a.duckdb");
    init_schema(&db1).unwrap();
    let source = parse_str(yaml).unwrap();
    apply(
        &db1,
        &source,
        &MigrationRegistry::new(),
        "tester",
        "",
        &opts(),
    )
    .unwrap();
    let (exported, _) = export(&db1).unwrap();
    let p = &exported.classes[0].parameters[0];
    assert_eq!(
        p.value_list.as_deref(),
        Some("ΟΔΟΣ"),
        "bound to the stored list"
    );
    assert!(p.types.is_none(), "inferable from the list: elided");
    let db2 = dir.path().join("b.duckdb");
    init_schema(&db2).unwrap();
    apply(
        &db2,
        &exported,
        &MigrationRegistry::new(),
        "tester",
        "",
        &opts(),
    )
    .unwrap();
    let (again, _) = export(&db2).unwrap();
    assert_eq!(normalized_model(&exported), normalized_model(&again));
    assert_eq!(
        template_content_hash(&exported),
        template_content_hash(&again)
    );
}
