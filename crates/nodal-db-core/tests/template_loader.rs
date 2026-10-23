//! Loader v1 fresh-init acceptance (template_ir_plan §8.1.2/8.1.3/8.1.5/8.1.6
//! shape, on the sample vocabulary): load into a de-Based DB in one commit,
//! all structural audits empty, identity stamped, idempotent re-application,
//! writes at every declared member accepted / undeclared shapes rejected, and
//! the class-set-with-transposed-N-D-members case end-to-end (§8.2.2).

use std::path::Path;
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMicrosecondArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::write_parameter;
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::template::{
    apply, parse_str, template_content_hash, ApplyOptions, MigrationRegistry,
};
use nodal_db_core::types::{WarningKind, WriteOptions};

const SAMPLE: &str = include_str!("fixtures/template_sample.yaml");

fn fresh_db(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    db
}

fn load_sample(db: &Path) -> nodal_db_core::template::ApplyOutcome {
    let t = parse_str(SAMPLE).unwrap();
    apply(
        db,
        &t,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap()
}

fn q1(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn qs(db: &Path, sql: &str) -> Option<String> {
    let conn = Connection::open(db).unwrap();
    conn.query_row(sql, [], |r| r.get::<_, String>(0)).ok()
}

fn meta(db: &Path, key: &str) -> Option<String> {
    qs(
        db,
        &format!("SELECT value FROM metadata WHERE cur_name = '{key}'"),
    )
}

/// Every audit view that must be EMPTY after a clean load (§8.1.2 plus the
/// full referential/typing sweep). Completeness/advisory views that
/// legitimately report on a vocabulary-only DB (missing required values,
/// empty scenarios, redundant activation) are deliberately not listed.
const MUST_BE_EMPTY: &[&str] = &[
    "audit_dangling_ref",
    "audit_dangling_semantic_ref",
    "audit_class_set_entity_name_dup",
    "audit_element_class_mismatch",
    "audit_element_arity",
    "audit_value_class_mismatch",
    "audit_axis_integrity",
    "audit_position_range",
    "audit_type_conformance",
    "audit_array_integrity",
    "audit_array_payload_xor",
    "audit_index_accelerator",
    "audit_cross_tier_duplicate",
    "audit_tier_intent",
    "audit_value_list_membership",
    "audit_metadata_reserved_dup",
    "audit_membership_cycle",
    "audit_realizes_cycle",
    "audit_history_tiling",
    "audit_array_id_coherence",
];

fn assert_audits_empty(db: &Path) {
    for view in MUST_BE_EMPTY {
        let n = q1(db, &format!("SELECT count(*) FROM {view}"));
        assert_eq!(n, 0, "{view} is not empty after load");
    }
}

#[test]
fn fresh_load_is_one_commit_audits_empty_and_stamped() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    let commits_before = q1(&db, "SELECT max(id) FROM commit");

    let out = load_sample(&db);
    assert!(!out.no_op);
    // One commit for the whole load (§7: "a template load is a commit").
    assert_eq!(out.commits.len(), 1, "fresh load must be ONE commit");
    assert_eq!(q1(&db, "SELECT max(id) FROM commit"), commits_before + 1);

    assert_audits_empty(&db);

    // Identity stamps (§4.1), distinct from schema_version (§8.1.6).
    assert_eq!(meta(&db, "template_name").as_deref(), Some("sample"));
    assert_eq!(meta(&db, "template_version").as_deref(), Some("3"));
    let t = parse_str(SAMPLE).unwrap();
    assert_eq!(
        meta(&db, "template_content_hash").as_deref(),
        Some(template_content_hash(&t).as_str())
    );
    assert!(meta(&db, "schema_version").is_some());
    assert_ne!(meta(&db, "schema_version"), meta(&db, "template_version"));

    // Structure spot-checks against the store.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity_class WHERE valid_to_commit IS NULL"
        ),
        9
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity_class WHERE valid_to_commit IS NULL \
                 AND NOT active_by_default"
        ),
        2, // node + unit (fidelity §8.1.3 shape)
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM class_set WHERE valid_to_commit IS NULL"
        ),
        1
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM class_set_member WHERE valid_to_commit IS NULL"
        ),
        2
    );
    // 4-D class has 4 dimension rows.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity_class_dimension d \
                 JOIN entity_class c ON c.id = d.entity_class_id \
                 WHERE d.valid_to_commit IS NULL AND c.cur_name = 'reserve__updown__unit__node'"
        ),
        4
    );
    // Multi-member type set: node.demand declares float + datetime-indexed.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM parameter_type pt \
                 JOIN parameter_definition pd ON pd.id = pt.parameter_definition_id \
                 WHERE pt.valid_to_commit IS NULL AND pd.cur_name = 'demand'"
        ),
        2
    );
    // Value-list-inferred kind (§3.5): conversion_method got ONE string rank-0 row.
    assert_eq!(
        qs(
            &db,
            "SELECT pt.value_kind FROM parameter_type pt \
                 JOIN parameter_definition pd ON pd.id = pt.parameter_definition_id \
                 WHERE pt.valid_to_commit IS NULL AND pd.cur_name = 'conversion_method'"
        )
        .as_deref(),
        Some("string")
    );
    // Explicit types + value_list (review item 11 fixture): co2_cap keeps its
    // two declared members; co2_flag's explicit-but-inferable block landed as
    // the one string member; the float default 20 (int literal, kind float)
    // is in-list (audit_value_list_membership empty above).
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM parameter_type pt \
                 JOIN parameter_definition pd ON pd.id = pt.parameter_definition_id \
                 WHERE pt.valid_to_commit IS NULL AND pd.cur_name = 'co2_cap'"
        ),
        2
    );
    assert_eq!(
        qs(
            &db,
            "SELECT pt.value_kind FROM parameter_type pt \
                 JOIN parameter_definition pd ON pd.id = pt.parameter_definition_id \
                 WHERE pt.valid_to_commit IS NULL AND pd.cur_name = 'co2_flag'"
        )
        .as_deref(),
        Some("string")
    );
    // Typeless definition loaded with NO parameter_type row (§2.2).
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM parameter_type pt \
                 JOIN parameter_definition pd ON pd.id = pt.parameter_definition_id \
                 WHERE pt.valid_to_commit IS NULL AND pd.cur_name = 'notes'"
        ),
        0
    );
    // Defaults landed in the definition mirror with a D11 binding.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM definition_value_scalar \
                 WHERE valid_to_commit IS NULL AND slot = 'default_value' \
                 AND parameter_type_id IS NOT NULL"
        ),
        4 // demand 0.0, conversion_method, is_enabled "yes", co2_cap 20.0
    );
    // Seed: 9 entities incl. the N-D ones; elements; one base membership.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity WHERE valid_to_commit IS NULL"
        ),
        9
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM membership WHERE valid_to_commit IS NULL \
                 AND alternative_id IS NULL"
        ),
        1
    );
    // The INES-shaped case (§8.2.2): flowpair's elements are entities of the
    // TRANSPOSED 2-D member classes, accepted through the class-set — and
    // audit_element_class_mismatch / audit_element_arity are empty (above).
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity_element ee \
                 JOIN entity e ON e.id = ee.entity_id \
                 WHERE ee.valid_to_commit IS NULL AND e.cur_name = 'flowpair'"
        ),
        2
    );
    // Audit-rules document persisted (D4) and single-current (reserved key).
    let rules = meta(&db, "audit_rules").expect("audit_rules stamped");
    assert!(rules.contains("conditional_requirements"));
    // Groups persisted in the presentation document (D8).
    let pres = meta(&db, "presentation").expect("presentation stamped");
    let doc: serde_json::Value = serde_json::from_str(&pres).unwrap();
    assert!(doc.get("parameter_groups").is_some());
    assert_eq!(
        doc["parameter_group_membership"]["node.demand"],
        serde_json::json!("basics")
    );
}

#[test]
fn reapplication_is_a_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    load_sample(&db);
    let commits = q1(&db, "SELECT max(id) FROM commit");
    let out = load_sample(&db);
    assert!(
        out.no_op,
        "re-applying an unchanged template must be a no-op"
    );
    assert!(out.commits.is_empty());
    assert_eq!(
        q1(&db, "SELECT max(id) FROM commit"),
        commits,
        "no commit burned"
    );
}

// --- writes against the loaded declarations (D2a semantics, §8.1.4) ---

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

fn datetime_batch(entity: &str, dim: &str, ts: &[i64], vals: &[f64]) -> RecordBatch {
    let n = ts.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new(dim, DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; n])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(ts.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
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

#[test]
fn writes_match_declared_members_and_reject_undeclared_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    load_sample(&db);

    // Member 1: rank-0 float on the multi-member set.
    write_parameter(
        &db,
        "node",
        "demand",
        "alt",
        scalar_batch("n1", 2.5),
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap();
    // Member 2: datetime-indexed on the SAME definition (multi-type
    // end-to-end, the D2 acceptance shape) for a different entity.
    write_parameter(
        &db,
        "node",
        "demand",
        "alt",
        datetime_batch("n2", "t", &[1_000_000, 2_000_000], &[1.0, 2.0]),
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap();
    // Undeclared shape (label-indexed) rejects at entry, naming the set.
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("zone", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let bad = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["n1"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["z"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
        ],
    )
    .unwrap();
    let err = write_parameter(
        &db,
        "node",
        "demand",
        "alt",
        bad,
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("instantiates none of the 2 declared member"),
        "unexpected: {err}"
    );

    // Typeless definition materializes on first write, loudly (§2.2).
    let report = write_parameter(
        &db,
        "unit",
        "notes",
        "alt",
        string_batch("u1", "hello"),
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap();
    assert!(report
        .warnings
        .iter()
        .any(|w| matches!(w.kind, WarningKind::AutoCreated) && w.object_type == "parameter_type"));

    // The audits stay clean with data in place (value-list membership etc.).
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
    assert_audits_empty(&db);
}
