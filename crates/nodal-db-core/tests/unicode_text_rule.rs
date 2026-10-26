//! The text rule (`text.rs`; `specs/unicode_text_rule_PLAN.md` T1, T2, T6,
//! T7): names and index labels are stored and compared in NFC; string
//! values are stored as written.
//!
//! - Labels: an NFD label is written, read back in NFC, and reported once
//!   per segment; a domain and its data compare in NFC; a domain with both
//!   spellings is a repeat and a value with both is a duplicate tuple, each
//!   with a message that shows the two spellings.
//! - The axis bytes do not depend on which spelling was written first.
//! - The axis writer refuses a hand-built axis with a non-NFC or repeated
//!   label (`MalformedAxis`).
//! - Names: an NFD name is stored in NFC, every lookup finds it by either
//!   spelling (reads, value deletes, object deletes, structural renames,
//!   the verbs, erasure), and two spellings in one call are one name.
//! - Template names load in NFC and hash as their NFC spelling.

use std::collections::HashMap;
use std::sync::Arc;

use duckdb::arrow::array::{Array, ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{
    commit_structural, delete_objects, delete_values, ingest_segments, read_parameter, read_values,
    DeleteKey, Segment,
};
use nodal_db_core::axis::{dedup_or_insert, AxisCache};
use nodal_db_core::canon::{canonicalize, index_domains, CanonError, LabelDomain};
use nodal_db_core::conn::with_write_txn;
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis, LabelPos};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::template;
use nodal_db_core::types::{
    DeclaredType, IndexKind, ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef,
    StructuralChange, StructuralEffect, ValueKind, Warning, WarningKind, WriteOptions, WriteReport,
};
use nodal_db_core::verbs;

/// "é" precomposed (NFC) and decomposed (NFD).
const E_NFC: &str = "\u{e9}";
const E_NFD: &str = "e\u{301}";
/// "Pää" in NFC and NFD.
const PAA_NFC: &str = "P\u{e4}\u{e4}";
const PAA_NFD: &str = "Pa\u{308}a\u{308}";

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("text.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("schema");
    (dir, db)
}

fn query_strings(db: &std::path::Path, sql: &str) -> Vec<String> {
    let conn = Connection::open(db).unwrap();
    let mut stmt = conn.prepare(sql).unwrap();
    let out = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(stmt);
    conn.close().unwrap();
    out
}

fn count(db: &std::path::Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

/// Rank-1 label batch `(entity, mode, value)`; `domain` (if any) rides in
/// the `spinal:label_domain` field metadata.
fn label_batch(rows: &[(&str, &str, f64)], domain: Option<&[&str]>) -> RecordBatch {
    let mut mode = Field::new("mode", DataType::Utf8, false);
    if let Some(d) = domain {
        mode = mode.with_metadata(HashMap::from([(
            "spinal:label_domain".to_string(),
            serde_json::to_string(d).unwrap(),
        )]));
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        mode,
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Rank-0 batch `(entity, value)`.
fn scalar_batch(entity: &str, value: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity])) as ArrayRef,
            Arc::new(Float64Array::from(vec![value])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn seg(class: &str, parameter: &str, alternative: &str, batch: RecordBatch) -> Segment {
    Segment {
        class: class.into(),
        parameter: parameter.into(),
        alternative: alternative.into(),
        batches: vec![batch],
        domains: vec![],
    }
}

fn ingest(db: &std::path::Path, segments: Vec<Segment>) -> Result<WriteReport, DbError> {
    ingest_segments(db, segments, "tester", "", WriteOptions::default())
}

fn col_str(batch: &RecordBatch, name: &str) -> Vec<String> {
    let i = batch.schema().index_of(name).unwrap();
    let a = batch
        .column(i)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..a.len()).map(|r| a.value(r).to_string()).collect()
}

fn text_warnings(report: &WriteReport) -> Vec<&Warning> {
    report
        .warnings
        .iter()
        .filter(|w| matches!(w.kind, WarningKind::TextNormalized { .. }))
        .collect()
}

fn rank1() -> DeclaredType {
    DeclaredType {
        positions: vec![(1, IndexKind::Label)],
    }
}

fn domain(labels: &[&str]) -> LabelDomain {
    LabelDomain {
        position: 1,
        labels: labels.iter().map(|s| s.to_string()).collect(),
    }
}

/// The batch of `canonicalize`: `(mode, value)`.
fn canon_batch(labels: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("mode", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let values: Vec<f64> = (0..labels.len()).map(|i| i as f64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(labels.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// Labels
// ---------------------------------------------------------------------------

#[test]
fn canon_compares_and_stores_labels_in_nfc() {
    // NFD data against an NFC domain (and the reverse): one label, in NFC.
    for (data, dom) in [(E_NFD, E_NFC), (E_NFC, E_NFD), (E_NFD, E_NFD)] {
        let v = canonicalize(
            &canon_batch(&[data]),
            &rank1(),
            ValueKind::Float,
            &[domain(&[dom])],
        )
        .unwrap();
        match &v.axes[0].body {
            AxisBody::Label { positions } => {
                assert_eq!(
                    positions,
                    &vec![LabelPos {
                        ordinal: 0,
                        label: E_NFC.into()
                    }]
                )
            }
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn a_domain_with_both_spellings_repeats_the_label() {
    let err = index_domains(&[domain(&["a", E_NFC, E_NFD])], |_| Some("mode".into())).unwrap_err();
    match &err {
        CanonError::RepeatedDomainLabel {
            label,
            position: 1,
            other: Some(other),
            ..
        } => {
            assert_eq!(label, E_NFD);
            assert_eq!(other, E_NFC);
        }
        e => panic!("{e:?}"),
    }
    assert_eq!(
        err.to_string(),
        "the label domain of dimension 1 (\"mode\") repeats the label \"e\\u{301}\" \
         (\"\u{e9}\" (U+00E9) and \"e\\u{301}\" (U+0065 U+0301) differ only in Unicode \
         normalization, and labels are compared in NFC); each label may appear only once \
         in a domain"
    );
}

#[test]
fn rows_whose_labels_differ_only_in_normalization_are_one_tuple() {
    let (_dir, db) = fresh_db();
    let batch = label_batch(&[("n1", E_NFC, 1.0), ("n1", E_NFD, 2.0)], Some(&[E_NFC]));
    let err = ingest(&db, vec![seg("node", "cost", "Base", batch)]).expect_err("duplicate");
    assert!(
        matches!(&err, DbError::Canon(CanonError::DuplicateTuple { respelled, .. })
            if respelled == &vec![(E_NFD.to_string(), E_NFC.to_string())]),
        "{err:?}"
    );
    assert_eq!(
        err.to_string(),
        "entity \"n1\" (class \"node\", parameter \"cost\", alternative \"Base\"): duplicate \
         index tuple (\"\u{e9}\"); a value maps one measure per index tuple (the label \
         \"\u{e9}\" (U+00E9) was also written as \"e\\u{301}\" (U+0065 U+0301): spellings \
         that differ only in Unicode normalization are one label)"
    );
}

#[test]
fn label_errors_number_dimensions_from_one() {
    // LabelNotInDomain and NullCoord name the dimension by its position,
    // like RepeatedDomainLabel (critique §10: it said "dimension 0").
    let err = canonicalize(
        &canon_batch(&["x"]),
        &rank1(),
        ValueKind::Float,
        &[domain(&["a"])],
    )
    .unwrap_err();
    assert!(matches!(err, CanonError::LabelNotInDomain(_, 1)), "{err:?}");
    assert_eq!(
        err.to_string(),
        "label \"x\" in dimension 1 is not in the caller-supplied ordered domain"
    );
}

#[test]
fn an_nfd_label_reads_back_in_nfc_with_one_warning() {
    let (_dir, db) = fresh_db();
    let batch = label_batch(
        &[("n1", E_NFD, 1.0), ("n2", E_NFD, 2.0), ("n2", "b", 3.0)],
        Some(&[E_NFD, "b"]),
    );
    let report = ingest(&db, vec![seg("node", "cost", "Base", batch)]).expect("write");
    let w = text_warnings(&report);
    assert_eq!(w.len(), 1, "one warning per segment and kind: {w:?}");
    assert_eq!(
        w[0].kind,
        WarningKind::TextNormalized {
            what: "label".into(),
            count: 1,
            written: E_NFD.into(),
            stored: E_NFC.into(),
        }
    );
    assert_eq!(w[0].object_type, "segment");
    assert_eq!(w[0].name, "node:cost/Base");

    let got = read_parameter(&db, "node", "cost", "Base").expect("read");
    let labels = col_str(&got, "idx1_text");
    assert_eq!(labels.len(), 3);
    assert_eq!(
        labels.iter().filter(|l| *l == E_NFC).count(),
        2,
        "{labels:?}"
    );
    assert!(!labels.iter().any(|l| l == E_NFD));
    assert_eq!(
        query_strings(&db, "SELECT DISTINCT label FROM axis_label ORDER BY label"),
        vec!["b".to_string(), E_NFC.to_string()]
    );
}

#[test]
fn nfc_input_gets_no_warning() {
    let (_dir, db) = fresh_db();
    let batch = label_batch(&[("n1", E_NFC, 1.0)], Some(&[E_NFC]));
    let report = ingest(&db, vec![seg("node", "cost", "Base", batch)]).expect("write");
    assert!(text_warnings(&report).is_empty(), "{:?}", report.warnings);
}

/// The dedup mix-up (plan §1.1, 3): the stored label bytes and the axis
/// count must not depend on which spelling was written first, and
/// rewriting the other spelling is unchanged.
#[test]
fn stored_label_bytes_do_not_depend_on_write_order() {
    let a = || label_batch(&[("n1", E_NFC, 1.0)], Some(&[E_NFC]));
    let b = || label_batch(&[("n2", E_NFD, 1.0)], Some(&[E_NFD]));
    let mut stored = Vec::new();
    for order in [[a(), b()], [b(), a()]] {
        let (_dir, db) = fresh_db();
        for batch in order {
            ingest(&db, vec![seg("node", "cost", "Base", batch)]).expect("write");
        }
        let hex = query_strings(&db, "SELECT hex(encode(label)) FROM axis_label");
        assert_eq!(
            count(&db, "SELECT count(*) FROM axis WHERE kind = 'label'"),
            1
        );
        // Rewriting either spelling changes nothing.
        for batch in [a(), b()] {
            let r = ingest(&db, vec![seg("node", "cost", "Base", batch)]).expect("rewrite");
            assert_eq!((r.commit_id, r.values_written), (0, 0), "{r:?}");
        }
        stored.push(hex);
    }
    assert_eq!(stored[0], vec!["C3A9".to_string()]);
    assert_eq!(stored[0], stored[1]);
}

/// T6: an axis that did not come from canon (a non-NFC label, a label
/// twice) is refused by the axis writer.
#[test]
fn the_axis_writer_refuses_non_nfc_and_repeated_labels() {
    let (_dir, db) = fresh_db();
    let axis = |labels: &[&str]| CanonicalAxis {
        length: labels.len() as i32,
        properties: AxisProperties::default(),
        body: AxisBody::Label {
            positions: labels
                .iter()
                .enumerate()
                .map(|(i, l)| LabelPos {
                    ordinal: i as i32,
                    label: l.to_string(),
                })
                .collect(),
        },
    };
    for (labels, problem) in [
        (
            vec![E_NFD],
            "the label \"e\\u{301}\" (U+0065 U+0301) at position 0 is not in NFC",
        ),
        (
            vec!["a", E_NFC, "a"],
            "the label \"a\" is at positions 0 and 2",
        ),
    ] {
        let err = with_write_txn(&db, "t", "t6", |conn, _c| {
            dedup_or_insert(conn, &mut AxisCache::new(), &axis(&labels))
        })
        .expect_err("malformed axis");
        match &err {
            DbError::MalformedAxis {
                kind: "label",
                problem: Some(p),
                ..
            } => assert_eq!(p, problem),
            e => panic!("{e:?}"),
        }
        assert_eq!(
            err.to_string(),
            format!("malformed axis (label): {problem}")
        );
    }
    assert_eq!(count(&db, "SELECT count(*) FROM axis"), 0);
    // A non-NFC timezone tag likewise.
    let mut tz = axis(&["a"]);
    tz.properties.timezone = Some(format!("Europe/{E_NFD}"));
    let err = with_write_txn(&db, "t", "t6", |conn, _c| {
        dedup_or_insert(conn, &mut AxisCache::new(), &tz)
    })
    .expect_err("non-NFC tag");
    assert!(
        matches!(&err, DbError::MalformedAxis { problem: Some(p), .. } if p.contains("timezone")),
        "{err:?}"
    );
}

// ---------------------------------------------------------------------------
// Names
// ---------------------------------------------------------------------------

#[test]
fn an_nfd_name_is_stored_in_nfc_and_found_by_both_spellings() {
    let (_dir, db) = fresh_db();
    let report = ingest(
        &db,
        vec![seg(
            PAA_NFD,
            "cap",
            "Base",
            scalar_batch(&format!("n_{PAA_NFD}"), 1.0),
        )],
    )
    .expect("write");
    let w = text_warnings(&report);
    assert_eq!(w.len(), 1, "{w:?}");
    assert_eq!(
        w[0].kind,
        WarningKind::TextNormalized {
            what: "name".into(),
            count: 2,
            written: PAA_NFD.into(),
            stored: PAA_NFC.into(),
        }
    );
    assert_eq!(w[0].name, format!("{PAA_NFC}:cap/Base"));
    assert_eq!(
        query_strings(&db, "SELECT name FROM entity_class"),
        vec![PAA_NFC.to_string()]
    );
    assert_eq!(
        query_strings(&db, "SELECT name FROM entity"),
        vec![format!("n_{PAA_NFC}")]
    );

    // Reads find it by either spelling.
    for class in [PAA_NFC, PAA_NFD] {
        let got = read_parameter(&db, class, "cap", "Base").expect("read");
        assert_eq!(got.num_rows(), 1, "read_parameter by {class:?}");
        for entity in [format!("n_{PAA_NFC}"), format!("n_{PAA_NFD}")] {
            let got = read_values(&db, class, "cap", &[entity.as_str()], None).expect("read");
            assert_eq!(got.num_rows(), 1, "read_values by {class:?} / {entity:?}");
        }
    }

    // A write by the other spelling is the same value: unchanged.
    let again = ingest(
        &db,
        vec![seg(
            PAA_NFC,
            "cap",
            "Base",
            scalar_batch(&format!("n_{PAA_NFC}"), 1.0),
        )],
    )
    .expect("rewrite");
    assert_eq!((again.commit_id, again.values_written), (0, 0), "{again:?}");
    assert!(text_warnings(&again).is_empty());
    assert_eq!(count(&db, "SELECT count(*) FROM entity_class"), 1);

    // A value delete by the NFD spelling finds it.
    let del = delete_values(
        &db,
        vec![DeleteKey {
            class: PAA_NFD.into(),
            parameter: "cap".into(),
            alternative: "Base".into(),
            entity: format!("n_{PAA_NFD}"),
        }],
        "tester",
        "",
        WriteOptions::default(),
    )
    .expect("delete");
    assert!(del.commit_id > 0 && del.rows_closed == 1, "{del:?}");
    assert!(text_warnings(&del).is_empty(), "lookups are not reported");
}

#[test]
fn two_spellings_in_one_batch_are_one_entity() {
    let (_dir, db) = fresh_db();
    let batch = label_batch(
        &[(PAA_NFC, "a", 1.0), (PAA_NFD, "b", 2.0)],
        Some(&["a", "b"]),
    );
    let report = ingest(&db, vec![seg("node", "cost", "Base", batch)]).expect("write");
    assert_eq!(report.values_written, 1, "{report:?}");
    assert_eq!(count(&db, "SELECT count(*) FROM entity"), 1);
    // ... so writing one tuple under both spellings is a duplicate tuple.
    let batch = label_batch(&[(PAA_NFC, "a", 1.0), (PAA_NFD, "a", 2.0)], Some(&["a"]));
    let err = ingest(&db, vec![seg("node", "cost2", "Base", batch)]).expect_err("dup");
    assert!(
        matches!(&err, DbError::Canon(CanonError::DuplicateTuple { .. })),
        "{err:?}"
    );
}

#[test]
fn two_segments_whose_targets_differ_only_in_normalization_collide() {
    let (_dir, db) = fresh_db();
    let err = ingest(
        &db,
        vec![
            seg(PAA_NFC, "cap", "Base", scalar_batch("n1", 1.0)),
            seg(PAA_NFD, "cap", "Base", scalar_batch("n2", 1.0)),
        ],
    )
    .expect_err("duplicate target");
    assert!(err.to_string().contains("duplicate target"), "{err}");
}

#[test]
fn structural_changes_and_object_deletes_use_nfc_names() {
    let (_dir, db) = fresh_db();
    let report = commit_structural(
        &db,
        vec![StructuralChange::CreateAlternative {
            name: format!("alt_{PAA_NFD}"),
            description: Some(format!("described as {PAA_NFD}")),
        }],
        "tester",
        "",
        WriteOptions::default(),
    )
    .expect("create");
    let w = text_warnings(&report);
    assert_eq!(w.len(), 1, "{w:?}");
    assert_eq!(w[0].object_type, "structural");
    assert_eq!(
        query_strings(
            &db,
            "SELECT name FROM alternative WHERE valid_to_commit IS NULL AND name <> 'Base'"
        ),
        vec![format!("alt_{PAA_NFC}")]
    );
    // Descriptions are free text: stored as written.
    assert_eq!(
        query_strings(
            &db,
            "SELECT description FROM alternative WHERE name <> 'Base'"
        ),
        vec![format!("described as {PAA_NFD}")]
    );
    // Creating it again under the NFC spelling is a no-op.
    let again = commit_structural(
        &db,
        vec![StructuralChange::CreateAlternative {
            name: format!("alt_{PAA_NFC}"),
            description: None,
        }],
        "tester",
        "",
        WriteOptions::default(),
    )
    .expect("create again");
    assert_eq!(again.structural[0].effect, StructuralEffect::NoOp);

    // The verbs resolve the NFD spelling to the same alternative.
    let id =
        verbs::ensure_alternative(&db, &format!("alt_{PAA_NFD}"), None, "tester").expect("ensure");
    assert_eq!(id, again.structural[0].object_id);

    // A rename by the NFD spelling of the current name.
    let renamed = commit_structural(
        &db,
        vec![StructuralChange::RenameAlternative {
            current: format!("alt_{PAA_NFD}"),
            new_name: format!("alt2_{PAA_NFD}"),
            description: None,
        }],
        "tester",
        "",
        WriteOptions::default(),
    )
    .expect("rename");
    assert_eq!(renamed.structural[0].effect, StructuralEffect::Renamed);

    // An object delete by the NFD spelling.
    let deleted = delete_objects(
        &db,
        vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Alternative,
                class: None,
                target: ObjectRef::Name(format!("alt2_{PAA_NFD}")),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        "tester",
        "",
        WriteOptions::default(),
    )
    .expect("object delete");
    assert!(
        matches!(
            deleted.object_deletes[0].result,
            nodal_db_core::types::ObjectDeleteResult::Deleted { .. }
        ),
        "{deleted:?}"
    );
    assert_eq!(
        count(
            &db,
            "SELECT count(*) FROM alternative WHERE valid_to_commit IS NULL"
        ),
        1,
        "only Base is left"
    );
}

#[test]
fn template_names_load_and_hash_in_nfc() {
    let doc = |class: &str| {
        format!(
            "template: {{name: t, version: '1'}}\n\
             classes:\n  - name: {class}\n    parameters:\n      - name: cap\n"
        )
    };
    let nfd = template::parse_str(&doc(PAA_NFD)).expect("parse NFD");
    let nfc = template::parse_str(&doc(PAA_NFC)).expect("parse NFC");
    assert_eq!(nfd.classes[0].name, PAA_NFC);
    assert_eq!(
        template::template_content_hash(&nfd),
        template::template_content_hash(&nfc)
    );
    // A template built in code with an NFD name hashes the same too.
    let mut built = nfc.clone();
    built.classes[0].name = PAA_NFD.into();
    assert_eq!(
        template::template_content_hash(&built),
        template::template_content_hash(&nfc)
    );
}
