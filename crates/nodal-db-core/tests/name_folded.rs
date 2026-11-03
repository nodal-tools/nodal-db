//! `WarningKind::NameFolded` (name_fold_identity_PLAN N5 as tightened by
//! K8): a write-path reference that resolves, by the case-insensitive name
//! identity, to an existing object spelled differently is reported, one
//! warning per group and referent table.
//!
//! - every resolution site: value writes (class, parameter, alternative,
//!   entity), value deletes, object deletes, structural references and
//!   idempotent creates;
//! - the exact trigger: byte equality with the stored spelling of the row
//!   the lookup returned, including objects created or renamed earlier in
//!   the same call (only the creating reference is silent);
//! - counting: distinct written spellings, first pair in input order, not
//!   double-counted by a conflict retry or changed by the resolve cache;
//! - not a write: an unchanged re-write spelled differently commits nothing.
#![cfg(debug_assertions)]

use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;

use nodal_db_core::api::test_hooks::{inject_conflicts, without_resolve_cache};
use nodal_db_core::api::{commit_changes, DeleteKey, Segment};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, StructuralChange, Warning,
    WarningKind, WriteOptions, WriteReport,
};

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("fold.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("schema");
    (dir, db)
}

fn scalar(entities: &[&str], v: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let vals: Vec<f64> = (0..entities.len()).map(|i| v + i as f64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(entities.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vals)) as ArrayRef,
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
        domains: Vec::new(),
    }
}

fn commit(
    db: &std::path::Path,
    segments: Vec<Segment>,
    structural: Vec<StructuralChange>,
    deletes: Vec<DeleteKey>,
    object_deletes: Vec<(ObjectDeleteKey, ObjectDeleteOpts)>,
) -> WriteReport {
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
    .expect("commit")
}

fn write(db: &std::path::Path, segments: Vec<Segment>) -> WriteReport {
    commit(db, segments, Vec::new(), Vec::new(), Vec::new())
}

/// The `NameFolded` warnings as `(what, object_type, name, count, written,
/// stored)`, in report order.
type Folded = (String, String, String, usize, String, String);

fn folded(report: &WriteReport) -> Vec<Folded> {
    report
        .warnings
        .iter()
        .filter_map(|w| match &w.kind {
            WarningKind::NameFolded {
                what,
                count,
                written,
                stored,
                spellings,
            } => Some({
                // The list is the warning; count and the first pair repeat it.
                assert_eq!(*count, spellings.len(), "{w:?}");
                let first = &spellings[0];
                assert_eq!(
                    (&first.written, &first.stored, first.id),
                    (written, stored, w.id),
                    "{w:?}"
                );
                (
                    what.clone(),
                    w.object_type.clone(),
                    w.name.clone(),
                    *count,
                    written.clone(),
                    stored.clone(),
                )
            }),
            _ => None,
        })
        .collect()
}

fn f(what: &str, ty: &str, name: &str, count: usize, written: &str, stored: &str) -> Folded {
    (
        what.into(),
        ty.into(),
        name.into(),
        count,
        written.into(),
        stored.into(),
    )
}

/// Seed: class `Node`, parameter `Price`, alternative `High`, entities
/// `N1`/`N2`, all spelled with capitals.
fn seeded() -> (tempfile::TempDir, std::path::PathBuf) {
    let (dir, db) = fresh_db();
    let r = write(
        &db,
        vec![seg("Node", "Price", "High", scalar(&["N1", "N2"], 1.0))],
    );
    assert_eq!(folded(&r), vec![], "creating references are silent");
    (dir, db)
}

#[test]
fn value_write_with_case_variant_names_warns_per_referent() {
    let (_dir, db) = seeded();
    let r = write(
        &db,
        vec![seg("node", "PRICE", "high", scalar(&["n1", "N2"], 5.0))],
    );
    let at = "node:PRICE/high";
    assert_eq!(
        folded(&r),
        vec![
            f("segment", "entity_class", at, 1, "node", "Node"),
            f("segment", "parameter_definition", at, 1, "PRICE", "Price"),
            f("segment", "alternative", at, 1, "high", "High"),
            f("segment", "entity", at, 1, "n1", "N1"),
        ]
    );
    // The resolved ids are the stored rows (the write landed on them).
    let class = r
        .warnings
        .iter()
        .find(|w| {
            w.object_type == "entity_class" && matches!(w.kind, WarningKind::NameFolded { .. })
        })
        .unwrap();
    assert!(class.id > 0);
    // Exact spellings: no warning at all.
    let r = write(
        &db,
        vec![seg("Node", "Price", "High", scalar(&["N1", "N2"], 9.0))],
    );
    assert_eq!(folded(&r), vec![]);
}

#[test]
fn count_is_distinct_spellings_and_first_is_in_input_order() {
    let (_dir, db) = fresh_db();
    write(
        &db,
        vec![seg("node", "p", "Base", scalar(&["Aa", "Bb", "Cc"], 1.0))],
    );
    let r = write(
        &db,
        vec![seg("node", "p", "Base", scalar(&["Cc", "bB", "AA"], 2.0))],
    );
    assert_eq!(
        folded(&r),
        vec![f("segment", "entity", "node:p/Base", 2, "bB", "Bb")]
    );
}

#[test]
fn same_call_created_objects_warn_for_later_case_variants_only() {
    let (_dir, db) = fresh_db();
    // Segment 1 auto-creates `Foo` (silent); segment 2 spells it `FOO`.
    let r = write(
        &db,
        vec![
            seg("node", "p", "Base", scalar(&["Foo"], 1.0)),
            seg("node", "q", "Base", scalar(&["FOO"], 2.0)),
        ],
    );
    assert_eq!(
        folded(&r),
        vec![f("segment", "entity", "node:q/Base", 1, "FOO", "Foo")]
    );
    // A structural create, then a value write spelled differently.
    let r = commit(
        &db,
        vec![seg("node", "p", "Scen_Alt", scalar(&["Foo"], 3.0))],
        vec![StructuralChange::CreateAlternative {
            name: "scen_alt".into(),
            description: None,
        }],
        Vec::new(),
        Vec::new(),
    );
    assert_eq!(
        folded(&r),
        vec![f(
            "segment",
            "alternative",
            "node:p/Scen_Alt",
            1,
            "Scen_Alt",
            "scen_alt"
        )]
    );
}

#[test]
fn in_call_case_only_rename_moves_the_silent_spelling() {
    let (_dir, db) = fresh_db();
    commit(
        &db,
        Vec::new(),
        vec![StructuralChange::CreateAlternative {
            name: "foo".into(),
            description: None,
        }],
        Vec::new(),
        Vec::new(),
    );
    // R4: `foo` -> `Foo` is a real rename. Afterwards `Foo` is silent and
    // `foo` warns (stored `Foo`); the rename's `current` (`foo`) is the
    // stored spelling at that point, so it is silent too.
    let r = commit(
        &db,
        vec![
            seg("node", "p", "Foo", scalar(&["n1"], 1.0)),
            seg("node", "q", "foo", scalar(&["n1"], 2.0)),
        ],
        vec![StructuralChange::RenameAlternative {
            current: "foo".into(),
            new_name: "Foo".into(),
            description: None,
        }],
        Vec::new(),
        Vec::new(),
    );
    assert_eq!(
        folded(&r),
        vec![f("segment", "alternative", "node:q/foo", 1, "foo", "Foo")]
    );
}

#[test]
fn structural_references_and_idempotent_creates_warn() {
    let (_dir, db) = seeded();
    let r = commit(
        &db,
        Vec::new(),
        vec![
            // Idempotent create of an existing alternative, spelled differently.
            StructuralChange::CreateAlternative {
                name: "HIGH".into(),
                description: None,
            },
            // Rename: the `current` reference warns, the new name never does.
            StructuralChange::RenameAlternative {
                current: "high".into(),
                new_name: "Peak".into(),
                description: None,
            },
            StructuralChange::SetEntityActivation {
                class: "NODE".into(),
                entity: "n2".into(),
                alternative: "peak".into(),
                active: Some(false),
            },
        ],
        Vec::new(),
        Vec::new(),
    );
    assert_eq!(
        folded(&r),
        vec![
            f("structural", "alternative", "", 3, "HIGH", "High"),
            f("structural", "entity_class", "", 1, "NODE", "Node"),
            f("structural", "entity", "", 1, "n2", "N2"),
        ]
    );
}

#[test]
fn value_and_object_deletes_warn_in_their_own_groups() {
    let (_dir, db) = seeded();
    let r = commit(
        &db,
        Vec::new(),
        Vec::new(),
        vec![DeleteKey {
            class: "node".into(),
            parameter: "price".into(),
            alternative: "High".into(),
            entity: "n1".into(),
        }],
        vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Entity,
                class: Some("NODE".into()),
                target: ObjectRef::Name("n2".into()),
            },
            ObjectDeleteOpts { cascade: true },
        )],
    );
    assert_eq!(
        folded(&r),
        vec![
            f("delete", "entity_class", "", 1, "node", "Node"),
            f("delete", "parameter_definition", "", 1, "price", "Price"),
            f("delete", "entity", "", 1, "n1", "N1"),
            f("object_delete", "entity_class", "", 1, "NODE", "Node"),
            f("object_delete", "entity", "", 1, "n2", "N2"),
        ]
    );
}

#[test]
fn an_unchanged_rewrite_spelled_differently_commits_nothing() {
    let (_dir, db) = seeded();
    let r = write(
        &db,
        vec![seg("NODE", "price", "high", scalar(&["n1", "n2"], 1.0))],
    );
    assert_eq!(r.commit_id, 0, "NameFolded is not a write");
    assert_eq!(r.values_unchanged, 2);
    assert_eq!(folded(&r).len(), 4);
}

#[test]
fn a_conflict_retry_and_the_resolve_cache_do_not_change_the_counts() {
    let segments = || {
        vec![
            seg("node", "price", "high", scalar(&["n1"], 7.0)),
            seg("NODE", "Price", "BASE", scalar(&["N1", "n2"], 8.0)),
        ]
    };
    let warnings = |r: WriteReport| -> Vec<Warning> {
        r.warnings
            .into_iter()
            .filter(|w| matches!(w.kind, WarningKind::NameFolded { .. }))
            .collect()
    };
    let (_d1, plain) = seeded();
    let reference = warnings(write(&plain, segments()));
    assert!(!reference.is_empty());

    let (_d2, retried) = seeded();
    inject_conflicts(1);
    let r = write(&retried, segments());
    inject_conflicts(0);
    assert_eq!(warnings(r), reference, "a retry counts once");

    let (_d3, uncached) = seeded();
    let r = without_resolve_cache(|| write(&uncached, segments()));
    assert_eq!(warnings(r), reference, "the cache is invisible");
}

/// The warning lists every (written spelling, resolved row) pair: one
/// spelling that resolves to entities of two classes in one group is two
/// references (two objects), and a repeated reference is listed once.
#[test]
fn spellings_list_each_object_a_spelling_resolved_to() {
    let (_dir, db) = seeded();
    write(&db, vec![seg("Unit", "Cap", "High", scalar(&["N1"], 1.0))]);
    let activation = |class: &str| StructuralChange::SetEntityActivation {
        class: class.into(),
        entity: "n1".into(),
        alternative: "High".into(),
        active: Some(false),
    };
    let r = commit(
        &db,
        Vec::new(),
        vec![activation("Node"), activation("Unit"), activation("Node")],
        Vec::new(),
        Vec::new(),
    );
    let w = r
        .warnings
        .iter()
        .find(|w| w.object_type == "entity")
        .expect("entity warning");
    let WarningKind::NameFolded { spellings, .. } = &w.kind else {
        panic!("{w:?}")
    };
    assert_eq!(spellings.len(), 2, "{spellings:?}");
    assert!(spellings
        .iter()
        .all(|s| s.written == "n1" && s.stored == "N1"));
    assert_ne!(spellings[0].id, spellings[1].id);
    assert_eq!(
        folded(&r),
        vec![f("structural", "entity", "", 2, "n1", "N1")]
    );
}
