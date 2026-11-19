//! The metadata channel of `commit_change_set` (`ChangeSet::metadata`):
//! the pool (sources, licenses, pairs, roles, properties), object and value
//! links, the keep / set / clear statements for new value versions and the
//! refusal of an unstated one, pool deletes on the post-call state, and the
//! same-call rules. Every test ends with the link audits empty.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{
    commit_change_set, database_properties, database_property, ChangeSet, Segment,
};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    Action, CoveredWrites, LicenseFields, LinkSpec, MetadataChange as M, MetadataEffect,
    ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, PairRef, Ref, RoleSel, Scope,
    SourceFields, Subject, Target, ValueAddr, WarningKind, WriteOptions, WriteReport,
};

fn scalar(entities: &[&str], vals: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(entities.to_vec())),
        Arc::new(Float64Array::from(vals.to_vec())),
    ];
    RecordBatch::try_new(schema, cols).unwrap()
}

fn series(entity: &str, vals: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let n = vals.len();
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![entity; n])),
        Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(vals.to_vec())),
    ];
    RecordBatch::try_new(schema, cols).unwrap()
}

fn seg(param: &str, b: RecordBatch) -> Segment {
    Segment {
        class: "node".into(),
        parameter: param.into(),
        alternative: "Base".into(),
        batches: vec![b],
        domains: vec![],
    }
}

fn run(db: &Path, set: ChangeSet) -> Result<WriteReport, DbError> {
    commit_change_set(db, set, "t", "m", WriteOptions::default())
}

fn meta(db: &Path, items: Vec<M>) -> Result<WriteReport, DbError> {
    run(
        db,
        ChangeSet {
            metadata: items,
            ..Default::default()
        },
    )
}

fn write(db: &Path, segs: Vec<Segment>, items: Vec<M>) -> Result<WriteReport, DbError> {
    run(
        db,
        ChangeSet {
            segments: segs,
            metadata: items,
            ..Default::default()
        },
    )
}

fn q(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

fn qs(db: &Path, sql: &str) -> String {
    let conn = Connection::open(db).unwrap();
    let n = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

fn clean(db: &Path) {
    assert_eq!(
        q(
            db,
            "SELECT count(*) FROM audit_dangling_ref WHERE src_table IN \
             ('value_metadata_link', 'object_metadata_link', 'source_license')"
        ),
        0,
        "dangling metadata links"
    );
    assert_eq!(q(db, "SELECT count(*) FROM audit_value_link_version"), 0);
}

fn fresh(dir: &Path) -> PathBuf {
    let db = dir.join("m.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();
    db
}

fn src(n: &str) -> Ref {
    Ref::Name(n.into())
}

fn put_source(n: &str) -> M {
    M::PutSource {
        source: src(n),
        rename: None,
        fields: SourceFields::default(),
    }
}

fn vaddr(entity: &str) -> ValueAddr {
    ValueAddr {
        class: "node".into(),
        parameter: "p".into(),
        alternative: "Base".into(),
        entity: entity.into(),
    }
}

fn to_source(n: &str) -> LinkSpec {
    LinkSpec {
        target: Target::Source(src(n)),
        role: RoleSel::NoRole,
    }
}

fn stmt(scope: Scope, action: Action) -> M {
    M::ValueMetadata { scope, action }
}

fn live_links(db: &Path) -> i64 {
    q(
        db,
        "SELECT count(*) FROM value_metadata_link WHERE valid_to_commit IS NULL",
    )
}

/// node.a / node.b of p, with a's value linked to the source IEA.
fn annotated(dir: &Path) -> PathBuf {
    let db = fresh(dir);
    write(
        &db,
        vec![seg("p", scalar(&["a", "b"], &[1.0, 2.0]))],
        vec![
            put_source("IEA"),
            M::LinkValue {
                value: vaddr("a"),
                link: to_source("IEA"),
            },
        ],
    )
    .unwrap();
    assert_eq!(live_links(&db), 1);
    db
}

#[test]
fn sources_and_licenses_create_revise_and_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    let r = meta(
        &db,
        vec![
            M::PutSource {
                source: src("IEA"),
                rename: None,
                fields: SourceFields {
                    title: Some("World Energy Outlook".into()),
                    issued: Some("2024-10-16".into()),
                    ..Default::default()
                },
            },
            M::PutLicense {
                license: src("CC-BY-4.0"),
                rename: None,
                fields: LicenseFields {
                    spdx: true,
                    policy: Some(r#"{"@type": "Policy"}"#.into()),
                    ..Default::default()
                },
            },
        ],
    )
    .unwrap();
    assert!(r.commit_id > 0);
    assert_eq!(r.metadata[0].effect, MetadataEffect::Created);
    assert_eq!(r.metadata[1].effect, MetadataEffect::Created);
    // The same fields again: a no-op call, no commit.
    let r = meta(
        &db,
        vec![M::PutSource {
            source: src("iea"),
            rename: None,
            fields: SourceFields {
                title: Some("World Energy Outlook".into()),
                issued: Some("2024-10-16".into()),
                ..Default::default()
            },
        }],
    )
    .unwrap();
    assert_eq!(r.metadata[0].effect, MetadataEffect::NoOp);
    assert_eq!(r.commit_id, 0, "an all-NoOp metadata call makes no commit");
    assert!(r
        .warnings
        .iter()
        .any(|w| matches!(w.kind, WarningKind::NameFolded { .. })));
    // A revision keeps the id and bumps the version; a rename too.
    let r = meta(
        &db,
        vec![M::PutSource {
            source: src("IEA"),
            rename: Some("IEA WEO".into()),
            fields: SourceFields::default(),
        }],
    )
    .unwrap();
    assert_eq!(r.metadata[0].effect, MetadataEffect::Renamed);
    assert_eq!(
        q(&db, "SELECT version FROM source WHERE valid_to_commit IS NULL"),
        2
    );
    // A rename onto another source's name is a clean error.
    meta(&db, vec![put_source("Eurostat")]).unwrap();
    let e = meta(
        &db,
        vec![M::PutSource {
            source: src("eurostat"),
            rename: Some("iea weo".into()),
            fields: SourceFields::default(),
        }],
    )
    .unwrap_err();
    assert!(e.to_string().contains("already exists"), "{e}");
    // An invalid policy and a bad date are refused before any SQL.
    let e = meta(
        &db,
        vec![M::PutLicense {
            license: src("x"),
            rename: None,
            fields: LicenseFields {
                policy: Some("{not json".into()),
                ..Default::default()
            },
        }],
    )
    .unwrap_err();
    assert!(e.to_string().contains("not valid JSON"), "{e}");
    let e = meta(
        &db,
        vec![M::PutSource {
            source: src("y"),
            rename: None,
            fields: SourceFields {
                issued: Some("16.10.2024".into()),
                ..Default::default()
            },
        }],
    )
    .unwrap_err();
    assert!(e.to_string().contains("ISO date"), "{e}");
    // An unknown id never creates.
    assert!(meta(
        &db,
        vec![M::PutSource {
            source: Ref::Id(999),
            rename: None,
            fields: SourceFields::default()
        }]
    )
    .is_err());
    clean(&db);
}

#[test]
fn pairs_fold_their_key_and_rename_by_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    meta(
        &db,
        vec![M::CreatePair {
            name: "Source".into(),
            value: "a".into(),
        }],
    )
    .unwrap();
    let r = meta(
        &db,
        vec![M::CreatePair {
            name: "source".into(),
            value: "b".into(),
        }],
    )
    .unwrap();
    assert!(r
        .warnings
        .iter()
        .any(|w| matches!(w.kind, WarningKind::NameFolded { .. })));
    assert_eq!(
        q(&db, "SELECT count(*) FROM metadata_pair WHERE name = 'Source'"),
        2,
        "the existing key spelling is stored"
    );
    let r = meta(
        &db,
        vec![M::RenamePairKey {
            from: "SOURCE".into(),
            to: "origin".into(),
        }],
    )
    .unwrap();
    assert_eq!(r.metadata[0].effect, MetadataEffect::Renamed);
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM metadata_pair WHERE name = 'origin' AND valid_to_commit IS NULL"
        ),
        2
    );
    let r = meta(
        &db,
        vec![M::RevisePair {
            pair: PairRef::Pair {
                name: "origin".into(),
                value: "a".into(),
            },
            value: "c".into(),
        }],
    )
    .unwrap();
    assert_eq!(r.metadata[0].effect, MetadataEffect::Updated);
    // A revision onto an existing pair collides.
    assert!(meta(
        &db,
        vec![M::RevisePair {
            pair: PairRef::Pair {
                name: "origin".into(),
                value: "c".into()
            },
            value: "b".into()
        }]
    )
    .is_err());
    clean(&db);
}

#[test]
fn user_and_tool_properties_and_the_core_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    let set = |k: &str, v: Option<&str>| M::SetProperty {
        key: k.into(),
        value: v.map(str::to_string),
    };
    let r = meta(
        &db,
        vec![set("title", Some("Nordic grid")), set("flextool:frozen", Some("1"))],
    )
    .unwrap();
    assert_eq!(r.metadata[0].effect, MetadataEffect::Created);
    assert_eq!(
        database_property(&db, "TITLE").unwrap().as_deref(),
        Some("Nordic grid")
    );
    let all = database_properties(&db).unwrap();
    assert!(all.iter().any(|p| p.key == "flextool:frozen" && p.owner == "tool"));
    assert!(all.iter().any(|p| p.key == "schema_version" && p.owner == "core"));
    // Clear, then the version continues.
    let r = meta(&db, vec![set("title", None)]).unwrap();
    assert_eq!(r.metadata[0].effect, MetadataEffect::Cleared);
    meta(&db, vec![set("title", Some("again"))]).unwrap();
    assert_eq!(
        q(&db, "SELECT max(version) FROM database_property WHERE key = 'title'"),
        2
    );
    for bad in ["schema_version", "Template_Name", ":x", "tool:", " title", "a: b"] {
        let e = meta(&db, vec![set(bad, Some("v"))]).unwrap_err();
        assert!(matches!(e, DbError::Api(_)), "{bad}: {e}");
    }
}

#[test]
fn an_annotated_value_rewrite_needs_a_statement() {
    let dir = tempfile::tempdir().unwrap();
    let db = annotated(dir.path());
    let before = q(&db, "SELECT max(id) FROM commit");
    // Changing a: refused, nothing written.
    let e = write(&db, vec![seg("p", scalar(&["a", "b"], &[5.0, 6.0]))], vec![]).unwrap_err();
    match &e {
        DbError::ValueMetadataUnstated { count, sample, .. } => {
            assert_eq!(*count, 1);
            assert_eq!(sample, &["node.a / p / Base".to_string()]);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(q(&db, "SELECT max(id) FROM commit"), before);
    // Not refused: an unchanged write of a, a change of the link-free b, a
    // new value c, another parameter.
    write(&db, vec![seg("p", scalar(&["a", "b", "c"], &[1.0, 7.0, 8.0]))], vec![]).unwrap();
    write(&db, vec![seg("q", scalar(&["a"], &[1.0]))], vec![]).unwrap();
    assert_eq!(live_links(&db), 1);
    // Keep: a new version keeps the link (closed and reopened, version 2).
    let r = write(
        &db,
        vec![seg("p", scalar(&["a"], &[5.0]))],
        vec![stmt(Scope::Save, Action::Keep)],
    )
    .unwrap();
    assert_eq!(r.value_links.kept, 1);
    assert_eq!(
        q(&db, "SELECT version FROM value_metadata_link WHERE valid_to_commit IS NULL"),
        2
    );
    // As of the first commit, the first version's link.
    assert_eq!(
        q(
            &db,
            &format!("SELECT count(*) FROM value_metadata_link_asof({before})")
        ),
        1
    );
    clean(&db);
    // An indexed value: a set annotates the new value, a partial change
    // (a long cell diff) is a new version that keep carries.
    write(
        &db,
        vec![seg("s", series("a", &[1.0, 2.0, 3.0]))],
        vec![stmt(Scope::Save, Action::Set(vec![to_source("IEA")]))],
    )
    .unwrap();
    let r = write(
        &db,
        vec![seg("s", series("a", &[1.0, 2.0, 4.0]))],
        vec![stmt(Scope::Save, Action::Keep)],
    )
    .unwrap();
    assert_eq!(r.value_links.kept, 1);
    clean(&db);
    // Back to p for the clear below.
    // Clear: the new version has none.
    let r = write(
        &db,
        vec![seg("p", scalar(&["a"], &[9.0]))],
        vec![stmt(Scope::Save, Action::Clear)],
    )
    .unwrap();
    assert_eq!(r.value_links.cleared, 1);
    assert_eq!(live_links(&db), 1, "s keeps its link");
    clean(&db);
}

#[test]
fn precedence_and_set_defaults_annotate_new_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = annotated(dir.path());
    meta(&db, vec![put_source("Eurostat")]).unwrap();
    // Save says clear, the target says keep, the value b says set Eurostat:
    // a keeps IEA (target over save), b gets Eurostat (value over target),
    // and the new value c is annotated by nothing but... the target keep
    // (c has no old links), so stays bare.
    let r = write(
        &db,
        vec![seg("p", scalar(&["a", "b", "c"], &[11.0, 12.0, 13.0]))],
        vec![
            stmt(Scope::Save, Action::Clear),
            stmt(
                Scope::Target {
                    class: "node".into(),
                    parameter: "p".into(),
                    alternative: "Base".into(),
                },
                Action::Keep,
            ),
            stmt(
                Scope::Value(vaddr("b")),
                Action::Set(vec![to_source("Eurostat")]),
            ),
        ],
    )
    .unwrap();
    assert_eq!(r.value_links.kept, 1);
    assert_eq!(r.value_links.set, 1);
    let linked = |e: &str| {
        qs(
            &db,
            &format!(
                "SELECT coalesce(string_agg(s.name, ','), '') FROM value_metadata_link l \
                 JOIN source s ON s.id = l.target_id AND s.valid_to_commit IS NULL \
                 JOIN entity e ON e.id = l.entity_id AND e.cur_name = '{e}' \
                 WHERE l.valid_to_commit IS NULL"
            ),
        )
    };
    assert_eq!(linked("a"), "IEA");
    assert_eq!(linked("b"), "Eurostat");
    assert_eq!(linked("c"), "");
    // A save-level set annotates new values too ("everything in this
    // import is from IEA").
    write(
        &db,
        vec![seg("p", scalar(&["d"], &[1.0]))],
        vec![stmt(Scope::Save, Action::Set(vec![to_source("IEA")]))],
    )
    .unwrap();
    assert_eq!(linked("d"), "IEA");
    clean(&db);
}

#[test]
fn value_scope_edits_and_edge_items() {
    let dir = tempfile::tempdir().unwrap();
    let db = annotated(dir.path());
    meta(&db, vec![put_source("Eurostat")]).unwrap();
    // Set on an unchanged value is an edit.
    let r = meta(
        &db,
        vec![stmt(
            Scope::Value(vaddr("a")),
            Action::Set(vec![to_source("Eurostat")]),
        )],
    )
    .unwrap();
    assert!(r.commit_id > 0);
    assert_eq!((r.value_links.set, r.value_links.cleared), (1, 1));
    // Keep on an unchanged value is a no-op.
    let r = meta(&db, vec![stmt(Scope::Value(vaddr("a")), Action::Keep)]).unwrap();
    assert_eq!(r.commit_id, 0);
    // Edge items with a keep across a change: base = old, + add.
    let r = write(
        &db,
        vec![seg("p", scalar(&["a"], &[3.0]))],
        vec![
            stmt(Scope::Save, Action::Keep),
            M::LinkValue {
                value: vaddr("a"),
                link: to_source("IEA"),
            },
        ],
    )
    .unwrap();
    assert_eq!((r.value_links.kept, r.value_links.set), (1, 1));
    assert_eq!(live_links(&db), 2);
    // An edge item alone is not a statement for a changed annotated value.
    let e = write(
        &db,
        vec![seg("p", scalar(&["a"], &[4.0]))],
        vec![M::UnlinkValue {
            value: vaddr("a"),
            target: Target::Source(src("IEA")),
            role: RoleSel::NoRole,
        }],
    )
    .unwrap_err();
    assert!(matches!(e, DbError::ValueMetadataUnstated { .. }), "{e}");
    // A value-scope set next to an edge item on the same value is refused.
    let e = meta(
        &db,
        vec![
            stmt(Scope::Value(vaddr("a")), Action::Clear),
            M::UnlinkValue {
                value: vaddr("a"),
                target: Target::Source(src("IEA")),
                role: RoleSel::NoRole,
            },
        ],
    );
    assert!(e.is_ok(), "clear + unlink is allowed (only set is exact)");
    // Unlinking a missing edge warns.
    let r = meta(
        &db,
        vec![M::UnlinkValue {
            value: vaddr("b"),
            target: Target::Source(src("IEA")),
            role: RoleSel::NoRole,
        }],
    )
    .unwrap();
    assert!(r
        .warnings
        .iter()
        .any(|w| matches!(w.kind, WarningKind::NothingToDelete { .. })));
    let e = meta(
        &db,
        vec![
            stmt(
                Scope::Value(vaddr("b")),
                Action::Set(vec![to_source("IEA")]),
            ),
            M::LinkValue {
                value: vaddr("b"),
                link: to_source("Eurostat"),
            },
        ],
    )
    .unwrap_err();
    assert!(e.to_string().contains("exact link list"), "{e}");
    // A value links to a source or a pair, never a license.
    let e = meta(
        &db,
        vec![M::LinkValue {
            value: vaddr("b"),
            link: LinkSpec {
                target: Target::License(src("x")),
                role: RoleSel::NoRole,
            },
        }],
    )
    .unwrap_err();
    assert!(e.to_string().contains("not a license"), "{e}");
    clean(&db);
}

#[test]
fn roles_are_created_on_purpose_and_enter_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    meta(&db, vec![put_source("IEA")]).unwrap();
    let with_role = |r: &str| LinkSpec {
        target: Target::Source(src("IEA")),
        role: RoleSel::Named(r.into()),
    };
    let e = meta(
        &db,
        vec![M::Link {
            subject: Subject::Dataset,
            link: with_role("primary_source"),
        }],
    )
    .unwrap_err();
    assert!(e.to_string().contains("PutRole"), "{e}");
    meta(
        &db,
        vec![
            M::PutRole {
                role: src("primary_source"),
                rename: None,
                description: None,
                iri: Some("http://www.w3.org/ns/prov#hadPrimarySource".into()),
            },
            M::Link {
                subject: Subject::Dataset,
                link: with_role("primary_source"),
            },
            M::Link {
                subject: Subject::Dataset,
                link: to_source("IEA"),
            },
        ],
    )
    .unwrap();
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM object_metadata_link WHERE valid_to_commit IS NULL \
             AND object_type = 'dataset'"
        ),
        2,
        "one source under a role and under none: two links"
    );
    // A link is idempotent.
    let r = meta(
        &db,
        vec![M::Link {
            subject: Subject::Dataset,
            link: to_source("IEA"),
        }],
    )
    .unwrap();
    assert_eq!(r.metadata[0].effect, MetadataEffect::NoOp);
    // A role in use is restricted.
    let r = meta(&db, vec![M::DeleteRole { role: src("primary_source") }]).unwrap();
    assert_eq!(r.metadata[0].effect, MetadataEffect::Restricted);
    clean(&db);
}

#[test]
fn pool_deletes_judge_the_post_call_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = annotated(dir.path());
    // Restricted by the value link.
    let r = meta(
        &db,
        vec![M::DeleteSource {
            source: src("IEA"),
            cascade: false,
        }],
    )
    .unwrap();
    assert_eq!(r.metadata[0].effect, MetadataEffect::Restricted);
    assert_eq!(r.metadata[0].blockers[0].table, "value_metadata_link");
    // A keep in the same call reopens the link: still restricted.
    let r = write(
        &db,
        vec![seg("p", scalar(&["a"], &[2.5]))],
        vec![
            stmt(Scope::Save, Action::Keep),
            M::DeleteSource {
                source: src("IEA"),
                cascade: false,
            },
        ],
    )
    .unwrap();
    assert_eq!(r.metadata[1].effect, MetadataEffect::Restricted);
    // Unlinking in the same call frees it (links this call removes do not
    // block).
    let r = meta(
        &db,
        vec![
            M::UnlinkValue {
                value: vaddr("a"),
                target: Target::Source(src("IEA")),
                role: RoleSel::NoRole,
            },
            M::DeleteSource {
                source: src("IEA"),
                cascade: false,
            },
        ],
    )
    .unwrap();
    assert_eq!(r.metadata[1].effect, MetadataEffect::Deleted);
    assert_eq!(q(&db, "SELECT count(*) FROM source WHERE valid_to_commit IS NULL"), 0);
    clean(&db);
    // A cascade closes the links; a keep in the same call does not reopen
    // them.
    meta(&db, vec![put_source("S2")]).unwrap();
    meta(
        &db,
        vec![M::LinkValue {
            value: vaddr("a"),
            link: to_source("S2"),
        }],
    )
    .unwrap();
    let r = write(
        &db,
        vec![seg("p", scalar(&["a"], &[2.75]))],
        vec![
            stmt(Scope::Save, Action::Keep),
            M::DeleteSource {
                source: src("S2"),
                cascade: true,
            },
        ],
    )
    .unwrap();
    assert_eq!(r.metadata[1].effect, MetadataEffect::Deleted);
    assert_eq!(live_links(&db), 0);
    clean(&db);
    // A license is restricted by the sources published under it.
    meta(
        &db,
        vec![
            put_source("S3"),
            M::PutLicense {
                license: src("MIT"),
                rename: None,
                fields: LicenseFields {
                    spdx: true,
                    ..Default::default()
                },
            },
        ],
    )
    .unwrap();
    meta(
        &db,
        vec![M::SetSourceLicenses {
            source: src("S3"),
            licenses: vec![src("MIT")],
        }],
    )
    .unwrap();
    let r = meta(
        &db,
        vec![M::DeleteLicense {
            license: src("MIT"),
            cascade: false,
        }],
    )
    .unwrap();
    assert_eq!(r.metadata[0].effect, MetadataEffect::Restricted);
    // Deleting the source closes its license rows with it (identity legs).
    let r = meta(
        &db,
        vec![M::DeleteSource {
            source: src("S3"),
            cascade: false,
        }],
    )
    .unwrap();
    assert_eq!(r.metadata[0].effect, MetadataEffect::Deleted);
    assert_eq!(
        q(&db, "SELECT count(*) FROM source_license WHERE valid_to_commit IS NULL"),
        0
    );
    clean(&db);
}

#[test]
fn same_call_rules() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    write(&db, vec![seg("p", scalar(&["a"], &[1.0]))], vec![]).unwrap();
    // Create + link in one call is fine.
    meta(
        &db,
        vec![
            put_source("IEA"),
            M::Link {
                subject: Subject::Entity {
                    class: "node".into(),
                    name: "a".into(),
                },
                link: to_source("IEA"),
            },
        ],
    )
    .unwrap();
    // Revise + link refused (by name in prepare).
    let e = meta(
        &db,
        vec![
            M::PutSource {
                source: src("IEA"),
                rename: None,
                fields: SourceFields {
                    title: Some("t".into()),
                    ..Default::default()
                },
            },
            M::Link {
                subject: Subject::Dataset,
                link: to_source("IEA"),
            },
        ],
    )
    .unwrap_err();
    assert!(e.to_string().contains("one item per object"), "{e}");
    // An id and a name aliasing one source: caught on resolved ids.
    let id = q(&db, "SELECT id FROM source");
    let e = meta(
        &db,
        vec![
            M::PutSource {
                source: Ref::Id(id),
                rename: None,
                fields: SourceFields {
                    title: Some("t".into()),
                    ..Default::default()
                },
            },
            M::DeleteSource {
                source: src("iea"),
                cascade: true,
            },
        ],
    )
    .unwrap_err();
    assert!(e.to_string().contains("one item per object"), "{e}");
    // Two statements of one scope.
    let e = meta(
        &db,
        vec![stmt(Scope::Save, Action::Keep), stmt(Scope::Save, Action::Clear)],
    )
    .unwrap_err();
    assert!(e.to_string().contains("second keep"), "{e}");
    // A link on an entity deleted in the same call: refused, or dropped
    // under DeleteWins; the entity's own link closes with it either way.
    let set = || ChangeSet {
        object_deletes: vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Entity,
                class: Some("node".into()),
                target: ObjectRef::Name("a".into()),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        metadata: vec![M::Link {
            subject: Subject::Entity {
                class: "node".into(),
                name: "a".into(),
            },
            link: LinkSpec {
                target: Target::Pair(PairRef::Pair {
                    name: "k".into(),
                    value: "v".into(),
                }),
                role: RoleSel::NoRole,
            },
        }],
        ..Default::default()
    };
    meta(
        &db,
        vec![M::CreatePair {
            name: "k".into(),
            value: "v".into(),
        }],
    )
    .unwrap();
    let e = commit_change_set(&db, set(), "t", "m", WriteOptions::default()).unwrap_err();
    assert!(e.to_string().contains("deleted by this call"), "{e}");
    let r = commit_change_set(
        &db,
        set(),
        "t",
        "m",
        WriteOptions {
            covered_writes: CoveredWrites::DeleteWins,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(r.metadata[0].effect, MetadataEffect::DroppedByDelete);
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM object_metadata_link WHERE valid_to_commit IS NULL"
        ),
        0
    );
    clean(&db);
}

#[test]
fn the_stable_views_name_the_links() {
    let dir = tempfile::tempdir().unwrap();
    let db = annotated(dir.path());
    meta(
        &db,
        vec![
            M::PutLicense {
                license: src("CC-BY-4.0"),
                rename: None,
                fields: LicenseFields {
                    spdx: true,
                    ..Default::default()
                },
            },
            M::CreatePair {
                name: "keyword".into(),
                value: "grid".into(),
            },
            M::SetProperty {
                key: "title".into(),
                value: Some("Nordic grid".into()),
            },
        ],
    )
    .unwrap();
    let first = q(&db, "SELECT max(id) FROM commit");
    meta(
        &db,
        vec![
            M::SetSourceLicenses {
                source: src("IEA"),
                licenses: vec![src("CC-BY-4.0")],
            },
            M::Link {
                subject: Subject::Dataset,
                link: LinkSpec {
                    target: Target::Pair(PairRef::Pair {
                        name: "keyword".into(),
                        value: "grid".into(),
                    }),
                    role: RoleSel::NoRole,
                },
            },
        ],
    )
    .unwrap();
    assert_eq!(
        qs(
            &db,
            "SELECT class || '.' || entity || '/' || parameter || '/' || alternative || ' -> ' \
             || target_name FROM value_metadata_named"
        ),
        "node.a/p/Base -> IEA"
    );
    assert_eq!(
        qs(&db, "SELECT target_name || '=' || pair_value FROM dataset_metadata"),
        "keyword=grid"
    );
    assert_eq!(
        qs(&db, "SELECT entity || ' under ' || license FROM value_licenses"),
        "a under CC-BY-4.0"
    );
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM database_properties WHERE key = 'title' AND well_known \
             AND owner = 'user'"
        ),
        1
    );
    // As of the first commits, the value's link already existed under its
    // names; before it, nothing.
    assert_eq!(
        q(&db, &format!("SELECT count(*) FROM value_metadata_asof_named({first})")),
        1
    );
    assert_eq!(q(&db, "SELECT count(*) FROM value_metadata_asof_named(1)"), 0);
    assert_eq!(q(&db, "SELECT count(*) FROM audit_license_non_spdx"), 0);
    clean(&db);
}

/// A metadata item that names a name a rename of the same call freed is
/// refused (R-3), under either delete policy: for every subject kind, a
/// value address and a target scope.
#[test]
fn renamed_away_names_are_refused_in_metadata_items() {
    use nodal_db_core::types::StructuralChange as SC;
    let dir = tempfile::tempdir().unwrap();
    let db = annotated(dir.path());
    run(
        &db,
        ChangeSet {
            structural: vec![SC::CreateScenario {
                name: "s".into(),
                description: None,
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let link = LinkSpec {
        target: Target::Source(src("IEA")),
        role: RoleSel::NoRole,
    };
    let alt = SC::RenameAlternative {
        current: "Base".into(),
        new_name: "Main".into(),
        description: None,
    };
    let class = SC::RenameEntityClass {
        current: "node".into(),
        new_name: "bus".into(),
        description: None,
        allow_on_template: false,
    };
    let par = SC::RenameParameter {
        class: "node".into(),
        current: "p".into(),
        new_name: "p2".into(),
        description: None,
        allow_on_template: false,
    };
    let cases: Vec<(SC, M)> = vec![
        (
            alt.clone(),
            M::Link {
                subject: Subject::Alternative("Base".into()),
                link: link.clone(),
            },
        ),
        (
            SC::RenameScenario {
                current: "s".into(),
                new_name: "s2".into(),
                description: None,
            },
            M::Link {
                subject: Subject::Scenario("s".into()),
                link: link.clone(),
            },
        ),
        (
            class.clone(),
            M::Link {
                subject: Subject::Class("node".into()),
                link: link.clone(),
            },
        ),
        (
            class,
            M::Link {
                subject: Subject::Entity {
                    class: "node".into(),
                    name: "a".into(),
                },
                link: link.clone(),
            },
        ),
        (
            par.clone(),
            M::Link {
                subject: Subject::Parameter {
                    class: "node".into(),
                    name: "p".into(),
                },
                link: link.clone(),
            },
        ),
        (
            par,
            M::LinkValue {
                value: vaddr("a"),
                link: link.clone(),
            },
        ),
        (
            alt,
            stmt(
                Scope::Target {
                    class: "node".into(),
                    parameter: "p".into(),
                    alternative: "Base".into(),
                },
                Action::Keep,
            ),
        ),
    ];
    for (rename, item) in cases {
        for covered in [CoveredWrites::Refuse, CoveredWrites::DeleteWins] {
            let e = commit_change_set(
                &db,
                ChangeSet {
                    structural: vec![rename.clone()],
                    metadata: vec![item.clone()],
                    ..Default::default()
                },
                "t",
                "m",
                WriteOptions {
                    covered_writes: covered,
                    ..Default::default()
                },
            )
            .unwrap_err();
            assert!(e.to_string().contains("renamed to"), "{item:?}: {e}");
        }
    }
    clean(&db);
}

/// A SetSourceLicenses item and a delete of its source or one of its
/// licenses in one call, aliased by id and name, are refused (no live
/// source_license row may dangle).
#[test]
fn set_source_licenses_and_an_aliased_delete_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    meta(
        &db,
        vec![
            put_source("S"),
            M::PutLicense {
                license: src("L"),
                rename: None,
                fields: LicenseFields::default(),
            },
        ],
    )
    .unwrap();
    let sid = q(&db, "SELECT id FROM source");
    let lid = q(&db, "SELECT id FROM license");
    for del in [
        M::DeleteSource {
            source: src("S"),
            cascade: true,
        },
        M::DeleteLicense {
            license: src("L"),
            cascade: true,
        },
    ] {
        let e = meta(
            &db,
            vec![
                M::SetSourceLicenses {
                    source: Ref::Id(sid),
                    licenses: vec![Ref::Id(lid)],
                },
                del,
            ],
        )
        .unwrap_err();
        assert!(e.to_string().contains("sets licenses"), "{e}");
    }
    assert_eq!(q(&db, "SELECT count(*) FROM source_license"), 0);
    clean(&db);
}

/// A target-scope set tracks only its target (plus the linked ones): a
/// write of another, link-free target in the same call is not annotated.
#[test]
fn a_target_scope_set_annotates_only_its_target() {
    let dir = tempfile::tempdir().unwrap();
    let db = annotated(dir.path());
    write(
        &db,
        vec![
            seg("p", scalar(&["c"], &[1.0])),
            seg("q", scalar(&["c"], &[1.0])),
        ],
        vec![stmt(
            Scope::Target {
                class: "node".into(),
                parameter: "p".into(),
                alternative: "Base".into(),
            },
            Action::Set(vec![to_source("IEA")]),
        )],
    )
    .unwrap();
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM value_metadata_named WHERE entity = 'c' AND parameter = 'p'"
        ),
        1
    );
    assert_eq!(
        q(&db, "SELECT count(*) FROM value_metadata_named WHERE parameter = 'q'"),
        0
    );
    clean(&db);
}

/// Two items on one object aliased by id and by name are refused with the
/// one-item-per-object message (never a raw unique-key error, never two
/// merged license sets).
#[test]
fn aliased_items_on_one_object_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    let lic = |n: &str| M::PutLicense {
        license: src(n),
        rename: None,
        fields: LicenseFields::default(),
    };
    meta(
        &db,
        vec![
            put_source("S"),
            lic("L1"),
            lic("L2"),
            M::PutRole {
                role: src("r"),
                rename: None,
                description: None,
                iri: None,
            },
            M::CreatePair {
                name: "k".into(),
                value: "v".into(),
            },
        ],
    )
    .unwrap();
    let id = |t: &str| q(&db, &format!("SELECT id FROM {t} WHERE valid_to_commit IS NULL"));
    let titled = |r: Ref| M::PutSource {
        source: r,
        rename: None,
        fields: SourceFields {
            title: Some("t".into()),
            ..Default::default()
        },
    };
    let role = |r: Ref, d: &str| M::PutRole {
        role: r,
        rename: None,
        description: Some(d.into()),
        iri: None,
    };
    let cases: Vec<Vec<M>> = vec![
        vec![
            M::SetSourceLicenses {
                source: Ref::Id(id("source")),
                licenses: vec![src("L1")],
            },
            M::SetSourceLicenses {
                source: src("S"),
                licenses: vec![src("L2")],
            },
        ],
        vec![titled(Ref::Id(id("source"))), titled(src("s"))],
        vec![role(Ref::Id(id("metadata_role")), "a"), role(src("R"), "b")],
        vec![
            M::RevisePair {
                pair: PairRef::Pair {
                    name: "k".into(),
                    value: "v".into(),
                },
                value: "u".into(),
            },
            M::RevisePair {
                pair: PairRef::Id(id("metadata_pair")),
                value: "w".into(),
            },
        ],
    ];
    for items in cases {
        let e = meta(&db, items.clone()).unwrap_err();
        assert!(
            matches!(&e, DbError::Api(m) if m.contains("one item per object")),
            "{items:?}: {e}"
        );
    }
    assert_eq!(q(&db, "SELECT count(*) FROM source_license"), 0);
    clean(&db);
}
