//! Same-call guards where two channels of one `commit` meet on
//! one object (pre-release review 1):
//!
//! - an activation or membership set in the same call as an object delete
//!   whose cascade reaches its entity (a class delete, an element entity
//!   cascading into its N-D container, a delete by element tuple) is refused
//!   under both policies, never committed as a live row over a closed entity;
//! - a metadata pool object created and then revised, renamed or deleted in
//!   the same call is refused ("one item per object per call"), never a raw
//!   unique-key error or a delete that silently does nothing;
//! - a metadata item on a value or subject whose class, parameter or
//!   alternative an object delete of the call closes is `DroppedByDelete`
//!   under `DeleteWins` and named "deleted by this call" under `Refuse`.
//!
//! Every committing case ends with the integrity audits empty.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{commit, head_commit, ChangeSet, Segment};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    Action, CoveredWrites, EntityAddr, LicenseFields, LinkSpec, MetadataChange as M,
    MetadataEffect, ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, PairRef, Ref,
    RoleSel, Scope, SourceFields, StructuralChange as SC, Subject, Target, ValueAddr, WriteOptions,
    WriteReport,
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

fn seg(param: &str, alt: &str, b: RecordBatch) -> Segment {
    Segment {
        class: "node".into(),
        parameter: param.into(),
        alternative: alt.into(),
        batches: vec![b],
        domains: vec![],
    }
}

fn opts(covered: CoveredWrites) -> WriteOptions {
    WriteOptions {
        covered_writes: covered,
        ..Default::default()
    }
}

const POLICIES: [CoveredWrites; 2] = [CoveredWrites::Refuse, CoveredWrites::DeleteWins];

fn run(db: &Path, set: ChangeSet, o: WriteOptions) -> Result<WriteReport, DbError> {
    commit(db, set, "t", "m", o)
}

fn q(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

/// Every audit view that must be empty on a core-written file (the
/// advisory ones excluded).
fn assert_audits_clean(db: &Path, what: &str) {
    let conn = Connection::open(db).unwrap();
    let views: Vec<String> = {
        let mut stmt = conn
            .prepare(
                "SELECT view_name FROM duckdb_views() WHERE view_name LIKE 'audit_%' \
                 AND view_name NOT IN ('audit_health', 'audit_license_non_spdx', \
                 'audit_erasure', 'audit_tier_intent', 'audit_entity_alternative_redundant') \
                 ORDER BY 1",
            )
            .unwrap();
        let v = stmt
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        v
    };
    let mut bad = Vec::new();
    for v in views {
        let n: i64 = conn
            .query_row(&format!("SELECT count(*) FROM {v}"), [], |r| r.get(0))
            .unwrap();
        if n != 0 {
            bad.push(format!("{v}={n}"));
        }
    }
    conn.close().unwrap();
    assert!(bad.is_empty(), "{what}: audits fired: {bad:?}");
}

fn addr(class: &str, name: &str) -> EntityAddr {
    EntityAddr {
        class: class.into(),
        name: name.into(),
    }
}

fn okey(kind: ObjectKind, class: Option<&str>, target: ObjectRef) -> ObjectDeleteKey {
    ObjectDeleteKey {
        kind,
        class: class.map(Into::into),
        target,
    }
}

fn name(n: &str) -> ObjectRef {
    ObjectRef::Name(n.into())
}

fn link_pair(name: &str, value: &str) -> LinkSpec {
    LinkSpec {
        target: Target::Pair(PairRef::Pair {
            name: name.into(),
            value: value.into(),
        }),
        role: RoleSel::NoRole,
    }
}

fn va(class: &str, p: &str, a: &str, e: &str) -> ValueAddr {
    ValueAddr {
        class: class.into(),
        parameter: p.into(),
        alternative: a.into(),
        entity: e.into(),
    }
}

/// `node {a, b}`, `link(node, node) {ab = (a, b)}`, alternatives `Base` and
/// `alt2`, values `node.p` on `a` and `b` in `Base`, pairs `note = x / y`
/// and a link from `node.p / Base / a` to `note = x`.
fn base_db(dir: &Path, file: &str) -> PathBuf {
    let db = dir.join(file);
    init_schema_with_default(&db, Some("Base")).unwrap();
    let entity = |class: &str, n: &str, el: &[&str]| SC::CreateEntity {
        class: class.into(),
        name: Some(n.into()),
        elements: el.iter().map(|e| e.to_string()).collect(),
        description: None,
    };
    run(
        &db,
        ChangeSet {
            structural: vec![
                SC::CreateEntityClass {
                    name: "node".into(),
                    dims: vec![],
                    description: None,
                },
                SC::CreateEntityClass {
                    name: "link".into(),
                    dims: vec!["node".into(), "node".into()],
                    description: None,
                },
                entity("node", "a", &[]),
                entity("node", "b", &[]),
                entity("link", "ab", &["a", "b"]),
                SC::CreateAlternative {
                    name: "alt2".into(),
                    description: None,
                },
            ],
            segments: vec![seg("p", "Base", scalar(&["a", "b"], &[1.0, 2.0]))],
            metadata: vec![
                M::CreatePair {
                    name: "note".into(),
                    value: "x".into(),
                },
                M::CreatePair {
                    name: "note".into(),
                    value: "y".into(),
                },
                M::LinkValue {
                    value: va("node", "p", "Base", "a"),
                    link: link_pair("note", "x"),
                },
            ],
            ..Default::default()
        },
        WriteOptions::default(),
    )
    .unwrap();
    db
}

// ---------------------------------------------------------------------------
// B1: activation / membership + a cascading object delete
// ---------------------------------------------------------------------------

fn deactivate(class: &str, entity: &str) -> SC {
    SC::SetEntityActivation {
        class: class.into(),
        entity: entity.into(),
        alternative: "Base".into(),
        active: Some(false),
    }
}

fn include_b_in_ab() -> SC {
    SC::SetMembership {
        group: addr("link", "ab"),
        member: addr("node", "b"),
        alternative: None,
        included: Some(true),
    }
}

/// Each case's activation or membership row would be written at this
/// commit, so the cascade (which closes only rows written before it) would
/// leave it live over a closed entity: refused under both policies, with
/// nothing written.
#[test]
fn activation_or_membership_under_a_cascading_delete_is_refused() {
    let cases: Vec<(&str, SC, ObjectDeleteKey)> = vec![
        (
            "activation of node.a + delete class node",
            deactivate("node", "a"),
            okey(ObjectKind::Class, None, name("node")),
        ),
        (
            "activation of link.ab + delete element node.a",
            deactivate("link", "ab"),
            okey(ObjectKind::Entity, Some("node"), name("a")),
        ),
        (
            "activation of link.ab + delete link by elements [a, b]",
            deactivate("link", "ab"),
            okey(
                ObjectKind::Entity,
                Some("link"),
                ObjectRef::Elements(vec!["a".into(), "b".into()]),
            ),
        ),
        (
            "membership link.ab > node.b + delete class link",
            include_b_in_ab(),
            okey(ObjectKind::Class, None, name("link")),
        ),
        (
            "membership link.ab > node.b + delete element node.a",
            include_b_in_ab(),
            okey(ObjectKind::Entity, Some("node"), name("a")),
        ),
    ];
    let dir = tempfile::tempdir().unwrap();
    for (label, change, key) in cases {
        for policy in POLICIES {
            let db = base_db(
                dir.path(),
                &format!("b1_{}.duckdb", file_label(label, policy)),
            );
            let head = head_commit(&db).unwrap();
            let err = run(
                &db,
                ChangeSet {
                    structural: vec![change.clone()],
                    object_deletes: vec![(key.clone(), ObjectDeleteOpts { cascade: true })],
                    ..Default::default()
                },
                opts(policy),
            )
            .expect_err(label);
            assert!(
                matches!(&err, DbError::Api(m)
                    if m.contains("structural change of this call creates or revises")),
                "{label} / {policy:?}: {err:?}"
            );
            assert_eq!(
                head_commit(&db).unwrap(),
                head,
                "{label}: nothing committed"
            );
            assert_audits_clean(&db, label);
        }
    }
}

/// A file name per case and policy.
fn file_label(label: &str, policy: CoveredWrites) -> String {
    let mut s: String = label
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    s.push_str(&format!("_{policy:?}"));
    s
}

/// The guard counts only rows the call leaves written: an activation that
/// changes nothing (`NoOp`) or a membership it clears does not stop the
/// delete, and the cascade then closes the pre-call rows as usual.
#[test]
fn a_noop_activation_or_cleared_membership_does_not_block_the_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = base_db(dir.path(), "noop.duckdb");
    run(
        &db,
        ChangeSet {
            structural: vec![deactivate("link", "ab"), include_b_in_ab()],
            ..Default::default()
        },
        WriteOptions::default(),
    )
    .unwrap();
    let r = run(
        &db,
        ChangeSet {
            structural: vec![
                deactivate("link", "ab"),
                SC::SetMembership {
                    group: addr("link", "ab"),
                    member: addr("node", "b"),
                    alternative: None,
                    included: None,
                },
            ],
            object_deletes: vec![(
                okey(ObjectKind::Entity, Some("node"), name("a")),
                ObjectDeleteOpts { cascade: true },
            )],
            ..Default::default()
        },
        WriteOptions::default(),
    )
    .expect("a NoOp activation and a cleared membership write no row");
    assert_eq!(r.object_deletes.len(), 1);
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM entity WHERE valid_to_commit IS NULL AND name = 'ab'"
        ),
        0
    );
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM entity_alternative WHERE valid_to_commit IS NULL"
        ),
        0
    );
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM membership WHERE valid_to_commit IS NULL"
        ),
        0
    );
    assert_audits_clean(&db, "noop activation + cleared membership");
}

// ---------------------------------------------------------------------------
// S1: a pool object created and touched again in the same call
// ---------------------------------------------------------------------------

fn create_pair(k: &str, v: &str) -> M {
    M::CreatePair {
        name: k.into(),
        value: v.into(),
    }
}

fn put_source(r: Ref, rename: Option<&str>) -> M {
    M::PutSource {
        source: r,
        rename: rename.map(Into::into),
        fields: SourceFields::default(),
    }
}

fn put_license(r: Ref, rename: Option<&str>) -> M {
    M::PutLicense {
        license: r,
        rename: rename.map(Into::into),
        fields: LicenseFields::default(),
    }
}

fn put_role(r: Ref, rename: Option<&str>) -> M {
    M::PutRole {
        role: r,
        rename: rename.map(Into::into),
        description: None,
        iri: None,
    }
}

fn id_of(db: &Path, table: &str) -> i64 {
    q(db, &format!("SELECT coalesce(max(id), 0) + 1 FROM {table}"))
}

#[test]
fn a_pool_object_created_and_touched_again_in_one_call_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let probe = base_db(dir.path(), "ids.duckdb");
    // The id the next create of each kind gets (sequences start fresh on a
    // file like `probe`, and every case runs on such a file).
    let next_source = id_of(&probe, "source");
    let next_license = id_of(&probe, "license");
    let next_role = id_of(&probe, "metadata_role");
    let pair_k = || PairRef::Pair {
        name: "k".into(),
        value: "v".into(),
    };
    let cases: Vec<(&str, Vec<M>)> = vec![
        (
            "CreatePair + RevisePair",
            vec![
                create_pair("k", "v"),
                M::RevisePair {
                    pair: pair_k(),
                    value: "w".into(),
                },
            ],
        ),
        (
            "CreatePair + RenamePairKey",
            vec![
                create_pair("k", "v"),
                M::RenamePairKey {
                    from: "k".into(),
                    to: "k2".into(),
                },
            ],
        ),
        (
            "CreatePair + DeletePair",
            vec![
                create_pair("k", "v"),
                M::DeletePair {
                    pair: pair_k(),
                    cascade: true,
                },
            ],
        ),
        (
            "PutSource create + PutSource by its new id",
            vec![
                put_source(Ref::Name("s8".into()), None),
                put_source(Ref::Id(next_source), Some("s8b")),
            ],
        ),
        (
            "PutSource create + DeleteSource by its new id",
            vec![
                put_source(Ref::Name("s8".into()), None),
                M::DeleteSource {
                    source: Ref::Id(next_source),
                    cascade: true,
                },
            ],
        ),
        (
            "PutLicense create + PutLicense by its new id",
            vec![
                put_license(Ref::Name("l8".into()), None),
                put_license(Ref::Id(next_license), Some("l8b")),
            ],
        ),
        (
            "PutLicense create + DeleteLicense by its new id",
            vec![
                put_license(Ref::Name("l8".into()), None),
                M::DeleteLicense {
                    license: Ref::Id(next_license),
                    cascade: false,
                },
            ],
        ),
        (
            "PutRole create + PutRole by its new id",
            vec![
                put_role(Ref::Name("r8".into()), None),
                put_role(Ref::Id(next_role), Some("r8b")),
            ],
        ),
        (
            "PutRole create + DeleteRole by its new id",
            vec![
                put_role(Ref::Name("r8".into()), None),
                M::DeleteRole {
                    role: Ref::Id(next_role),
                },
            ],
        ),
    ];
    for (label, items) in cases {
        let db = base_db(
            dir.path(),
            &format!("s1_{}.duckdb", file_label(label, CoveredWrites::Refuse)),
        );
        let head = head_commit(&db).unwrap();
        let err = run(
            &db,
            ChangeSet {
                metadata: items,
                ..Default::default()
            },
            WriteOptions::default(),
        )
        .expect_err(label);
        assert!(
            matches!(&err, DbError::Api(m) if m.contains("one item per object per call")),
            "{label}: {err:?}"
        );
        assert_eq!(
            head_commit(&db).unwrap(),
            head,
            "{label}: nothing committed"
        );
    }
}

/// Creating an object and linking to it (or licensing it) in one call
/// stays allowed: only a revise, rename or delete is a second item on it.
#[test]
fn a_pool_object_created_and_linked_in_one_call_still_commits() {
    let dir = tempfile::tempdir().unwrap();
    let db = base_db(dir.path(), "link.duckdb");
    let r = run(
        &db,
        ChangeSet {
            metadata: vec![
                create_pair("k", "v"),
                M::Link {
                    subject: Subject::Dataset,
                    link: link_pair("k", "v"),
                },
                put_source(Ref::Name("s7".into()), None),
                put_license(Ref::Name("l7".into()), None),
                M::SetSourceLicenses {
                    source: Ref::Name("s7".into()),
                    licenses: vec![Ref::Name("l7".into())],
                },
                create_pair("k", "v"),
            ],
            ..Default::default()
        },
        WriteOptions::default(),
    )
    .unwrap();
    let effects: Vec<MetadataEffect> = r.metadata.iter().map(|o| o.effect).collect();
    assert_eq!(effects[0], MetadataEffect::Created);
    assert_eq!(
        effects[5],
        MetadataEffect::NoOp,
        "a repeated create is idempotent"
    );
    assert_audits_clean(&db, "create + link");
}

// ---------------------------------------------------------------------------
// S2: metadata on a value or subject whose class, parameter or alternative
// an object delete of the call closes
// ---------------------------------------------------------------------------

#[test]
fn metadata_under_a_deleted_class_parameter_or_alternative_is_covered() {
    let deletes: Vec<(&str, ObjectDeleteKey, &str)> = vec![
        (
            "parameter",
            okey(ObjectKind::Parameter, Some("node"), name("p")),
            "its parameter \"p\"",
        ),
        (
            "alternative",
            okey(ObjectKind::Alternative, None, name("Base")),
            "its alternative \"Base\"",
        ),
        (
            "class",
            okey(ObjectKind::Class, None, name("node")),
            "its class \"node\"",
        ),
        (
            "entity",
            okey(ObjectKind::Entity, Some("node"), name("a")),
            "",
        ),
    ];
    let dir = tempfile::tempdir().unwrap();
    for (label, key, names) in deletes {
        // Value-level items: a value link, a value-scope statement and an
        // unlink, all on node.p / Base values.
        let value_items = || {
            vec![
                M::LinkValue {
                    value: va("node", "p", "Base", "a"),
                    link: link_pair("note", "y"),
                },
                M::ValueMetadata {
                    scope: Scope::Value(va("node", "p", "Base", "a")),
                    action: Action::Clear,
                },
                M::UnlinkValue {
                    value: va("node", "p", "Base", "a"),
                    target: Target::Pair(PairRef::Pair {
                        name: "note".into(),
                        value: "x".into(),
                    }),
                    role: RoleSel::NoRole,
                },
            ]
        };
        let db = base_db(dir.path(), &format!("s2v_{label}.duckdb"));
        let r = run(
            &db,
            ChangeSet {
                object_deletes: vec![(key.clone(), ObjectDeleteOpts { cascade: true })],
                metadata: value_items(),
                ..Default::default()
            },
            opts(CoveredWrites::DeleteWins),
        )
        .unwrap_or_else(|e| panic!("value items under deleted {label}: {e:?}"));
        // The link and the statement are dropped; the unlink is a NoOp (the
        // delete closed the link it names), as for a deleted entity.
        assert_eq!(
            r.metadata.iter().map(|o| o.effect).collect::<Vec<_>>(),
            vec![
                MetadataEffect::DroppedByDelete,
                MetadataEffect::DroppedByDelete,
                MetadataEffect::NoOp
            ],
            "value items under deleted {label}"
        );
        assert_audits_clean(&db, label);
        // Under the default policy: refused, naming the deletion.
        let db = base_db(dir.path(), &format!("s2r_{label}.duckdb"));
        let head = head_commit(&db).unwrap();
        let err = run(
            &db,
            ChangeSet {
                object_deletes: vec![(key.clone(), ObjectDeleteOpts { cascade: true })],
                metadata: value_items(),
                ..Default::default()
            },
            WriteOptions::default(),
        )
        .expect_err(label);
        assert!(
            matches!(&err, DbError::Api(m) if m.contains("is deleted by this call")
                && m.contains(names)),
            "value items under deleted {label}: {err:?}"
        );
        assert_eq!(head_commit(&db).unwrap(), head);

        // Subject-level items: links from an entity and a parameter of node.
        if label == "alternative" {
            continue; // neither subject resolves through an alternative
        }
        let subject_items = || {
            vec![
                M::Link {
                    subject: Subject::Entity {
                        class: "node".into(),
                        name: "a".into(),
                    },
                    link: link_pair("note", "x"),
                },
                M::Link {
                    subject: Subject::Parameter {
                        class: "node".into(),
                        name: "p".into(),
                    },
                    link: link_pair("note", "x"),
                },
            ]
        };
        let db = base_db(dir.path(), &format!("s2s_{label}.duckdb"));
        let r = run(
            &db,
            ChangeSet {
                object_deletes: vec![(key.clone(), ObjectDeleteOpts { cascade: true })],
                metadata: subject_items(),
                ..Default::default()
            },
            opts(CoveredWrites::DeleteWins),
        )
        .unwrap_or_else(|e| panic!("subject items under deleted {label}: {e:?}"));
        let want: Vec<MetadataEffect> = match label {
            // The parameter subject is live when only the entity goes, and
            // the entity subject when only the parameter goes.
            "entity" => vec![MetadataEffect::DroppedByDelete, MetadataEffect::Created],
            "parameter" => vec![MetadataEffect::Created, MetadataEffect::DroppedByDelete],
            _ => vec![MetadataEffect::DroppedByDelete; 2],
        };
        assert_eq!(
            r.metadata.iter().map(|o| o.effect).collect::<Vec<_>>(),
            want,
            "subject items under deleted {label}"
        );
        assert_audits_clean(&db, label);
        if label == "class" {
            let db = base_db(dir.path(), "s2sr_class.duckdb");
            let err = run(
                &db,
                ChangeSet {
                    object_deletes: vec![(key.clone(), ObjectDeleteOpts { cascade: true })],
                    metadata: subject_items(),
                    ..Default::default()
                },
                WriteOptions::default(),
            )
            .expect_err("subject under deleted class, Refuse");
            assert!(
                matches!(&err, DbError::Api(m)
                    if m.contains("entity_class \"node\" is deleted by this call")),
                "{err:?}"
            );
        }
    }
}

/// An unknown class, parameter or alternative is still "unknown", not
/// "deleted": the closed-here check looks only at this call's commit.
#[test]
fn metadata_on_an_unknown_parameter_is_still_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let db = base_db(dir.path(), "unknown.duckdb");
    for (v, what) in [
        (va("node", "nope", "Base", "a"), "unknown parameter"),
        (va("node", "p", "nope", "a"), "unknown alternative"),
        (va("nope", "p", "Base", "a"), "unknown class"),
    ] {
        let err = run(
            &db,
            ChangeSet {
                metadata: vec![M::LinkValue {
                    value: v,
                    link: link_pair("note", "y"),
                }],
                ..Default::default()
            },
            opts(CoveredWrites::DeleteWins),
        )
        .expect_err(what);
        assert!(
            matches!(&err, DbError::Api(m) if m.contains(what)),
            "{what}: {err:?}"
        );
    }
}
