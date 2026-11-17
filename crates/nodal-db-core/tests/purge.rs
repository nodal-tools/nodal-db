//! Purge (entity_descriptions_purge_PLAN §3): a bulk, versioned close-out of
//! every item in a named scope, in one commit, through the object delete's
//! leg registry in set form. Covers each scope, true replace (survivors at
//! each grain, spared upward, V-B1), the same-commit guard, reach outside a
//! scope, names (NFC, renamed-away, unknown), no-op calls, overlap with an
//! object delete, the conflict retry and the statement-count scale guard.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{commit_change_set, purge, ChangeSet, Segment};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::types::{
    EntityAddr, ObjectDeleteKey, ObjectDeleteOpts, ObjectDeleteResult, ObjectKind, ObjectRef,
    PurgeRequest, PurgeScope as PS, StructuralChange as SC, WriteOptions, WriteReport,
};

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

fn o() -> WriteOptions {
    WriteOptions::default()
}

fn scalars(rows: &[(&str, f64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

/// A full rank-1 integer-indexed float series (array tier).
fn series(entity: &str, vals: &[f64]) -> RecordBatch {
    let n = vals.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; n])) as ArrayRef,
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn seg(class: &str, param: &str, alt: &str, b: RecordBatch) -> Segment {
    Segment {
        class: class.into(),
        parameter: param.into(),
        alternative: alt.into(),
        batches: vec![b],
        domains: vec![],
    }
}

fn class(name: &str, dims: &[&str]) -> SC {
    SC::CreateEntityClass {
        name: name.into(),
        dims: dims.iter().map(|d| d.to_string()).collect(),
        description: None,
    }
}

fn entity(class: &str, name: &str) -> SC {
    SC::CreateEntity {
        class: class.into(),
        name: Some(name.into()),
        elements: vec![],
        description: None,
    }
}

fn nd(class: &str, elements: &[&str]) -> SC {
    SC::CreateEntity {
        class: class.into(),
        name: None,
        elements: elements.iter().map(|e| e.to_string()).collect(),
        description: None,
    }
}

fn alt(name: &str) -> SC {
    SC::CreateAlternative {
        name: name.into(),
        description: None,
    }
}

fn activation(class: &str, entity: &str, alt: &str, active: bool) -> SC {
    SC::SetEntityActivation {
        class: class.into(),
        entity: entity.into(),
        alternative: alt.into(),
        active: Some(active),
    }
}

fn membership(group: (&str, &str), member: (&str, &str), alt: Option<&str>) -> SC {
    SC::SetMembership {
        group: EntityAddr {
            class: group.0.into(),
            name: group.1.into(),
        },
        member: EntityAddr {
            class: member.0.into(),
            name: member.1.into(),
        },
        alternative: alt.map(Into::into),
        included: Some(true),
    }
}

fn ranking(scenario: &str, alts: &[&str]) -> SC {
    SC::SetScenarioAlternatives {
        scenario: scenario.into(),
        alternatives: alts.iter().map(|a| a.to_string()).collect(),
    }
}

fn req(scope: PS) -> PurgeRequest {
    PurgeRequest::new(scope)
}

fn call(db: &Path, set: ChangeSet) -> Result<WriteReport, DbError> {
    commit_change_set(db, set, "t", "m", o())
}

fn purge1(db: &Path, scope: PS) -> WriteReport {
    purge(db, vec![req(scope)], "t", "purge", o()).unwrap()
}

/// node n1..n2, unit u1..u2, link(node, unit) n1__u1 and n2__u2;
/// alternatives Base and high, scenario sc = [Base, high]; activations,
/// memberships (u1 groups n1 at base and n2 in high), scalar values in
/// three classes and two alternatives, a node location and an array.
fn fixture(dir: &tempfile::TempDir) -> PathBuf {
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    call(
        &db,
        ChangeSet {
            structural: vec![
                class("node", &[]),
                class("unit", &[]),
                class("link", &["node", "unit"]),
                alt("Base"),
                alt("high"),
                entity("node", "n1"),
                entity("node", "n2"),
                entity("unit", "u1"),
                entity("unit", "u2"),
                nd("link", &["n1", "u1"]),
                nd("link", &["n2", "u2"]),
                ranking("sc", &["Base", "high"]),
                activation("node", "n1", "Base", true),
                activation("node", "n2", "high", false),
                membership(("unit", "u1"), ("node", "n1"), None),
                membership(("unit", "u1"), ("node", "n2"), Some("high")),
            ],
            segments: vec![
                seg(
                    "node",
                    "demand",
                    "Base",
                    scalars(&[("n1", 1.0), ("n2", 2.0)]),
                ),
                seg("node", "demand", "high", scalars(&[("n1", 10.0)])),
                seg("unit", "cap", "Base", scalars(&[("u1", 5.0), ("u2", 6.0)])),
                seg(
                    "link",
                    "flow",
                    "Base",
                    scalars(&[("n1__u1", 7.0), ("n2__u2", 8.0)]),
                ),
                seg("node", "profile", "Base", series("n1", &[1.0, 2.0, 3.0])),
            ],
            ..Default::default()
        },
    )
    .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        "INSERT INTO commit (user, message) VALUES ('t', 'locations'); \
         INSERT INTO entity_location (entity_id, lat, lon, valid_from_commit) \
         SELECT id, 60, 25, (SELECT max(id) FROM commit) FROM entity \
         WHERE valid_to_commit IS NULL AND name IN ('n1', 'n2');",
    )
    .unwrap();
    db
}

fn q(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    conn.query_row(sql, [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn live(db: &Path, table: &str) -> i64 {
    q(
        db,
        &format!("SELECT count(*) FROM {table} WHERE valid_to_commit IS NULL"),
    )
}

fn names(db: &Path, sql: &str) -> Vec<String> {
    let conn = Connection::open(db).unwrap();
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn live_entities(db: &Path) -> Vec<String> {
    names(
        db,
        "SELECT name FROM entity WHERE valid_to_commit IS NULL ORDER BY name",
    )
}

fn head(db: &Path) -> i64 {
    q(db, "SELECT max(id) FROM commit")
}

/// Every table's row count (any validity): a purge only closes rows.
fn all_rows(db: &Path) -> Vec<(String, i64)> {
    [
        "entity",
        "entity_element",
        "entity_alternative",
        "membership",
        "entity_location",
        "value_scalar",
        "value_array",
        "value_array_hist",
        "alternative",
        "scenario",
        "scenario_alternative",
    ]
    .iter()
    .map(|t| (t.to_string(), q(db, &format!("SELECT count(*) FROM {t}"))))
    .collect()
}

// ---------------------------------------------------------------------------
// Each scope (test 4)
// ---------------------------------------------------------------------------

#[test]
fn entities_of_a_class_close_with_the_full_cascade() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let before = head(&db);
    // Entities{class: unit} reaches the links (class link) through B1:
    // outside its scope, so it is refused unless allowed.
    let err = purge(
        &db,
        vec![req(PS::Entities {
            class: Some("unit".into()),
        })],
        "t",
        "m",
        o(),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("outside its scope"), "{msg}");
    assert!(msg.contains("2 entity of link (n1__u1, n2__u2)"), "{msg}");
    assert_eq!(head(&db), before, "refused: nothing written");

    let r = purge(
        &db,
        vec![PurgeRequest {
            allow_outside_scope: true,
            ..req(PS::Entities {
                class: Some("unit".into()),
            })
        }],
        "t",
        "m",
        o(),
    )
    .unwrap();
    assert_eq!(live_entities(&db), ["n1", "n2"]);
    let p = &r.purges[0];
    assert_eq!(p.request_index, 0);
    assert_eq!(p.objects, 4); // u1, u2 and the two links
                              // u1, u2, both links' values; membership rows grouped by u1; the links'
                              // element rows (identity).
    let table = |t: &str| {
        p.closed_out
            .per_table
            .iter()
            .find(|c| c.table == t)
            .map(|c| c.rows)
            .unwrap_or(0)
    };
    assert_eq!(table("value_scalar"), 4);
    assert_eq!(table("membership"), 2);
    assert_eq!(p.closed_out.object_rows, 4 + 4); // 4 entities + 4 element rows
    assert_eq!(live(&db, "membership"), 0);
    // Node values, activations and locations stay.
    assert_eq!(live(&db, "value_scalar"), 3);
    assert_eq!(live(&db, "entity_alternative"), 2);
    assert_eq!(live(&db, "entity_location"), 2);
    // History is kept: as-of the commit before, everything is visible, and
    // no erasure marker exists.
    assert_eq!(
        q(&db, &format!("SELECT count(*) FROM entity_asof({before})")),
        6
    );
    assert_eq!(q(&db, "SELECT count(*) FROM erasure_marker"), 0);
}

#[test]
fn entities_all_alternatives_scenarios_activations_memberships() {
    let dir = tempfile::tempdir().unwrap();

    // Activations{class: node, alternative: high}: only n2@high.
    let db = fixture(&dir);
    let r = purge1(
        &db,
        PS::Activations {
            class: Some("node".into()),
            alternative: Some("high".into()),
        },
    );
    assert_eq!(r.purges[0].closed_out.total_rows, 1);
    assert_eq!(r.purges[0].objects, 0);
    assert_eq!(live(&db, "entity_alternative"), 1);
    // Activations{} closes the rest.
    purge1(
        &db,
        PS::Activations {
            class: None,
            alternative: None,
        },
    );
    assert_eq!(live(&db, "entity_alternative"), 0);
    assert_eq!(live_entities(&db).len(), 6, "rows only, never objects");

    // Memberships{group_class: unit}.
    let r = purge1(
        &db,
        PS::Memberships {
            group_class: Some("unit".into()),
        },
    );
    assert_eq!(r.purges[0].closed_out.total_rows, 2);
    assert_eq!(live(&db, "membership"), 0);

    // Scenarios: the scenario row and its ranking.
    let r = purge1(&db, PS::Scenarios);
    assert_eq!(r.purges[0].objects, 1);
    assert_eq!(live(&db, "scenario"), 0);
    assert_eq!(live(&db, "scenario_alternative"), 0);

    // Alternatives{except: [Base]}: high and its values.
    let r = purge1(
        &db,
        PS::Alternatives {
            except: vec!["Base".into()],
        },
    );
    assert_eq!(r.purges[0].objects, 1);
    assert_eq!(
        names(
            &db,
            "SELECT name FROM alternative WHERE valid_to_commit IS NULL"
        ),
        ["Base"]
    );
    assert_eq!(live(&db, "value_scalar"), 6);

    // Entities{} closes every entity and everything keyed on them; the
    // array moves to its history twin.
    let r = purge1(&db, PS::Entities { class: None });
    assert_eq!(r.purges[0].objects, 6);
    assert_eq!(live(&db, "entity"), 0);
    assert_eq!(live(&db, "value_scalar"), 0);
    assert_eq!(live(&db, "entity_location"), 0);
    assert_eq!(live(&db, "entity_element"), 0);
    assert_eq!(q(&db, "SELECT count(*) FROM value_array"), 0);
    assert_eq!(q(&db, "SELECT count(*) FROM value_array_hist"), 1);
}

#[test]
fn a_purge_only_closes_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let rows = all_rows(&db);
    purge(
        &db,
        vec![
            req(PS::Entities { class: None }),
            req(PS::Alternatives { except: vec![] }),
            req(PS::Scenarios),
        ],
        "t",
        "m",
        o(),
    )
    .unwrap();
    for ((t, before), (_, after)) in rows.iter().zip(all_rows(&db)) {
        if t == "value_array" {
            assert_eq!(after, 0);
        } else if t == "value_array_hist" {
            assert_eq!(after, before + 1);
        } else {
            assert_eq!(after, *before, "{t}: rows are closed, never removed");
        }
    }
}

// ---------------------------------------------------------------------------
// True replace (test 7, V-B1)
// ---------------------------------------------------------------------------

#[test]
fn entities_purge_and_reimport_keeps_restated_closes_unstated() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    // A re-import of n1 (with its Base activation unchanged and one
    // unchanged value) and nothing else.
    let r = call(
        &db,
        ChangeSet {
            structural: vec![entity("node", "n1"), activation("node", "n1", "Base", true)],
            segments: vec![seg("node", "demand", "Base", scalars(&[("n1", 1.0)]))],
            purges: vec![PurgeRequest {
                allow_outside_scope: true,
                ..req(PS::Entities { class: None })
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(r.commit_id > 0);
    // n1 survives (and its class with it); every other entity closes.
    assert_eq!(live_entities(&db), ["n1"]);
    // Its re-stated value and activation survive; its unstated values (the
    // high demand, the profile array), its location and its membership
    // close.
    assert_eq!(live(&db, "value_scalar"), 1);
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM value_scalar s JOIN alternative a ON a.id = s.alternative_id \
             WHERE s.valid_to_commit IS NULL AND a.name = 'Base' AND s.v_float = 1"
        ),
        1
    );
    assert_eq!(live(&db, "entity_alternative"), 1);
    assert_eq!(q(&db, "SELECT count(*) FROM value_array"), 0);
    assert_eq!(live(&db, "entity_location"), 0);
    assert_eq!(live(&db, "membership"), 0);
    let p = &r.purges[0];
    assert_eq!(p.spared.object_rows, 1, "n1");
    assert!(p
        .spared
        .per_table
        .iter()
        .any(|t| t.table == "value_scalar" && t.rows == 1));
    assert!(p
        .spared
        .per_table
        .iter()
        .any(|t| t.table == "entity_alternative" && t.rows == 1));
    // The unchanged value was not rewritten: still version 1.
    assert_eq!(
        q(
            &db,
            "SELECT max(version) FROM value_scalar WHERE valid_to_commit IS NULL"
        ),
        1
    );
}

#[test]
fn noop_restatements_survive_unstated_close() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    call(
        &db,
        ChangeSet {
            structural: vec![
                activation("node", "n1", "Base", true), // NoOp re-statement
                membership(("unit", "u1"), ("node", "n1"), None), // NoOp
                ranking("sc", &["Base", "high"]),       // NoOp
            ],
            purges: vec![
                req(PS::Activations {
                    class: None,
                    alternative: None,
                }),
                req(PS::Memberships { group_class: None }),
                req(PS::Scenarios),
            ],
            ..Default::default()
        },
    )
    .unwrap();
    // The re-stated rows survive; n2@high (activation) and u1/n2@high
    // (membership) close; the ranked scenario survives with its ranking.
    assert_eq!(
        names(
            &db,
            "SELECT e.name FROM entity_alternative ea JOIN entity e ON e.id = ea.entity_id \
             WHERE ea.valid_to_commit IS NULL"
        ),
        ["n1"]
    );
    assert_eq!(live(&db, "membership"), 1);
    assert_eq!(
        q(&db, "SELECT count(*) FROM membership WHERE valid_to_commit IS NULL AND alternative_id IS NULL"),
        1
    );
    assert_eq!(live(&db, "scenario"), 1);
    assert_eq!(live(&db, "scenario_alternative"), 2);
}

#[test]
fn spared_nd_entity_spares_its_elements_transitively() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    // flow(link, unit): a three-level chain over the fixture's links.
    call(
        &db,
        ChangeSet {
            structural: vec![
                class("flow", &["link", "unit"]),
                nd("flow", &["n1__u1", "u2"]),
            ],
            ..Default::default()
        },
    )
    .unwrap();
    // Re-stating only the flow entity spares its link, the link's node and
    // unit, and u2; n2 and its link close.
    call(
        &db,
        ChangeSet {
            structural: vec![nd("flow", &["n1__u1", "u2"])],
            purges: vec![req(PS::Entities { class: None })],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        live_entities(&db),
        ["n1", "n1__u1", "n1__u1__u2", "u1", "u2"]
    );
}

#[test]
fn frontier_reaching_a_survivor_is_internal() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let before = head(&db);
    let err = nodal_db_core::purge::test_hooks::without_upward_sparing(|| {
        call(
            &db,
            ChangeSet {
                structural: vec![nd("link", &["n1", "u1"])],
                purges: vec![req(PS::Entities { class: None })],
                ..Default::default()
            },
        )
    })
    .unwrap_err();
    assert!(matches!(err, DbError::Internal(_)), "{err}");
    assert_eq!(head(&db), before);
}

// ---------------------------------------------------------------------------
// Same-commit guard (test 8), overlap and no-op (test 9)
// ---------------------------------------------------------------------------

#[test]
fn rows_written_this_call_are_never_closed() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    // With the survivor sets forced empty, only the guard protects what the
    // call writes: a new entity, a changed value, a new activation.
    let r = nodal_db_core::purge::test_hooks::without_survivors(|| {
        call(
            &db,
            ChangeSet {
                structural: vec![entity("node", "n9"), activation("node", "n9", "Base", true)],
                segments: vec![seg(
                    "node",
                    "demand",
                    "Base",
                    scalars(&[("n1", 99.0), ("n9", 9.0)]),
                )],
                purges: vec![PurgeRequest {
                    allow_outside_scope: true,
                    ..req(PS::Entities { class: None })
                }],
                ..Default::default()
            },
        )
    })
    .unwrap();
    assert!(r.commit_id > 0);
    // n9 was created this commit; n1 existed (its old value row closed by
    // the write, the new one written this commit): neither row closes.
    assert_eq!(live_entities(&db), ["n9"]);
    assert_eq!(
        q(
            &db,
            &format!(
                "SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NULL \
                 AND valid_from_commit = {}",
                r.commit_id
            )
        ),
        2
    );
    assert_eq!(live(&db, "entity_alternative"), 1);
}

#[test]
fn overlap_with_an_object_delete_counts_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let r = call(
        &db,
        ChangeSet {
            object_deletes: vec![(
                ObjectDeleteKey {
                    kind: ObjectKind::Alternative,
                    class: None,
                    target: ObjectRef::Name("high".into()),
                },
                ObjectDeleteOpts { cascade: true },
            )],
            purges: vec![
                req(PS::Alternatives { except: vec![] }),
                req(PS::Alternatives { except: vec![] }),
            ],
            ..Default::default()
        },
    )
    .unwrap();
    let ObjectDeleteResult::Deleted { closed_out } = &r.object_deletes[0].result else {
        panic!("deleted");
    };
    assert_eq!(closed_out.object_rows, 1);
    // The purge closes only Base (high is gone already), once.
    assert_eq!(r.purges[0].objects, 1);
    assert_eq!(r.purges[1].objects, 0);
    assert_eq!(r.purges[1].closed_out.total_rows, 0);
    assert_eq!(live(&db, "alternative"), 0);
    assert_eq!(live(&db, "value_scalar"), 0);
}

#[test]
fn a_purge_that_closes_nothing_makes_no_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    purge1(&db, PS::Scenarios);
    let before = head(&db);
    let r = purge1(&db, PS::Scenarios);
    assert_eq!(r.commit_id, 0);
    assert_eq!(head(&db), before);
    // A purge whose every row the call re-states: nothing closes.
    let r = call(
        &db,
        ChangeSet {
            structural: vec![activation("node", "n1", "Base", true)],
            purges: vec![req(PS::Activations {
                class: Some("node".into()),
                alternative: Some("Base".into()),
            })],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(r.commit_id, 0);
    assert_eq!(head(&db), before);
}

#[test]
fn a_refused_request_rolls_back_the_whole_call() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let before = head(&db);
    let err = call(
        &db,
        ChangeSet {
            structural: vec![alt("new_alt")],
            segments: vec![seg("node", "demand", "Base", scalars(&[("n1", 123.0)]))],
            purges: vec![
                req(PS::Scenarios),
                req(PS::Entities {
                    class: Some("node".into()),
                }),
            ],
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("request 1"), "{err}");
    assert_eq!(head(&db), before);
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM alternative WHERE name = 'new_alt'"
        ),
        0
    );
}

// ---------------------------------------------------------------------------
// Names (test 6, 13)
// ---------------------------------------------------------------------------

#[test]
fn names_resolve_by_fold_and_nfc_unknown_and_renamed_away_refuse() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    call(
        &db,
        ChangeSet {
            structural: vec![alt("Café")],
            ..Default::default()
        },
    )
    .unwrap();
    // An NFD spelling, case-folded, resolves (NameFolded warns).
    let r = purge1(
        &db,
        PS::Alternatives {
            except: vec!["BASE".into(), "HIGH".into(), "cafe\u{301}".into()],
        },
    );
    assert_eq!(r.purges[0].objects, 0);
    assert!(r
        .warnings
        .iter()
        .any(|w| matches!(&w.kind, nodal_db_core::types::WarningKind::NameFolded { what, .. } if what == "purge")));
    // Unknown names refuse.
    for scope in [
        PS::Entities {
            class: Some("nope".into()),
        },
        PS::Alternatives {
            except: vec!["nope".into()],
        },
        PS::Values {
            class: Some("node".into()),
            parameter: Some("nope".into()),
            alternative: None,
        },
    ] {
        let err = purge(&db, vec![req(scope)], "t", "m", o()).unwrap_err();
        assert!(err.to_string().contains("unknown"), "{err}");
    }
    // A parameter needs its class; a blank name refuses.
    for scope in [
        PS::Values {
            class: None,
            parameter: Some("demand".into()),
            alternative: None,
        },
        PS::Entities {
            class: Some(" ".into()),
        },
    ] {
        assert!(purge(&db, vec![req(scope)], "t", "m", o()).is_err());
    }
    // A rename and a filter on the old name in one call refuse.
    let err = call(
        &db,
        ChangeSet {
            structural: vec![SC::RenameAlternative {
                current: "high".into(),
                new_name: "peak".into(),
                description: None,
            }],
            purges: vec![req(PS::Activations {
                class: None,
                alternative: Some("high".into()),
            })],
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("renamed to \"peak\""), "{err}");
}

// ---------------------------------------------------------------------------
// Retry and scale (tests 11, 13)
// ---------------------------------------------------------------------------

#[test]
fn a_conflict_mid_purge_retries_to_the_same_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let a = fixture(&dir);
    let dir2 = tempfile::tempdir().unwrap();
    let b = fixture(&dir2);
    let reqs = || {
        vec![
            req(PS::Scenarios),
            PurgeRequest {
                allow_outside_scope: true,
                ..req(PS::Entities {
                    class: Some("unit".into()),
                })
            },
        ]
    };
    let ra = purge(&a, reqs(), "t", "m", o()).unwrap();
    nodal_db_core::purge::test_hooks::inject_conflicts_mid_purge(1);
    let rb = purge(&b, reqs(), "t", "m", o()).unwrap();
    assert_eq!(ra.purges, rb.purges);
    assert_eq!(live_entities(&a), live_entities(&b));
}

#[test]
fn statement_count_does_not_depend_on_the_number_of_entities() {
    let count = |n: usize| -> u64 {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("t.duckdb");
        init_schema(&db).unwrap();
        let rows: Vec<(String, f64)> = (0..n).map(|i| (format!("e{i}"), i as f64)).collect();
        let rows: Vec<(&str, f64)> = rows.iter().map(|(e, v)| (e.as_str(), *v)).collect();
        call(
            &db,
            ChangeSet {
                segments: vec![seg("node", "demand", "Base", scalars(&rows))],
                ..Default::default()
            },
        )
        .unwrap();
        let before = nodal_db_core::purge::test_hooks::statements();
        let r = purge1(&db, PS::Entities { class: None });
        assert_eq!(r.purges[0].objects, n);
        nodal_db_core::purge::test_hooks::statements() - before
    };
    assert_eq!(count(10), count(10_000));
}

// ---------------------------------------------------------------------------
// Values (tests 5, 7, 12; V-S3 retention)
// ---------------------------------------------------------------------------

/// Live scalar values as `class.parameter/alternative/entity=value`.
fn live_values(db: &Path) -> Vec<String> {
    names(
        db,
        "SELECT ec.name || '.' || pd.name || '/' || a.name || '/' || e.name || '=' || \
                CAST(s.v_float AS VARCHAR) \
         FROM value_scalar s \
         JOIN entity e ON e.id = s.entity_id AND e.valid_to_commit IS NULL \
         JOIN entity_class ec ON ec.id = e.entity_class_id AND ec.valid_to_commit IS NULL \
         JOIN parameter_definition pd ON pd.id = s.parameter_definition_id \
           AND pd.valid_to_commit IS NULL \
         JOIN alternative a ON a.id = s.alternative_id AND a.valid_to_commit IS NULL \
         WHERE s.valid_to_commit IS NULL ORDER BY 1",
    )
}

#[test]
fn values_filters_close_exactly_their_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    let all = live_values(&db);
    assert_eq!(all.len(), 7);
    // class + parameter + alternative
    purge1(
        &db,
        PS::Values {
            class: Some("node".into()),
            parameter: Some("demand".into()),
            alternative: Some("high".into()),
        },
    );
    assert_eq!(live_values(&db).len(), 6);
    assert!(!live_values(&db).contains(&"node.demand/high/n1=10.0".to_string()));
    // class + alternative: the node values in Base (scalars + the array).
    let r = purge1(
        &db,
        PS::Values {
            class: Some("node".into()),
            parameter: None,
            alternative: Some("Base".into()),
        },
    );
    assert_eq!(r.purges[0].objects, 0);
    assert_eq!(
        live_values(&db),
        [
            "link.flow/Base/n1__u1=7.0",
            "link.flow/Base/n2__u2=8.0",
            "unit.cap/Base/u1=5.0",
            "unit.cap/Base/u2=6.0"
        ]
    );
    assert_eq!(q(&db, "SELECT count(*) FROM value_array"), 0);
    // alternative only
    purge1(
        &db,
        PS::Values {
            class: None,
            parameter: None,
            alternative: Some("Base".into()),
        },
    );
    assert!(live_values(&db).is_empty());
    // Values never close objects; nothing left for Values{}.
    assert_eq!(live_entities(&db).len(), 6);
    let r = purge1(
        &db,
        PS::Values {
            class: None,
            parameter: None,
            alternative: None,
        },
    );
    assert_eq!(r.commit_id, 0);
}

#[test]
fn values_purge_and_half_rewrite_keeps_only_written_keys() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    // Values{class: node} with a write of n1's Base demand (unchanged):
    // only that key stays, though n2 and the high value are on entities
    // the call names.
    let r = call(
        &db,
        ChangeSet {
            structural: vec![entity("node", "n2")],
            segments: vec![seg("node", "demand", "Base", scalars(&[("n1", 1.0)]))],
            purges: vec![req(PS::Values {
                class: Some("node".into()),
                parameter: None,
                alternative: None,
            })],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(r.values_unchanged, 1);
    let node: Vec<String> = live_values(&db)
        .into_iter()
        .filter(|v| v.starts_with("node."))
        .collect();
    assert_eq!(node, ["node.demand/Base/n1=1.0"]);
    assert!(r.purges[0]
        .spared
        .per_table
        .iter()
        .any(|t| t.table == "value_scalar" && t.rows == 1));
}

#[test]
fn values_retention_follows_the_definition_and_the_override() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    // unit.cap does not retain content; a default on node.demand.
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        "UPDATE parameter_definition SET retain_content = FALSE WHERE name = 'cap'; \
         INSERT INTO definition_value_scalar (parameter_definition_id, slot, valid_from_commit, \
           version, value_kind, v_float) \
         SELECT id, 'default_value', (SELECT max(id) FROM commit), 1, 'float', 42 \
         FROM parameter_definition WHERE name = 'demand';",
    )
    .unwrap();
    drop(conn);
    let r = purge1(
        &db,
        PS::Values {
            class: None,
            parameter: None,
            alternative: Some("Base".into()),
        },
    );
    let c = r.commit_id;
    let stubs = |table: &str| {
        q(
            &db,
            &format!(
                "SELECT count(*) FROM {table} WHERE valid_to_commit = {c} AND NOT content_retained"
            ),
        )
    };
    let kept = |table: &str| {
        q(
            &db,
            &format!(
                "SELECT count(*) FROM {table} WHERE valid_to_commit = {c} AND content_retained \
                 AND v_float IS NOT NULL"
            ),
        )
    };
    assert_eq!(stubs("value_scalar"), 2, "unit.cap: retain_content = FALSE");
    assert_eq!(kept("value_scalar"), 4);
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM value_scalar WHERE NOT content_retained AND v_float IS NOT NULL"
        ),
        0
    );
    // The array's pre-image is retained (node.profile retains content) and
    // readable as of the commit before.
    assert_eq!(
        q(
            &db,
            &format!(
                "SELECT count(*) FROM value_array_hist WHERE valid_to_commit = {c} \
                 AND content_retained AND len(values) = 3"
            )
        ),
        1
    );
    assert_eq!(
        q(
            &db,
            &format!(
                "SELECT count(*) FROM value_array_asof({}) WHERE len(values) = 3",
                c - 1
            )
        ),
        1
    );
    // Defaults are never purged.
    assert_eq!(live(&db, "definition_value_scalar"), 1);

    // retain_override = Some(false) stubs everything; Some(true) keeps it.
    let dir2 = tempfile::tempdir().unwrap();
    let db2 = fixture(&dir2);
    let r = purge(
        &db2,
        vec![req(PS::Values {
            class: None,
            parameter: None,
            alternative: None,
        })],
        "t",
        "m",
        WriteOptions {
            retain_override: Some(false),
            ..o()
        },
    )
    .unwrap();
    let c = r.commit_id;
    assert_eq!(
        q(&db2, &format!("SELECT count(*) FROM value_scalar WHERE valid_to_commit = {c} AND content_retained")),
        0
    );
    assert_eq!(
        q(&db2, &format!("SELECT count(*) FROM value_array_hist WHERE valid_to_commit = {c} AND NOT content_retained AND values IS NULL")),
        1
    );
    let dir3 = tempfile::tempdir().unwrap();
    let db3 = fixture(&dir3);
    let conn = Connection::open(&db3).unwrap();
    conn.execute_batch("UPDATE parameter_definition SET retain_content = FALSE")
        .unwrap();
    drop(conn);
    let r = purge(
        &db3,
        vec![req(PS::Values {
            class: None,
            parameter: None,
            alternative: None,
        })],
        "t",
        "m",
        WriteOptions {
            retain_override: Some(true),
            ..o()
        },
    )
    .unwrap();
    let c = r.commit_id;
    assert_eq!(
        q(&db3, &format!("SELECT count(*) FROM value_scalar WHERE valid_to_commit = {c} AND NOT content_retained")),
        0
    );
}

#[test]
fn a_purged_alternative_can_still_be_erased() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(&dir);
    purge1(
        &db,
        PS::Values {
            class: None,
            parameter: None,
            alternative: Some("Base".into()),
        },
    );
    let rep = nodal_db_core::erasure::erase_alternative(
        &db,
        &ObjectDeleteKey {
            kind: ObjectKind::Alternative,
            class: None,
            target: ObjectRef::Name("Base".into()),
        },
        &nodal_db_core::erasure::EraseOpts {
            note: None,
            expect_present: &[],
        },
        "t",
    )
    .unwrap();
    assert!(rep.erased);
    assert_eq!(q(&db, "SELECT count(*) FROM value_array_hist"), 0);
}
