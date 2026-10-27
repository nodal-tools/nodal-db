//! The sanctioned structural (non-value) write path — the fourth
//! `commit_changes` channel (structural_write_api_plan). Covers create/rename
//! identity rows (incl. same-commit in-place edit + collision), A-min scenario
//! ranking (rotation regression, minimal-diff, clear, no-op), per-edge
//! activation + membership (base sentinel), the D-S9 creates, the
//! ObjectKind::Scenario delete cascade, the pure prepare-time guards, and one
//! composed editor save.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{
    commit_changes, commit_structural, delete_objects, preview_object_deletes, Segment,
};
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::types::{
    EntityAddr, ObjectDeleteKey, ObjectDeleteOpts, ObjectDeleteResult, ObjectKind, ObjectRef,
    StructuralChange as SC, StructuralEffect as SE, WriteOptions, WriteReport,
};

fn fresh_db(dir: &tempfile::TempDir) -> PathBuf {
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    db
}

fn o() -> WriteOptions {
    WriteOptions::default()
}

fn cs(db: &Path, changes: Vec<SC>) -> WriteReport {
    commit_structural(db, changes, "t", "m", o()).unwrap()
}

fn q1(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn commits(db: &Path) -> i64 {
    q1(db, "SELECT count(*) FROM commit")
}

fn create_alt(name: &str) -> SC {
    SC::CreateAlternative {
        name: name.into(),
        description: None,
    }
}

fn ranking(db: &Path, scenario: &str) -> Vec<String> {
    let conn = Connection::open(db).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT lower(a.name) FROM scenario_alternative sa \
             JOIN alternative a ON a.cur_id = sa.alternative_id \
             JOIN scenario s ON s.cur_id = sa.cur_s \
             WHERE lower(s.name) = lower(?) ORDER BY sa.cur_r",
        )
        .unwrap();
    stmt.query_map([scenario], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

// ---------------------------------------------------------------------------
// Alternatives + scenarios
// ---------------------------------------------------------------------------

#[test]
fn create_rename_alternative() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);

    let r = cs(&db, vec![create_alt("hydro")]);
    assert_eq!(r.structural[0].effect, SE::Created);
    let id = r.structural[0].object_id;
    assert!(id > 0);

    // Idempotent create ⇒ NoOp, same id.
    let r = cs(&db, vec![create_alt("HYDRO")]);
    assert_eq!(r.structural[0].effect, SE::NoOp);
    assert_eq!(r.structural[0].object_id, id);

    // Rename: id preserved, version bumps, old row closed.
    let r = cs(
        &db,
        vec![SC::RenameAlternative {
            current: "hydro".into(),
            new_name: "wet_year".into(),
            description: None,
        }],
    );
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(r.structural[0].object_id, id);
    assert_eq!(
        q1(
            &db,
            "SELECT version FROM alternative WHERE cur_name = 'wet_year'"
        ),
        2
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM alternative WHERE valid_to_commit IS NOT NULL"
        ),
        1
    );

    // Case-only self-restyle (fold-equal, no desc change) ⇒ a REAL rename
    // (editor request R4, owner decision 2026-10-27: the client decides
    // whether case matters, not the core — a `commit_changes` caller that
    // wants it treated as a no-op does so itself), id preserved, version
    // bumps again, and — critically — NOT a false collision with itself
    // (m5's self-exclusion in the uniqueness check still holds: "wet_year"
    // and "WET_YEAR" resolve to the SAME row by id, so the collision check
    // does not refuse it).
    let r = cs(
        &db,
        vec![SC::RenameAlternative {
            current: "wet_year".into(),
            new_name: "WET_YEAR".into(),
            description: None,
        }],
    );
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(r.structural[0].object_id, id);
    assert_eq!(
        q1(
            &db,
            "SELECT version FROM alternative WHERE cur_name = lower('WET_YEAR')"
        ),
        3
    );
    // The new spelling is stored (not just resolvable through `lower()`).
    let conn = Connection::open(&db).unwrap();
    let stored_name: String = conn
        .query_row(
            "SELECT name FROM alternative WHERE cur_name = lower('WET_YEAR')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored_name, "WET_YEAR");
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM alternative WHERE valid_to_commit IS NOT NULL"
        ),
        2,
        "two close-outs so far: hydro->wet_year, then wet_year->WET_YEAR"
    );
}

/// An NFD -> NFC re-spelling of the SAME name is a byte-identical rename
/// once both sides are normalized (editor request R4): `new_name` is NFC'd
/// at the API entry point before `rename_identity` ever sees it, and the
/// stored spelling is already NFC (every writer of this table goes
/// through the same rule), so `"Pa\u{308}a\u{308}"` (NFD "Pää") renaming to
/// `"P\u{e4}\u{e4}"` (NFC "Pää") is the NoOp this R4 carve-out keeps: same
/// bytes once both are NFC, nothing to store. This is the one case R4
/// explicitly keeps as `NoOp`.
#[test]
fn rename_to_the_nfc_form_of_an_nfd_spelling_is_byte_identical_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    const PAA_NFD: &str = "Pa\u{308}a\u{308}";
    const PAA_NFC: &str = "P\u{e4}\u{e4}";
    let r = cs(
        &db,
        vec![SC::CreateAlternative {
            name: PAA_NFD.into(),
            description: None,
        }],
    );
    assert_eq!(r.structural[0].effect, SE::Created);
    let id = r.structural[0].object_id;
    // The create itself stored the NFC form (the text rule normalizes at
    // every entry point, create included).
    let conn = Connection::open(&db).unwrap();
    let stored: String = conn
        .query_row("SELECT name FROM alternative WHERE cur_id = ?", [id], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(stored, PAA_NFC);
    drop(conn);

    // Renaming the (already-NFC-stored) alternative to the NFD spelling's
    // NFC form is byte-identical post-NFC: NoOp, no version bump.
    let r = cs(
        &db,
        vec![SC::RenameAlternative {
            current: PAA_NFD.into(),
            new_name: PAA_NFD.into(),
            description: None,
        }],
    );
    assert_eq!(r.structural[0].effect, SE::NoOp);
    assert_eq!(r.structural[0].object_id, id);
    let conn = Connection::open(&db).unwrap();
    let version: i64 = conn
        .query_row(
            "SELECT version FROM alternative WHERE cur_id = ?",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        version, 1,
        "no version bump: the NoOp carve-out, not a real rename"
    );
}

#[test]
fn rename_alternative_collision_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(&db, vec![create_alt("a1"), create_alt("a2")]);
    let err = commit_structural(
        &db,
        vec![SC::RenameAlternative {
            current: "a1".into(),
            new_name: "A2".into(),
            description: None,
        }],
        "t",
        "m",
        o(),
    )
    .unwrap_err();
    assert!(format!("{err}").contains("already names a different"));
}

#[test]
fn same_commit_create_then_rename_is_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    // Create + rename one alternative in ONE commit: the uncommitted row is
    // edited in place (no close-out, no version bump) — D-S5.
    let r = cs(
        &db,
        vec![
            create_alt("foo"),
            SC::RenameAlternative {
                current: "foo".into(),
                new_name: "bar".into(),
                description: None,
            },
        ],
    );
    assert_eq!(r.structural[1].effect, SE::Renamed);
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM alternative WHERE cur_name = 'bar'"
        ),
        1
    );
    assert_eq!(
        q1(
            &db,
            "SELECT version FROM alternative WHERE cur_name = 'bar'"
        ),
        1
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM alternative WHERE cur_name = 'foo'"
        ),
        0
    );
    // No closed alternative rows: the create was never visible.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM alternative WHERE valid_to_commit IS NOT NULL"
        ),
        0
    );
}

#[test]
fn create_rename_scenario() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    let r = cs(
        &db,
        vec![SC::CreateScenario {
            name: "s".into(),
            description: None,
        }],
    );
    assert_eq!(r.structural[0].effect, SE::Created);
    let r = cs(
        &db,
        vec![SC::RenameScenario {
            current: "s".into(),
            new_name: "s2".into(),
            description: Some("desc".into()),
        }],
    );
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(
        q1(&db, "SELECT version FROM scenario WHERE cur_name = 's2'"),
        2
    );

    // A case-only self-rename (R4) is a real rename for scenarios too —
    // not just alternatives: `rename_identity` is shared, so the kind
    // doesn't matter.
    let r = cs(
        &db,
        vec![SC::RenameScenario {
            current: "s2".into(),
            new_name: "S2".into(),
            description: None,
        }],
    );
    assert_eq!(r.structural[0].effect, SE::Renamed);
    assert_eq!(
        q1(
            &db,
            "SELECT version FROM scenario WHERE cur_name = lower('S2')"
        ),
        3
    );
}

// ---------------------------------------------------------------------------
// SetScenarioAlternatives — A-min (§4.3)
// ---------------------------------------------------------------------------

fn set_ranks(scenario: &str, alts: &[&str]) -> SC {
    SC::SetScenarioAlternatives {
        scenario: scenario.into(),
        alternatives: alts.iter().map(|s| s.to_string()).collect(),
    }
}

#[test]
fn a_min_rotation_regression() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(&db, vec![create_alt("A"), create_alt("B"), create_alt("C")]);
    cs(&db, vec![set_ranks("s", &["A", "B", "C"])]);
    // The rotation must NOT raise a uq_scenario_alt_rank violation (F1).
    let r = cs(&db, vec![set_ranks("s", &["C", "A", "B"])]);
    assert_eq!(r.structural[0].effect, SE::Updated);
    assert_eq!(ranking(&db, "s"), vec!["c", "a", "b"]);
}

#[test]
fn a_min_minimal_diff() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(&db, vec![create_alt("A"), create_alt("B"), create_alt("C")]);
    cs(&db, vec![set_ranks("s", &["A", "B"])]);

    // Append a new winner at the top (highest rank): touches ONE row.
    let r = cs(&db, vec![set_ranks("s", &["A", "B", "C"])]);
    assert_eq!(r.structural[0].effect, SE::Updated);
    assert_eq!(ranking(&db, "s"), vec!["a", "b", "c"]);
    // A and B untouched: nothing closed, all versions still 1.
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM scenario_alternative WHERE valid_to_commit IS NOT NULL"
        ),
        0
    );
    assert_eq!(
        q1(
            &db,
            "SELECT max(version) FROM scenario_alternative WHERE valid_to_commit IS NULL"
        ),
        1
    );

    // Remove from the middle: only B closes, C shifts, A untouched.
    let r = cs(&db, vec![set_ranks("s", &["A", "C"])]);
    assert_eq!(r.structural[0].effect, SE::Updated);
    assert_eq!(ranking(&db, "s"), vec!["a", "c"]);
    // A stayed version 1 with its original valid_from (never rewritten).
    assert_eq!(
        q1(
            &db,
            "SELECT sa.version FROM scenario_alternative sa JOIN alternative a \
             ON a.cur_id = sa.cur_a WHERE a.cur_name = 'a' AND sa.valid_to_commit IS NULL"
        ),
        1
    );
}

#[test]
fn a_min_floor_insert_and_clear_and_noop() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(&db, vec![create_alt("A"), create_alt("B"), create_alt("X")]);
    cs(&db, vec![set_ranks("s", &["A", "B"])]);

    // Floor-insert below rank 1: everything shifts up (the v38 shape).
    cs(&db, vec![set_ranks("s", &["X", "A", "B"])]);
    assert_eq!(ranking(&db, "s"), vec!["x", "a", "b"]);

    // Idempotent no-op: same order ⇒ no write.
    let r = cs(&db, vec![set_ranks("s", &["X", "A", "B"])]);
    assert_eq!(r.structural[0].effect, SE::NoOp);

    // Empty list clears the ranking.
    let r = cs(&db, vec![set_ranks("s", &[])]);
    assert_eq!(r.structural[0].effect, SE::Cleared);
    assert_eq!(ranking(&db, "s"), Vec::<String>::new());
}

#[test]
fn a_min_over_non_contiguous_ranks() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(
        &db,
        vec![
            create_alt("A"),
            create_alt("B"),
            create_alt("C"),
            create_alt("D"),
        ],
    );
    cs(&db, vec![set_ranks("s", &["A", "B", "C"])]); // ranks 1,2,3

    // Object-delete the mid-stack alternative B: its ranking row closes, leaving
    // LIVE ranks {1 (A), 3 (C)} — a gap at rank 2.
    delete_objects(
        &db,
        vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Alternative,
                class: None,
                target: ObjectRef::Name("B".into()),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        "t",
        "m",
        o(),
    )
    .unwrap();
    assert_eq!(ranking(&db, "s"), vec!["a", "c"]); // A@1, C@3 (non-contiguous)

    // A-min over the gapped current ranks stays collision-free and correct.
    let r = cs(&db, vec![set_ranks("s", &["A", "C", "D"])]);
    assert_eq!(r.structural[0].effect, SE::Updated);
    assert_eq!(ranking(&db, "s"), vec!["a", "c", "d"]);
}

// ---------------------------------------------------------------------------
// Activation + membership
// ---------------------------------------------------------------------------

fn class(name: &str) -> SC {
    SC::CreateEntityClass {
        name: name.into(),
        dims: vec![],
    }
}

fn entity(class: &str, name: &str) -> SC {
    SC::CreateEntity {
        class: class.into(),
        name: Some(name.into()),
        elements: vec![],
    }
}

#[test]
fn set_entity_activation_write_flip_close() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(
        &db,
        vec![class("node"), entity("node", "n1"), create_alt("Base")],
    );

    let act = |active: Option<bool>| SC::SetEntityActivation {
        class: "node".into(),
        entity: "n1".into(),
        alternative: "Base".into(),
        active,
    };
    let r = cs(&db, vec![act(Some(true))]);
    assert_eq!(r.structural[0].effect, SE::Updated);
    assert_eq!(
        q1(
            &db,
            "SELECT active::INT FROM entity_alternative WHERE valid_to_commit IS NULL"
        ),
        1
    );
    // Flip ⇒ close v1, insert v2.
    cs(&db, vec![act(Some(false))]);
    assert_eq!(
        q1(
            &db,
            "SELECT version FROM entity_alternative WHERE valid_to_commit IS NULL"
        ),
        2
    );
    // None ⇒ close, no live row.
    let r = cs(&db, vec![act(None)]);
    assert_eq!(r.structural[0].effect, SE::Cleared);
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity_alternative WHERE valid_to_commit IS NULL"
        ),
        0
    );
}

#[test]
fn same_commit_activation_create_then_close() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(
        &db,
        vec![class("node"), entity("node", "n1"), create_alt("Base")],
    );
    let a = |active| SC::SetEntityActivation {
        class: "node".into(),
        entity: "n1".into(),
        alternative: "Base".into(),
        active,
    };
    // Create then close in ONE commit: the uncommitted insert is physically
    // DELETEd (D-S5), never closed — the span CHECK is never tripped, no error.
    let r = cs(&db, vec![a(Some(true)), a(None)]);
    assert_eq!(r.structural[1].effect, SE::Cleared);
    assert_eq!(q1(&db, "SELECT count(*) FROM entity_alternative"), 0);
}

#[test]
fn same_commit_activation_inplace_flip() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(
        &db,
        vec![class("node"), entity("node", "n1"), create_alt("Base")],
    );
    let a = |active| SC::SetEntityActivation {
        class: "node".into(),
        entity: "n1".into(),
        alternative: "Base".into(),
        active,
    };
    // Create then flip in ONE commit: the uncommitted row's `active` is edited
    // in place — exactly one row, final active=false, version still 1.
    let r = cs(&db, vec![a(Some(true)), a(Some(false))]);
    assert_eq!(r.structural[1].effect, SE::Updated);
    assert_eq!(q1(&db, "SELECT count(*) FROM entity_alternative"), 1);
    assert_eq!(q1(&db, "SELECT active::INT FROM entity_alternative"), 0);
    assert_eq!(q1(&db, "SELECT version FROM entity_alternative"), 1);
    assert_eq!(
        q1(&db, "SELECT valid_from_commit FROM entity_alternative"),
        q1(&db, "SELECT max(id) FROM commit")
    );
}

#[test]
fn same_commit_membership_create_then_close() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(
        &db,
        vec![class("node"), entity("node", "g"), entity("node", "m")],
    );
    let addr = |n: &str| EntityAddr {
        class: "node".into(),
        name: n.into(),
    };
    let mem = |included| SC::SetMembership {
        group: addr("g"),
        member: addr("m"),
        alternative: None,
        included,
    };
    // Create then close the base row in ONE commit: uncommitted insert DELETEd.
    let r = cs(&db, vec![mem(Some(true)), mem(None)]);
    assert_eq!(r.structural[1].effect, SE::Cleared);
    assert_eq!(q1(&db, "SELECT count(*) FROM membership"), 0);
}

#[test]
fn same_commit_create_entity_then_activate() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    // Create the class + entity AND activate it in ONE save (D-S9 + §4.1):
    // structural items apply sequentially, so the strict activation resolves.
    let r = cs(
        &db,
        vec![
            create_alt("Base"),
            class("gen"),
            entity("gen", "g1"),
            SC::SetEntityActivation {
                class: "gen".into(),
                entity: "g1".into(),
                alternative: "Base".into(),
                active: Some(true),
            },
        ],
    );
    assert_eq!(r.structural[3].effect, SE::Updated);
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity_alternative WHERE valid_to_commit IS NULL"
        ),
        1
    );
}

#[test]
fn set_membership_base_and_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(
        &db,
        vec![
            class("node"),
            entity("node", "g"),
            entity("node", "m"),
            create_alt("Base"),
        ],
    );
    let addr = |n: &str| EntityAddr {
        class: "node".into(),
        name: n.into(),
    };

    // Base row: stored alternative_id NULL, NEVER 0 (critique M4).
    let r = cs(
        &db,
        vec![SC::SetMembership {
            group: addr("g"),
            member: addr("m"),
            alternative: None,
            included: Some(true),
        }],
    );
    assert_eq!(r.structural[0].effect, SE::Updated);
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM membership WHERE alternative_id IS NULL AND valid_to_commit IS NULL"
        ),
        1
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM membership WHERE alternative_id = 0"
        ),
        0
    );

    // Alternative-scoped override: stored alternative_id = Base id.
    cs(
        &db,
        vec![SC::SetMembership {
            group: addr("g"),
            member: addr("m"),
            alternative: Some("Base".into()),
            included: Some(false),
        }],
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM membership WHERE alternative_id IS NOT NULL AND valid_to_commit IS NULL"
        ),
        1
    );

    // Base None-close via cur_a = 0: the base row closes, the scoped one stays.
    let r = cs(
        &db,
        vec![SC::SetMembership {
            group: addr("g"),
            member: addr("m"),
            alternative: None,
            included: None,
        }],
    );
    assert_eq!(r.structural[0].effect, SE::Cleared);
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM membership WHERE alternative_id IS NULL AND valid_to_commit IS NULL"
        ),
        0
    );
}

// ---------------------------------------------------------------------------
// D-S9 creates
// ---------------------------------------------------------------------------

#[test]
fn create_class_parameter_entity() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    let r = cs(
        &db,
        vec![
            class("unit"),
            SC::CreateParameter {
                class: "unit".into(),
                name: "cap".into(),
                types: vec![nodal_db_core::template::ir::TypeDecl {
                    kind: "float".into(),
                    dims: None,
                }],
                value_list: None,
                description: None,
            },
            entity("unit", "u1"),
        ],
    );
    assert_eq!(r.structural[0].effect, SE::Created);
    assert_eq!(r.structural[1].effect, SE::Created);
    assert_eq!(r.structural[2].effect, SE::Created);
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM entity_class WHERE cur_name = 'unit'"
        ),
        1
    );
    assert_eq!(
        q1(
            &db,
            "SELECT count(*) FROM parameter_definition WHERE cur_name = 'cap'"
        ),
        1
    );
    assert_eq!(
        q1(&db, "SELECT count(*) FROM entity WHERE cur_name = 'u1'"),
        1
    );
}

// ---------------------------------------------------------------------------
// ObjectKind::Scenario delete
// ---------------------------------------------------------------------------

#[test]
fn scenario_delete_cascade_preview_and_restrict() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(&db, vec![create_alt("A")]);
    cs(&db, vec![set_ranks("sd", &["A"])]);

    let key = ObjectDeleteKey {
        kind: ObjectKind::Scenario,
        class: None,
        target: ObjectRef::Name("sd".into()),
    };

    // Preview: found, one dependent ranking row.
    let pv = preview_object_deletes(&db, vec![key.clone()]).unwrap();
    assert!(pv[0].found);
    assert!(pv[0].cascade.total_rows >= 1);

    // cascade:false with a live ranking row ⇒ Restricted.
    let r = delete_objects(
        &db,
        vec![(key.clone(), ObjectDeleteOpts { cascade: false })],
        "t",
        "m",
        o(),
    )
    .unwrap();
    assert!(matches!(
        r.object_deletes[0].result,
        ObjectDeleteResult::Restricted { .. }
    ));

    // cascade:true ⇒ scenario + ranking closed.
    let r = delete_objects(
        &db,
        vec![(key, ObjectDeleteOpts { cascade: true })],
        "t",
        "m",
        o(),
    )
    .unwrap();
    assert!(matches!(
        r.object_deletes[0].result,
        ObjectDeleteResult::Deleted { .. }
    ));
    assert_eq!(
        q1(&db, "SELECT count(*) FROM scenario WHERE cur_name = 'sd'"),
        0
    );
    assert_eq!(ranking(&db, "sd"), Vec::<String>::new());
}

// ---------------------------------------------------------------------------
// Pure prepare-time guards (§4.5)
// ---------------------------------------------------------------------------

#[test]
fn prepare_guards() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);

    // Two SetScenarioAlternatives for one scenario in one commit (M3).
    let err = commit_structural(
        &db,
        vec![set_ranks("s", &["A"]), set_ranks("S", &["A"])],
        "t",
        "m",
        o(),
    )
    .unwrap_err();
    assert!(format!("{err}").contains("one declarative set per scenario"));

    // Contradictory renames of one object.
    let err = commit_structural(
        &db,
        vec![
            SC::RenameAlternative {
                current: "a".into(),
                new_name: "b".into(),
                description: None,
            },
            SC::RenameAlternative {
                current: "A".into(),
                new_name: "c".into(),
                description: None,
            },
        ],
        "t",
        "m",
        o(),
    )
    .unwrap_err();
    assert!(format!("{err}").contains("contradictory renames"));

    // Contradictory renames of one object to two DIFFERENT case spellings
    // of the same folded target (R4 critique K6, structural.rs
    // `check_rename_coherence`): before the fix, "Foo" and "FOO" folded to
    // one value, so this looked coherent and the second would silently
    // overwrite the first (last-wins) instead of being refused. Now that
    // a case-only rename is itself a real rename, these are genuinely
    // different target spellings and must be caught here, up front,
    // exactly like the "b" vs "c" case above.
    cs(&db, vec![create_alt("k")]);
    let err = commit_structural(
        &db,
        vec![
            SC::RenameAlternative {
                current: "k".into(),
                new_name: "Foo".into(),
                description: None,
            },
            SC::RenameAlternative {
                current: "K".into(),
                new_name: "FOO".into(),
                description: None,
            },
        ],
        "t",
        "m",
        o(),
    )
    .unwrap_err();
    assert!(format!("{err}").contains("contradictory renames"), "{err}");

    // Both object-delete AND structurally reference one object (M2).
    let err = commit_changes(
        &db,
        vec![],
        vec![set_ranks("s", &["victim"])],
        vec![],
        vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Alternative,
                class: None,
                target: ObjectRef::Name("victim".into()),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        "t",
        "m",
        o(),
    )
    .unwrap_err();
    assert!(format!("{err}").contains("live orphan"));

    // Activation referencing an alternative that is also object-deleted (M2,
    // the alternative endpoint — FIX 1). Pure, name-level: names need not exist.
    let err = commit_changes(
        &db,
        vec![],
        vec![SC::SetEntityActivation {
            class: "node".into(),
            entity: "n1".into(),
            alternative: "X".into(),
            active: Some(true),
        }],
        vec![],
        vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Alternative,
                class: None,
                target: ObjectRef::Name("X".into()),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        "t",
        "m",
        o(),
    )
    .unwrap_err();
    assert!(format!("{err}").contains("live orphan"));
}

// ---------------------------------------------------------------------------
// Composed editor save
// ---------------------------------------------------------------------------

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

#[test]
fn composed_editor_save_one_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir);
    cs(&db, vec![create_alt("old")]); // a throwaway to object-delete

    let before = commits(&db);
    let seg = Segment {
        class: "node".into(),
        parameter: "demand".into(),
        alternative: "Base".into(),
        batches: vec![scalar_batch("n1", 5.0)],
        domains: vec![],
    };
    let r = commit_changes(
        &db,
        vec![seg],
        vec![
            create_alt("wet"),
            SC::CreateScenario {
                name: "sc".into(),
                description: None,
            },
            set_ranks("sc", &["wet"]),
        ],
        vec![],
        vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Alternative,
                class: None,
                target: ObjectRef::Name("old".into()),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        "t",
        "editor save",
        o(),
    )
    .unwrap();

    // One commit row for the whole save.
    assert_eq!(commits(&db) - before, 1);
    // Three structural outcomes + one object-delete outcome + the value write.
    assert_eq!(r.structural.len(), 3);
    assert_eq!(r.structural[0].effect, SE::Created); // wet
    assert_eq!(r.structural[1].effect, SE::Created); // sc
    assert_eq!(r.structural[2].effect, SE::Updated); // ranking
    assert_eq!(r.object_deletes.len(), 1);
    assert_eq!(r.rows_written, 1);
    assert_eq!(ranking(&db, "sc"), vec!["wet"]);
}
