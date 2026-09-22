//! End-to-end tests for the erasure subsystem (`specs/erasure_compaction_plan.md`
//! acceptance criteria 1–5 + the consumer-shape guard sweep of criterion 3).
//!
//! Fixture: an output-DB-shaped database with two "runs" (alternatives runA,
//! runB) carrying values on all three tiers plus raw-planted rows on the
//! narrow alternative-keyed legs (entity_alternative, membership,
//! scenario_alternative, parameter_value_metadata) and a `run` provenance row
//! — every table of the plan's §2.2 scope list is exercised.
//!
//! The guard sweep here runs on the BUNDLED engine (duckdb-rs); the smoke
//! suite runs the same sweep on the venv engine. Both are the permanent
//! engine-upgrade tripwire the plan mandates (weakest-point (c)) — the guard's
//! soundness rests on optimizer behavior, so this sweep must never be removed.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::write_parameter;
use nodal_db_core::compaction::compact;
use nodal_db_core::erasure::{erase_alternative, preview_erase_alternative, EraseOpts};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, WriteOptions};

// --- batch builders / helpers ----------------------------------------------

fn scalar_batch(entities: &[&str], vals: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(entities.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Rank-1 integer-indexed float batch — full Cartesian ⇒ array tier.
fn indexed_float_batch(entity: &str, n: usize, offset: f64) -> RecordBatch {
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
            Arc::new(Float64Array::from(
                (0..n).map(|i| offset + i as f64).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Rank-1 integer-indexed INT batch — int measure ⇒ long tier.
fn indexed_int_batch(entity: &str, coords: &[i64], vals: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; coords.len()])) as ArrayRef,
            Arc::new(Int64Array::from(coords.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn write(db: &Path, class: &str, param: &str, alt: &str, batch: RecordBatch) {
    write_parameter(
        db, class, param, alt, batch, &[], WriteOptions::default(), "tester", "erasure fixture",
    )
    .expect("write_parameter");
}

fn alt_key(name: &str) -> ObjectDeleteKey {
    ObjectDeleteKey {
        kind: ObjectKind::Alternative,
        class: None,
        target: ObjectRef::Name(name.into()),
    }
}

fn scalar(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

/// Order-independent fingerprint of any query's full result set.
fn fingerprint(db: &Path, query: &str) -> (i64, String) {
    let conn = Connection::open(db).unwrap();
    let sql =
        format!("SELECT count(*), CAST(COALESCE(bit_xor(hash(t)), 0) AS VARCHAR) FROM ({query}) t");
    let out = conn
        .query_row(&sql, [], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        .unwrap();
    conn.close().unwrap();
    out
}

/// The eight tables of the erase scope (plan §2.2: seven alternative-keyed
/// tables + the value_array_hist twin).
const SCOPE_TABLES: &[&str] = &[
    "value_scalar",
    "value_long",
    "value_array",
    "value_array_hist",
    "entity_alternative",
    "membership",
    "scenario_alternative",
    "parameter_value_metadata",
];

/// Rows keyed on `alt_id` (any validity) in one scope table.
fn alt_rows(db: &Path, table: &str, alt_id: i64) -> i64 {
    scalar(db, &format!("SELECT count(*) FROM {table} WHERE alternative_id = {alt_id}"))
}

/// Build the two-run fixture. Returns (db, runA id, runB id).
fn seed_two_runs(dir: &Path, array_cells: usize) -> (PathBuf, i64, i64) {
    let db = dir.join("runs.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    for (alt, off) in [("runA", 0.0), ("runB", 1000.0)] {
        write(&db, "Node", "demand", alt, scalar_batch(&["n1", "n2", "n3"], &[off, off + 1.0, off + 2.0]));
        // Array tier, written TWICE so the second write moves a pre-image
        // into value_array_hist (the M3-sensitive twin).
        write(&db, "Node", "profile", alt, indexed_float_batch("n1", array_cells, off));
        write(&db, "Node", "profile", alt, indexed_float_batch("n1", array_cells, off + 0.5));
        // Ragged int-measure ⇒ long tier.
        write(&db, "Node", "ragged", alt, indexed_int_batch("n1", &[0, 2, 5], &[1, 2, 3]));
    }
    // Raw-plant the narrow alternative-keyed legs + a run row (a test fixture
    // writes structure the public API does not yet cover, like the smoke
    // suite does).
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        "INSERT INTO commit (user, message) VALUES ('tester', 'fixture structure');
         INSERT INTO scenario (id, name, valid_from_commit)
           VALUES (nextval('seq_scenario'), 's_main', currval('seq_commit'));
         INSERT INTO scenario_alternative (scenario_id, alternative_id, rank, valid_from_commit)
           SELECT s.id, a.id, 1, currval('seq_commit')
           FROM scenario s, alternative a WHERE s.cur_name = 's_main' AND a.cur_name = 'runa';
         INSERT INTO scenario_alternative (scenario_id, alternative_id, rank, valid_from_commit)
           SELECT s.id, a.id, 2, currval('seq_commit')
           FROM scenario s, alternative a WHERE s.cur_name = 's_main' AND a.cur_name = 'runb';
         INSERT INTO entity_alternative (entity_id, alternative_id, active, valid_from_commit)
           SELECT e.id, a.id, TRUE, currval('seq_commit')
           FROM entity e, alternative a WHERE e.cur_name = 'n1' AND a.cur_name = 'runa';
         INSERT INTO membership (group_entity_id, member_entity_id, included, valid_from_commit)
           SELECT e1.id, e2.id, TRUE, currval('seq_commit')
           FROM entity e1, entity e2 WHERE e1.cur_name = 'n1' AND e2.cur_name = 'n2';
         INSERT INTO membership (group_entity_id, member_entity_id, alternative_id, included, valid_from_commit)
           SELECT e1.id, e2.id, a.id, FALSE, currval('seq_commit')
           FROM entity e1, entity e2, alternative a
           WHERE e1.cur_name = 'n1' AND e2.cur_name = 'n2' AND a.cur_name = 'runa';
         INSERT INTO metadata (name, value, valid_from_commit)
           VALUES ('note_key', 'note_val', currval('seq_commit'));
         INSERT INTO parameter_value_metadata
             (entity_id, parameter_definition_id, alternative_id, metadata_id, valid_from_commit)
           SELECT e.id, pd.id, a.id, m.id, currval('seq_commit')
           FROM entity e, parameter_definition pd, alternative a, metadata m
           WHERE e.cur_name = 'n1' AND pd.cur_name = 'demand'
             AND a.cur_name = 'runa' AND m.cur_name = 'note_key';
         INSERT INTO run (id, tool_entity_id, result_alternative_id, input_commit_id, status)
           SELECT nextval('seq_run'), e.id, a.id, 1, 'optimal'
           FROM entity e, alternative a WHERE e.cur_name = 'n1' AND a.cur_name = 'runa';",
    )
    .expect("plant fixture structure");
    let ids = |name: &str| -> i64 {
        conn.query_row(
            "SELECT id FROM alternative WHERE cur_name = lower(?)",
            [name],
            |r| r.get(0),
        )
        .unwrap()
    };
    let (a, b) = (ids("runA"), ids("runB"));
    conn.close().unwrap();
    (db, a, b)
}

/// Queries whose results must be byte-identical for runB before/after erasing
/// runA (acceptance 2).
fn b_probes() -> Vec<String> {
    vec![
        "SELECT * FROM read_parameter('Node','demand') WHERE lower(alternative) = 'runb'".into(),
        "SELECT * FROM read_parameter('Node','profile') WHERE lower(alternative) = 'runb'".into(),
        "SELECT * FROM read_parameter('Node','ragged') WHERE lower(alternative) = 'runb'".into(),
        "SELECT * FROM \"commit\"".into(),
        "SELECT * FROM run".into(),
    ]
}

// --- (1) preview + erase end-to-end -----------------------------------------

#[test]
fn preview_and_erase_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let (db, a_id, _b_id) = seed_two_runs(dir.path(), 24);

    // Every scope table actually holds runA rows (fixture completeness guard —
    // an empty leg would make the assertions below vacuous).
    for t in SCOPE_TABLES {
        assert!(alt_rows(&db, t, a_id) > 0, "fixture gap: no runA rows in {t}");
    }

    let preview = preview_erase_alternative(&db, &alt_key("runA")).expect("preview");
    assert!(preview.found);
    assert_eq!(preview.alternative_id, a_id);
    assert_eq!(preview.alternative_name, "runA");
    assert!(preview.still_current);
    assert_eq!(preview.referencing_scenarios.len(), 1);
    assert_eq!(preview.referencing_scenarios[0].name, "s_main");
    assert_eq!(preview.referencing_runs.len(), 1);
    assert!(preview.bytes_estimate > 0);
    // Preview counts == the raw any-validity counts, per table.
    for t in SCOPE_TABLES {
        let expected = alt_rows(&db, t, a_id) as usize;
        let got = preview.per_table.iter().find(|tc| tc.table == *t).map(|tc| tc.rows);
        assert_eq!(got, Some(expected), "preview count mismatch on {t}");
    }
    assert_eq!(
        preview.rows_total as usize,
        preview.per_table.iter().map(|t| t.rows).sum::<usize>()
    );

    let before_fps: Vec<_> = b_probes().iter().map(|q| fingerprint(&db, q)).collect();
    let commits_before = scalar(&db, "SELECT count(*) FROM \"commit\"");
    let runs_before = scalar(&db, "SELECT count(*) FROM run");

    let report = erase_alternative(
        &db,
        &alt_key("runA"),
        &EraseOpts { note: Some("test erase"), expect_present: &["runB"] },
        "tester",
    )
    .expect("erase");

    // Report vs preview: identical scope (nothing moved between the calls).
    assert!(report.erased);
    assert!(report.identity_closed);
    assert_eq!(report.alternative_id, a_id);
    assert_eq!(report.per_table, preview.per_table, "preview drifted from erase");
    assert_eq!(report.rows_total, preview.rows_total);
    assert!(report.marker_id > 0);

    // Post-state: zero runA rows anywhere in scope, any validity.
    for t in SCOPE_TABLES {
        assert_eq!(alt_rows(&db, t, a_id), 0, "rows survived erase in {t}");
    }
    // Identity retained + closed at the erasure commit (D-E2).
    let conn = Connection::open(&db).unwrap();
    let (n_ident, closed_at): (i64, Option<i64>) = conn
        .query_row(
            "SELECT count(*), max(valid_to_commit) FROM alternative WHERE id = ?",
            [a_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(n_ident, 1, "identity row must be retained");
    assert_eq!(closed_at, Some(report.commit_id), "identity closed at the erasure commit");
    // Marker present with correct content, same transaction's commit.
    let (m_kind, m_alt, m_at, m_rows, m_note): (String, i64, i64, i64, String) = conn
        .query_row(
            "SELECT kind, alternative_id, at_commit, rows_removed, note FROM erasure_marker",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();
    assert_eq!((m_kind.as_str(), m_alt, m_at), ("alternative", a_id, report.commit_id));
    assert_eq!(m_rows as u64, report.rows_total);
    assert_eq!(m_note, "test erase");
    // audit_erasure resolves the marker to the retained identity.
    let (a_name, a_unres): (String, bool) = conn
        .query_row(
            "SELECT scope_name, scope_unresolved FROM audit_erasure",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(a_name, "runA");
    assert!(!a_unres);
    // Audits clean after erase (verified expectation — critique confirmed #7).
    for audit in ["audit_dangling_ref", "audit_history_tiling", "audit_cross_tier_duplicate"] {
        let n: i64 = conn
            .query_row(&format!("SELECT count(*) FROM {audit}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "{audit} not clean after erase");
    }
    conn.close().unwrap();

    // Commit log: exactly ONE new commit (the erasure commit); runs untouched.
    assert_eq!(scalar(&db, "SELECT count(*) FROM \"commit\""), commits_before + 1);
    assert_eq!(scalar(&db, "SELECT count(*) FROM run"), runs_before);

    // Current-state reads for runA are empty; runB byte-identical (acceptance 2)
    // — note b_probes' commit-log probe was fingerprinted BEFORE the erase
    // commit existed, so compare all but that one, then re-check runB probes.
    assert_eq!(
        fingerprint(&db, "SELECT * FROM read_parameter('Node','demand') WHERE lower(alternative) = 'runa'").0,
        0
    );
    for (q, before) in b_probes().iter().zip(&before_fps) {
        if q.contains("commit") {
            continue; // the erase commit legitimately appended a row
        }
        assert_eq!(&fingerprint(&db, q), before, "runB result drifted: {q}");
    }

    // ...and everything still holds after a compaction (acceptance 2/4: the
    // marker is one more base table; current reads stay exact; the guard
    // survives the physical rebuild).
    let cr = compact(&db).expect("compact after erase");
    assert!(cr.swapped);
    assert_eq!(scalar(&db, "SELECT count(*) FROM erasure_marker"), 1);
    for (q, before) in b_probes().iter().zip(&before_fps) {
        if q.contains("commit") {
            continue;
        }
        assert_eq!(&fingerprint(&db, q), before, "runB result drifted post-compact: {q}");
    }
    let conn = Connection::open(&db).unwrap();
    let err = conn
        .prepare(&format!("SELECT count(*) FROM value_scalar_asof({})", m_at - 1))
        .and_then(|mut s| s.query_row([], |r| r.get::<_, i64>(0)));
    assert!(err.is_err(), "guard must still fire after compaction");
    conn.close().unwrap();
}

// --- (2) idempotency ---------------------------------------------------------

#[test]
fn second_erase_is_a_noop_without_commit_or_marker() {
    let dir = tempfile::tempdir().unwrap();
    let (db, a_id, _) = seed_two_runs(dir.path(), 8);

    let first = erase_alternative(&db, &alt_key("runA"), &EraseOpts::default(), "tester")
        .expect("first erase");
    assert!(first.erased);
    let commits = scalar(&db, "SELECT count(*) FROM \"commit\"");

    let second = erase_alternative(&db, &alt_key("runA"), &EraseOpts::default(), "tester")
        .expect("second erase");
    assert!(!second.erased, "second erase must be a no-op");
    assert_eq!(second.alternative_id, a_id);
    assert_eq!(second.marker_id, 0);
    assert_eq!(scalar(&db, "SELECT count(*) FROM \"commit\""), commits, "no-op burned a commit");
    assert_eq!(scalar(&db, "SELECT count(*) FROM erasure_marker"), 1, "no-op wrote a marker");
}

// --- (3) composition with a prior structural delete --------------------------

#[test]
fn erase_composes_with_prior_object_delete() {
    let dir = tempfile::tempdir().unwrap();
    let (db, a_id, _) = seed_two_runs(dir.path(), 8);

    // Structural delete first: closes the identity + dependents (history kept).
    nodal_db_core::api::delete_objects(
        &db,
        vec![(alt_key("runA"), ObjectDeleteOpts { cascade: true })],
        "tester",
        "delete runA",
        WriteOptions::default(),
    )
    .expect("delete_objects");
    // Rows still exist (closed) — delete keeps history.
    let closed_rows: i64 = SCOPE_TABLES.iter().map(|t| alt_rows(&db, t, a_id)).sum();
    assert!(closed_rows > 0, "delete should have kept closed history rows");

    // Erase composes: resolves the CLOSED identity, removes everything.
    let report = erase_alternative(&db, &alt_key("runA"), &EraseOpts::default(), "tester")
        .expect("erase after delete");
    assert!(report.erased);
    assert!(!report.identity_closed, "identity was already closed by the delete");
    for t in SCOPE_TABLES {
        assert_eq!(alt_rows(&db, t, a_id), 0, "rows survived erase in {t}");
    }
    assert_eq!(scalar(&db, "SELECT count(*) FROM erasure_marker"), 1);
    // The identity row itself is still there (closed by the delete, retained).
    assert_eq!(scalar(&db, &format!("SELECT count(*) FROM alternative WHERE id = {a_id}")), 1);
}

// --- (4) expect_present ordering belt (acceptance 5) --------------------------

#[test]
fn expect_present_refuses_and_touches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (db, a_id, _) = seed_two_runs(dir.path(), 8);
    let commits = scalar(&db, "SELECT count(*) FROM \"commit\"");

    // Replacement missing ⇒ refuse, nothing changed.
    let err = erase_alternative(
        &db,
        &alt_key("runA"),
        &EraseOpts { note: None, expect_present: &["runC_not_there"] },
        "tester",
    )
    .expect_err("must refuse");
    assert!(format!("{err}").contains("expect_present"), "unexpected error: {err}");
    assert!(alt_rows(&db, "value_scalar", a_id) > 0, "refusal must touch nothing");
    assert_eq!(scalar(&db, "SELECT count(*) FROM erasure_marker"), 0);
    assert_eq!(scalar(&db, "SELECT count(*) FROM \"commit\""), commits, "refusal burned a commit");

    // Naming the erased alternative itself is a caller bug ⇒ refuse.
    let err = erase_alternative(
        &db,
        &alt_key("runA"),
        &EraseOpts { note: None, expect_present: &["runA"] },
        "tester",
    )
    .expect_err("self-reference must refuse");
    assert!(format!("{err}").contains("being erased"), "unexpected error: {err}");

    // Replacement present ⇒ proceeds.
    let report = erase_alternative(
        &db,
        &alt_key("runA"),
        &EraseOpts { note: None, expect_present: &["runB"] },
        "tester",
    )
    .expect("erase with satisfied expect_present");
    assert!(report.erased);
}

// --- (5) kind / key validation ----------------------------------------------

#[test]
fn non_alternative_kinds_and_bad_keys_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _, _) = seed_two_runs(dir.path(), 8);

    for kind in [ObjectKind::Entity, ObjectKind::Parameter, ObjectKind::Class] {
        let key = ObjectDeleteKey { kind, class: None, target: ObjectRef::Name("x".into()) };
        let err = erase_alternative(&db, &key, &EraseOpts::default(), "tester")
            .expect_err("non-alternative kind must refuse");
        assert!(
            format!("{err}").contains("not implemented in v1"),
            "unexpected error for {kind:?}: {err}"
        );
    }
    // Unknown name: loud error (not a silent no-op).
    let err = erase_alternative(&db, &alt_key("never_existed"), &EraseOpts::default(), "tester")
        .expect_err("unknown name must error");
    assert!(format!("{err}").contains("not found"), "unexpected error: {err}");
    // Preview of an unknown name: found = false, everything else empty.
    let p = preview_erase_alternative(&db, &alt_key("never_existed")).expect("preview");
    assert!(!p.found);
    assert_eq!(p.rows_total, 0);
}

// --- (6) transaction atomicity (acceptance 4) --------------------------------

/// Inject a mid-transaction failure AFTER the deletes: plant a raw marker row
/// occupying the id the erase's marker insert will draw from the sequence —
/// the PK collision aborts the transaction after steps 2–3 already ran, so
/// rollback must restore every deleted row and the identity close-out, and
/// burn no commit. (A crash at the same point has the same visible outcome —
/// transaction atomicity is the mechanism under test.)
#[test]
fn erase_transaction_is_atomic_under_mid_txn_failure() {
    let dir = tempfile::tempdir().unwrap();
    let (db, a_id, _) = seed_two_runs(dir.path(), 8);

    let before: Vec<i64> = SCOPE_TABLES.iter().map(|t| alt_rows(&db, t, a_id)).collect();
    let commits = scalar(&db, "SELECT count(*) FROM \"commit\"");
    {
        let conn = Connection::open(&db).unwrap();
        // seq_erasure_marker is unused ⇒ the next nextval() is 1; occupy it.
        conn.execute_batch(
            "INSERT INTO erasure_marker (id, kind, alternative_id, at_commit, rows_removed) \
             VALUES (1, 'alternative', 999999, 1, 0)",
        )
        .unwrap();
        conn.close().unwrap();
    }

    let err = erase_alternative(&db, &alt_key("runA"), &EraseOpts::default(), "tester")
        .expect_err("marker PK collision must abort the erase");
    assert!(format!("{err}").to_lowercase().contains("key"), "unexpected error: {err}");

    // Rollback restored EVERYTHING: data rows, live identity, commit count.
    let after: Vec<i64> = SCOPE_TABLES.iter().map(|t| alt_rows(&db, t, a_id)).collect();
    assert_eq!(before, after, "rollback must restore all deleted rows");
    assert_eq!(
        scalar(&db, &format!("SELECT count(*) FROM alternative WHERE id = {a_id} AND valid_to_commit IS NULL")),
        1,
        "identity must still be current after rollback"
    );
    assert_eq!(scalar(&db, "SELECT count(*) FROM \"commit\""), commits);

    // Clear the planted collision; the erase then succeeds normally.
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch("DELETE FROM erasure_marker WHERE alternative_id = 999999").unwrap();
        conn.close().unwrap();
    }
    let report = erase_alternative(&db, &alt_key("runA"), &EraseOpts::default(), "tester")
        .expect("erase after clearing the collision");
    assert!(report.erased);
}

// --- (3) the consumer-shape guard sweep (acceptance 3) ------------------------

/// All 31 as-of macros in 110_views_history.sql — keep in sync with the file
/// (the sweep hard-fails if a macro vanishes, which is the point).
const ASOF_MACROS: &[&str] = &[
    "metadata_asof",
    "entity_metadata_asof",
    "parameter_value_metadata_asof",
    "entity_class_asof",
    "class_set_asof",
    "class_set_member_asof",
    "entity_class_dimension_asof",
    "entity_asof",
    "entity_location_asof",
    "entity_element_asof",
    "alternative_asof",
    "scenario_asof",
    "scenario_alternative_asof",
    "entity_alternative_asof",
    "membership_asof",
    "units_of_measure_asof",
    "parameter_value_list_asof",
    "list_value_asof",
    "parameter_definition_asof",
    "entity_parameter_unit_asof",
    "parameter_type_asof",
    "parameter_type_dimension_asof",
    "display_mode_asof",
    "entity_class_display_mode_asof",
    "semantic_annotation_asof",
    "value_scalar_asof",
    "value_long_asof",
    "value_array_asof",
    "definition_value_scalar_asof",
    "definition_value_long_asof",
    "definition_value_array_asof",
];

/// The consumer shapes of the B1 sweep: bare, aggregate (projection fully
/// pruned), column-pruned constant projection, and a join into another
/// relation. Every macro exposes valid_from_commit, so the join is generic.
fn shapes(macro_name: &str, c: i64) -> Vec<String> {
    vec![
        format!("SELECT * FROM {macro_name}({c})"),
        format!("SELECT count(*) FROM {macro_name}({c})"),
        format!("SELECT 1 FROM {macro_name}({c})"),
        format!(
            "SELECT count(*) FROM {macro_name}({c}) t \
             JOIN \"commit\" cm ON cm.id = t.valid_from_commit"
        ),
    ]
}

fn run_shape(conn: &Connection, sql: &str) -> Result<(), duckdb::Error> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;
    while rows.next()?.is_some() {}
    Ok(())
}

#[test]
fn guard_consumer_shape_sweep_all_31_macros() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _, _) = seed_two_runs(dir.path(), 8);

    // Control direction: pre-erase, every macro is silent in every shape.
    let pre_commit = scalar(&db, "SELECT max(id) FROM \"commit\"");
    {
        let conn = Connection::open(&db).unwrap();
        for m in ASOF_MACROS {
            for sql in shapes(m, pre_commit) {
                run_shape(&conn, &sql)
                    .unwrap_or_else(|e| panic!("marker-free {sql} must succeed: {e}"));
            }
        }
        conn.close().unwrap();
    }

    let report = erase_alternative(&db, &alt_key("runA"), &EraseOpts::default(), "tester")
        .expect("erase");
    let c_erase = report.commit_id;

    let conn = Connection::open(&db).unwrap();
    for m in ASOF_MACROS {
        // Error direction: any as-of BEFORE the marker fails loudly, in every
        // consumer shape (this is the permanent engine-upgrade tripwire).
        for sql in shapes(m, c_erase - 1) {
            match run_shape(&conn, &sql) {
                Ok(()) => panic!("guard FAILED to fire (silent partial history): {sql}"),
                Err(e) => {
                    let msg = e.to_string();
                    assert!(
                        msg.contains("unreliable") && msg.contains("erasure"),
                        "wrong error for {sql}: {msg}"
                    );
                }
            }
        }
        // Boundary: as-of AT the erasure commit is exact and silent.
        for sql in shapes(m, c_erase) {
            run_shape(&conn, &sql)
                .unwrap_or_else(|e| panic!("as-of at the marker commit must succeed: {sql}: {e}"));
        }
    }
    conn.close().unwrap();
}

// --- (5b) the two-call composition wrapper -----------------------------------

/// The convenience chain runs erase then compact as two separate operations
/// (never fused — different lock scopes); the compacted file carries the
/// marker, the closed identity, and exact current reads. The crash-safety
/// property between the two calls is the erased-but-unpacked state, which the
/// end-to-end test already pins (everything correct pre-compaction).
#[test]
fn erase_and_compact_wrapper_chains_the_two_primitives() {
    let dir = tempfile::tempdir().unwrap();
    let (db, a_id, _) = seed_two_runs(dir.path(), 8);

    let (erase, compaction) = nodal_db_core::erasure::erase_alternative_and_compact(
        &db,
        &alt_key("runA"),
        &EraseOpts::default(),
        "tester",
    )
    .expect("erase + compact");
    assert!(erase.erased);
    assert!(compaction.swapped);
    assert_eq!(scalar(&db, "SELECT count(*) FROM erasure_marker"), 1);
    for t in SCOPE_TABLES {
        assert_eq!(alt_rows(&db, t, a_id), 0);
    }
    assert_eq!(
        scalar(&db, &format!("SELECT count(*) FROM alternative WHERE id = {a_id}")),
        1,
        "identity survives the compaction copy"
    );
}

// --- (1) reclaim + no hist-move write-amplification --------------------------

/// Acceptance 1: erasing run A then compacting lands close to an
/// identically-seeded B-only database compacted the same way, with B's values
/// byte-identical; and the erase transaction grows the file by bookkeeping /
/// survivor-row-group rewrite only, NOT by A's payload size (the M3
/// regression bound).
///
/// Fixture note (asymmetric on purpose): A's array payload (~8 MB) dominates
/// B's (~0.25 MB), so DuckDB's legitimate delete-time behavior — the
/// checkpoint rewrites row groups that still hold SURVIVING rows — stays far
/// below the payload/2 bound, while any surviving copy of A's payload would
/// blow it.
///
/// Engine finding (measured on the bundled 1.5.5 line, recorded here so the
/// bound is read honestly): a same-transaction INSERT-then-DELETE is netted
/// out by the engine at COMMIT — a verbatim hist-move-then-delete writes
/// ~nothing to disk (0.3 MB vs 26 MB when the copies survive). So the file
/// CANNOT observe an in-transaction copy that the same transaction deletes;
/// this bound catches any variant where copied payload SURVIVES (the
/// realistic drift), and the erase arm's no-hist-move design stands on the
/// M3 semantics (no live-guard, any-validity scope, no dependence on engine
/// netting) rather than on measured amplification.
#[test]
fn reclaim_matches_single_run_control_and_erase_does_not_amplify() {
    let dir = tempfile::tempdir().unwrap();
    const CELLS_A: usize = 524_288; // 2 writes x 4 MB
    const CELLS_B: usize = 16_384; // 2 writes x 128 KB
    let payload_a = (2 * CELLS_A * 8) as u64;

    let db_ab = dir.path().join("runs_ab.duckdb");
    init_schema_with_default(&db_ab, Some("Base")).expect("init ab");
    let db_b = dir.path().join("b_only.duckdb");
    init_schema_with_default(&db_b, Some("Base")).expect("init control");
    for db in [&db_ab, &db_b] {
        write(db, "Node", "demand", "runB", scalar_batch(&["n1", "n2"], &[1000.0, 1001.0]));
        write(db, "Node", "profile", "runB", indexed_float_batch("n1", CELLS_B, 1000.0));
        write(db, "Node", "profile", "runB", indexed_float_batch("n1", CELLS_B, 1000.5));
    }
    write(&db_ab, "Node", "demand", "runA", scalar_batch(&["n1", "n2"], &[0.0, 1.0]));
    write(&db_ab, "Node", "profile", "runA", indexed_float_batch("n1", CELLS_A, 0.0));
    write(&db_ab, "Node", "profile", "runA", indexed_float_batch("n1", CELLS_A, 0.5));

    let checkpoint = |db: &Path| {
        let c = Connection::open(db).unwrap();
        c.execute_batch("CHECKPOINT").unwrap();
        c.close().unwrap();
    };
    checkpoint(&db_ab);
    let bytes_pre_erase = std::fs::metadata(&db_ab).unwrap().len();
    let b_hist_rows = scalar(&db_ab, "SELECT count(*) FROM value_array_hist");
    assert!(b_hist_rows >= 2, "fixture: both runs must have hist pre-images");

    let a_id = scalar(&db_ab, "SELECT id FROM alternative WHERE cur_name = 'runa'");
    let report = erase_alternative(&db_ab, &alt_key("runA"), &EraseOpts::default(), "tester")
        .expect("erase runA");
    assert!(report.erased);
    assert!(report.bytes_estimate >= payload_a, "estimate must see A's array payload");
    checkpoint(&db_ab);
    let growth = std::fs::metadata(&db_ab)
        .unwrap()
        .len()
        .saturating_sub(bytes_pre_erase);
    assert!(
        growth < payload_a / 2,
        "erase grew the file by {growth} B (A payload {payload_a} B) — \
         O(payload) growth means copied payload survived the transaction (M3)"
    );
    for t in SCOPE_TABLES {
        assert_eq!(alt_rows(&db_ab, t, a_id), 0);
    }
    // B's hist pre-image survived the erase (scope precision: any-validity
    // deletion is keyed on A only).
    assert!(
        scalar(&db_ab, "SELECT count(*) FROM value_array_hist") >= 1,
        "runB's hist rows must survive"
    );

    // Reclaim: compact both; the erased+compacted file lands at the B-only
    // control's size within 10% plus two 256 KB block-quantization allowances
    // (the AB file legitimately carries a few extra rows: runA's closed
    // identity, the marker, and its commits).
    let b_probe = "SELECT * FROM read_parameter('Node','profile') WHERE lower(alternative) = 'runb'";
    let fp_control = fingerprint(&db_b, b_probe);
    compact(&db_ab).expect("compact erased");
    compact(&db_b).expect("compact control");
    let (sz_ab, sz_b) = (
        std::fs::metadata(&db_ab).unwrap().len() as f64,
        std::fs::metadata(&db_b).unwrap().len() as f64,
    );
    assert!(
        sz_ab <= sz_b * 1.10 + 2.0 * 262_144.0,
        "reclaim shortfall: erased+compacted {sz_ab} B vs B-only control {sz_b} B"
    );
    // B's values byte-identical through the read surface, on both files.
    assert_eq!(fingerprint(&db_ab, b_probe), fp_control, "runB values drifted");
}
