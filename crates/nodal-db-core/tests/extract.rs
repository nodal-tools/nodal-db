//! Acceptance tests for the history-free extract (`extract::extract`).
//!
//! Fixture: a churned source built through the public write path (all three
//! tiers with repeated overwrites, a value delete, an entity delete, an
//! erase-by-alternative, template identity restamps, a scenario rename) plus
//! raw-planted structure the API does not cover (a `run` row whose result
//! alternative is later erased and whose tool entity is later deleted,
//! definition values on the scalar and array shapes, membership).
//!
//! Comparisons between two files go through `ATTACH` only for VIEWS, which
//! bind to their own catalog. Table macros are always called on a connection
//! whose default catalog is the file in question: a macro reached through an
//! attached catalog binds to the caller's catalog (pinned by T11 below).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{
    commit_structural, db_identity, delete_objects, delete_values, write_parameter, DeleteKey,
};
use nodal_db_core::compaction::compact;
use nodal_db_core::erasure::{erase_alternative, EraseOpts};
use nodal_db_core::error::DbError;
use nodal_db_core::extract::{extract, ExtractOpts, ExtractReport};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, StructuralChange, WriteOptions,
};
use nodal_db_core::verbs::stamp_template_identity;

// --- batch builders --------------------------------------------------------

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

/// Rank-1 integer-indexed float batch: full Cartesian, so array tier.
fn array_batch(entity: &str, n: usize, offset: f64) -> RecordBatch {
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

/// Rank-1 integer-indexed INT batch: an int measure, so long tier.
fn long_batch(entity: &str, coords: &[i64], vals: &[i64]) -> RecordBatch {
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
        db,
        class,
        param,
        alt,
        batch,
        &[],
        WriteOptions::default(),
        "tester",
        "extract fixture",
    )
    .expect("write_parameter");
}

// --- query helpers -----------------------------------------------------------

fn scalar(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

fn string(db: &Path, sql: &str) -> String {
    let conn = Connection::open(db).unwrap();
    let s: String = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    s
}

fn strings(db: &Path, sql: &str) -> Vec<String> {
    let conn = Connection::open(db).unwrap();
    let mut stmt = conn.prepare(sql).unwrap();
    let v: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    drop(stmt);
    conn.close().unwrap();
    v
}

/// Order-independent fingerprint of a query on ONE file (its own default
/// catalog, so table macros bind correctly). `Err` carries the error text.
fn fingerprint(db: &Path, query: &str) -> Result<(i64, String), String> {
    let conn = Connection::open(db).unwrap();
    let sql =
        format!("SELECT count(*), CAST(COALESCE(bit_xor(hash(t)), 0) AS VARCHAR) FROM ({query}) t");
    let out = conn
        .query_row(&sql, [], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
        })
        .map_err(|e| e.to_string());
    conn.close().unwrap();
    out
}

/// Rows of `left` not in `right`, both VIEW/table expressions over a
/// connection on `main_db` with `other` attached read-only as `src`.
fn except_count(main_db: &Path, other: &Path, left: &str, right: &str) -> i64 {
    let conn = Connection::open(main_db).unwrap();
    conn.execute_batch(&format!("ATTACH '{}' AS src (READ_ONLY)", other.display()))
        .unwrap();
    let n: i64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM (SELECT * FROM {left} EXCEPT ALL SELECT * FROM {right})"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap_or_else(|e| panic!("{left} vs {right}: {e}"));
    conn.close().unwrap();
    n
}

fn file_bytes(p: &Path) -> Vec<u8> {
    std::fs::read(p).unwrap()
}

fn wal(p: &Path) -> PathBuf {
    let mut w = p.as_os_str().to_os_string();
    w.push(".wal");
    PathBuf::from(w)
}

/// Any extract temp left in `dir`?
fn temps_in(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter_map(|e| e.file_name().to_str().map(str::to_string))
        .filter(|n| n.contains(".extract."))
        .collect()
}

fn opts(user: &str) -> ExtractOpts<'_> {
    ExtractOpts {
        as_of: None,
        user,
        note: None,
    }
}

const ALL_TABLES_SQL: &str = "SELECT table_name FROM information_schema.tables \
     WHERE table_type = 'BASE TABLE' AND table_catalog = current_database() ORDER BY 1";

/// The 31 as-of table macros (the same census the erasure sweep uses).
fn asof_macros(db: &Path) -> Vec<String> {
    strings(
        db,
        "SELECT DISTINCT function_name FROM duckdb_functions() \
         WHERE NOT internal AND function_type = 'table_macro' \
           AND ends_with(function_name, '_asof') ORDER BY 1",
    )
}

// --- the churned fixture -----------------------------------------------------

/// Commits of interest in the fixture, for the as-of tests.
struct Fixture {
    db: PathBuf,
    /// After the Base writes of round 1, before the run alternatives exist.
    c_base_only: i64,
    /// After the first round of writes (before any overwrite).
    c_initial: i64,
    /// After the overwrites, before the deletes (and the erase).
    c_churned: i64,
    /// After the value and entity deletes (before the erase, if any).
    c_deleted: i64,
}

fn churned_source(dir: &Path) -> Fixture {
    churned_source_with(dir, true)
}

/// `erase = false` leaves out the final erase-by-alternative, so as-of
/// extracts at every earlier commit are possible (an erasure marker after N
/// refuses an as-of-N extract).
fn churned_source_with(dir: &Path, erase: bool) -> Fixture {
    let db = dir.join("source.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");

    // Round 1: all three tiers, plus results under two run alternatives.
    write(
        &db,
        "Node",
        "demand",
        "Base",
        scalar_batch(&["n1", "n2", "n3", "n4"], &[1.0, 2.0, 3.0, 4.0]),
    );
    write(&db, "Node", "profile", "Base", array_batch("n1", 12, 0.0));
    write(&db, "Node", "profile", "Base", array_batch("n2", 24, 0.0));
    write(
        &db,
        "Node",
        "ragged",
        "Base",
        long_batch("n1", &[0, 2, 5], &[1, 2, 3]),
    );
    let c_base_only = scalar(&db, "SELECT max(id) FROM \"commit\"");
    write(
        &db,
        "Node",
        "demand",
        "runA",
        scalar_batch(&["n1"], &[10.0]),
    );
    write(&db, "Node", "profile", "runA", array_batch("n1", 24, 100.0));
    write(
        &db,
        "Node",
        "demand",
        "runB",
        scalar_batch(&["n1"], &[20.0]),
    );
    commit_structural(
        &db,
        vec![StructuralChange::SetScenarioAlternatives {
            scenario: "s_main".into(),
            alternatives: vec!["Base".into(), "runB".into()],
        }],
        "tester",
        "scenario",
        WriteOptions::default(),
    )
    .expect("scenario");
    commit_structural(
        &db,
        vec![StructuralChange::SetEntityActivation {
            class: "Node".into(),
            entity: "n2".into(),
            alternative: "runB".into(),
            active: Some(false),
        }],
        "tester",
        "activation",
        WriteOptions::default(),
    )
    .expect("activation");

    // Raw-planted structure: membership, a run (tool n4, result runA), a
    // metadata note, definition values on the scalar and array shapes (the
    // array one references the 12-cell axis, which n1's profile later
    // abandons, so it survives only through the definition value).
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        "INSERT INTO commit (user, message) VALUES ('tester', 'fixture structure');
         INSERT INTO membership (group_entity_id, member_entity_id, included, valid_from_commit)
           SELECT e1.id, e2.id, TRUE, currval('seq_commit')
           FROM entity e1, entity e2 WHERE e1.cur_name = 'n1' AND e2.cur_name = 'n2';
         INSERT INTO metadata (name, value, valid_from_commit)
           VALUES ('note_key', 'note_val', currval('seq_commit'));
         INSERT INTO run (id, tool_entity_id, result_alternative_id, input_commit_id, status)
           SELECT nextval('seq_run'), e.id, a.id, 1, 'optimal'
           FROM entity e, alternative a WHERE e.cur_name = 'n4' AND a.cur_name = 'runa';
         INSERT INTO definition_value_scalar
             (parameter_definition_id, slot, valid_from_commit, version, value_kind, v_float)
           SELECT id, 'default_value', currval('seq_commit'), 1, 'float', 5.0
           FROM parameter_definition WHERE cur_name = 'demand';
         INSERT INTO definition_value_array
             (parameter_definition_id, slot, valid_from_commit, version, axis1_id,
              \"values\", v_min, v_max)
           SELECT pd.id, 'default_value', currval('seq_commit'), 1, va.axis1_id,
                  va.\"values\", va.v_min, va.v_max
           FROM parameter_definition pd, value_array va, entity e
           WHERE pd.cur_name = 'profile' AND va.entity_id = e.id AND e.cur_name = 'n1'
             AND va.alternative_id = (SELECT id FROM alternative WHERE cur_name = 'base');",
    )
    .expect("plant structure");
    let c_initial: i64 = conn
        .query_row("SELECT max(id) FROM \"commit\"", [], |r| r.get(0))
        .unwrap();
    conn.close().unwrap();

    // Round 2: churn every tier.
    for i in 1..=3 {
        let f = i as f64;
        write(
            &db,
            "Node",
            "demand",
            "Base",
            scalar_batch(&["n1", "n2", "n3"], &[1.0 + f, 2.0 + f, 3.0 + f]),
        );
        write(&db, "Node", "profile", "Base", array_batch("n2", 24, f));
    }
    // n1's profile moves to 24 cells (the 12-cell axis now only serves the
    // definition value), and the ragged long value moves to new coordinates
    // (its old axis becomes unreferenced: the extract drops it).
    write(&db, "Node", "profile", "Base", array_batch("n1", 24, 7.0));
    write(
        &db,
        "Node",
        "ragged",
        "Base",
        long_batch("n1", &[0, 3, 7], &[4, 5, 6]),
    );
    // ...then one of its cells changes: a cell diff, so its current cells
    // carry two valid_from commits.
    write(
        &db,
        "Node",
        "ragged",
        "Base",
        long_batch("n1", &[0, 3, 7], &[4, 9, 6]),
    );
    // A definition-value supersede (close-out + successor).
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        "INSERT INTO commit (user, message) VALUES ('tester', 'default 6');
         UPDATE definition_value_scalar SET valid_to_commit = currval('seq_commit')
           WHERE valid_to_commit IS NULL;",
    )
    .unwrap();
    conn.execute_batch(
        "INSERT INTO definition_value_scalar
             (parameter_definition_id, slot, valid_from_commit, version, value_kind, v_float)
           SELECT id, 'default_value', (SELECT max(id) FROM \"commit\"), 2, 'float', 6.0
           FROM parameter_definition WHERE cur_name = 'demand';",
    )
    .unwrap();
    conn.close().unwrap();
    stamp_template_identity(&db, "tpl", "1.0", false, "tester").expect("stamp");
    stamp_template_identity(&db, "tpl", "2.0", true, "tester").expect("restamp");
    commit_structural(
        &db,
        vec![StructuralChange::RenameScenario {
            current: "s_main".into(),
            new_name: "s_renamed".into(),
            description: None,
        }],
        "tester",
        "rename",
        WriteOptions::default(),
    )
    .expect("rename");
    let c_churned = scalar(&db, "SELECT max(id) FROM \"commit\"");

    // Round 3: a value delete, an entity delete, an erase.
    delete_values(
        &db,
        vec![DeleteKey {
            class: "Node".into(),
            parameter: "demand".into(),
            alternative: "Base".into(),
            entity: "n3".into(),
        }],
        "tester",
        "delete n3 demand",
        WriteOptions::default(),
    )
    .expect("delete value");
    delete_objects(
        &db,
        vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Entity,
                class: Some("Node".into()),
                target: ObjectRef::Name("n4".into()),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        "tester",
        "delete n4",
        WriteOptions::default(),
    )
    .expect("delete entity");
    let c_deleted = scalar(&db, "SELECT max(id) FROM \"commit\"");
    if erase {
        let r = erase_alternative(
            &db,
            &ObjectDeleteKey {
                kind: ObjectKind::Alternative,
                class: None,
                target: ObjectRef::Name("runA".into()),
            },
            &EraseOpts::default(),
            "tester",
        )
        .expect("erase runA");
        assert!(r.erased);
    }

    Fixture {
        db,
        c_base_only,
        c_initial,
        c_churned,
        c_deleted,
    }
}

/// The non-audit views every comparison covers, minus the documented
/// exclusions (each asserted separately in T1).
const T1_EXCLUDED_VIEWS: &[&str] = &[
    "axis_position",          // only the carried axes (asserted restricted)
    "dataset_version",        // E (asserted)
    "metadata_current",       // the identity row differs (asserted)
    "value_timeline",         // history view: only the surviving versions (asserted)
    "history_untrusted_span", // history view (asserted)
];

fn non_audit_views(db: &Path) -> Vec<String> {
    strings(
        db,
        "SELECT view_name FROM duckdb_views() WHERE NOT internal \
           AND database_name = current_database() \
           AND view_name NOT LIKE 'audit\\_%' ESCAPE '\\' ORDER BY 1",
    )
}

/// Every (class, parameter) pair, current scenario and display mode, for the
/// macro comparisons.
fn macro_calls(db: &Path) -> Vec<String> {
    let mut calls = Vec::new();
    let pairs = strings(
        db,
        "SELECT ec.name || '\u{1}' || pd.name FROM parameter_definition_current pd \
         JOIN entity_class_current ec ON ec.id = pd.entity_class_id ORDER BY 1",
    );
    for p in pairs {
        let (c, n) = p.split_once('\u{1}').unwrap();
        for m in [
            "read_parameter",
            "read_parameter_dense",
            "read_parameter_scalar",
        ] {
            calls.push(format!("SELECT * FROM {m}('{c}', '{n}')"));
        }
    }
    let scenarios = strings(db, "SELECT name FROM scenario_current ORDER BY 1");
    for s in &scenarios {
        calls.push(format!("SELECT * FROM resolved_value_rows('{s}')"));
        calls.push(format!("SELECT * FROM scenario_contents('{s}')"));
        for s2 in &scenarios {
            calls.push(format!("SELECT * FROM scenario_diff('{s}', '{s2}')"));
        }
    }
    calls.push("SELECT * FROM entity_class_display('none')".into());
    calls
}

// --- T1: head equality ----------------------------------------------------------

#[test]
fn t1_head_extract_reads_identically() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source(dir.path());
    let dest = dir.path().join("extract.duckdb");
    let report = extract(&fx.db, &dest, &opts("tester")).expect("extract");
    assert!(
        report.axes_dropped > 0,
        "fixture must leave an unreferenced axis"
    );
    assert!(report.rows_removed > 0);

    // Every non-audit view, except the documented exclusions, is identical
    // (views compared across files through ATTACH: views bind to their own
    // catalog).
    let views = non_audit_views(&fx.db);
    assert!(views.len() > 40, "view census looks wrong: {views:?}");
    for v in &views {
        if T1_EXCLUDED_VIEWS.contains(&v.as_str()) {
            continue;
        }
        let a = fingerprint(&fx.db, &format!("SELECT * FROM {v}"));
        let b = fingerprint(&dest, &format!("SELECT * FROM {v}"));
        assert_eq!(a, b, "view {v} differs");
        assert!(a.is_ok(), "view {v} errors: {a:?}");
    }
    // Every read macro and scenario macro, called on each file directly.
    let calls = macro_calls(&fx.db);
    assert!(calls.len() > 10);
    for c in &calls {
        assert_eq!(fingerprint(&fx.db, c), fingerprint(&dest, c), "{c}");
    }

    // The exclusions, each asserted.
    assert_eq!(
        except_count(
            &dest,
            &fx.db,
            "main.axis_position",
            "src.axis_position WHERE axis_id IN (SELECT id FROM main.axis)"
        ),
        0
    );
    assert_eq!(
        except_count(
            &dest,
            &fx.db,
            "src.axis_position WHERE axis_id IN (SELECT id FROM main.axis)",
            "main.axis_position"
        ),
        0
    );
    assert_eq!(
        scalar(&dest, "SELECT version FROM dataset_version"),
        report.extract_commit
    );
    let md = "SELECT * FROM metadata_current WHERE lower(name) <> 'db_identity'";
    assert_eq!(fingerprint(&fx.db, md), fingerprint(&dest, md));
    // value_timeline: the scalar and array legs carry the surviving
    // versions verbatim. The long leg is value-level change spans derived
    // from the cells (skip_unchanged_writes_PLAN §4.3), clipped at the
    // horizon: the extract carries only the cells current at H, whose
    // valid_from commits (several, for a cell-diffed value) are not the
    // value's change points, so every long value is exactly one open span
    // from the extract commit E, with the source's current retention.
    let e = report.extract_commit;
    assert!(
        scalar(
            &fx.db,
            "SELECT count(*) FROM (SELECT entity_id, parameter_definition_id, alternative_id              FROM value_long WHERE valid_to_commit IS NULL GROUP BY ALL              HAVING count(DISTINCT valid_from_commit) > 1)"
        ) > 0,
        "the fixture has a cell-diffed long value"
    );
    for view in ["value_timeline", "history_untrusted_span"] {
        assert_eq!(
            fingerprint(
                &fx.db,
                &format!("SELECT * FROM {view} WHERE valid_to_commit IS NULL AND tier <> 'long'")
            ),
            fingerprint(&dest, &format!("SELECT * FROM {view} WHERE tier <> 'long'")),
            "{view}"
        );
        let long = "SELECT entity_id, parameter_definition_id, alternative_id, \
                    version::BIGINT AS version, valid_from_commit::BIGINT AS f, \
                    valid_to_commit::BIGINT AS t, content_retained";
        assert_eq!(
            fingerprint(
                &fx.db,
                &format!(
                    "SELECT entity_id, parameter_definition_id, alternative_id, \
                     1::BIGINT AS version, {e}::BIGINT AS f, NULL::BIGINT AS t, content_retained \
                     FROM {view} WHERE tier = 'long' AND valid_to_commit IS NULL"
                )
            ),
            fingerprint(&dest, &format!("{long} FROM {view} WHERE tier = 'long'")),
            "{view} long"
        );
    }
    // The cell-exploding views (kept out of the runtime self-check) were
    // covered above; make sure they are non-trivial here.
    assert!(scalar(&dest, "SELECT count(*) FROM value_array_cells") > 40);
    assert_eq!(scalar(&dest, "SELECT count(*) FROM value_array_hist"), 0);
}

// --- T3: loud before the horizon, exact at E ---------------------------------

#[test]
fn t3_asof_before_e_fails_loudly_and_is_exact_at_e() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source(dir.path());
    let dest = dir.path().join("extract.duckdb");
    let r = extract(&fx.db, &dest, &opts("tester")).expect("extract");
    let e = r.extract_commit;
    let macros = asof_macros(&dest);
    assert_eq!(macros.len(), 31);

    let conn = Connection::open(&dest).unwrap();
    for m in &macros {
        for c in [1, fx.c_initial, r.horizon_commit, e - 1] {
            for shape in [
                format!("SELECT * FROM {m}({c})"),
                format!("SELECT count(*) FROM {m}({c})"),
                format!("SELECT 1 FROM {m}({c})"),
                format!(
                    "SELECT count(*) FROM {m}({c}) t JOIN \"commit\" cm \
                     ON cm.id = t.valid_from_commit"
                ),
            ] {
                let err = conn
                    .prepare(&shape)
                    .and_then(|mut s| s.query([]).map(|mut rows| rows.next().map(|_| ())))
                    .and_then(|r| r);
                let msg = match err {
                    Ok(_) => panic!("{shape}: silent before the horizon"),
                    Err(e) => e.to_string(),
                };
                assert!(msg.contains("unreliable"), "{shape}: {msg}");
            }
        }
    }
    conn.close().unwrap();

    // At E every macro is exact: it returns the current rows.
    for m in &macros {
        let base = m.trim_end_matches("_asof");
        let (at_e, cur) = if base == "value_array" {
            (
                format!("SELECT * EXCLUDE (valid_to_commit, content_retained) FROM {m}({e})"),
                "SELECT * FROM value_array_current".to_string(),
            )
        } else {
            (
                format!("SELECT * FROM {m}({e})"),
                format!("SELECT * FROM {base}_current"),
            )
        };
        let a = fingerprint(&dest, &at_e);
        assert!(a.is_ok(), "{at_e}: {a:?}");
        assert_eq!(a, fingerprint(&dest, &cur), "{m}({e})");
    }
}

// --- T4 + T5: the marker, identity and log -------------------------------------

#[test]
fn t4_t5_marker_identity_and_log() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source(dir.path());
    let src_id = db_identity(&fx.db).unwrap();
    let src_max = scalar(&fx.db, "SELECT max(id) FROM \"commit\"");
    let dest = dir.path().join("extract.duckdb");
    let r = extract(
        &fx.db,
        &dest,
        &ExtractOpts {
            as_of: None,
            user: "alice",
            note: Some("slim copy"),
        },
    )
    .expect("extract");

    // T5: new identity, exactly one current identity row; log verbatim; E >
    // every source commit.
    let new_id = db_identity(&dest).unwrap();
    assert_ne!(new_id, src_id);
    assert_eq!(new_id, r.extract_identity);
    assert_eq!(src_id, r.source_identity);
    assert_eq!(
        scalar(&dest, "SELECT count(*) FROM audit_metadata_reserved_dup"),
        0
    );
    assert_eq!(r.horizon_commit, src_max);
    assert!(r.extract_commit > src_max);
    let log = format!("SELECT * FROM \"commit\" WHERE id <= {src_max}");
    assert_eq!(fingerprint(&fx.db, &log), fingerprint(&dest, &log));
    assert_eq!(
        string(
            &dest,
            &format!(
                "SELECT user FROM \"commit\" WHERE id = {}",
                r.extract_commit
            )
        ),
        "alice"
    );
    assert_eq!(
        scalar(
            &dest,
            "SELECT valid_from_commit FROM metadata WHERE lower(name) = 'db_identity' \
             AND valid_to_commit IS NULL"
        ),
        r.extract_commit
    );

    // T4: exactly one new horizon marker with H, E, the source identity, the
    // exact rows_removed; prior markers carried verbatim.
    assert_eq!(
        scalar(
            &dest,
            "SELECT count(*) FROM erasure_marker WHERE kind = 'horizon'"
        ),
        1
    );
    assert_eq!(
        fingerprint(&fx.db, "SELECT * FROM erasure_marker"),
        fingerprint(
            &dest,
            "SELECT * FROM erasure_marker WHERE kind <> 'horizon'"
        )
    );
    let (h, at, cont, removed, note): (i64, i64, String, i64, String) = {
        let conn = Connection::open(&dest).unwrap();
        let v = conn
            .query_row(
                "SELECT horizon_commit, at_commit, continuation_db, rows_removed, note \
                 FROM erasure_marker WHERE kind = 'horizon'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .unwrap();
        conn.close().unwrap();
        v
    };
    assert_eq!((h, at), (src_max, r.extract_commit));
    assert_eq!(cont, src_id);
    assert_eq!(note, "slim copy");
    // rows_removed = Σ source rows − Σ carried rows (E's commit, identity row
    // and marker are not "carried").
    let tables = strings(&fx.db, ALL_TABLES_SQL);
    let total = |db: &Path| -> i64 {
        tables
            .iter()
            .map(|t| scalar(db, &format!("SELECT count(*) FROM \"{t}\"")))
            .sum()
    };
    let expected = total(&fx.db) - (total(&dest) - 3);
    assert_eq!(removed, expected);
    assert_eq!(r.rows_removed as i64, expected);
    let carried: u64 = r.rows_per_table.iter().map(|(_, n)| n).sum();
    assert_eq!(carried as i64, total(&dest) - 3);

    let conn = Connection::open(&dest).unwrap();
    let (kind, unresolved, cont2): (String, bool, String) = conn
        .query_row(
            "SELECT kind, scope_unresolved, continuation_db FROM audit_erasure \
             WHERE kind = 'horizon'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    conn.close().unwrap();
    assert_eq!(kind, "horizon");
    assert!(!unresolved);
    assert_eq!(cont2, src_id);
}

// --- T6: the source is untouched -----------------------------------------------

#[test]
fn t6_source_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source(dir.path());
    // Settle the source first (no pending WAL), then snapshot it.
    let conn = Connection::open(&fx.db).unwrap();
    conn.execute_batch("CHECKPOINT").unwrap();
    conn.close().unwrap();
    assert!(!wal(&fx.db).exists());
    let bytes = file_bytes(&fx.db);
    let mtime = std::fs::metadata(&fx.db).unwrap().modified().unwrap();

    let dest = dir.path().join("extract.duckdb");
    extract(&fx.db, &dest, &opts("tester")).expect("extract");

    assert!(file_bytes(&fx.db) == bytes, "source bytes changed");
    assert_eq!(
        std::fs::metadata(&fx.db).unwrap().modified().unwrap(),
        mtime
    );
    assert!(!wal(&fx.db).exists(), "a source .wal appeared");
    assert!(!wal(&dest).exists(), "the extract left a .wal");
    assert!(temps_in(dir.path()).is_empty());
}

// --- T7: audits ----------------------------------------------------------------

#[test]
fn t7_audits_add_nothing_and_axis_set_is_the_closure() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source(dir.path());
    let dest = dir.path().join("extract.duckdb");
    extract(&fx.db, &dest, &opts("tester")).expect("extract");
    let audits = strings(
        &dest,
        "SELECT view_name FROM duckdb_views() WHERE NOT internal \
           AND database_name = current_database() \
           AND view_name LIKE 'audit\\_%' ESCAPE '\\' ORDER BY 1",
    );
    assert!(audits.len() > 20);
    for a in &audits {
        // Report views: the file size and the erasure inventory differ by
        // design (the inventory is T4's job).
        if a == "audit_health" || a == "audit_erasure" {
            continue;
        }
        assert_eq!(
            except_count(&dest, &fx.db, &format!("main.{a}"), &format!("src.{a}")),
            0,
            "{a} reports rows on the extract that the source does not"
        );
    }
    assert_eq!(
        scalar(&dest, "SELECT count(*) FROM audit_history_tiling"),
        0
    );
    assert_eq!(
        scalar(&dest, "SELECT count(*) FROM audit_axis_integrity"),
        0
    );
    // Axis set == referenced closure.
    let referenced = "SELECT DISTINCT a FROM (\
        SELECT unnest([axis1_id, axis2_id, axis3_id, axis4_id]) a FROM value_long UNION ALL \
        SELECT unnest([axis1_id, axis2_id, axis3_id, axis4_id]) FROM value_array UNION ALL \
        SELECT unnest([axis1_id, axis2_id, axis3_id, axis4_id]) FROM definition_value_long UNION ALL \
        SELECT unnest([axis1_id, axis2_id, axis3_id, axis4_id]) FROM definition_value_array) \
        WHERE a IS NOT NULL";
    assert_eq!(
        fingerprint(&dest, "SELECT id FROM axis"),
        fingerprint(&dest, referenced)
    );
    // The trace closure kept the closed identity rows the run and the erase
    // marker point at (runA erased, n4 deleted): no dangling run legs.
    assert_eq!(
        scalar(
            &dest,
            "SELECT count(*) FROM run r WHERE NOT EXISTS \
             (SELECT 1 FROM alternative a WHERE a.id = r.result_alternative_id)"
        ),
        0
    );
    assert_eq!(
        scalar(
            &dest,
            "SELECT count(*) FROM run r WHERE NOT EXISTS \
             (SELECT 1 FROM entity e WHERE e.id = r.tool_entity_id)"
        ),
        0
    );
    assert_eq!(
        scalar(
            &dest,
            "SELECT count(*) FROM alternative WHERE valid_to_commit IS NOT NULL"
        ),
        1,
        "exactly the erased runA identity row is carried closed"
    );
}

// --- T8: the extract is a live database ----------------------------------------

#[test]
fn t8_extract_is_live() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source(dir.path());
    let dest = dir.path().join("extract.duckdb");
    extract(&fx.db, &dest, &opts("tester")).expect("extract");

    // Versions continue at current + 1; auto-ids do not collide.
    let v_before = scalar(
        &dest,
        "SELECT vs.version FROM value_scalar vs JOIN entity e ON e.id = vs.entity_id \
         JOIN alternative a ON a.id = vs.alternative_id \
         WHERE e.name = 'n1' AND a.name = 'Base' AND vs.valid_to_commit IS NULL",
    );
    write(
        &dest,
        "Node",
        "demand",
        "Base",
        scalar_batch(&["n1", "n9"], &[42.0, 9.0]),
    );
    write(&dest, "Node", "profile", "Base", array_batch("n9", 24, 1.0));
    write(&dest, "Node", "profile", "Base", array_batch("n1", 24, 2.0));
    let v_after = scalar(
        &dest,
        "SELECT vs.version FROM value_scalar vs JOIN entity e ON e.id = vs.entity_id \
         JOIN alternative a ON a.id = vs.alternative_id \
         WHERE e.name = 'n1' AND a.name = 'Base' AND vs.valid_to_commit IS NULL",
    );
    assert_eq!(v_after, v_before + 1);
    assert_eq!(
        scalar(&dest, "SELECT count(*) FROM audit_history_tiling"),
        0
    );
    assert_eq!(
        scalar(&dest, "SELECT count(*) FROM audit_array_id_coherence"),
        0
    );

    // Compaction of the extract passes its self-check; erasure works.
    compact(&dest).expect("compact the extract");
    let r = erase_alternative(
        &dest,
        &ObjectDeleteKey {
            kind: ObjectKind::Alternative,
            class: None,
            target: ObjectRef::Name("runB".into()),
        },
        &EraseOpts::default(),
        "tester",
    )
    .expect("erase on the extract");
    assert!(r.erased);

    // Extract-of-extract: two horizon markers, and still clean.
    let dest2 = dir.path().join("extract2.duckdb");
    let r2 = extract(&dest, &dest2, &opts("tester")).expect("extract of extract");
    assert_eq!(
        scalar(
            &dest2,
            "SELECT count(*) FROM erasure_marker WHERE kind = 'horizon'"
        ),
        2
    );
    assert_eq!(r2.source_identity, db_identity(&dest).unwrap());
    assert_eq!(
        scalar(&dest2, "SELECT count(*) FROM audit_history_tiling"),
        0
    );
    write(
        &dest2,
        "Node",
        "demand",
        "Base",
        scalar_batch(&["n1"], &[43.0]),
    );
    assert_eq!(
        scalar(&dest2, "SELECT count(*) FROM audit_history_tiling"),
        0
    );
}

// --- array_id: copied as-is, odd, stride 2 ---------------------------------------

#[test]
fn extract_keeps_array_ids_and_sequence_stride() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source(dir.path());
    let dest = dir.path().join("extract.duckdb");
    extract(&fx.db, &dest, &opts("tester")).expect("extract");
    let live = "SELECT array_id, entity_id, parameter_definition_id, alternative_id, version \
                FROM value_array";
    assert_eq!(fingerprint(&fx.db, live), fingerprint(&dest, live));
    assert_eq!(
        scalar(
            &dest,
            "SELECT count(*) FROM value_array WHERE array_id % 2 = 0"
        ),
        0,
        "every array_id stays odd"
    );
    let seq = "SELECT increment_by FROM duckdb_sequences() WHERE sequence_name = 'seq_value_array'";
    assert_eq!(scalar(&dest, seq), 2, "stride 2 survives the extract");
    let last_src = scalar(
        &fx.db,
        "SELECT last_value FROM duckdb_sequences() WHERE sequence_name = 'seq_value_array'",
    );
    write(&dest, "Node", "profile", "Base", array_batch("n7", 24, 0.0));
    let new_id = scalar(
        &dest,
        "SELECT va.array_id FROM value_array va JOIN entity e ON e.id = va.entity_id \
         WHERE e.name = 'n7'",
    );
    assert_eq!(
        new_id,
        last_src + 2,
        "ids continue from the source's sequence"
    );
    assert_eq!(new_id % 2, 1);
}

// --- T9: refusals (head mode) ----------------------------------------------------

fn assert_refused(r: Result<ExtractReport, DbError>, needle: &str) {
    match r {
        Ok(_) => panic!("expected a refusal containing {needle:?}"),
        Err(e) => assert!(e.to_string().contains(needle), "{e}"),
    }
}

#[test]
fn t9_refusals_leave_no_dest_and_no_temp() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source(dir.path());
    let src = &fx.db;

    // dest exists: untouched.
    let existing = dir.path().join("existing.duckdb");
    std::fs::write(&existing, b"user data").unwrap();
    assert_refused(extract(src, &existing, &opts("t")), "already exists");
    assert_eq!(std::fs::read(&existing).unwrap(), b"user data");

    // A stale <dest>.wal: refused, the WAL is not deleted.
    let dest = dir.path().join("fresh.duckdb");
    std::fs::write(wal(&dest), b"foreign wal").unwrap();
    assert_refused(extract(src, &dest, &opts("t")), "write-ahead log");
    assert!(wal(&dest).exists());
    std::fs::remove_file(wal(&dest)).unwrap();
    assert!(!dest.exists());

    // dest == src, directly, through a relative alias, and through a symlink.
    assert_refused(extract(src, src, &opts("t")), "is the source itself");
    let alias = dir
        .path()
        .join("sub")
        .join("..")
        .join(src.file_name().unwrap());
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    assert_refused(extract(src, &alias, &opts("t")), "is the source itself");
    #[cfg(unix)]
    {
        let link = dir.path().join("link.duckdb");
        std::os::unix::fs::symlink(src, &link).unwrap();
        assert_refused(extract(src, &link, &opts("t")), "is the source itself");
    }

    // An empty user.
    assert_refused(extract(src, &dest, &opts(" ")), "user must be non-empty");

    // Schema mismatch and identity count: on copies of the source.
    let bad_schema = dir.path().join("bad_schema.duckdb");
    std::fs::copy(src, &bad_schema).unwrap();
    let conn = Connection::open(&bad_schema).unwrap();
    conn.execute_batch(
        "UPDATE metadata SET value = '0.0-old' \
         WHERE lower(name) = 'schema_version' AND valid_to_commit IS NULL",
    )
    .unwrap();
    conn.close().unwrap();
    let r = extract(&bad_schema, &dest, &opts("t"));
    assert!(
        matches!(&r, Err(DbError::SchemaMismatch { found, .. }) if found == "0.0-old"),
        "{r:?}"
    );
    assert_refused(r, "schema_version");

    let no_id = dir.path().join("no_id.duckdb");
    std::fs::copy(src, &no_id).unwrap();
    let conn = Connection::open(&no_id).unwrap();
    conn.execute_batch("DELETE FROM metadata WHERE lower(name) = 'db_identity'")
        .unwrap();
    conn.close().unwrap();
    assert_refused(extract(&no_id, &dest, &opts("t")), "0 current db_identity");

    let two_ids = dir.path().join("two_ids.duckdb");
    std::fs::copy(src, &two_ids).unwrap();
    let conn = Connection::open(&two_ids).unwrap();
    conn.execute_batch(
        "INSERT INTO metadata (name, value, valid_from_commit) \
         VALUES ('db_identity', 'second', (SELECT max(id) FROM \"commit\"))",
    )
    .unwrap();
    conn.close().unwrap();
    assert_refused(
        extract(&two_ids, &dest, &opts("t")),
        "2 current db_identity",
    );

    // Nothing anywhere.
    assert!(!dest.exists());
    assert!(!wal(&dest).exists());
    assert!(
        temps_in(dir.path()).is_empty(),
        "{:?}",
        temps_in(dir.path())
    );

    // Files named like a catalog alias still work (DuckDB names a file's
    // catalog after its name up to the first dot).
    let src_named = dir.path().join("src.duckdb");
    extract(src, &src_named, &opts("t")).expect("dest named src.duckdb");
    let again = dir.path().join("src.again.duckdb");
    extract(&src_named, &again, &opts("t")).expect("source named src.duckdb");

    // An orphan temp from a crashed extract is swept by the next one.
    let orphan = dir.path().join("fresh.duckdb.extract.999999.0.tmp");
    std::fs::write(&orphan, b"orphan").unwrap();
    extract(src, &dest, &opts("t")).expect("extract after refusals");
    assert!(!orphan.exists());
    assert!(temps_in(dir.path()).is_empty());
}

/// Lock-holder child: opens the source READ-WRITE (which blocks the extract's
/// read-only ATTACH), signals via a sentinel file, and holds the lock until
/// the parent creates the release file (or 60 s pass).
#[test]
#[ignore = "lock-holder helper; spawned as a child process by the busy test"]
fn helper_hold_rw_lock() {
    let db = std::env::var("NODAL_DB_TEST_DB").expect("child db path");
    let sentinel = std::env::var("NODAL_DB_TEST_SENTINEL").expect("sentinel path");
    let release = std::env::var("NODAL_DB_TEST_RELEASE").expect("release path");
    let conn = Connection::open(&db).expect("child RW open");
    std::fs::write(&sentinel, b"held").unwrap();
    let t0 = std::time::Instant::now();
    while !Path::new(&release).exists() && t0.elapsed().as_secs() < 60 {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    conn.close().unwrap();
}

#[test]
fn t9_busy_source_cross_process() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source(dir.path());
    let sentinel = dir.path().join("lock.sentinel");
    let release = dir.path().join("lock.release");
    let exe = std::env::current_exe().expect("test exe");
    let mut child = std::process::Command::new(exe)
        .args(["--exact", "helper_hold_rw_lock", "--ignored", "--nocapture"])
        .env("NODAL_DB_TEST_DB", &fx.db)
        .env("NODAL_DB_TEST_SENTINEL", &sentinel)
        .env("NODAL_DB_TEST_RELEASE", &release)
        .spawn()
        .expect("spawn lock holder");
    let t0 = std::time::Instant::now();
    while !sentinel.exists() {
        assert!(t0.elapsed().as_secs() < 20, "lock holder never signalled");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let dest = dir.path().join("extract.duckdb");
    let r = extract(&fx.db, &dest, &opts("t"));
    std::fs::write(&release, b"go").unwrap();
    child.wait().unwrap();
    assert!(matches!(r, Err(DbError::Busy { .. })), "{r:?}");
    assert!(!dest.exists());
    assert!(temps_in(dir.path()).is_empty());
    // And once the holder is gone it works.
    extract(&fx.db, &dest, &opts("t")).expect("extract after the lock is released");
}

/// In-process: a writer of the same process holds the source's per-path lock
/// for the span of its commit; the extract waits for it (it never runs a
/// second DuckDB instance beside it) and then carries that commit.
#[test]
fn t9_in_process_writer_serializes_with_extract() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source(dir.path());
    let src = fx.db.clone();
    let started = Arc::new(std::sync::Barrier::new(2));
    let s2 = started.clone();
    let writer = std::thread::spawn(move || {
        nodal_db_core::conn::with_write_txn(&src, "writer", "slow commit", |conn, c| {
            s2.wait();
            std::thread::sleep(std::time::Duration::from_millis(800));
            conn.execute(
                "INSERT INTO metadata (name, value, valid_from_commit) VALUES ('slow', 'x', ?)",
                [c],
            )?;
            Ok(c)
        })
        .expect("slow write")
    });
    started.wait();
    let dest = dir.path().join("extract.duckdb");
    let r = extract(&fx.db, &dest, &opts("t")).expect("extract waits, then runs");
    let c = writer.join().unwrap();
    assert_eq!(
        r.horizon_commit, c,
        "the extract ran after the writer's commit"
    );
    assert_eq!(
        scalar(
            &dest,
            "SELECT count(*) FROM metadata WHERE name = 'slow' AND valid_to_commit IS NULL"
        ),
        1
    );
}

// --- T10: native storage format ----------------------------------------------------

#[test]
fn t10_extract_is_native_2_0_storage() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source(dir.path());
    let dest = dir.path().join("extract.duckdb");
    extract(&fx.db, &dest, &opts("t")).expect("extract");
    assert_eq!(
        string(
            &dest,
            "SELECT tags['storage_version'] FROM duckdb_databases() \
             WHERE database_name = current_database()"
        ),
        "v2.0.0+"
    );
    // The extract is freshly packed: its size is the compaction baseline.
    let baseline = nodal_db_core::compaction::read_baseline(&dest).expect("v2 sidecar");
    assert_eq!(baseline.bytes, std::fs::metadata(&dest).unwrap().len());
}

// --- T11: the attached-catalog macro binding tripwire --------------------------------

/// Pins DuckDB's current behaviour: a table macro called through an attached
/// catalog binds its body's unqualified names in the CALLER's default
/// catalog and silently returns the caller's rows. Views bind to their own
/// catalog, and `USE src` makes the macro read the source. If an engine
/// upgrade changes this, this test fails: re-decide the rule "never call a
/// nodal-db table macro through an attached catalog".
#[test]
fn t11_attached_macro_binds_to_callers_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.duckdb");
    let b = dir.path().join("b.duckdb");
    init_schema_with_default(&a, Some("Base")).unwrap();
    init_schema_with_default(&b, None).unwrap();
    for n in ["x", "y"] {
        commit_structural(
            &a,
            vec![StructuralChange::CreateAlternative {
                name: n.into(),
                description: None,
            }],
            "t",
            "alt",
            WriteOptions::default(),
        )
        .unwrap();
    }
    let c = scalar(&a, "SELECT max(id) FROM \"commit\"");
    let conn = Connection::open(&b).unwrap();
    conn.execute_batch(&format!("ATTACH '{}' AS src (READ_ONLY)", a.display()))
        .unwrap();
    let q = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };
    assert_eq!(q("SELECT count(*) FROM src.alternative_current"), 3);
    assert_eq!(
        q(&format!("SELECT count(*) FROM src.alternative_asof({c})")),
        0,
        "tripwire: the attached macro now binds to its own catalog — re-decide the rule"
    );
    assert_eq!(
        q(&format!(
            "SELECT count(*) FROM src.main.alternative_asof({c})"
        )),
        0
    );
    conn.execute_batch("USE src").unwrap();
    assert_eq!(q(&format!("SELECT count(*) FROM alternative_asof({c})")), 3);
    conn.close().unwrap();
}

// --- T2: as-of N equality ------------------------------------------------------------

fn asof(user: &str, n: i64) -> ExtractOpts<'_> {
    ExtractOpts {
        as_of: Some(n),
        user,
        note: None,
    }
}

/// The source's as-of macro at N (called on the source as its own default
/// catalog, never through an attached one) against the extract's current
/// rows, for every as-of macro.
fn assert_asof_equal(src: &Path, dest: &Path, n: i64) {
    for m in asof_macros(src) {
        let base = m.trim_end_matches("_asof");
        let (src_q, dest_q) = if base == "value_array" {
            (
                format!("SELECT * EXCLUDE (valid_to_commit, content_retained) FROM {m}({n})"),
                "SELECT * FROM value_array_current".to_string(),
            )
        } else if base == "metadata" {
            (
                format!(
                    "SELECT * EXCLUDE (valid_to_commit) FROM {m}({n}) \
                     WHERE lower(name) <> 'db_identity'"
                ),
                "SELECT * EXCLUDE (valid_to_commit) FROM metadata_current \
                 WHERE lower(name) <> 'db_identity'"
                    .to_string(),
            )
        } else {
            (
                format!("SELECT * EXCLUDE (valid_to_commit) FROM {m}({n})"),
                format!("SELECT * EXCLUDE (valid_to_commit) FROM {base}_current"),
            )
        };
        let a = fingerprint(src, &src_q);
        assert!(a.is_ok(), "{src_q}: {a:?}");
        assert_eq!(a, fingerprint(dest, &dest_q), "{m} at {n}");
    }
}

#[test]
fn t2_asof_extract_equals_source_asof_macros() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source_with(dir.path(), false);
    let src = &fx.db;
    let ns = [
        fx.c_base_only,
        fx.c_initial,
        fx.c_initial + 2,
        fx.c_churned,
        fx.c_deleted,
    ];
    for (i, n) in ns.into_iter().enumerate() {
        let dest = dir.path().join(format!("asof_{i}.duckdb"));
        let r = extract(
            src,
            &dest,
            &ExtractOpts {
                as_of: Some(n),
                user: "tester",
                note: Some("for the auditors"),
            },
        )
        .unwrap_or_else(|e| panic!("as-of {n}: {e}"));
        assert_eq!(r.horizon_commit, n);
        assert_asof_equal(src, &dest, n);

        // The log is cut at N (verbatim), then E.
        let log = format!("SELECT * FROM \"commit\" WHERE id <= {n}");
        assert_eq!(fingerprint(src, &log), fingerprint(&dest, &log));
        assert_eq!(
            scalar(
                &dest,
                &format!("SELECT count(*) FROM \"commit\" WHERE id > {n}")
            ),
            1
        );
        assert_eq!(
            string(
                &dest,
                "SELECT note FROM erasure_marker WHERE kind = 'horizon'"
            ),
            format!("as-of {n} extract; for the auditors")
        );
        // No close-out edge points past N: every carried versioned row is
        // current, apart from the trace-closure identity rows.
        assert_eq!(scalar(&dest, "SELECT count(*) FROM value_array_hist"), 0);
        assert_eq!(
            scalar(
                &dest,
                "SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NOT NULL"
            ),
            0
        );
        assert_eq!(
            scalar(
                &dest,
                "SELECT count(*) FROM value_array WHERE array_id % 2 = 0"
            ),
            0
        );
        // X14: the run (result runA, tool n4) exists only once runA does.
        let runs = scalar(&dest, "SELECT count(*) FROM run");
        assert_eq!(runs, if n == fx.c_base_only { 0 } else { 1 }, "runs at {n}");
        // Audits: tiling clean; nothing dangling the source did not have at N.
        assert_eq!(
            scalar(&dest, "SELECT count(*) FROM audit_history_tiling"),
            0
        );
        assert_eq!(
            scalar(&dest, "SELECT count(*) FROM audit_axis_integrity"),
            0
        );
        assert_eq!(
            scalar(&dest, "SELECT count(*) FROM audit_array_id_coherence"),
            0
        );
        assert_eq!(
            scalar(&dest, "SELECT count(*) FROM audit_metadata_reserved_dup"),
            0
        );
        // Loud before E, exact at E.
        let e = r.extract_commit;
        let conn = Connection::open(&dest).unwrap();
        let err = conn
            .query_row(
                &format!("SELECT count(*) FROM value_array_asof({})", e - 1),
                [],
                |r| r.get::<_, i64>(0),
            )
            .expect_err("as-of before E must fail");
        assert!(err.to_string().contains("unreliable"), "{err}");
        conn.close().unwrap();
        // Live afterwards.
        write(
            &dest,
            "Node",
            "demand",
            "Base",
            scalar_batch(&["n1"], &[99.0]),
        );
        assert_eq!(
            scalar(&dest, "SELECT count(*) FROM audit_history_tiling"),
            0
        );
    }

    // As-of the head equals the head extract (same rows, same marker shape).
    let head = dir.path().join("head.duckdb");
    let at_head = dir.path().join("at_head.duckdb");
    let h = scalar(src, "SELECT max(id) FROM \"commit\"");
    extract(src, &head, &opts("tester")).unwrap();
    extract(src, &at_head, &asof("tester", h)).unwrap();
    for t in strings(src, ALL_TABLES_SQL) {
        if t == "commit" || t == "metadata" || t == "erasure_marker" {
            continue;
        }
        let q = format!("SELECT * FROM \"{t}\"");
        assert_eq!(fingerprint(&head, &q), fingerprint(&at_head, &q), "{t}");
    }
}

// --- T9: as-of refusals --------------------------------------------------------------

#[test]
fn t9_asof_refusals() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source(dir.path());
    let src = &fx.db;
    let dest = dir.path().join("asof.duckdb");
    let max = scalar(src, "SELECT max(id) FROM \"commit\"");

    assert_refused(extract(src, &dest, &asof("t", 0)), "not a commit id");
    assert_refused(
        extract(src, &dest, &asof("t", max + 100)),
        "not a commit id",
    );
    // The erase of runA happened after c_churned: the state there cannot be
    // reconstructed.
    assert_refused(
        extract(src, &dest, &asof("t", fx.c_churned)),
        "erased later",
    );
    // At the erase commit itself it is fine.
    extract(src, &dest, &asof("t", max)).expect("as-of the last commit");
    std::fs::remove_file(&dest).unwrap();
    let _ = std::fs::remove_file(nodal_db_core::compaction::baseline_path(&dest));

    // A history stub valid at N.
    let stub_db = dir.path().join("stub.duckdb");
    init_schema_with_default(&stub_db, Some("Base")).unwrap();
    write(
        &stub_db,
        "Node",
        "profile",
        "Base",
        array_batch("n1", 24, 0.0),
    );
    let c1 = scalar(&stub_db, "SELECT max(id) FROM \"commit\"");
    write_parameter(
        &stub_db,
        "Node",
        "profile",
        "Base",
        array_batch("n1", 24, 1.0),
        &[],
        WriteOptions {
            force_replace: true,
            retain_override: None,
            expect_commit: None,
            covered_writes: nodal_db_core::types::CoveredWrites::Refuse,
        },
        "t",
        "replace without keeping content",
    )
    .unwrap();
    let c2 = scalar(&stub_db, "SELECT max(id) FROM \"commit\"");
    assert_eq!(
        scalar(
            &stub_db,
            "SELECT count(*) FROM value_array_hist WHERE NOT content_retained"
        ),
        1
    );
    assert_refused(
        extract(&stub_db, &dest, &asof("t", c1)),
        "not retained for parameter(s) Node.profile",
    );
    extract(&stub_db, &dest, &asof("t", c2)).expect("the live version at c2 is fine");
    std::fs::remove_file(&dest).unwrap();
    let _ = std::fs::remove_file(nodal_db_core::compaction::baseline_path(&dest));

    // The schema_version valid at N differs (hand-edited history).
    let old_schema = dir.path().join("old_schema.duckdb");
    init_schema_with_default(&old_schema, Some("Base")).unwrap();
    write(
        &old_schema,
        "Node",
        "demand",
        "Base",
        scalar_batch(&["n1"], &[1.0]),
    );
    let c_old = scalar(&old_schema, "SELECT max(id) FROM \"commit\"");
    let conn = Connection::open(&old_schema).unwrap();
    for sql in [
        "INSERT INTO \"commit\" (user, message) VALUES ('t', 'schema row history')",
        "UPDATE metadata SET valid_to_commit = (SELECT max(id) FROM \"commit\") \
         WHERE lower(name) = 'schema_version' AND valid_to_commit IS NULL",
        "INSERT INTO metadata (id, name, value, valid_from_commit, version) \
         SELECT id, name, value, valid_to_commit, 2 FROM metadata \
         WHERE lower(name) = 'schema_version'",
        "UPDATE metadata SET value = '0.3-old' \
         WHERE lower(name) = 'schema_version' AND valid_to_commit IS NOT NULL",
    ] {
        conn.execute_batch(sql).unwrap();
    }
    conn.close().unwrap();
    let r = extract(&old_schema, &dest, &asof("t", c_old));
    assert!(
        matches!(&r, Err(DbError::SchemaMismatch { found, .. }) if found == "0.3-old"),
        "{r:?}"
    );
    assert_refused(r, "schema_version valid at commit");
    extract(&old_schema, &dest, &opts("t")).expect("the head extract is fine");
    std::fs::remove_file(&dest).unwrap();

    assert!(
        temps_in(dir.path()).is_empty(),
        "{:?}",
        temps_in(dir.path())
    );
}

// --- sequences vs the ids actually present ------------------------------------------

/// For every sequence-backed column: the sequence's next value is above every
/// id the file holds (for `seq_value_array` also the `_hist` ids), and
/// `array_id`s keep their odd parity. `duckdb_sequences()` reports a NULL
/// `last_value` for a sequence re-created with `START WITH n` and unused since,
/// so the next value is `last_value + increment_by`, else `start_value`.
fn assert_sequences_above_ids(db: &Path, label: &str) {
    let cols = strings(
        db,
        "SELECT table_name || '|' || column_name || '|' || \
                split_part(column_default, '''', 2) \
         FROM information_schema.columns \
         WHERE column_default LIKE 'nextval(%' AND table_catalog = current_database() \
         ORDER BY 1",
    );
    assert!(cols.len() >= 19, "{label}: {cols:?}");
    for c in cols {
        let parts: Vec<&str> = c.split('|').collect();
        let (table, column, seq) = (parts[0], parts[1], parts[2]);
        let next = scalar(
            db,
            &format!(
                "SELECT coalesce(last_value + increment_by, start_value) \
                 FROM duckdb_sequences() \
                 WHERE sequence_name = '{seq}' AND database_name = current_database()"
            ),
        );
        let mut max_sql = format!("SELECT coalesce(max(\"{column}\"), 0) FROM \"{table}\"");
        if seq == "seq_value_array" {
            max_sql = format!(
                "SELECT greatest(({max_sql}), \
                   (SELECT coalesce(max(array_id), 0) FROM value_array_hist))"
            );
            assert_eq!(next % 2, 1, "{label}: seq_value_array next {next} is even");
        }
        let max = scalar(db, &max_sql);
        assert!(
            next > max,
            "{label}: {seq} would re-issue: next {next} <= max({table}.{column}) {max}"
        );
    }
}

#[test]
fn sequences_stay_above_every_carried_id() {
    let dir = tempfile::tempdir().unwrap();
    let fx = churned_source(dir.path());
    assert_sequences_above_ids(&fx.db, "source");

    // Head extract of the source, then an extract of that extract (no write
    // in between: the first extract's sequences are fresh, unused since).
    let e1 = dir.path().join("e1.duckdb");
    extract(&fx.db, &e1, &opts("t")).expect("extract");
    assert_sequences_above_ids(&e1, "extract");
    let e2 = dir.path().join("e2.duckdb");
    extract(&e1, &e2, &opts("t")).expect("extract of an extract");
    assert_sequences_above_ids(&e2, "extract of an extract");

    // Compact the source twice with no write in between, then extract it.
    compact(&fx.db).expect("compact 1");
    compact(&fx.db).expect("compact 2");
    assert_sequences_above_ids(&fx.db, "compacted twice");
    let e3 = dir.path().join("e3.duckdb");
    extract(&fx.db, &e3, &opts("t")).expect("extract of the compacted source");
    assert_sequences_above_ids(&e3, "extract of a compacted source");

    // As of an early commit (the erase-free fixture: an as-of N extract
    // refuses past a later marker), then a write in every extract.
    let dir2 = tempfile::tempdir().unwrap();
    let fx2 = churned_source_with(dir2.path(), false);
    let e4 = dir2.path().join("e4.duckdb");
    extract(&fx2.db, &e4, &asof("t", fx2.c_initial)).expect("as-of extract");
    assert_sequences_above_ids(&e4, "as-of extract");
    for db in [&e2, &e3, &e4] {
        write(db, "Node", "profile", "Base", array_batch("n9", 6, 1.0));
        write(
            db,
            "Node",
            "demand",
            "fresh_alt",
            scalar_batch(&["n9"], &[9.0]),
        );
        assert_sequences_above_ids(db, "after a write");
    }
}
