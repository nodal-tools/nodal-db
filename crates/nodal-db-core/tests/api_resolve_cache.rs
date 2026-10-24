//! The per-commit class / parameter / alternative resolution cache of the
//! write path's segment phase (set_based_long_and_delete_PLAN §10 C11).
//!
//! The cache must be invisible: a multi-segment commit writes the same rows
//! and returns the same report with and without it (the test hook
//! `without_resolve_cache` is the reference arm), errors included. It lives
//! in one transaction attempt, so a conflict retry, which rolls back the ids
//! the first attempt minted, rebuilds it from scratch.
//!
//! The hooks exist in debug builds only, hence the file-level cfg.
#![cfg(debug_assertions)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::test_hooks::{inject_conflicts, without_resolve_cache};
use nodal_db_core::api::{commit_changes, ingest_segments, DeleteKey, Segment};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{StructuralChange, WarningKind, WriteOptions, WriteReport};

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

/// A rank-1 integer-indexed series of `n` points per entity.
fn series(entities: &[&str], n: i64, v: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let mut es = Vec::new();
    let mut ts = Vec::new();
    let mut vs = Vec::new();
    for (k, e) in entities.iter().enumerate() {
        for t in 0..n {
            es.push(*e);
            ts.push(t);
            vs.push(v + (k as i64 * n + t) as f64 * 0.5);
        }
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(es)) as ArrayRef,
            Arc::new(Int64Array::from(ts)) as ArrayRef,
            Arc::new(Float64Array::from(vs)) as ArrayRef,
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

fn fresh_db(dir: &Path, name: &str) -> PathBuf {
    let db = dir.join(name);
    init_schema_with_default(&db, Some("Base")).expect("schema");
    db
}

/// Every row of every base table, rendered by DuckDB and sorted: the
/// commit log without its wall-clock `timestamp`, `metadata` without the
/// per-file `db_identity` row. Both arms run on one engine, so `::VARCHAR`
/// rendering is a sound comparison here.
fn dump(db: &Path) -> String {
    let conn = Connection::open(db).unwrap();
    let tables: Vec<String> = conn
        .prepare(
            "SELECT table_name FROM duckdb_tables() \
             WHERE database_name = current_database() AND schema_name = 'main' \
             ORDER BY table_name",
        )
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let mut out = String::new();
    for t in tables {
        let src = match t.as_str() {
            "commit" => "(SELECT * EXCLUDE (timestamp) FROM commit)".to_string(),
            "metadata" => "(SELECT * FROM metadata WHERE lower(name) <> 'db_identity')".to_string(),
            _ => format!("\"{t}\""),
        };
        let rows: Option<String> = conn
            .query_row(
                &format!(
                    "SELECT string_agg(r, chr(10) ORDER BY r) \
                     FROM (SELECT CAST(x AS VARCHAR) AS r FROM {src} x)"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        out.push_str(&format!("== {t}\n{}\n", rows.unwrap_or_default()));
    }
    conn.close().unwrap();
    out
}

/// A seeding commit, then a 14-segment commit that repeats classes,
/// alternatives and parameters under several spellings, auto-creates a
/// class, definitions and alternatives mid-commit (each then reused by
/// later segments), mixes scalar and series values, and runs a structural
/// change and a delete in the same save.
fn workload(db: &Path) -> Vec<Result<WriteReport, String>> {
    let seed = ingest_segments(
        db,
        vec![
            seg("unit", "capacity", "Base", scalar(&["u1", "u2"], 10.0)),
            seg("unit", "profile", "Base", series(&["u1"], 30, 1.0)),
            seg("node", "demand", "Base", scalar(&["n1", "n2"], 5.0)),
        ],
        "t",
        "seed",
        WriteOptions::default(),
    )
    .map_err(|e| e.to_string());
    let save = commit_changes(
        db,
        vec![
            seg("unit", "capacity", "alt_a", scalar(&["u1", "u3"], 11.0)),
            seg("unit", "profile", "alt_a", series(&["u1", "u2"], 30, 2.0)),
            seg("Unit", "capacity", "Base", scalar(&["u2"], 12.0)),
            seg("unit", "Profile", "alt_b", series(&["u3"], 30, 3.0)),
            seg("unit", "profile", "ALT_c", series(&["u2"], 30, 4.0)),
            seg("unit", "capacity", "Alt_A2", scalar(&["u1"], 13.0)),
            seg("unit", "capacity", "alt_b", scalar(&["u1"], 14.0)),
            seg("node", "demand", "alt_a", scalar(&["n1"], 6.0)),
            seg("node", "price", "alt_a", scalar(&["n1", "n3"], 7.0)),
            seg("node", "price", "Base", scalar(&["n2"], 8.0)),
            seg("Node", "price", "alt_b", scalar(&["n2"], 9.0)),
            seg("storage", "level", "alt_a", series(&["s1"], 30, 5.0)),
            seg("storage", "level", "Base", series(&["s1", "s2"], 30, 6.0)),
            seg("storage", "cap", "alt_b", scalar(&["s1"], 1.0)),
        ],
        vec![StructuralChange::CreateAlternative {
            name: "alt_b".into(),
            description: None,
        }],
        vec![DeleteKey {
            class: "node".into(),
            parameter: "demand".into(),
            alternative: "Base".into(),
            entity: "n2".into(),
        }],
        vec![],
        "t",
        "the multi-segment save",
        WriteOptions::default(),
    )
    .map_err(|e| e.to_string());
    // A cached definition whose later segment's batch matches none of its
    // declared members: the same error, with or without the cache.
    let mismatch = ingest_segments(
        db,
        vec![
            seg("unit", "capacity", "alt_a", scalar(&["u1"], 20.0)),
            seg("unit", "capacity", "Base", series(&["u1"], 3, 1.0)),
        ],
        "t",
        "a shape mismatch on a cached definition",
        WriteOptions::default(),
    )
    .map_err(|e| e.to_string());
    vec![seed, save, mismatch]
}

#[test]
fn multi_segment_commit_is_identical_with_and_without_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let cached = fresh_db(dir.path(), "cached.duckdb");
    let reference = fresh_db(dir.path(), "reference.duckdb");

    let with_cache = workload(&cached);
    let without = without_resolve_cache(|| workload(&reference));

    assert_eq!(with_cache, without, "reports and errors");
    assert!(with_cache[0].is_ok() && with_cache[1].is_ok());
    assert!(
        with_cache[2]
            .as_ref()
            .is_err_and(|e| e.contains("instantiates none")),
        "{:?}",
        with_cache[2]
    );
    // The cached arm auto-created each new referent once.
    let report = with_cache[1].as_ref().unwrap();
    let created = |ty: &str| {
        report
            .warnings
            .iter()
            .filter(|w| w.kind == WarningKind::AutoCreated && w.object_type == ty)
            .count()
    };
    assert_eq!(created("alternative"), 3, "alt_a, ALT_c, Alt_A2");
    assert_eq!(created("entity_class"), 1, "storage");
    assert_eq!(created("parameter_definition"), 3, "price, level, cap");
    assert_eq!(dump(&cached), dump(&reference), "every table");
}

#[test]
fn a_conflict_retry_rebuilds_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let retried = fresh_db(dir.path(), "retried.duckdb");
    let reference = fresh_db(dir.path(), "reference.duckdb");
    // Every referent below is auto-created by the first segment that names
    // it and reused by the later ones. The first attempt mints ids and
    // rolls back; sequences are not transactional, so the retry mints new
    // ids, and a cache carried over from the first attempt would hand the
    // later segments dead ones (a foreign-key failure or a value on a
    // rolled-back referent).
    let segments = || {
        vec![
            seg("grid", "flow", "alt_x", scalar(&["g1"], 1.0)),
            seg("grid", "flow", "alt_y", scalar(&["g1"], 2.0)),
            seg("grid", "loss", "alt_x", series(&["g1"], 30, 3.0)),
            seg("grid", "loss", "alt_y", series(&["g2"], 30, 4.0)),
        ]
    };
    let report_ref = ingest_segments(&reference, segments(), "t", "m", WriteOptions::default())
        .expect("reference");

    inject_conflicts(1);
    let report = ingest_segments(&retried, segments(), "t", "m", WriteOptions::default())
        .expect("the retry commits");
    inject_conflicts(0);

    // The report is the retry's own: one AutoCreated per referent, not the
    // first attempt's carried over. Ids may differ from the reference (the
    // rolled-back attempt consumed sequence values); they are checked
    // against the rows below.
    let names = |r: &WriteReport| {
        r.warnings
            .iter()
            .map(|w| (w.object_type.clone(), w.name.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(names(&report), names(&report_ref));
    assert_eq!(report.values_written, report_ref.values_written);

    // Every referent a value row points at is a current row, and the
    // reported ids are those rows.
    let conn = Connection::open(&retried).unwrap();
    for w in &report.warnings {
        let table = w.object_type.as_str();
        let n: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM {table} WHERE id = ? AND lower(name) = lower(?) \
                     AND valid_to_commit IS NULL"
                ),
                duckdb::params![w.id, w.name],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "{table} {} id {}", w.name, w.id);
    }
    let orphans: i64 = conn
        .query_row(
            "SELECT (SELECT count(*) FROM value_scalar v WHERE NOT EXISTS \
                       (SELECT 1 FROM alternative a WHERE a.id = v.alternative_id \
                          AND a.valid_to_commit IS NULL)) \
                  + (SELECT count(*) FROM value_array v WHERE NOT EXISTS \
                       (SELECT 1 FROM alternative a WHERE a.id = v.alternative_id \
                          AND a.valid_to_commit IS NULL)) \
                  + (SELECT count(*) FROM value_long v WHERE NOT EXISTS \
                       (SELECT 1 FROM alternative a WHERE a.id = v.alternative_id \
                          AND a.valid_to_commit IS NULL))",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(orphans, 0);
    conn.close().unwrap();
}

#[test]
fn injected_conflicts_exhaust_the_retry_budget() {
    // The hook drives the real retry loop: five injected conflicts are the
    // whole budget.
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(dir.path(), "exhaust.duckdb");
    inject_conflicts(5);
    let err = ingest_segments(
        &db,
        vec![seg("grid", "flow", "Base", scalar(&["g1"], 1.0))],
        "t",
        "m",
        WriteOptions::default(),
    )
    .expect_err("the budget runs out");
    inject_conflicts(0);
    assert!(matches!(err, DbError::Conflict { attempts: 5 }), "{err}");
}
