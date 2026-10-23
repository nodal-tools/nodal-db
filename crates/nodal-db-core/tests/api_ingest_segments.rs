//! Multi-segment ingest (`api::ingest_segments`, specs/arrow_ipc_bulk_format.md
//! §9.2 OPEN-Q3-6): all segments run in ONE atomic, conflict-retried
//! transaction with ONE commit row — all-or-nothing.

use std::sync::Arc;

use duckdb::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{ingest_segments, read_parameter, Segment};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::WriteOptions;

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

/// Rank-1 label-indexed float batch WITHOUT any label domain: canonicalization
/// fails (MissingDomain) only inside the transaction, after other segments
/// already wrote — the roll-back probe.
fn undomained_label_batch(entity: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("level", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity])) as ArrayRef,
            Arc::new(StringArray::from(vec!["low"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
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

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("ingest.duckdb");
    // D3: seed `Base` explicitly (a fresh DB no longer bootstraps it).
    init_schema_with_default(&db, Some("Base")).expect("schema");
    (dir, db)
}

fn count(db: &std::path::Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

#[test]
fn two_segments_commit_under_one_commit_row() {
    let (_dir, db) = fresh_db();
    let report = ingest_segments(
        &db,
        vec![
            seg("node", "demand", "Base", scalar_batch("n1", 1.0)),
            seg("unit", "capacity", "Base", scalar_batch("u1", 2.0)),
        ],
        "tester",
        "two segments",
        WriteOptions::default(),
    )
    .expect("ingest");

    assert_eq!(report.values_written, 2);
    // Exactly one non-bootstrap commit; both writes stamped with it.
    assert_eq!(
        count(&db, "SELECT count(*) FROM commit"),
        2,
        "bootstrap + ONE ingest commit"
    );
    assert_eq!(
        count(
            &db,
            &format!(
                "SELECT count(DISTINCT valid_from_commit) FROM value_scalar \
                      WHERE valid_from_commit = {}",
                report.commit_id
            )
        ),
        1
    );
    assert_eq!(
        read_parameter(&db, "node", "demand", "Base")
            .unwrap()
            .num_rows(),
        1
    );
    assert_eq!(
        read_parameter(&db, "unit", "capacity", "Base")
            .unwrap()
            .num_rows(),
        1
    );
}

#[test]
fn failing_late_segment_rolls_back_the_whole_ingest() {
    let (_dir, db) = fresh_db();
    let err = ingest_segments(
        &db,
        vec![
            seg("node", "demand", "Base", scalar_batch("n1", 1.0)),
            // Fails at canonicalization (MissingDomain), i.e. mid-transaction,
            // after segment 1 already wrote.
            seg("node", "level_param", "Base", undomained_label_batch("n1")),
        ],
        "tester",
        "partial must not persist",
        WriteOptions::default(),
    )
    .expect_err("undomained label segment must fail the ingest");
    // The error names the failing segment (spec §9.1).
    let msg = err.to_string();
    assert!(
        msg.contains("segment 2") && msg.contains("level_param"),
        "unexpected: {msg}"
    );

    // Nothing persisted — not even segment 1's writes or auto-creates.
    assert_eq!(
        count(&db, "SELECT count(*) FROM entity_class"),
        0,
        "rolled back"
    );
    assert_eq!(
        count(&db, "SELECT count(*) FROM value_scalar"),
        0,
        "rolled back"
    );
    assert_eq!(
        count(&db, "SELECT count(*) FROM commit"),
        1,
        "bootstrap commit only"
    );
}

/// Scale pin (critique F4): one segment holding 1000 entities × 50 rows each
/// (50k rows) ingests correctly through the per-entity row-index lists —
/// O(rows) grouping, where the boolean masks these replaced were
/// O(entities × rows). A correctness-at-scale pin, not a benchmark.
///
/// Slow (~45 s debug): tiered out of the quick gate. Runs in `just full`
/// (`cargo test -- --include-ignored`) before a release.
#[ignore = "slow: pre-release tier; run via `just full` / --include-ignored"]
#[test]
fn thousand_entity_segment_ingests_correctly() {
    const ENTITIES: usize = 1000;
    const STEPS: usize = 50;
    let (_dir, db) = fresh_db();

    // Rank-1 int-indexed float rows, entity-major; cell value = e * STEPS + s.
    let mut ents: Vec<String> = Vec::with_capacity(ENTITIES * STEPS);
    let mut coords: Vec<i64> = Vec::with_capacity(ENTITIES * STEPS);
    let mut vals: Vec<f64> = Vec::with_capacity(ENTITIES * STEPS);
    for e in 0..ENTITIES {
        for s in 0..STEPS {
            ents.push(format!("unit_{e:04}"));
            coords.push(s as i64);
            vals.push((e * STEPS + s) as f64);
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("step", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ents)) as ArrayRef,
            Arc::new(Int64Array::from(coords)) as ArrayRef,
            Arc::new(Float64Array::from(vals)) as ArrayRef,
        ],
    )
    .unwrap();

    let report = ingest_segments(
        &db,
        vec![seg("unit", "flow", "Base", batch)],
        "tester",
        "scale pin",
        WriteOptions::default(),
    )
    .expect("ingest");
    assert_eq!(report.values_written, ENTITIES);

    let got = read_parameter(&db, "unit", "flow", "Base").expect("read");
    assert_eq!(got.num_rows(), ENTITIES * STEPS);

    // Spot-check two entities' cells by (idx1_num → v_float).
    let sch = got.schema();
    let ent = got
        .column(sch.index_of("entity").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let coord = got
        .column(sch.index_of("idx1_num").unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let val = got
        .column(sch.index_of("v_float").unwrap())
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for e in [0usize, ENTITIES - 1] {
        let target = format!("unit_{e:04}");
        let mut cells: Vec<(i64, f64)> = (0..got.num_rows())
            .filter(|&r| ent.value(r) == target)
            .map(|r| (coord.value(r), val.value(r)))
            .collect();
        cells.sort_by_key(|c| c.0);
        let want: Vec<(i64, f64)> = (0..STEPS)
            .map(|s| (s as i64, (e * STEPS + s) as f64))
            .collect();
        assert_eq!(cells, want, "entity {target}");
    }
}

#[test]
fn duplicate_case_aliased_target_is_rejected_before_db_work() {
    let (_dir, db) = fresh_db();
    let err = ingest_segments(
        &db,
        vec![
            seg("node", "Flow", "Base", scalar_batch("n1", 1.0)),
            seg("Node", "flow", "base", scalar_batch("n2", 2.0)),
        ],
        "tester",
        "dup targets",
        WriteOptions::default(),
    )
    .expect_err("case-aliased duplicate target must be rejected");
    assert!(
        err.to_string().contains("duplicate target"),
        "unexpected: {err}"
    );
    assert_eq!(
        count(&db, "SELECT count(*) FROM commit"),
        1,
        "no DB work happened"
    );
}

/// A missing database path is refused before any open: DuckDB's RW open would
/// otherwise create an empty file on a typo'd path (non-CLI callers too).
#[test]
fn ingest_into_missing_path_errors_without_creating_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("missing.duckdb");
    let err = ingest_segments(
        &db,
        vec![seg("node", "demand", "Base", scalar_batch("n1", 1.0))],
        "tester",
        "",
        WriteOptions::default(),
    )
    .unwrap_err();
    assert!(
        matches!(&err, nodal_db_core::error::DbError::Api(m) if m.contains("no database file")),
        "{err:?}"
    );
    assert!(!db.exists(), "no file may be created");
    assert!(!dir.path().join("missing.duckdb.wal").exists());
}

/// An existing DuckDB file that is not an initialized nodal-db database is a
/// clean API error naming the cause (not a raw Catalog Error), and the
/// rollback leaves the foreign file untouched.
#[test]
fn ingest_into_uninitialized_db_is_api_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("foreign.duckdb");
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch("CREATE TABLE x(i INT); INSERT INTO x VALUES (7);")
            .unwrap();
        conn.close().unwrap();
    }
    let err = ingest_segments(
        &db,
        vec![seg("node", "demand", "Base", scalar_batch("n1", 1.0))],
        "tester",
        "",
        WriteOptions::default(),
    )
    .unwrap_err();
    assert!(
        matches!(&err, nodal_db_core::error::DbError::Api(m) if m.contains("not an initialized")),
        "{err:?}"
    );
    assert_eq!(
        count(&db, "SELECT count(*) FROM x"),
        1,
        "foreign table intact"
    );
    assert_eq!(
        count(
            &db,
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'commit'"
        ),
        0,
        "no commit table created"
    );
}
