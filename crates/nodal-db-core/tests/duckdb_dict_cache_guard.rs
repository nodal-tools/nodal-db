//! Guard for the DuckDB 2.0 "dict-surviving cache" INTERNAL error and the
//! core's workaround (`debug_enable_caching_operators = false` on every core
//! connection, `conn::core_config`). See `docs/dev/engine_rules.md` §5 and
//! `specs/duckdb_dict_cache_bug_FINDINGS.md`.
//!
//! The bug: a join whose build side is emitted as a global dictionary (a
//! perfect hash join over dense ids), then `unnest` of a list longer than 2048
//! elements whose length is not a multiple of 2048, then a selective filter
//! (a caching operator) throws `INTERNAL Error: dict-surviving cache: global
//! dictionary column 0 changed identity …` and invalidates the database
//! instance. In nodal-db that is `read_parameter` on 8760-long array values
//! with a time-window or rare-value filter. No data is damaged.
//!
//! - [`raw_duckdb_still_has_the_bug`] is a deliberate TRIPWIRE: it asserts the
//!   bug is still present on the bundled DuckDB with caching operators on.
//!   When a DuckDB upgrade fixes it, this test fails and says so; then remove
//!   the workaround (see engine_rules.md §5).
//! - [`repro_passes_with_caching_operators_off`]: the setting exists on the
//!   bundled DuckDB and the minimal repro gives the right answer with it.
//! - [`core_connection_reads_filtered_arrays_correctly`] runs the nodal-db
//!   query shape through the core's own read connection on a fixture that
//!   triggers the bug on a raw connection, and checks the exact result; it
//!   also checks that the core's read and write connections carry the
//!   setting.

use std::path::Path;
use std::sync::Arc;

use nodal_db_core::api::write_parameter;
use nodal_db_core::conn::{core_config, with_read, with_write_txn};
use nodal_db_core::duckdb::arrow::array::{
    ArrayRef, Float64Array, StringArray, TimestampMicrosecondArray,
};
use nodal_db_core::duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use nodal_db_core::duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::duckdb::{AccessMode, Config, Connection, Error as DuckError};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::WriteOptions;

const FIXED_MSG: &str = "the DuckDB dict-cache bug appears fixed; remove the \
     debug_enable_caching_operators workaround (conn::core_config; see \
     docs/dev/engine_rules.md §5)";

/// The minimal repro (no nodal-db): 1000 dense keys, 8760-element lists.
const REPRO_SETUP: &str = "
    CREATE TABLE k AS SELECT i AS id, 1 AS a FROM range(1000) t(i);
    CREATE TABLE p AS SELECT i AS id, range(8760) AS l FROM range(1000) t(i);";
const REPRO_QUERY: &str = "SELECT count(*), sum(a) \
     FROM (SELECT k.a, unnest(p.l) AS v FROM p JOIN k ON k.id = p.id) \
     WHERE v % 977 = 0";

fn run_repro(conn: &Connection) -> Result<(i64, i64), DuckError> {
    conn.execute_batch(REPRO_SETUP).expect("repro setup");
    conn.query_row(REPRO_QUERY, [], |r| Ok((r.get(0)?, r.get(1)?)))
}

fn is_dict_cache_error(e: &DuckError) -> bool {
    let s = e.to_string();
    s.contains("INTERNAL") && s.contains("dict-surviving cache")
}

#[test]
fn raw_duckdb_still_has_the_bug() {
    let conn = Connection::open_in_memory().expect("open");
    match run_repro(&conn) {
        Err(e) if is_dict_cache_error(&e) => {} // still present: workaround needed
        Err(e) => panic!("the minimal repro failed with an unexpected error: {e}"),
        Ok(r) => panic!("{FIXED_MSG}. (The minimal repro returned {r:?} on a raw connection.)"),
    }
}

/// The same repro with the core's setting gives the right answer: in each
/// 0..8760 list, 0, 977, …, 7816 are the 9 multiples of 977.
#[test]
fn repro_passes_with_caching_operators_off() {
    let config = Config::default()
        .with("debug_enable_caching_operators", "false")
        .expect("setting exists on the bundled DuckDB");
    let conn = Connection::open_in_memory_with_flags(config).expect("open");
    assert_eq!(
        run_repro(&conn).expect("repro with caching off"),
        (9000, 9000)
    );
}

/// Number of 8760-long array values in the fixture. With this query shape
/// the bug needs more than 650 and at most 700 (measured on a release-build
/// DuckDB 2.0.0); 800 leaves a margin. The raw-connection check in the test
/// fails loudly if the fixture stops triggering it.
const N_ENTITIES: usize = 800;
const N_HOURS: i64 = 8760;
const HOUR_US: i64 = 3_600_000_000;
/// 2025-01-01T00:00:00Z in µs.
const T0_US: i64 = 1_735_689_600_000_000;

/// Cell value of entity `e` at hour `h`: exact in f64, unique per cell.
fn cell(e: usize, h: i64) -> f64 {
    (e as i64 * N_HOURS + h) as f64
}

/// A database holding `n_entities` hourly 8760-long array values of
/// `node.inflow` (entities `e0000`..). Entity `e0000` is written through the
/// core's pipeline; the others are cloned from it in SQL, with the cell
/// values shifted by `e * 8760`, because pushing millions of cells through a
/// debug-build pipeline takes tens of seconds. The clone keeps the shape the
/// bug needs: dense `array_id`s from `seq_value_array` (stride 2) and one
/// shared 8760-position time axis.
fn fixture(dir: &Path, n_entities: usize) -> std::path::PathBuf {
    let db = dir.join("dict_cache.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    let ts: Vec<i64> = (0..N_HOURS).map(|h| T0_US + h * HOUR_US).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["e0000"; N_HOURS as usize])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(ts)) as ArrayRef,
            Arc::new(Float64Array::from_iter_values(
                (0..N_HOURS).map(|h| cell(0, h)),
            )) as ArrayRef,
        ],
    )
    .unwrap();
    write_parameter(
        &db,
        "node",
        "inflow",
        "Base",
        batch,
        &[],
        WriteOptions::default(),
        "tester",
        "fixture",
    )
    .expect("write fixture");

    let conn = Connection::open_with_flags(&db, core_config(AccessMode::ReadWrite).unwrap())
        .expect("open RW");
    conn.execute_batch(&format!(
        "BEGIN;
         INSERT INTO entity (entity_class_id, name, valid_from_commit)
         SELECT entity_class_id, printf('e%04d', i), valid_from_commit
         FROM entity, range(1, {n_entities}) t(i) WHERE name = 'e0000'
         ORDER BY i;
         INSERT INTO value_array (entity_id, parameter_definition_id, alternative_id,
                                  parameter_type_id, valid_from_commit, version, axis1_id,
                                  values, v_min, v_max)
         SELECT s.id, va.parameter_definition_id, va.alternative_id, va.parameter_type_id,
                va.valid_from_commit, va.version, va.axis1_id,
                list_transform(va.values, lambda x: x + s.shift), va.v_min + s.shift,
                va.v_max + s.shift
         FROM value_array va,
              (SELECT id, CAST(substr(name, 2) AS BIGINT) * {N_HOURS} AS shift
               FROM entity WHERE name <> 'e0000') s
         ORDER BY s.id;
         COMMIT;"
    ))
    .expect("clone fixture rows");
    conn.close().expect("close");
    db
}

/// A six-hour window starting at hour `H0` (2025-03-01 00:00), through the
/// public `read_parameter` macro: the realistic trigger from the findings.
const H0: i64 = 59 * 24;
const WINDOW_QUERY: &str = "SELECT count(*), sum(v_float) \
     FROM read_parameter('node', 'inflow') \
     WHERE idx1_ts BETWEEN TIMESTAMP '2025-03-01 00:00' AND TIMESTAMP '2025-03-01 05:00'";

fn window_query(conn: &Connection) -> Result<(i64, f64), DuckError> {
    conn.query_row(WINDOW_QUERY, [], |r| Ok((r.get(0)?, r.get(1)?)))
}

#[test]
fn core_connection_reads_filtered_arrays_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path(), N_ENTITIES);

    // Non-vacuity: the fixture must trigger the bug on a raw connection.
    {
        let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
        let raw = Connection::open_with_flags(&db, config).expect("raw open");
        match window_query(&raw) {
            Err(e) if is_dict_cache_error(&e) => {}
            Err(e) => panic!("raw window query failed with an unexpected error: {e}"),
            Ok(r) => panic!(
                "the fixture no longer triggers the bug on a raw connection (got {r:?}): \
                 either {FIXED_MSG}, or the fixture/query shape changed and this test \
                 is now vacuous"
            ),
        }
        // The instance is invalidated; closing may report that. Drop it.
        let _ = raw.close();
    }

    let got = with_read(&db, |conn| Ok(window_query(conn)?)).expect("core read");
    let mut expect_sum = 0.0;
    for e in 0..N_ENTITIES {
        for h in H0..H0 + 6 {
            expect_sum += cell(e, h);
        }
    }
    assert_eq!(got, ((N_ENTITIES * 6) as i64, expect_sum));

    let setting = "SELECT current_setting('debug_enable_caching_operators')";
    let on_read: bool = with_read(&db, |c| Ok(c.query_row(setting, [], |r| r.get(0))?)).unwrap();
    let on_write: bool = with_write_txn(&db, "tester", "probe", |c, _| {
        Ok(c.query_row(setting, [], |r| r.get(0))?)
    })
    .unwrap();
    assert!(
        !on_read && !on_write,
        "core connections must disable caching operators"
    );
}
