//! Plan-shape guard for the `array_id` surrogate key on `value_array`.
//!
//! The point of `array_id` is that DuckDB fetches a list payload through an ART
//! index scan only for a single-column key, and only while the key set reaching
//! the scan is an `IN` list (or `=`). Three engine behaviours make that fragile,
//! and every one is a SILENT fallback to a full payload scan (~100x slower on a
//! realistic table), never an error:
//!
//! - an exactly contiguous id set (`{500, 501}`) is rewritten into a two-sided
//!   range filter, which never uses the index — hence `seq_value_array`'s
//!   stride 2 (all ids share one parity, so no two are adjacent);
//! - a sequence re-created without `INCREMENT BY` (compaction did this before
//!   the fix) brings dense ids back;
//! - plain `EXPLAIN` always prints "Sequential Scan" (the choice is made at run
//!   time), so only `EXPLAIN ANALYZE` shows what happened.
//!
//! So this test runs `EXPLAIN (ANALYZE, FORMAT json)` over the guarded query
//! set on a small fixture (tiny fixtures make the same choices as 10^4-row
//! tables) and asserts that every `value_array` scan projecting a payload
//! column is an `Index Scan`. It is non-vacuous (each query must match at least
//! one scan) and carries a stride-1 negative control that must still fall back
//! — if the control ever gets an index scan, the engine stopped rewriting dense
//! sets and the stride may no longer be needed.
//!
//! Runs on every `cargo test`, so every DuckDB bump re-checks it.
//!
//! **Debug-build engine.** A DuckDB compiled with `DEBUG` (what `cargo test`
//! builds by default: libduckdb-sys mirrors cargo's debug setting) applies
//! compressed materialization to EVERY join regardless of cardinality, "so
//! that it is well-tested" (`compress_comparison_join.cpp`, `#ifndef DEBUG`).
//! The compressed join key turns the dynamic filter into a
//! `TRY_CAST(array_id AS TINYINT) IN (…)` expression, which no index can
//! serve. Release builds compress only when the build side is estimated at
//! 1 048 576 rows or more. So when `engine_forces_join_compression` detects the
//! debug behaviour, the guard connections disable that one optimizer, which
//! restores the release plan shape; `cargo test --release` runs the guard
//! with no override.

use std::path::Path;
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;
use serde_json::Value as Json;

use nodal_db_core::api::write_parameter;
use nodal_db_core::compaction::compact;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::WriteOptions;

const N_ENTITIES: usize = 40;
const N_STEPS: i64 = 8;

/// One batch of `N_ENTITIES` dense rank-1 float series (entities `e00`..),
/// written in entity order so the minted array_ids follow the stride.
fn fixture_batch(scale: f64) -> RecordBatch {
    let mut ent = Vec::new();
    let mut step = Vec::new();
    let mut val = Vec::new();
    for e in 0..N_ENTITIES {
        for s in 0..N_STEPS {
            ent.push(format!("e{e:02}"));
            step.push(s);
            val.push(scale * (e as f64 * 100.0 + s as f64));
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("step", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ent)) as ArrayRef,
            Arc::new(Int64Array::from(step)) as ArrayRef,
            Arc::new(Float64Array::from(val)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn write(db: &Path, scale: f64, msg: &str) {
    write_parameter(
        db,
        "node",
        "profile",
        "Base",
        fixture_batch(scale),
        &[],
        WriteOptions::default(),
        "tester",
        msg,
    )
    .expect("write fixture");
}

/// A fresh DB with 40 array values, each replaced once (so `_hist` holds a
/// pre-image per value).
fn fixture(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("guard.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    write(&db, 1.0, "v1");
    write(&db, 2.0, "v2");
    db
}

fn engine_version(conn: &Connection) -> String {
    conn.query_row("SELECT version()", [], |r| r.get(0))
        .unwrap_or_else(|_| "?".into())
}

/// `(scan type, projections)` for every TABLE_SCAN of `table` in an
/// `EXPLAIN (ANALYZE, FORMAT json)` plan. JSON shape (DuckDB 2.0): the root
/// object's `operator[]`, nodes keyed `type` with `children[]`, and
/// `extra_info.{Table, Type, Projections}`; `Table` is catalog-qualified and
/// `Projections` is a string for one column or a list.
fn table_scans(plan: &Json, table: &str) -> Vec<(String, Vec<String>)> {
    fn walk(node: &Json, table: &str, out: &mut Vec<(String, Vec<String>)>) {
        if node.get("type").and_then(Json::as_str) == Some("TABLE_SCAN") {
            let info = &node["extra_info"];
            let t = info.get("Table").and_then(Json::as_str).unwrap_or("");
            if t.rsplit('.').next() == Some(table) {
                let proj: Vec<String> = match info.get("Projections") {
                    Some(Json::String(s)) => s.lines().map(str::to_string).collect(),
                    Some(Json::Array(a)) => a
                        .iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect(),
                    _ => vec![],
                };
                let ty = info
                    .get("Type")
                    .and_then(Json::as_str)
                    .unwrap_or("<none>")
                    .to_string();
                out.push((ty, proj));
            }
        }
        if let Some(ch) = node.get("children").and_then(Json::as_array) {
            for c in ch {
                walk(c, table, out);
            }
        }
    }
    let mut out = Vec::new();
    let roots = plan
        .get("operator")
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_else(|| vec![plan.clone()]);
    for r in &roots {
        walk(r, table, &mut out);
    }
    out
}

/// Does this engine compress join keys even for a tiny build side (the
/// DEBUG-build behaviour described in the module doc)? Probed on a temp table.
fn engine_forces_join_compression(conn: &Connection) -> bool {
    conn.execute_batch(
        "CREATE TEMP TABLE cm_probe (id BIGINT PRIMARY KEY, e BIGINT); \
         INSERT INTO cm_probe SELECT 2 * i + 1, i FROM range(40) t(i);",
    )
    .unwrap();
    let plan = explain_raw(
        conn,
        "WITH k AS (SELECT id FROM cm_probe WHERE e = 10) \
         SELECT p.e FROM k JOIN cm_probe p ON p.id = k.id",
    );
    conn.execute_batch("DROP TABLE cm_probe").unwrap();
    plan.contains("TRY_CAST(")
}

/// Open a guard connection: on a DEBUG-build engine, disable compressed
/// materialization so the plan matches what a release engine chooses.
fn guard_conn(db: &Path) -> Connection {
    let conn = Connection::open(db).unwrap();
    if engine_forces_join_compression(&conn) {
        conn.execute_batch("SET disabled_optimizers = 'compressed_materialization'")
            .unwrap();
        assert!(
            !engine_forces_join_compression(&conn),
            "join-key compression still forced after disabling the optimizer"
        );
    }
    conn
}

fn explain_raw(conn: &Connection, sql: &str) -> String {
    conn.query_row(&format!("EXPLAIN (ANALYZE, FORMAT json) {sql}"), [], |r| {
        r.get(1)
    })
    .unwrap_or_else(|e| panic!("EXPLAIN ANALYZE failed for {sql}: {e}"))
}

fn explain(conn: &Connection, sql: &str) -> Json {
    serde_json::from_str(&explain_raw(conn, sql)).expect("plan JSON")
}

fn is_payload(proj: &[String]) -> bool {
    proj.iter().any(|p| p == "values" || p == "values_int")
}

/// Assert every payload-projecting scan of `table` is an index scan, and that
/// there is at least one (non-vacuity).
fn assert_payload_index_scan(conn: &Connection, table: &str, label: &str, sql: &str) {
    let scans: Vec<_> = table_scans(&explain(conn, sql), table)
        .into_iter()
        .filter(|(_, p)| is_payload(p))
        .collect();
    let ver = engine_version(conn);
    assert!(
        !scans.is_empty(),
        "{label}: no payload scan of {table} found in the plan (walker vacuous? \
         DuckDB {ver}; see the array_id surrogate-key design record)\n{sql}"
    );
    for (ty, proj) in &scans {
        assert_eq!(
            ty, "Index Scan",
            "{label}: payload scan of {table} {proj:?} fell back to {ty} on DuckDB \
             {ver} — array payload reads would silently scan the whole table. \
             Check the seq_value_array stride and the engine's IN/range rewrite \
             (array_id surrogate-key design record, F1/F4).\n{sql}"
        );
    }
}

/// Writer statements: their scans of `value_array` project no payload column,
/// so assert on every scan of the table, regardless of projections.
fn assert_all_scans_index(conn: &Connection, label: &str, sql: &str) {
    let scans = table_scans(&explain(conn, sql), "value_array");
    let ver = engine_version(conn);
    assert!(
        !scans.is_empty(),
        "{label}: no value_array scan found (DuckDB {ver})\n{sql}"
    );
    for (ty, proj) in &scans {
        assert_eq!(
            ty, "Index Scan",
            "{label}: value_array scan {proj:?} is {ty} on DuckDB {ver}\n{sql}"
        );
    }
}

fn ids(conn: &Connection, sql: &str) -> Vec<i64> {
    conn.prepare(sql)
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn entity_id(conn: &Connection, name: &str) -> i64 {
    conn.query_row(
        "SELECT id FROM entity WHERE name = ? AND valid_to_commit IS NULL",
        [name],
        |r| r.get(0),
    )
    .unwrap()
}

/// The read-side guarded queries (1–4 of the design's query set).
fn assert_read_queries(conn: &Connection, phase: &str) {
    let e10 = entity_id(conn, "e10");
    let e11 = entity_id(conn, "e11");
    assert_eq!(e11, e10 + 1, "fixture entities are contiguous");
    let twenty: Vec<String> = (10..30)
        .map(|i| entity_id(conn, &format!("e{i:02}")).to_string())
        .collect();
    let twenty = twenty.join(", ");
    let (pd, alt): (i64, i64) = conn
        .query_row(
            "SELECT parameter_definition_id, alternative_id FROM value_array LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();

    let dense = "SELECT values FROM read_parameter_dense('node', 'profile')";
    assert_payload_index_scan(
        conn,
        "value_array",
        &format!("{phase} q1 dense, one entity"),
        &format!("{dense} WHERE entity_id = {e10}"),
    );
    assert_payload_index_scan(
        conn,
        "value_array",
        &format!("{phase} q2 dense, 2 adjacent entities (F1 tripwire)"),
        &format!("{dense} WHERE entity_id IN ({e10}, {e11})"),
    );
    assert_payload_index_scan(
        conn,
        "value_array",
        &format!("{phase} q2 dense, 20 adjacent entities (F1 tripwire)"),
        &format!("{dense} WHERE entity_id IN ({twenty})"),
    );
    assert_payload_index_scan(
        conn,
        "value_array",
        &format!("{phase} q3 dense joined to entity by name"),
        "SELECT d.values FROM read_parameter_dense('node', 'profile') d \
         JOIN entity e ON e.id = d.entity_id AND e.valid_to_commit IS NULL \
         WHERE e.name = 'e10'",
    );
    assert_payload_index_scan(
        conn,
        "value_array",
        &format!("{phase} q4 value_cells by triple"),
        &format!(
            "SELECT v_float FROM value_cells WHERE entity_id = {e10} \
             AND parameter_definition_id = {pd} AND alternative_id = {alt}"
        ),
    );
}

#[test]
fn array_payload_reads_and_writer_statements_use_the_array_id_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = guard_conn(&db);

    assert_eq!(
        ids(&conn, "SELECT count(*) FROM value_array")[0],
        N_ENTITIES as i64
    );
    assert_read_queries(&conn, "fresh");

    // q5: the writer's payload-touching statements, keyed by array_id. EXPLAIN
    // ANALYZE executes them, so run inside a transaction that is rolled back.
    let aid: i64 = conn
        .query_row(
            "SELECT array_id FROM value_array va JOIN entity e ON e.id = va.entity_id \
             WHERE e.name = 'e10'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let c: i64 = conn
        .query_row("SELECT max(id) FROM commit", [], |r| r.get(0))
        .unwrap();
    // q5a: the unchanged check's payload fetch (skip_unchanged_writes_PLAN
    // §3.2), the production statement with the bound id list inlined: one
    // id and two neighbouring ones (the stride's F1 tripwire).
    let aid11: i64 = conn
        .query_row(
            "SELECT array_id FROM value_array va JOIN entity e ON e.id = va.entity_id \
             WHERE e.name = 'e11'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    for ids in [format!("[{aid}]"), format!("[{aid}, {aid11}]")] {
        assert_payload_index_scan(
            &conn,
            "value_array",
            &format!("q5a unchanged-check payload fetch {ids}"),
            &nodal_db_core::history::ARRAY_PAYLOAD_SQL.replace('?', &ids),
        );
    }
    conn.execute_batch("BEGIN").unwrap();
    assert_payload_index_scan(
        &conn,
        "value_array",
        "q5 writer pre-image copy",
        &format!(
            "INSERT INTO value_array_hist SELECT *, {c} + 1, TRUE FROM value_array \
             WHERE array_id = {aid}"
        ),
    );
    assert_all_scans_index(
        &conn,
        "q5 writer in-place UPDATE",
        &format!(
            "UPDATE value_array SET version = version + 1, values = [1.0, 2.0], \
             values_int = NULL WHERE array_id = {aid}"
        ),
    );
    assert_all_scans_index(
        &conn,
        "q5 writer DELETE",
        &format!("DELETE FROM value_array WHERE array_id = {aid}"),
    );
    conn.execute_batch("ROLLBACK").unwrap();
    conn.close().unwrap();

    // q6: after compaction — the stride survives the sequence restore (F10),
    // ids are copied verbatim and stay odd, and reads still use the index.
    compact(&db).expect("compact");
    write(&db, 3.0, "after compaction"); // replaces through the restored writer path
    let conn = guard_conn(&db);
    assert_read_queries(&conn, "after compaction");
    let even = ids(
        &conn,
        "SELECT array_id FROM value_array WHERE array_id % 2 = 0 \
         UNION ALL SELECT array_id FROM value_array_hist WHERE array_id % 2 = 0",
    );
    assert!(
        even.is_empty(),
        "even array_ids after compaction break the parity: {even:?}"
    );
    let inc = ids(
        &conn,
        "SELECT increment_by FROM duckdb_sequences() WHERE sequence_name = 'seq_value_array'",
    );
    assert_eq!(
        inc,
        vec![2],
        "seq_value_array lost its stride across compaction"
    );
    let a = ids(&conn, "SELECT nextval('seq_value_array')")[0];
    let b = ids(&conn, "SELECT nextval('seq_value_array')")[0];
    let max_id = ids(
        &conn,
        "SELECT max(array_id) FROM (SELECT array_id FROM value_array \
         UNION ALL SELECT array_id FROM value_array_hist)",
    )[0];
    assert!(
        a % 2 == 1 && b == a + 2 && a > max_id,
        "nextval after compaction: {a}, {b} (max id {max_id})"
    );
    conn.close().unwrap();
}

/// Negative control: the same data with stride-1 (dense) ids must still fall
/// back to a sequential payload scan for two adjacent keys. Proves the walker
/// can see a fallback; if this ever gets an index scan, DuckDB stopped
/// rewriting exactly contiguous IN sets into range filters.
#[test]
fn stride_one_negative_control_still_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = guard_conn(&db);
    conn.execute_batch(
        "CREATE TABLE va_dense AS \
           SELECT row_number() OVER (ORDER BY array_id) AS array_id, \
                  * EXCLUDE (array_id) FROM value_array; \
         ALTER TABLE va_dense ADD PRIMARY KEY (array_id);",
    )
    .unwrap();
    // Two adjacent ids in the MIDDLE of the table (a set touching the table's
    // min/max id can leave a one-sided range, which does get an index scan).
    let sql = "WITH k AS (SELECT array_id FROM va_dense WHERE array_id IN (10, 11)) \
               SELECT p.values FROM k JOIN va_dense p ON p.array_id = k.array_id";
    let scans: Vec<_> = table_scans(&explain(&conn, sql), "va_dense")
        .into_iter()
        .filter(|(_, p)| is_payload(p))
        .collect();
    let ver = engine_version(&conn);
    assert!(
        !scans.is_empty(),
        "control: no payload scan found (walker vacuous? DuckDB {ver})"
    );
    assert!(
        scans.iter().all(|(ty, _)| ty != "Index Scan"),
        "control: a stride-1 contiguous key set got an INDEX scan on DuckDB {ver}: \
         {scans:?}. The engine no longer rewrites dense IN sets into ranges, so the \
         seq_value_array stride (2) may no longer be needed — re-evaluate decision \
         D2 of the array_id surrogate-key design record rather than deleting this test."
    );
    // And the same shape with a gap is an index scan, so the control isolates
    // the contiguity (not the CTAS table or the self-join form).
    assert_payload_index_scan(
        &conn,
        "va_dense",
        "control with a gap",
        "WITH k AS (SELECT array_id FROM va_dense WHERE array_id IN (10, 12)) \
         SELECT p.values FROM k JOIN va_dense p ON p.array_id = k.array_id",
    );
    conn.close().unwrap();
}
