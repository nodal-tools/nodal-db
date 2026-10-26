//! Axis-count independence and plan shape of the read paths
//! (axis_position_cost_PLAN §8 tests 2 and 3; docs/dev/engine_rules.md §7).
//!
//! No timing: `EXPLAIN ANALYZE` work counters (UNNEST output rows, CTE-scan
//! rows, hash-join build rows, rows read from the stored-axis child tables,
//! `value_long` rows scanned) for `read_parameter` of each tier and for
//! `value_rows` point reads, on a small core-built file, then again after
//! each of four batches of axes:
//!
//! 1. 200 unused parametric datetime axes x 2 000 positions;
//! 2. 200 unused stored (irregular) datetime axes x 2 000;
//! 3. 200 parametric axes x 2 000 in use by another parameter (2-cell long
//!    values);
//! 4. 200 stored axes x 2 000 in use by another parameter (an array value
//!    of 2 000 cells on each).
//!
//! The contract (D1-D3): after batches 1-3 every counter of every read grew
//! by at most a small multiple of the axis and value rows added, never by
//! the positions (400 000 per batch). Batch 4 is the shape where
//! `value_rows` is NOT axis-count independent: it hash-builds the stored
//! positions of every axis in use (plan §11 C2, R1); its growth is printed,
//! not asserted. Also pinned: `value_rows` evaluates its in-use axis set once
//! (`value_long` scanned at most 4x its current rows per call; the
//! per-dimension form scanned it ~7x).
//!
//! Test 3, the plan shape: no UNNEST that expands an axis (one whose subtree
//! scans `axis` and no `value_array`) in the plans of `value_rows`,
//! `read_parameter` or `resolved_value_rows`; `axis_position` itself has
//! one (the walker's non-vacuity check).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;
use serde_json::Value as Json;

use nodal_db_core::api::write_parameter;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::WriteOptions;

const T0_US: i64 = 1_735_689_600 * 1_000_000;
const HOUR_US: i64 = 3_600 * 1_000_000;
const STEPS: usize = 300;
const BATCH_AXES: i64 = 200;
const BATCH_LEN: i64 = 2_000;

fn ts_batch(names: &[String], float: bool, steps: usize) -> RecordBatch {
    let (mut e, mut t, mut vf, mut vi) = (vec![], vec![], vec![], vec![]);
    for (k, n) in names.iter().enumerate() {
        for s in 0..steps {
            e.push(n.clone());
            t.push(T0_US + s as i64 * HOUR_US);
            vf.push((k * 1000 + s) as f64 * 0.5);
            vi.push((k * 1000 + s) as i64);
        }
    }
    let (vfield, varr): (Field, ArrayRef) = if float {
        (
            Field::new("value", DataType::Float64, false),
            Arc::new(Float64Array::from(vf)),
        )
    } else {
        (
            Field::new("value", DataType::Int64, false),
            Arc::new(Int64Array::from(vi)),
        )
    };
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            vfield,
        ])),
        vec![
            Arc::new(StringArray::from(e)),
            Arc::new(TimestampMicrosecondArray::from(t)),
            varr,
        ],
    )
    .unwrap()
}

fn scalar_batch(names: &[String]) -> RecordBatch {
    let v: Vec<f64> = (0..names.len()).map(|i| i as f64).collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Float64Array::from(v)),
        ],
    )
    .unwrap()
}

fn write(db: &Path, class: &str, param: &str, b: RecordBatch) {
    write_parameter(
        db,
        class,
        param,
        "Base",
        b,
        &[],
        WriteOptions::default(),
        "tester",
        "scaling fixture",
    )
    .unwrap_or_else(|e| panic!("write {class}.{param}: {e}"));
}

fn names(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}{i:03}")).collect()
}

fn fixture(dir: &Path) -> PathBuf {
    let db = dir.join("scaling.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    write(
        &db,
        "bench_scalar",
        "p_scalar",
        scalar_batch(&names("s", 200)),
    );
    write(
        &db,
        "bench_long",
        "p_long",
        ts_batch(&names("l", 10), false, STEPS),
    );
    write(
        &db,
        "bench_array",
        "p_series",
        ts_batch(&names("a", 20), true, STEPS),
    );
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        "BEGIN;
         INSERT INTO commit (user, message) VALUES ('tester', 'scenario');
         INSERT INTO scenario (id, name, valid_from_commit)
           VALUES (nextval('seq_scenario'), 'scn', currval('seq_commit'));
         INSERT INTO scenario_alternative (scenario_id, alternative_id, rank, valid_from_commit)
           SELECT currval('seq_scenario'), id, 1, currval('seq_commit') FROM alternative;
         COMMIT; CHECKPOINT;",
    )
    .unwrap();
    conn.close().unwrap();
    db
}

/// Raw-SQL axis batches (test-only), each in its own commit.
fn add_batch(db: &Path, which: u8) {
    let (n, len) = (BATCH_AXES, BATCH_LEN);
    let body = match which {
        1 => format!(
            "INSERT INTO axis (id, kind, length, content_hash, p_start_ts, p_step_months, p_step_micros)
             SELECT nextval('seq_axis'), 'datetime', {len}, hash(i, 1),
                    TIMESTAMP '2030-01-01' + to_hours(i), 0, 3600000000
             FROM range({n}) t(i);"
        ),
        2 | 4 => format!(
            "CREATE TEMP TABLE nx AS SELECT nextval('seq_axis') AS id, i FROM range({n}) t(i);
             INSERT INTO axis (id, kind, length, content_hash)
               SELECT id, 'datetime', {len}, hash(i, {which}) FROM nx;
             INSERT INTO axis_datetime
               SELECT nx.id, p, TIMESTAMP '2031-01-01' + to_minutes(nx.i) + to_hours(p) + to_seconds(p % 7)
               FROM nx, range({len}) r(p);"
        ),
        3 => format!(
            "CREATE TEMP TABLE nx AS SELECT nextval('seq_axis') AS id, i FROM range({n}) t(i);
             INSERT INTO axis (id, kind, length, content_hash, p_start_ts, p_step_months, p_step_micros)
               SELECT id, 'datetime', {len}, hash(i, 3), TIMESTAMP '2032-01-01' + to_hours(i), 0, 3600000000
               FROM nx;"
        ),
        _ => unreachable!(),
    };
    // batches 3 and 4: the axes are in use by a parameter of their own
    let users = match which {
        3 => format!(
            "INSERT INTO entity (entity_class_id, name, valid_from_commit)
               SELECT ec.id, printf('o%03d', i), currval('seq_commit')
               FROM entity_class ec, range({n}) t(i) WHERE ec.name = 'bench_long';
             INSERT INTO parameter_definition (entity_class_id, name, valid_from_commit, version)
               SELECT id, 'p_other', currval('seq_commit'), 1 FROM entity_class WHERE name = 'bench_long';
             INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id, valid_from_commit,
                                     version, axis1_id, i1_pos, value_kind, v_float)
               SELECT e.id, (SELECT id FROM parameter_definition WHERE name = 'p_other'), 1,
                      currval('seq_commit'), 1, nx.id, 5 * nx.i % {len} + p, 'float', p
               FROM nx JOIN entity e ON e.name = printf('o%03d', nx.i), range(2) r(p);"
        ),
        4 => format!(
            "INSERT INTO entity (entity_class_id, name, valid_from_commit)
               SELECT ec.id, printf('i%03d', i), currval('seq_commit')
               FROM entity_class ec, range({n}) t(i) WHERE ec.name = 'bench_array';
             INSERT INTO parameter_definition (entity_class_id, name, valid_from_commit, version)
               SELECT id, 'p_irr', currval('seq_commit'), 1 FROM entity_class WHERE name = 'bench_array';
             INSERT INTO value_array (array_id, entity_id, parameter_definition_id, alternative_id,
                                      valid_from_commit, version, axis1_id, values)
               SELECT nextval('seq_value_array'), e.id, (SELECT id FROM parameter_definition WHERE name = 'p_irr'),
                      1, currval('seq_commit'), 1, nx.id, [x * 0.5 FOR x IN range({len})]
               FROM nx JOIN entity e ON e.name = printf('i%03d', nx.i) ORDER BY nx.i;"
        ),
        _ => String::new(),
    };
    let conn = Connection::open(db).unwrap();
    conn.execute_batch(&format!(
        "BEGIN; INSERT INTO commit (user, message) VALUES ('tester', 'axis batch {which}');
         {body} {users} COMMIT; CHECKPOINT;"
    ))
    .unwrap_or_else(|e| panic!("batch {which}: {e}"));
    conn.close().unwrap();
}

// --- plan counters -------------------------------------------------------------

fn children(node: &Json) -> &[Json] {
    node.get("children")
        .and_then(Json::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn node_type(node: &Json) -> &str {
    node.get("type").and_then(Json::as_str).unwrap_or("")
}

fn scan_table(node: &Json) -> Option<&str> {
    if node_type(node) != "TABLE_SCAN" {
        return None;
    }
    node["extra_info"]
        .get("Table")
        .and_then(Json::as_str)
        .and_then(|t| t.rsplit('.').next())
}

fn rows_out(node: &Json) -> u64 {
    node.get("operator_cardinality")
        .or_else(|| node.get("intermediate_rows"))
        .and_then(Json::as_u64)
        .unwrap_or(0)
}

fn walk<'a>(node: &'a Json, f: &mut impl FnMut(&'a Json)) {
    f(node);
    for c in children(node) {
        walk(c, f);
    }
}

fn roots(plan: &Json) -> Vec<Json> {
    plan.get("operator")
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_else(|| vec![plan.clone()])
}

fn explain(conn: &Connection, sql: &str) -> Json {
    let raw: String = conn
        .query_row(&format!("EXPLAIN (ANALYZE, FORMAT json) {sql}"), [], |r| {
            r.get(1)
        })
        .unwrap_or_else(|e| panic!("EXPLAIN failed for {sql}: {e}"));
    serde_json::from_str(&raw).expect("plan JSON")
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
struct Work {
    unnest: u64,
    cte_scan: u64,
    hash_build: u64,
    child_scan: u64,
    long_scan: u64,
}

const CHILD_TABLES: [&str; 6] = [
    "axis_datetime",
    "axis_integer",
    "axis_float",
    "axis_period",
    "axis_label",
    "axis_time_pattern",
];

fn work(conn: &Connection, sql: &str) -> Work {
    let mut w = Work::default();
    for r in roots(&explain(conn, sql)) {
        walk(&r, &mut |n| {
            let ty = node_type(n);
            if ty == "UNNEST" {
                w.unnest += rows_out(n);
            } else if ty == "CTE_SCAN" {
                w.cte_scan += rows_out(n);
            } else if ty == "HASH_JOIN" && children(n).len() == 2 {
                w.hash_build += rows_out(&children(n)[1]);
            }
            match scan_table(n) {
                Some(t) if CHILD_TABLES.contains(&t) => w.child_scan += rows_out(n),
                Some("value_long") => w.long_scan += rows_out(n),
                _ => {}
            }
        });
    }
    w
}

/// UNNESTs that expand an axis: their subtree scans `axis` and no
/// `value_array` (a payload unnest reads `value_array`; the in-use axis
/// sets unnest 4-element key lists over the tier scans, no `axis`).
fn axis_expansions(conn: &Connection, sql: &str) -> usize {
    let mut n = 0;
    for r in roots(&explain(conn, sql)) {
        walk(&r, &mut |u| {
            if node_type(u) != "UNNEST" {
                return;
            }
            let (mut axis, mut payload) = (false, false);
            walk(u, &mut |m| match scan_table(m) {
                Some("axis") => axis = true,
                Some("value_array") => payload = true,
                _ => {}
            });
            if axis && !payload {
                n += 1;
            }
        });
    }
    n
}

fn one_i64(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// The reads under test: (label, SQL, asserted constant through batch 4).
fn reads(conn: &Connection) -> Vec<(&'static str, String, bool)> {
    let key = |pd: &str, tbl: &str| {
        let (e, p): (i64, i64) = conn
            .query_row(
                &format!(
                    "SELECT min(t.entity_id), any_value(t.parameter_definition_id) FROM {tbl} t \
                     JOIN parameter_definition pd ON pd.id = t.parameter_definition_id \
                     WHERE pd.name = '{pd}'"
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        format!("entity_id = {e} AND parameter_definition_id = {p} AND alternative_id = 1")
    };
    vec![
        (
            "read_parameter scalar",
            "SELECT * FROM read_parameter('bench_scalar', 'p_scalar')".into(),
            false,
        ),
        (
            "read_parameter long",
            "SELECT * FROM read_parameter('bench_long', 'p_long')".into(),
            false,
        ),
        (
            "read_parameter array",
            "SELECT * FROM read_parameter('bench_array', 'p_series')".into(),
            false,
        ),
        (
            "value_rows point long",
            format!(
                "SELECT * FROM value_rows WHERE {}",
                key("p_long", "value_long")
            ),
            false,
        ),
        (
            "value_rows point scalar",
            format!(
                "SELECT * FROM value_rows WHERE {}",
                key("p_scalar", "value_scalar")
            ),
            false,
        ),
    ]
}

#[test]
fn read_work_does_not_grow_with_axis_positions() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let ver = {
        let c = Connection::open(&db).unwrap();
        let v: String = c.query_row("SELECT version()", [], |r| r.get(0)).unwrap();
        c.close().unwrap();
        v
    };
    let measure = |db: &Path| -> Vec<(&'static str, String, bool, Work)> {
        let conn = Connection::open(db).unwrap();
        let out = reads(&conn)
            .into_iter()
            .map(|(l, s, a)| {
                let w = work(&conn, &s);
                (l, s, a, w)
            })
            .collect();
        conn.close().unwrap();
        out
    };
    let current_long = |db: &Path| -> u64 {
        let conn = Connection::open(db).unwrap();
        let n = one_i64(
            &conn,
            "SELECT count(*) FROM value_long WHERE valid_to_commit IS NULL",
        );
        conn.close().unwrap();
        n as u64
    };
    let base = measure(&db);
    // value_rows evaluates its in-use axis set once (v3), not per dimension
    for (label, sql, _, w) in &base {
        if label.starts_with("value_rows") {
            let long = current_long(&db);
            assert!(
                w.long_scan <= 4 * long,
                "{label}: scanned {} value_long rows for {long} current rows: the in-use \
                 axis set is evaluated more than once per query (DuckDB {ver})\n{sql}",
                w.long_scan
            );
        }
        eprintln!("base   {label:26} {w:?}");
    }
    // per batch: the axes added and the value rows (not cells) added
    for (which, extra_rows) in [
        (1u8, 0u64),
        (2, 0),
        (3, 2 * BATCH_AXES as u64),
        (4, BATCH_AXES as u64),
    ] {
        let before = measure(&db);
        add_batch(&db, which);
        let after = measure(&db);
        let allowed = 8 * (BATCH_AXES as u64 + extra_rows) + 64;
        for ((label, sql, asserted4, b), (_, _, _, a)) in before.iter().zip(&after) {
            let grew = |x: u64, y: u64| y.saturating_sub(x);
            let g = [
                ("UNNEST rows", grew(b.unnest, a.unnest)),
                ("CTE-scan rows", grew(b.cte_scan, a.cte_scan)),
                ("hash-build rows", grew(b.hash_build, a.hash_build)),
                ("child-table rows", grew(b.child_scan, a.child_scan)),
                ("value_long rows", grew(b.long_scan, a.long_scan)),
            ];
            eprintln!("batch {which} {label:26} growth {g:?}");
            if which == 4 && !asserted4 {
                continue; // recorded, not asserted (plan §11 C2; R1)
            }
            for (what, d) in g {
                assert!(
                    d <= allowed,
                    "{label}: {what} grew by {d} after batch {which} ({BATCH_AXES} axes x \
                     {BATCH_LEN} positions; allowed {allowed}) on DuckDB {ver}. The read pays \
                     for axis positions it does not render: engine_rules §7.\nbefore {b:?}\n\
                     after  {a:?}\n{sql}"
                );
            }
        }
    }
}

#[test]
fn read_plans_never_expand_an_axis() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    add_batch(&db, 1);
    add_batch(&db, 2);
    let conn = Connection::open(&db).unwrap();
    let ver: String = conn
        .query_row("SELECT version()", [], |r| r.get(0))
        .unwrap();
    assert!(
        axis_expansions(&conn, "SELECT * FROM axis_position") > 0,
        "walker self-check: axis_position's expansion was not found (vacuous walker? DuckDB {ver})"
    );
    let mut sqls: Vec<String> = reads(&conn).into_iter().map(|(_, s, _)| s).collect();
    sqls.push("SELECT * FROM value_rows".into());
    sqls.push("SELECT * FROM value_rows WHERE idx1_ts > TIMESTAMP '2025-01-02'".into());
    sqls.push("SELECT * FROM resolved_value_rows('scn')".into());
    sqls.push(
        "SELECT * FROM read_parameter('bench_array', 'p_series') \
         WHERE idx1_ts BETWEEN TIMESTAMP '2025-01-01 02:00' AND TIMESTAMP '2025-01-01 05:00'"
            .into(),
    );
    for sql in &sqls {
        assert_eq!(
            axis_expansions(&conn, sql),
            0,
            "an UNNEST expands an axis in a read plan on DuckDB {ver}: the read pays for \
             every axis position in the file (engine_rules §7)\n{sql}"
        );
    }
    conn.close().unwrap();
}
