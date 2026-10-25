//! Plan-shape guards for the point-read macros `read_value`, `read_values`
//! and `value_rows_for` (`schema/080_views_read.sql`;
//! `docs/dev/engine_rules.md` §2 and §6).
//!
//! The macros fetch array payloads through ONE scalar key list,
//! `array_id = ANY(coalesce((SELECT list(DISTINCT array_id) …), [-1]))`.
//! DuckDB plans that as a SEMI join of the payload scan against the unnested
//! list with a dynamic IN filter, which it serves by an index scan for up to
//! 50 keys (`dynamic_or_filter_threshold`). Every part of that is optimizer
//! behaviour, not a documented contract, and every failure is SILENT (a full
//! payload scan, or the DuckDB 2.0 dict-cache error coming back), so:
//!
//! - **Index scans.** `EXPLAIN (ANALYZE, FORMAT json)` over the production
//!   macros on a realistic, core-built fixture (two classes sharing entity and
//!   parameter names, three alternatives, closed-out entity and value rows,
//!   scalar, long and array parameters, rank 1 and rank 2 arrays, more than 50
//!   unused axes): every `value_array` scan that projects a payload column is
//!   an `Index Scan`, for a hit, 0 keys (scalar, long, miss, wrong class),
//!   20 and 50 keys, 50 ids each listed twice, the omitted-alternative
//!   default, and the point wrapper under a time-window filter.
//! - **Negative control.** 51 keys must still be a sequential scan: it
//!   documents the cliff and proves the fixture can produce a fallback. If it
//!   flips, the 50-key limit in the docs can be revisited (array_id plan D15).
//! - **The `ap` pre-filter** (critique M4): `axis_position` is evaluated only
//!   for the read value's own axes. That works through a runtime join filter
//!   pushed into the `axis` scans, so the axis UNNESTs must emit no more rows
//!   than the value's axes hold.
//! - **No build-side column through an UNNEST** (engine_rules §6): below every
//!   UNNEST, the only joins on the column path are SEMI / ANTI / MARK (whose
//!   output is their probe side). A self-check confirms the walker flags
//!   `value_array_cells`, whose self-join is the bug shape.
//! - **The dict-cache data guard**: on a fixture with 1 000 arrays of 8 760
//!   cells and dense ids, a RAW DuckDB connection with default settings (caching
//!   operators on, NOT the core's open) runs the macros under the two
//!   failing filters and gets the exact counts, while `read_parameter`
//!   under the same filter still raises the INTERNAL error (the positive
//!   control; if it stops raising, the fixture no longer triggers the bug).
//! - **The canary**: the minimal repro of the bug still raises on the
//!   bundled engine. When an upgrade fixes it, this test fails on purpose:
//!   the engine_rules §6 rule and the docs' workaround note can be revisited.
//!
//! **Debug-build engine.** As in `array_id_index_scan.rs`: a DEBUG DuckDB
//! compresses every join key, which no index can serve, so the guard
//! connection disables `compressed_materialization` when the engine forces
//! it. `cargo test --release` (from `just full`) runs with no override.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;
use serde_json::Value as Json;

use duckdb::types::Value;
use nodal_db_core::api::{delete_objects, write_parameter, READ_VALUES_SQL};
use nodal_db_core::canon::LabelDomain;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, WriteOptions,
};

const N_ENT: usize = 60;
const STEPS: usize = 8;
const ALTS: [&str; 3] = ["Base", "alt1", "alt2"];
const T0_US: i64 = 1_735_689_600 * 1_000_000;
const HOUR_US: i64 = 3_600 * 1_000_000;

// --- fixture -----------------------------------------------------------------

fn ent(i: usize) -> String {
    format!("n{i:02}")
}

/// Rank-1 datetime series: `names` x `STEPS` hours from `start_h`, per entity.
fn series(names: &[String], start_h: impl Fn(usize) -> i64, scale: f64) -> RecordBatch {
    let mut e = Vec::new();
    let mut t = Vec::new();
    let mut v = Vec::new();
    for (k, name) in names.iter().enumerate() {
        for s in 0..STEPS {
            e.push(name.clone());
            t.push(T0_US + (start_h(k) + s as i64) * HOUR_US);
            v.push(scale * (k * 100 + s) as f64);
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(e)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(t)) as ArrayRef,
            Arc::new(Float64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Rank-2 (label x datetime) series for `names`.
fn rank2(names: &[String]) -> RecordBatch {
    let mut e = Vec::new();
    let mut l = Vec::new();
    let mut t = Vec::new();
    let mut v = Vec::new();
    for (k, name) in names.iter().enumerate() {
        for (j, lab) in ["x", "y", "z"].iter().enumerate() {
            for s in 0..STEPS {
                e.push(name.clone());
                l.push(lab.to_string());
                t.push(T0_US + s as i64 * HOUR_US);
                v.push((k * 1000 + j * 10 + s) as f64);
            }
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("lab", DataType::Utf8, false),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(e)) as ArrayRef,
            Arc::new(StringArray::from(l)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(t)) as ArrayRef,
            Arc::new(Float64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn scalars(names: &[String], base: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let v: Vec<f64> = (0..names.len()).map(|i| base + i as f64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Ragged int-measure series: lands in the long tier.
fn ragged(names: &[String]) -> RecordBatch {
    let mut e = Vec::new();
    let mut t = Vec::new();
    let mut v = Vec::new();
    for (k, name) in names.iter().enumerate() {
        for c in [0i64, 2, 5] {
            e.push(name.clone());
            t.push(c);
            v.push(k as i64 * 10 + c);
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(e)) as ArrayRef,
            Arc::new(Int64Array::from(t)) as ArrayRef,
            Arc::new(Int64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn write(db: &Path, class: &str, param: &str, alt: &str, b: RecordBatch, dom: &[LabelDomain]) {
    write_parameter(
        db,
        class,
        param,
        alt,
        b,
        dom,
        WriteOptions::default(),
        "tester",
        "point-read fixture",
    )
    .unwrap_or_else(|e| panic!("write {class}.{param}/{alt}: {e}"));
}

/// The realistic fixture (see the module doc), built through the core.
fn fixture(dir: &Path) -> PathBuf {
    let db = dir.join("points.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    let names: Vec<String> = (0..N_ENT).map(ent).collect();
    for (k, alt) in ALTS.iter().enumerate() {
        // 60 rank-1 arrays per alternative, on one shared axis.
        write(
            &db,
            "node",
            "profile",
            alt,
            series(&names, |_| 0, 1.0 + k as f64),
            &[],
        );
        write(
            &db,
            "node",
            "cap",
            alt,
            scalars(&names, 10.0 * k as f64),
            &[],
        );
        write(&db, "node", "ragged", alt, ragged(&names[..10]), &[]);
    }
    // Closed-out value rows: every Base array replaced once (value_array_hist).
    write(
        &db,
        "node",
        "profile",
        "Base",
        series(&names, |_| 0, 7.0),
        &[],
    );
    // Rank 2 (label x datetime) for a few entities.
    let dom = vec![LabelDomain {
        position: 1,
        labels: vec!["x".into(), "y".into(), "z".into()],
    }];
    write(&db, "node", "grid", "Base", rank2(&names[..5]), &dom);
    // More than 50 axes no point read here touches: one start per entity.
    write(
        &db,
        "node",
        "shifted",
        "Base",
        series(&names, |k| 100 + k as i64 * 24, 1.0),
        &[],
    );
    // Another class with the same entity and parameter names.
    write(
        &db,
        "unit",
        "profile",
        "Base",
        series(&names[..5], |_| 0, -1.0),
        &[],
    );
    // A closed-out entity: n59 deleted with its values.
    let rep = delete_objects(
        &db,
        vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Entity,
                class: Some("node".into()),
                target: ObjectRef::Name(ent(N_ENT - 1)),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        "tester",
        "delete n59",
        WriteOptions::default(),
    )
    .expect("delete entity");
    assert!(rep.commit_id > 0, "the entity delete committed");
    db
}

// --- plan helpers (as in array_id_index_scan.rs) -------------------------------

fn engine_version(conn: &Connection) -> String {
    conn.query_row("SELECT version()", [], |r| r.get(0))
        .unwrap_or_else(|_| "?".into())
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

fn roots(plan: &Json) -> Vec<Json> {
    plan.get("operator")
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_else(|| vec![plan.clone()])
}

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

fn projections(node: &Json) -> Vec<String> {
    match node["extra_info"].get("Projections") {
        Some(Json::String(s)) => s.lines().map(str::to_string).collect(),
        Some(Json::Array(a)) => a
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        _ => vec![],
    }
}

/// Output rows of an operator (`operator_cardinality`; DuckDB 2.0 also
/// reports `intermediate_rows`).
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

/// `(scan type, projections)` of every payload-projecting scan of `table`.
fn payload_scans(plan: &Json, table: &str) -> Vec<(String, Vec<String>)> {
    let mut out = Vec::new();
    for r in roots(plan) {
        walk(&r, &mut |n| {
            if scan_table(n) == Some(table) {
                let proj = projections(n);
                if proj.iter().any(|p| p == "values" || p == "values_int") {
                    let ty = n["extra_info"]
                        .get("Type")
                        .and_then(Json::as_str)
                        .unwrap_or("<none>")
                        .to_string();
                    out.push((ty, proj));
                }
            }
        });
    }
    out
}

/// Does this engine compress join keys even for a tiny build side (DEBUG)?
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

/// A guard connection: on a DEBUG-build engine, compressed materialization
/// off, so the plan matches what a release engine chooses.
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

fn assert_index(conn: &Connection, label: &str, sql: &str) {
    assert_index_in(conn, label, sql, &explain(conn, sql));
}

/// `EXPLAIN (ANALYZE, FORMAT json)` of a prepared statement with bound values.
fn explain_bound(conn: &Connection, sql: &str, args: &[Value]) -> Json {
    let mut st = conn
        .prepare(&format!("EXPLAIN (ANALYZE, FORMAT json) {sql}"))
        .unwrap();
    let raw: String = st
        .query_row(duckdb::params_from_iter(args.iter()), |r| r.get(1))
        .unwrap_or_else(|e| panic!("EXPLAIN ANALYZE failed for {sql} {args:?}: {e}"));
    serde_json::from_str(&raw).expect("plan JSON")
}

fn assert_index_in(conn: &Connection, label: &str, sql: &str, plan: &Json) {
    let scans = payload_scans(plan, "value_array");
    let ver = engine_version(conn);
    assert!(
        !scans.is_empty(),
        "{label}: no payload scan of value_array in the plan (walker vacuous? \
         DuckDB {ver})\n{sql}"
    );
    for (ty, proj) in &scans {
        assert_eq!(
            ty, "Index Scan",
            "{label}: the payload scan {proj:?} fell back to {ty} on DuckDB {ver}: the \
             point-read macros would silently read every array payload. Check the \
             `= ANY(coalesce((SELECT list(DISTINCT …)), [-1]))` key-set form \
             (docs/dev/engine_rules.md §2, fast_point_reads_PLAN §2.3).\n{sql}"
        );
    }
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(&format!("SELECT count(*) FROM ({sql})"), [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn one_i64(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn names_list(range: std::ops::Range<usize>) -> String {
    let v: Vec<String> = range.map(|i| format!("'{}'", ent(i))).collect();
    format!("[{}]", v.join(", "))
}

fn id_of(conn: &Connection, sql: &str) -> i64 {
    one_i64(conn, sql)
}

// --- the guards ---------------------------------------------------------------

#[test]
fn point_read_macros_fetch_payloads_by_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = guard_conn(&db);

    // The fixture is as realistic as the module doc says.
    assert_eq!(
        one_i64(&conn, "SELECT count(*) FROM value_array"),
        (3 * (N_ENT - 1) + 5 + (N_ENT - 1) + 5) as i64,
        "arrays: node.profile (3 alternatives), grid, shifted, unit.profile; n59 deleted"
    );
    assert!(one_i64(&conn, "SELECT count(*) FROM value_array_hist") >= (N_ENT - 1) as i64);
    assert!(
        one_i64(&conn, "SELECT count(*) FROM axis") > 50,
        "more than 50 axes"
    );
    assert_eq!(
        one_i64(
            &conn,
            "SELECT count(*) FROM entity WHERE valid_to_commit IS NOT NULL"
        ),
        1,
        "one closed-out entity row"
    );
    assert!(
        one_i64(
            &conn,
            "SELECT count(*) FROM value_long WHERE valid_to_commit IS NULL"
        ) > 0
    );

    let rv = "SELECT * FROM read_value";
    let all = "SELECT * FROM read_values";
    let cases: Vec<(&str, String, i64)> = vec![
        ("1 key", format!("{rv}('node', 'profile', 'n10', 'Base')"), STEPS as i64),
        ("1 key, mixed case", format!("{rv}('NODE', 'Profile', 'N10', 'ALT1')"), STEPS as i64),
        ("0 keys: scalar", format!("{rv}('node', 'cap', 'n10', 'Base')"), 1),
        ("0 keys: long", format!("{rv}('node', 'ragged', 'n03', 'Base')"), 3),
        ("0 keys: miss", format!("{rv}('node', 'profile', 'nope', 'Base')"), 0),
        ("0 keys: deleted entity", format!("{rv}('node', 'profile', 'n59', 'Base')"), 0),
        ("0 keys: wrong class", format!("{rv}('unit', 'profile', 'n30', 'Base')"), 0),
        ("0 keys: empty list", format!("{all}('node', 'profile', []::VARCHAR[])"), 0),
        ("rank 2", format!("{rv}('node', 'grid', 'n02', 'Base')"), 3 * STEPS as i64),
        ("other class, same names", format!("{rv}('unit', 'profile', 'n02', 'Base')"), STEPS as i64),
        (
            "20 keys",
            format!("{all}('node', 'profile', {}, ['Base'])", names_list(10..30)),
            20 * STEPS as i64,
        ),
        (
            "50 keys",
            format!("{all}('node', 'profile', {}, ['Base'])", names_list(5..55)),
            50 * STEPS as i64,
        ),
        (
            "D3: alternatives omitted (16 entities x 3 = 48 keys)",
            format!("{all}('node', 'profile', {})", names_list(0..16)),
            48 * STEPS as i64,
        ),
        (
            "D3: alternatives NULL",
            format!("{all}('node', 'profile', {}, NULL)", names_list(0..16)),
            48 * STEPS as i64,
        ),
        (
            "D3/M2: read_value, alternative omitted",
            format!("{rv}('node', 'profile', 'n10')"),
            3 * STEPS as i64,
        ),
        (
            "time window on the point wrapper",
            format!(
                "{rv}('node', 'profile', 'n10', 'Base') \
                 WHERE idx1_ts BETWEEN TIMESTAMP '2025-01-01 02:00' AND TIMESTAMP '2025-01-01 04:00'"
            ),
            3,
        ),
    ];
    for (label, sql, want) in &cases {
        assert_eq!(count(&conn, sql), *want, "{label}: row count\n{sql}");
        assert_index(&conn, label, sql);
    }

    // value_rows_for: 50 ids, each listed twice (critique M1): still 50 keys.
    let pd = id_of(
        &conn,
        "SELECT pd.id FROM parameter_definition pd JOIN entity_class ec \
         ON ec.id = pd.entity_class_id WHERE ec.name = 'node' AND pd.name = 'profile'",
    );
    let base = id_of(&conn, "SELECT id FROM alternative WHERE name = 'Base'");
    let ids: Vec<i64> = {
        let mut st = conn
            .prepare(
                "SELECT e.id FROM entity e JOIN entity_class ec ON ec.id = e.entity_class_id \
                 WHERE ec.name = 'node' AND e.valid_to_commit IS NULL ORDER BY e.name LIMIT 50",
            )
            .unwrap();
        st.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    assert_eq!(ids.len(), 50);
    let twice: Vec<String> = ids.iter().chain(ids.iter()).map(i64::to_string).collect();
    let sql = format!(
        "SELECT * FROM value_rows_for({pd}, [{}], [{base}, {base}])",
        twice.join(", ")
    );
    assert_eq!(
        count(&conn, &sql),
        50 * STEPS as i64,
        "M1: duplicates deduplicated"
    );
    assert_index(&conn, "M1: 50 ids each listed twice", &sql);

    // Negative control: 51 value hits fall back to a sequential scan.
    let sql = format!("{all}('node', 'profile', {}, ['Base'])", names_list(0..51));
    assert_eq!(count(&conn, &sql), 51 * STEPS as i64);
    let scans = payload_scans(&explain(&conn, &sql), "value_array");
    let ver = engine_version(&conn);
    assert!(
        !scans.is_empty(),
        "51-key control: no payload scan (DuckDB {ver})"
    );
    assert!(
        scans.iter().all(|(ty, _)| ty != "Index Scan"),
        "51-key control: 51 keys got an INDEX scan on DuckDB {ver}: {scans:?}. The \
         dynamic IN filter no longer stops at 50 keys, so the documented 50-value-hit \
         limit of read_values (docs/usage.md, engine_rules §2) can be revisited; \
         re-evaluate array_id plan D15 rather than deleting this control."
    );
    conn.close().unwrap();
}

/// The core's prepared statement (`api::READ_VALUES_SQL`) with the name lists
/// BOUND as parameters, as `api::read_values` runs it: the payload scan stays
/// an index scan for a hit, the omitted-alternative default (NULL), a scalar
/// parameter and empty lists.
#[test]
fn prepared_read_values_with_bound_lists_uses_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = guard_conn(&db);
    let list = |xs: &[&str]| Value::List(xs.iter().map(|x| Value::Text(x.to_string())).collect());
    let t = |s: &str| Value::Text(s.into());
    let twenty: Vec<String> = (10..30).map(ent).collect();
    let twenty: Vec<&str> = twenty.iter().map(String::as_str).collect();
    for (label, args, want) in [
        (
            "bound: 1 key",
            vec![t("node"), t("profile"), list(&["n10"]), list(&["Base"])],
            STEPS,
        ),
        (
            "bound: alternatives NULL",
            vec![t("node"), t("profile"), list(&["N10"]), Value::Null],
            3 * STEPS,
        ),
        (
            "bound: 20 keys",
            vec![t("node"), t("profile"), list(&twenty), list(&["base"])],
            20 * STEPS,
        ),
        (
            "bound: scalar parameter",
            vec![t("node"), t("cap"), list(&["n10"]), Value::Null],
            3,
        ),
        (
            "bound: empty lists",
            vec![t("node"), t("profile"), list(&[]), list(&[])],
            0,
        ),
    ] {
        let mut st = conn.prepare(READ_VALUES_SQL).unwrap();
        let mut rows = st.query(duckdb::params_from_iter(args.iter())).unwrap();
        let mut n = 0;
        while rows.next().unwrap().is_some() {
            n += 1;
        }
        assert_eq!(n, want, "{label}: row count");
        assert_index_in(
            &conn,
            label,
            READ_VALUES_SQL,
            &explain_bound(&conn, READ_VALUES_SQL, &args),
        );
    }
    conn.close().unwrap();
}

/// Critique M4: the `ap` CTE evaluates `axis_position` only for the value's own
/// axes. The axis UNNESTs (the parametric legs of `axis_position`; the only
/// UNNESTs with no `value_array` scan below them that read `axis`) must emit no
/// more rows than those axes hold, on a file with more than 50 other axes.
#[test]
fn point_read_axis_positions_are_limited_to_the_values_axes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = guard_conn(&db);
    let ver = engine_version(&conn);
    let total: i64 = one_i64(&conn, "SELECT sum(length) FROM axis");
    for (label, sql, own) in [
        (
            "rank 1",
            "SELECT * FROM read_value('node', 'profile', 'n10', 'Base')",
            STEPS,
        ),
        (
            "rank 2",
            "SELECT * FROM read_value('node', 'grid', 'n02', 'Base')",
            STEPS,
        ),
        (
            "shifted",
            "SELECT * FROM read_value('node', 'shifted', 'n07', 'Base')",
            STEPS,
        ),
    ] {
        let (n_unnest, axis_rows) = axis_unnest_rows(&explain(&conn, sql));
        assert!(
            n_unnest > 0 && axis_rows > 0,
            "{label}: no axis_position UNNEST with output found (walker vacuous? DuckDB {ver})"
        );
        assert!(
            axis_rows as usize <= own,
            "{label}: axis_position emitted {axis_rows} positions for a value whose \
             parametric axes hold {own} (file total {total}) on DuckDB {ver}. The runtime \
             join filter no longer reaches the axis scans, so every point read pays for \
             every axis in the file (fast_point_reads_PLAN critique M4).\n{sql}"
        );
    }
    // Control: value_rows evaluates axis_position unfiltered, so the same walk
    // sees every parametric position in the file. Proves the check can see
    // the fixture's unused axes.
    let (pd, e, a): (i64, i64, i64) = conn
        .query_row(
            "SELECT va.parameter_definition_id, va.entity_id, va.alternative_id \
             FROM value_array va JOIN entity e ON e.id = va.entity_id \
             JOIN alternative al ON al.id = va.alternative_id \
             JOIN parameter_definition pd ON pd.id = va.parameter_definition_id \
             JOIN entity_class ec ON ec.id = pd.entity_class_id \
             WHERE e.name = 'n10' AND al.name = 'Base' AND pd.name = 'profile' \
               AND ec.name = 'node'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    let sql = format!(
        "SELECT * FROM value_rows WHERE parameter_definition_id = {pd} \
         AND entity_id = {e} AND alternative_id = {a}"
    );
    let (_, unfiltered) = axis_unnest_rows(&explain(&conn, &sql));
    assert!(
        unfiltered as usize > 50 * STEPS,
        "control: value_rows emitted only {unfiltered} axis positions (file total \
         {total}); the walker or the fixture no longer shows the unfiltered cost \
         (DuckDB {ver})"
    );
    conn.close().unwrap();
}

/// `(count, output rows)` of the axis UNNESTs in a plan: those that read
/// `axis` and no `value_array` below them (the parametric legs of
/// `axis_position`).
fn axis_unnest_rows(plan: &Json) -> (usize, u64) {
    let mut axis_rows = 0u64;
    let mut n_unnest = 0;
    for r in roots(plan) {
        walk(&r, &mut |n| {
            if node_type(n) != "UNNEST" {
                return;
            }
            let (mut reads_axis, mut reads_payload) = (false, false);
            walk(n, &mut |m| match scan_table(m) {
                Some("axis") => reads_axis = true,
                Some("value_array") => reads_payload = true,
                _ => {}
            });
            if reads_axis && !reads_payload {
                n_unnest += 1;
                axis_rows += rows_out(n);
            }
        });
    }
    (n_unnest, axis_rows)
}

/// Joins below an UNNEST on the column path that are not SEMI / ANTI / MARK.
/// At a SEMI / ANTI / MARK join only the probe side (child 0) carries columns
/// up, so only that side is followed. An aggregate is a barrier (it emits
/// freshly computed columns, nothing passes through it: the key list's
/// `list(…)` sits on one), and a CTE_SCAN is an opaque leaf (the materialized
/// CTE's definition is walked from its own UNNESTs).
fn unnest_violations(plan: &Json) -> Vec<String> {
    fn below(node: &Json, out: &mut Vec<String>) {
        let ty = node_type(node);
        if ty.contains("GROUP_BY") || ty.contains("AGGREGATE") {
            return;
        }
        let is_join = ty.contains("JOIN") || ty == "CROSS_PRODUCT" || ty == "POSITIONAL_SCAN";
        if is_join {
            let jt = node["extra_info"]
                .get("Join Type")
                .and_then(Json::as_str)
                .unwrap_or("");
            if matches!(jt, "SEMI" | "ANTI" | "MARK") {
                if let Some(probe) = children(node).first() {
                    below(probe, out);
                }
                return;
            }
            out.push(format!("{ty} {jt}"));
        }
        for c in children(node) {
            below(c, out);
        }
    }
    let mut out = Vec::new();
    for r in roots(plan) {
        walk(&r, &mut |n| {
            if node_type(n) == "UNNEST" {
                for c in children(n) {
                    below(c, &mut out);
                }
            }
        });
    }
    out
}

#[test]
fn no_build_side_column_reaches_an_unnest() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = guard_conn(&db);
    let pd = id_of(
        &conn,
        "SELECT pd.id FROM parameter_definition pd JOIN entity_class ec \
         ON ec.id = pd.entity_class_id WHERE ec.name = 'node' AND pd.name = 'profile'",
    );
    for sql in [
        "SELECT * FROM read_value('node', 'profile', 'n10', 'Base')".to_string(),
        "SELECT * FROM read_value('node', 'grid', 'n02')".to_string(),
        format!(
            "SELECT * FROM read_values('node', 'profile', {}, ['Base'])",
            names_list(0..20)
        ),
        // all keys: the payload scan is sequential, the join shape must hold
        format!(
            "SELECT * FROM read_values('node', 'profile', {})",
            names_list(0..N_ENT)
        ),
        format!(
            "SELECT * FROM value_rows_for({pd}, (SELECT list(id) FROM entity), [1, 2, 3]) \
             WHERE v_float > 5000"
        ),
        "SELECT * FROM read_value('node', 'profile', 'n10', 'Base') \
         WHERE idx1_ts BETWEEN TIMESTAMP '2025-01-01 02:00' AND TIMESTAMP '2025-01-01 04:00'"
            .to_string(),
    ] {
        let v = unnest_violations(&explain(&conn, &sql));
        assert!(
            v.is_empty(),
            "a join that can emit build-side columns sits below an UNNEST: {v:?} on \
             DuckDB {}. That is the DuckDB 2.0 dict-cache bug shape \
             (docs/dev/engine_rules.md §6).\n{sql}",
            engine_version(&conn)
        );
    }
    // Self-check: the walker flags value_array_cells' self-join (k JOIN p, k's
    // columns passed through the unnest), the shape the findings reduced the
    // bug to.
    let v = unnest_violations(&explain(&conn, "SELECT * FROM value_array_cells"));
    assert!(
        !v.is_empty(),
        "walker self-check: value_array_cells' self-join was not flagged (vacuous walker?)"
    );
    conn.close().unwrap();
}

// --- the dict-cache bug: data guard and canary ---------------------------------

const BUG_ARRAYS: i64 = 1000;
const BUG_CELLS: i64 = 8760;

/// 1 000 arrays of 8 760 cells with dense (stride-2) ids (the A/B gate's
/// full-scale shape; 600 did not trigger the bug through read_parameter): one array written
/// through the core (class, parameter, axis, alternative), then cloned by SQL.
fn bug_fixture(dir: &Path) -> PathBuf {
    let db = dir.join("dictbug.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    let mut e = Vec::new();
    let mut t = Vec::new();
    let mut v = Vec::new();
    for h in 0..BUG_CELLS {
        e.push("a0000".to_string());
        t.push(T0_US + h * HOUR_US);
        v.push((h % 977) as f64 * 1.7 - 800.0);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
    ]));
    let b = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(e)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(t)) as ArrayRef,
            Arc::new(Float64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap();
    write(&db, "bench_array", "p_series", "Base", b, &[]);
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(&format!(
        "BEGIN;
         INSERT INTO commit (user, message) VALUES ('tester', 'clone arrays');
         INSERT INTO entity (entity_class_id, name, valid_from_commit)
           SELECT e.entity_class_id, printf('a%04d', i), currval('seq_commit')
           FROM entity e, range(1, {BUG_ARRAYS}) t(i) WHERE e.name = 'a0000';
         INSERT INTO value_array (entity_id, parameter_definition_id, alternative_id,
             valid_from_commit, version, axis1_id, values, v_min, v_max)
           SELECT e.id, va.parameter_definition_id, va.alternative_id, currval('seq_commit'), 1,
                  va.axis1_id,
                  list_transform(range({BUG_CELLS}),
                      lambda h: ((i * {BUG_CELLS} + h) % 977) * 1.7 - 800.0),
                  -800.0, 860.0
           FROM value_array va, range(1, {BUG_ARRAYS}) t(i)
           JOIN entity e ON e.name = printf('a%04d', i)
           ORDER BY i;
         COMMIT;
         CHECKPOINT;"
    ))
    .unwrap();
    conn.close().unwrap();
    db
}

/// Run `sql` on a FRESH raw DuckDB connection with default settings (caching
/// operators on): never the core's open, which may one day turn them off
/// (dict-bug mitigation A) and make this guard vacuous. One connection per
/// query, because a failure invalidates the instance. The rows are fetched
/// with every column: a `count(*)` wrapper would let the optimizer prune the
/// pass-through columns, and with them the bug shape.
fn raw_count(db: &Path, sql: &str) -> Result<i64, String> {
    let conn = Connection::open(db).map_err(|e| e.to_string())?;
    let r = (|| -> Result<i64, duckdb::Error> {
        let mut st = conn.prepare(sql)?;
        let mut rows = st.query([])?;
        let mut n = 0;
        while rows.next()?.is_some() {
            n += 1;
        }
        Ok(n)
    })()
    .map_err(|e| e.to_string());
    let _ = conn.close();
    r
}

fn expect_bug(r: Result<i64, String>, what: &str) {
    match r {
        Err(e) if e.contains("INTERNAL") || e.contains("dict") => {}
        other => panic!(
            "{what}: expected DuckDB's INTERNAL 'dict-surviving cache' error, got {other:?}. \
             If the bundled DuckDB fixed the bug, re-evaluate docs/dev/engine_rules.md §6 \
             and the workaround notes in docs/usage.md (specs: \
             duckdb_dict_cache_bug_FINDINGS.md) instead of deleting this check."
        ),
    }
}

/// About 70 MB of payload, a few seconds.
#[test]
fn point_read_macros_are_immune_to_the_dict_cache_bug() {
    let dir = tempfile::tempdir().unwrap();
    let db = bug_fixture(dir.path());
    let window = "WHERE idx1_ts BETWEEN TIMESTAMP '2025-03-01' AND TIMESTAMP '2025-03-01 05:00'";
    let rare = "WHERE v_float > 858";
    // The exact counts, from the generator: 6 hours per array in the window;
    // a cell is > 858 when ((i * 8760 + h) % 977) == 976.
    let in_window = |n: i64| 6 * n;
    let rare_count = |ids: &[i64]| -> i64 {
        ids.iter()
            .map(|i| {
                (0..BUG_CELLS)
                    .filter(|h| (i * BUG_CELLS + h) % 977 == 976)
                    .count() as i64
            })
            .sum()
    };
    let all: Vec<i64> = (0..BUG_ARRAYS).collect();

    // Positive control first: the fixture triggers the bug through the
    // self-join form (read_parameter -> value_rows -> value_array_cells).
    expect_bug(
        raw_count(
            &db,
            &format!("SELECT * FROM read_parameter('bench_array', 'p_series') {window}"),
        ),
        "positive control (read_parameter, time window)",
    );

    let names: Vec<String> = all.iter().map(|i| format!("'a{i:04}'")).collect();
    let every = format!(
        "SELECT * FROM read_values('bench_array', 'p_series', [{}], ['Base'])",
        names.join(", ")
    );
    assert_eq!(
        raw_count(&db, &format!("{every} {window}")),
        Ok(in_window(BUG_ARRAYS))
    );
    assert_eq!(
        raw_count(&db, &format!("{every} {rare}")),
        Ok(rare_count(&all))
    );
    let point = "SELECT * FROM read_value('bench_array', 'p_series', 'a0300', 'Base')";
    assert_eq!(
        raw_count(&db, &format!("{point} {window}")),
        Ok(in_window(1))
    );
    assert_eq!(
        raw_count(&db, &format!("{point} {rare}")),
        Ok(rare_count(&[300]))
    );
    let forty: Vec<String> = (100..140).map(|i| format!("'a{i:04}'")).collect();
    let forty_sql = format!(
        "SELECT * FROM read_values('bench_array', 'p_series', [{}]) {rare}",
        forty.join(", ")
    );
    let forty_ids: Vec<i64> = (100..140).collect();
    assert_eq!(raw_count(&db, &forty_sql), Ok(rare_count(&forty_ids)));
    let ids_sql = "SELECT * FROM value_rows_for(\
         (SELECT id FROM parameter_definition WHERE name = 'p_series'), \
         (SELECT list(id) FROM entity), (SELECT list(id) FROM alternative))";
    assert_eq!(
        raw_count(&db, &format!("{ids_sql} {window}")),
        Ok(in_window(BUG_ARRAYS))
    );
}

/// The canary: the minimal repro of the DuckDB 2.0 dict-cache bug (no
/// nodal-db objects) still raises on the bundled engine. When an upgrade
/// fixes it, this fails on purpose (see `expect_bug`).
#[test]
fn dict_cache_bug_canary_still_present() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE k AS SELECT i AS id, 1 AS a FROM range(1000) t(i);
         CREATE TABLE p AS SELECT i AS id, range(8760) AS l FROM range(1000) t(i);",
    )
    .unwrap();
    let r: Result<i64, String> = conn
        .query_row(
            "SELECT count(*), sum(a) \
             FROM (SELECT k.a, unnest(p.l) AS v FROM p JOIN k ON k.id = p.id) \
             WHERE v % 977 = 0",
            [],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string());
    expect_bug(r, "canary (minimal repro)");
}
