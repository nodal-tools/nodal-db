//! Plan-shape guard for the `ix_entity_lname` expression index (and the
//! `axis_hash` index), queried behind an `OFFSET 0` fence.
//!
//! DuckDB 2.0 scans through an index only when the scan carries exactly one
//! filter (`table_scan.cpp`: `FilterCount() != 1` falls back to a sequential
//! scan), on a single-key index. The plain form `lower(name) = lower(?) AND
//! entity_class_id = ? AND valid_to_commit IS NULL` gets an index scan only
//! when zone-map statistics prove the other two filters always true and drop
//! them: one class and no closed-out rows. That is the shape of every toy
//! fixture and of no real model, so a guard on a toy file passes vacuously.
//! The production statements therefore run the index filter alone in a
//! subquery fenced by `OFFSET 0` and apply the class / current-row filters
//! outside it.
//!
//! This test runs the PRODUCTION SQL (the core's exposed consts, never a copy)
//! under `EXPLAIN (ANALYZE, FORMAT json)` (plain `EXPLAIN` always prints a
//! sequential scan: the choice is made at run time) on a realistic fixture:
//! two entity classes, closed-out entity rows, an entity deleted and re-created
//! under a differently cased name, the same name in the other class, and an
//! axis table holding two kinds. The fixture's realism is asserted first.
//!
//! - Positive: every scan of `entity` (`axis`) in the guarded statements is an
//!   `Index Scan`, for a present and a missing key.
//! - Negative control: the unfenced form on the same fixture must still be a
//!   sequential scan. If it flips, DuckDB has learned to combine an index scan
//!   with residual filters and the fence may no longer be needed: re-evaluate
//!   decision D1 of `name_index_and_unused_indexes_PLAN.md` rather than
//!   deleting this test.
//! - Behaviour: the fenced statements return what the `cur_cls` / `cur_name`
//!   form returns, through the public write path too.
//!
//! The fence is an optimizer behaviour, not a documented contract; if a DuckDB
//! bump breaks it, the pre-agreed fallbacks are documented at
//! `api::ENTITY_NAME_LOOKUP_SQL`. Runs on every `cargo test`, and in release
//! from `just full`.
//!
//! **Debug-build engine.** As in `array_id_index_scan.rs`, the guard
//! connection disables `compressed_materialization` when the engine forces it
//! (DEBUG builds), so a scalar-subquery join plans as in release. Harmless when
//! not needed.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;
use serde_json::Value as Json;

use nodal_db_core::api::{
    commit_changes, delete_objects, write_parameter, Segment, ENTITY_LOOKUP_ONE_SQL,
    ENTITY_NAME_LOOKUP_SQL,
};
use nodal_db_core::axis::AXIS_CANDIDATES_SQL;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, WriteOptions,
};

/// The unfenced three-conjunct form (negative control), same parameters as
/// `ENTITY_NAME_LOOKUP_SQL` (`?1` class id, `?2` name).
const UNFENCED_ENTITY_SQL: &str = "SELECT id FROM entity WHERE lower(name) = lower(?2) \
     AND entity_class_id = ?1 AND valid_to_commit IS NULL";
/// The unfenced axis form (negative control), same parameters as
/// `AXIS_CANDIDATES_SQL` (`?1` hash, `?2` kind).
const UNFENCED_AXIS_SQL: &str = "SELECT id FROM axis WHERE content_hash = ?1 AND kind = ?2";
/// The reference semantics of a single-name entity lookup.
const REFERENCE_SQL: &str = "SELECT id FROM entity WHERE cur_cls = ?1 AND cur_name = lower(?2)";

// --- fixture -----------------------------------------------------------------

fn scalar_batch(entities: &[String], v: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let vals = vec![v; entities.len()];
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(entities.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vals)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// One rank-1 series for `entity` over an integer index (an `integer` axis).
fn int_series(entity: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; 4])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef,
        ],
    )
    .unwrap()
}

/// One rank-1 hourly series for `entity` over a timestamp index (a `datetime`
/// axis).
fn datetime_series(entity: &str) -> RecordBatch {
    const T0: i64 = 1_735_689_600 * 1_000_000;
    const H: i64 = 3_600_000_000;
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; 3])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![
                T0,
                T0 + H,
                T0 + 2 * H,
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn write(db: &Path, class: &str, param: &str, batch: RecordBatch) {
    write_parameter(
        db,
        class,
        param,
        "Base",
        batch,
        &[],
        WriteOptions::default(),
        "tester",
        "fixture",
    )
    .expect("write fixture");
}

fn delete_entity(db: &Path, class: &str, name: &str) {
    delete_objects(
        db,
        vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Entity,
                class: Some(class.into()),
                target: ObjectRef::Name(name.into()),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        "tester",
        "delete entity",
        WriteOptions::default(),
    )
    .expect("delete entity");
}

/// Two classes (`node`: 150 entities, `unit`: 100), each with a scalar
/// parameter; `Äbc` in `node` deleted and re-created as `äBC`, `ÄBC` in `unit`
/// too, `n007` deleted for good; and an axis table holding two kinds.
fn fixture(dir: &Path) -> PathBuf {
    let db = dir.join("lname_guard.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    let mut nodes: Vec<String> = (0..150).map(|i| format!("n{i:03}")).collect();
    nodes.push("Äbc".into());
    write(&db, "node", "demand", scalar_batch(&nodes, 1.0));
    let mut units: Vec<String> = (0..100).map(|i| format!("u{i:03}")).collect();
    units.push("ÄBC".into());
    write(&db, "unit", "capacity", scalar_batch(&units, 2.0));
    delete_entity(&db, "node", "Äbc");
    delete_entity(&db, "node", "n007");
    write(
        &db,
        "node",
        "demand",
        scalar_batch(&["äBC".to_string()], 3.0),
    );
    write(&db, "node", "profile", int_series("n001"));
    write(&db, "node", "hourly", datetime_series("n002"));
    db
}

// --- plan inspection (as in array_id_index_scan.rs) ----------------------------

/// Scan types of every TABLE_SCAN of `table` in an `EXPLAIN (ANALYZE, FORMAT
/// json)` plan (DuckDB 2.0 JSON shape: root `operator[]`, nodes keyed `type`
/// with `children[]`, `extra_info.{Table, Type}`; `Table` catalog-qualified).
fn scan_types(plan: &Json, table: &str) -> Vec<String> {
    fn walk(node: &Json, table: &str, out: &mut Vec<String>) {
        if node.get("type").and_then(Json::as_str) == Some("TABLE_SCAN") {
            let info = &node["extra_info"];
            let t = info.get("Table").and_then(Json::as_str).unwrap_or("");
            if t.rsplit('.').next() == Some(table) {
                let ty = info.get("Type").and_then(Json::as_str).unwrap_or("<none>");
                out.push(ty.to_string());
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

fn explain<P: duckdb::Params>(conn: &Connection, sql: &str, params: P) -> Json {
    let raw: String = conn
        .prepare(&format!("EXPLAIN (ANALYZE, FORMAT json) {sql}"))
        .and_then(|mut s| s.query_row(params, |r| r.get(1)))
        .unwrap_or_else(|e| panic!("EXPLAIN ANALYZE failed for {sql}: {e}"));
    serde_json::from_str(&raw).expect("plan JSON")
}

fn engine_version(conn: &Connection) -> String {
    conn.query_row("SELECT version()", [], |r| r.get(0))
        .unwrap_or_else(|_| "?".into())
}

/// Does this engine compress join keys even for a tiny build side (the DEBUG
/// behaviour; see `array_id_index_scan.rs`)? Probed on a temp table.
fn engine_forces_join_compression(conn: &Connection) -> bool {
    conn.execute_batch(
        "CREATE TEMP TABLE cm_probe (id BIGINT PRIMARY KEY, e BIGINT); \
         INSERT INTO cm_probe SELECT 2 * i + 1, i FROM range(40) t(i);",
    )
    .unwrap();
    let raw: String = conn
        .query_row(
            "EXPLAIN (ANALYZE, FORMAT json) WITH k AS (SELECT id FROM cm_probe WHERE e = 10) \
             SELECT p.e FROM k JOIN cm_probe p ON p.id = k.id",
            [],
            |r| r.get(1),
        )
        .unwrap();
    conn.execute_batch("DROP TABLE cm_probe").unwrap();
    raw.contains("TRY_CAST(")
}

fn guard_conn(db: &Path) -> Connection {
    let conn = Connection::open(db).unwrap();
    if engine_forces_join_compression(&conn) {
        conn.execute_batch("SET disabled_optimizers = 'compressed_materialization'")
            .unwrap();
    }
    conn
}

fn assert_index_scan(label: &str, conn: &Connection, table: &str, plan: &Json) {
    let scans = scan_types(plan, table);
    let ver = engine_version(conn);
    assert!(
        !scans.is_empty(),
        "{label}: no scan of {table} in the plan (walker vacuous? DuckDB {ver})"
    );
    assert!(
        scans.iter().all(|t| t == "Index Scan"),
        "{label}: scan of {table} is {scans:?} on DuckDB {ver}, not an Index Scan. The \
         OFFSET 0 fence no longer isolates the index filter: switch to the fallback \
         documented at api::ENTITY_NAME_LOOKUP_SQL (D1: (d), then (c); never a \
         MATERIALIZED CTE)."
    );
}

fn assert_not_index_scan(label: &str, conn: &Connection, table: &str, plan: &Json) {
    let scans = scan_types(plan, table);
    let ver = engine_version(conn);
    assert!(
        !scans.is_empty(),
        "{label}: no scan of {table} in the plan (walker vacuous? DuckDB {ver})"
    );
    assert!(
        scans.iter().all(|t| t != "Index Scan"),
        "{label}: the UNFENCED form got an Index Scan on DuckDB {ver}: {scans:?}. The \
         engine now combines an index scan with residual filters, so the OFFSET 0 \
         fence may no longer be needed: re-evaluate decision D1 of \
         name_index_and_unused_indexes_PLAN.md rather than deleting this control."
    );
}

fn class_id(conn: &Connection, name: &str) -> i64 {
    conn.query_row(
        "SELECT id FROM entity_class WHERE cur_name = lower(?)",
        [name],
        |r| r.get(0),
    )
    .unwrap()
}

fn opt_id(conn: &Connection, sql: &str, cls: i64, name: &str) -> Option<i64> {
    match conn.query_row(sql, duckdb::params![cls, name], |r| r.get(0)) {
        Ok(id) => Some(id),
        Err(duckdb::Error::QueryReturnedNoRows) => None,
        Err(e) => panic!("{sql}: {e}"),
    }
}

/// `ENTITY_LOOKUP_ONE_SQL`'s (fold, id) for one name.
fn one(conn: &Connection, cls: i64, name: &str) -> (String, Option<i64>) {
    conn.query_row(ENTITY_LOOKUP_ONE_SQL, duckdb::params![name, cls], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })
    .unwrap()
}

// --- tests -------------------------------------------------------------------

#[test]
fn fenced_lookups_use_the_index_on_a_realistic_fixture() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = guard_conn(&db);

    // Fixture realism (otherwise statistics drop the residual filters and every
    // form gets an index scan: the guard would pass vacuously).
    let (classes, closed, live): (i64, i64, i64) = conn
        .query_row(
            "SELECT count(DISTINCT entity_class_id), count(valid_to_commit), \
                    count(*) FILTER (WHERE valid_to_commit IS NULL) FROM entity",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert!(
        classes >= 2 && closed >= 1 && live >= 200,
        "unrealistic fixture: {classes} classes, {closed} closed-out rows, {live} live"
    );
    let kinds: i64 = conn
        .query_row("SELECT count(DISTINCT kind) FROM axis", [], |r| r.get(0))
        .unwrap();
    assert!(kinds >= 2, "unrealistic fixture: {kinds} axis kind(s)");

    let node = class_id(&conn, "node");
    for name in ["n042", "ÄBC", "no-such-entity"] {
        assert_index_scan(
            &format!("ENTITY_NAME_LOOKUP_SQL({name})"),
            &conn,
            "entity",
            &explain(&conn, ENTITY_NAME_LOOKUP_SQL, duckdb::params![node, name]),
        );
        assert_index_scan(
            &format!("ENTITY_LOOKUP_ONE_SQL({name})"),
            &conn,
            "entity",
            &explain(&conn, ENTITY_LOOKUP_ONE_SQL, duckdb::params![name, node]),
        );
    }

    let (hash, kind): (u64, String) = conn
        .query_row(
            "SELECT content_hash, kind FROM axis ORDER BY id LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    // A missing hash INSIDE the column's min/max: one outside it lets zone-map
    // statistics prune the scan away entirely (no scan left to check).
    let (missing, taken): (u64, i64) = conn
        .query_row(
            "WITH m AS (SELECT min(content_hash) // 2 + max(content_hash) // 2 AS h FROM axis) \
             SELECT h, (SELECT count(*) FROM axis WHERE content_hash = h) FROM m",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(taken, 0, "the mid-range probe hash {missing} exists");
    for (label, h) in [("present", hash), ("missing", missing)] {
        assert_index_scan(
            &format!("AXIS_CANDIDATES_SQL({label})"),
            &conn,
            "axis",
            &explain(&conn, AXIS_CANDIDATES_SQL, duckdb::params![h, kind]),
        );
    }
    conn.close().unwrap();
}

#[test]
fn unfenced_negative_controls_still_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = guard_conn(&db);
    let node = class_id(&conn, "node");
    assert_not_index_scan(
        "unfenced entity lookup",
        &conn,
        "entity",
        &explain(&conn, UNFENCED_ENTITY_SQL, duckdb::params![node, "n042"]),
    );
    let (hash, kind): (u64, String) = conn
        .query_row(
            "SELECT content_hash, kind FROM axis ORDER BY id LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_not_index_scan(
        "unfenced axis candidates",
        &conn,
        "axis",
        &explain(&conn, UNFENCED_AXIS_SQL, duckdb::params![hash, kind]),
    );
    // And the fenced form on the same connection IS an index scan, so the
    // controls isolate the fence (not the fixture or the walker).
    assert_index_scan(
        "fenced entity lookup (control pair)",
        &conn,
        "entity",
        &explain(&conn, ENTITY_NAME_LOOKUP_SQL, duckdb::params![node, "n042"]),
    );
    conn.close().unwrap();
}

#[test]
fn fenced_lookups_match_the_cur_name_form() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = Connection::open(&db).unwrap();
    let node = class_id(&conn, "node");
    let unit = class_id(&conn, "unit");

    // Every name, both classes, plus case variants and missing names.
    let mut probes: Vec<String> = conn
        .prepare("SELECT DISTINCT name FROM entity ORDER BY name")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    probes.extend(["äbc", "äBC", "ÄBC", "N042", "U010", "n007", "missing"].map(String::from));
    for cls in [node, unit] {
        for name in &probes {
            let want = opt_id(&conn, REFERENCE_SQL, cls, name);
            assert_eq!(
                opt_id(&conn, ENTITY_NAME_LOOKUP_SQL, cls, name),
                want,
                "ENTITY_NAME_LOOKUP_SQL({cls}, {name:?})"
            );
            assert_eq!(
                one(&conn, cls, name).1,
                want,
                "ENTITY_LOOKUP_ONE_SQL({cls}, {name:?})"
            );
        }
    }

    // Deleted then re-created under a different casing: the new current row,
    // not the closed-out one; the same folded name in `unit` is its own row.
    let rows: Vec<(i64, Option<i64>)> = conn
        .prepare(
            "SELECT id, valid_to_commit FROM entity \
             WHERE entity_class_id = ? AND lower(name) = lower('Äbc') ORDER BY id",
        )
        .unwrap()
        .query_map([node], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(
        rows.len() == 2 && rows[0].1.is_some() && rows[1].1.is_none(),
        "expected a closed-out and a current row: {rows:?}"
    );
    let recreated = rows[1].0;
    assert_eq!(
        opt_id(&conn, ENTITY_NAME_LOOKUP_SQL, node, "Äbc"),
        Some(recreated)
    );
    assert_eq!(
        one(&conn, node, "äBC"),
        (
            conn.query_row("SELECT lower('äBC')", [], |r| r.get(0))
                .unwrap(),
            Some(recreated)
        )
    );
    let in_unit = opt_id(&conn, ENTITY_NAME_LOOKUP_SQL, unit, "äbc").expect("ÄBC in unit");
    assert_ne!(in_unit, recreated);
    // Deleted for good: nothing.
    assert_eq!(opt_id(&conn, ENTITY_NAME_LOOKUP_SQL, node, "n007"), None);
    assert_eq!(one(&conn, node, "N007").1, None);
    conn.close().unwrap();
}

#[test]
fn entity_created_in_one_segment_resolves_in_the_next() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let seg = |param: &str, entity: &str| Segment {
        class: "node".into(),
        parameter: param.into(),
        alternative: "Base".into(),
        batches: vec![scalar_batch(&[entity.to_string()], 5.0)],
        domains: vec![],
    };
    commit_changes(
        &db,
        vec![seg("demand", "Fresh"), seg("loss", "FRESH")],
        vec![],
        vec![],
        vec![],
        "tester",
        "two segments, one new entity",
        WriteOptions::default(),
    )
    .expect("commit");
    let conn = Connection::open(&db).unwrap();
    let node = class_id(&conn, "node");
    let ids: Vec<i64> = conn
        .prepare(
            "SELECT DISTINCT v.entity_id FROM value_scalar v \
             JOIN entity e ON e.id = v.entity_id \
             WHERE e.entity_class_id = ? AND lower(e.name) = 'fresh'",
        )
        .unwrap()
        .query_map([node], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(ids.len(), 1, "both segments must hit one entity: {ids:?}");
    assert_eq!(
        opt_id(&conn, ENTITY_NAME_LOOKUP_SQL, node, "fReSh"),
        Some(ids[0])
    );
    conn.close().unwrap();
}
