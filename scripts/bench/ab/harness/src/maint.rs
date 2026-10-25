//! Maintenance gate modes: compaction and the history-free extract, timed
//! (`mt_*`) and sized (`sz_*`, the value is bytes), on the shapes of the
//! criterion benches `compaction_size.rs` and `extract.rs`.
//!
//! - `build_hheavy <path> <quick|full>`: the history-heavy long table
//!   (`compaction_size.rs`): `cells` rank-1 long cells over `series`
//!   entities, rewritten 21 times, one commit and a CHECKPOINT per round.
//!   Built with raw SQL like that bench (compaction copies rows verbatim, so
//!   the write path adds nothing to what is measured): quick 50 x 1 000
//!   cells (1 M rows), full 500 x 1 000 (10.5 M rows).
//! - `build_xsrc <path> <quick|full>`: the extract source (`extract.rs`),
//!   through the public write API: quick 20 arrays x 8 760 cells and 4 long
//!   series x 168 cells, 5 versions; full 100 / 10 x 336, 10 versions.
//!
//! Checks: compaction keeps the current state (digest before = after) and
//! the row count of the long table; the extract's current state equals the
//! source's (digest of both parameters).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::api::write_parameter;
use nodal_db_core::compaction::compact;
use nodal_db_core::duckdb;
use nodal_db_core::duckdb::Connection;
use nodal_db_core::extract::{extract, ExtractOpts};
use nodal_db_core::schema_init::{init_schema, init_schema_with_default};
use nodal_db_core::types::WriteOptions;

use crate::check::{check, check_rows, elapsed_ms, read_hash, scalar_i64, Sample};

fn scale_name() -> String {
    std::env::var("NODAL_AB_SCALE").unwrap_or_else(|_| "quick".into())
}

fn hheavy_shape(scale: &str) -> (i64, i64, i64) {
    match scale {
        "full" => (500, 500_000, 21),
        _ => (50, 50_000, 21),
    }
}

/// `build_hheavy <path> <scale>` (module docs).
pub fn build_hheavy(db: &Path, scale: &str) {
    let (series, cells, rounds) = hheavy_shape(scale);
    let t = Instant::now();
    init_schema(db).unwrap();
    let per = cells / series;
    let conn = Connection::open(db).unwrap();
    conn.execute_batch(&format!(
        "INSERT INTO axis (kind, length, content_hash, p_start_int, p_step_int) \
         VALUES ('integer', {per}, 0, 0, 1);"
    ))
    .unwrap();
    let axis: i64 = conn
        .query_row("SELECT max(id) FROM axis", [], |r| r.get(0))
        .unwrap();
    let commits: Vec<i64> = (0..rounds)
        .map(|_| {
            conn.query_row(
                "INSERT INTO \"commit\" (user, message) VALUES ('b', 'round') RETURNING id",
                [],
                |r| r.get(0),
            )
            .unwrap()
        })
        .collect();
    for r in 0..rounds as usize {
        let to = commits
            .get(r + 1)
            .map_or_else(|| "NULL".to_string(), |c| c.to_string());
        conn.execute_batch(&format!(
            "INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id, \
               valid_from_commit, valid_to_commit, version, axis1_id, i1_pos, value_kind, v_float) \
             SELECT c // {per}, 1, 1, {from}, {to}, {ver}, {axis}, (c % {per})::INTEGER, \
                    'float', ((c * 7919 + {r} * 104729) % 100000) / 7.0 \
             FROM range({cells}) t(c) ORDER BY c; \
             CHECKPOINT;",
            from = commits[r],
            ver = r + 1,
        ))
        .unwrap();
    }
    conn.close().unwrap();
    eprintln!(
        "built {} ({scale}) in {:.1} s",
        db.display(),
        t.elapsed().as_secs_f64()
    );
}

fn xsrc_shape(scale: &str) -> (usize, usize, usize, usize) {
    match scale {
        "full" => (100, 10, 336, 10),
        _ => (20, 4, 168, 5),
    }
}

const CELLS: usize = 8760;

fn array_batch(entities: &[String], version: usize) -> RecordBatch {
    let n = entities.len() * CELLS;
    let mut ent = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    let mut v = Vec::with_capacity(n);
    for (k, e) in entities.iter().enumerate() {
        for h in 0..CELLS {
            ent.push(e.as_str());
            t.push(h as i64);
            v.push(((k * 7919 + h * 31 + version * 104_729) % 1_000_003) as f64 * 0.137);
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ent)) as ArrayRef,
            Arc::new(Int64Array::from(t)) as ArrayRef,
            Arc::new(Float64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn long_batch(entity: &str, cells: usize, version: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; cells])) as ArrayRef,
            Arc::new(Int64Array::from(
                (0..cells as i64).map(|i| i * 2).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                (0..cells as i64)
                    .map(|i| i * 3 + version as i64)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
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
        "bench",
        "extract source",
    )
    .expect("write_parameter");
}

/// `build_xsrc <path> <scale>` (module docs).
pub fn build_xsrc(db: &Path, scale: &str) {
    let (arrays, longs, long_cells, versions) = xsrc_shape(scale);
    let t = Instant::now();
    init_schema_with_default(db, Some("Base")).unwrap();
    let names: Vec<String> = (0..arrays).map(|i| format!("a{i}")).collect();
    for v in 0..versions {
        write(db, "Unit", "profile", array_batch(&names, v));
        for i in 0..longs {
            write(
                db,
                "Node",
                "ragged",
                long_batch(&format!("l{i}"), long_cells, v),
            );
        }
    }
    eprintln!(
        "built {} ({scale}) in {:.1} s",
        db.display(),
        t.elapsed().as_secs_f64()
    );
}

fn env_path(var: &str) -> PathBuf {
    PathBuf::from(std::env::var(var).unwrap_or_else(|_| panic!("set {var}")))
}

const HHEAVY_CURRENT: &str =
    "SELECT entity_id AS e, i1_pos, v_float FROM value_long WHERE valid_to_commit IS NULL";

/// Compact a copy of the history-heavy fixture; (ms, bytes after, digest).
fn compact_once(dir: &Path) -> (f64, u64, Sample) {
    let db = dir.join("hheavy.duckdb");
    std::fs::copy(env_path("NODAL_AB_HHEAVY"), &db).expect("copy hheavy");
    let (_, cells, rounds) = hheavy_shape(&scale_name());
    let before = read_hash(&db, HHEAVY_CURRENT);
    check_rows(
        "current long cells before",
        before.rows as usize,
        cells as usize,
    );
    let t = Instant::now();
    compact(&db).unwrap_or_else(|e| crate::check::query_failed(&format!("compact: {e}")));
    let ms = elapsed_ms(t);
    let after = read_hash(&db, HHEAVY_CURRENT);
    check(after == before, "compaction changed the current state");
    let total = scalar_i64(&db, "SELECT count(*) FROM value_long");
    check_rows(
        "value_long rows after",
        total as usize,
        (cells * rounds) as usize,
    );
    let bytes = std::fs::metadata(&db).unwrap().len();
    (
        ms,
        bytes,
        Sample::new(0.0).with("rows", total).hash("current", &after),
    )
}

const XSRC_CURRENT: [&str; 2] = [
    "SELECT * FROM read_parameter('Unit', 'profile')",
    "SELECT * FROM read_parameter('Node', 'ragged')",
];

/// Extract the head of the source into a new file; (ms, bytes, digest).
fn extract_once(dir: &Path) -> (f64, u64, Sample) {
    let src = env_path("NODAL_AB_XSRC");
    let dest = dir.join("extract.duckdb");
    let (arrays, longs, long_cells, _) = xsrc_shape(&scale_name());
    let t = Instant::now();
    extract(
        &src,
        &dest,
        &ExtractOpts {
            as_of: None,
            user: "ab",
            note: None,
        },
    )
    .unwrap_or_else(|e| crate::check::query_failed(&format!("extract: {e}")));
    let ms = elapsed_ms(t);
    let mut s = Sample::new(0.0);
    for (i, sql) in XSRC_CURRENT.iter().enumerate() {
        let a = read_hash(&src, sql);
        let b = read_hash(&dest, sql);
        check(
            a == b,
            "the extract's current state differs from the source's",
        );
        s = s.hash(&format!("p{i}"), &b);
    }
    check_rows(
        "extract array cells",
        s.digest[0].1.parse::<usize>().unwrap(),
        arrays * CELLS,
    );
    check_rows(
        "extract long cells",
        s.digest[2].1.parse::<usize>().unwrap(),
        longs * long_cells,
    );
    let bytes = std::fs::metadata(&dest).unwrap().len();
    (ms, bytes, s)
}

/// Dispatch a maintenance mode; `None` if `mode` is not one.
pub fn run(mode: &str, dir: &Path) -> Option<Sample> {
    let (ms, bytes, s) = match mode {
        "mt_compact_history_heavy" | "sz_compacted_bytes" => compact_once(dir),
        "mt_extract_head" | "sz_extract_bytes" => extract_once(dir),
        _ => return None,
    };
    let mut s = s;
    s.ms = if mode.starts_with("sz_") {
        bytes as f64
    } else {
        ms
    };
    Some(s)
}
